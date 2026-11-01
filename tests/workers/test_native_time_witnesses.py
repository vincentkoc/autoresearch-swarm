from __future__ import annotations

import subprocess
import sys
from dataclasses import replace
from typing import Any

import pytest

from research_swarm.control.native_time_witnesses import NativeWitnessSource
from research_swarm.control.time_witnesses import PendingTimeWitness
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.workers.clocks import ClockReading, NativeClock, native_clock
from tests.workers.test_time_witnesses import SEED, encoded, identifier, scope


class ControlledClock:
    """A synthetic continuity fault fixture, not hardware qualification."""

    def __init__(self) -> None:
        native = native_clock()
        self.profile = native.profile
        self.reading = native.sample()
        self.reads = 0

    def sample(self) -> ClockReading:
        self.reads += 1
        return self.reading


def source(clock: NativeClock | None = None) -> NativeWitnessSource:
    return NativeWitnessSource(scope().authority, identifier(1), SEED, clock=clock)


def pending(native: NativeWitnessSource, **scope_changes: Any) -> PendingTimeWitness:
    scoped = scope().model_dump(mode="json")
    scoped.update(scope_changes)
    return PendingTimeWitness(
        native.source,
        scoped,
        identifier(6),
        record_digest("fixture.operation", {"command": "observe"}),
    )


def test_generation_change_does_not_reanchor_clock_or_expand_source_scope() -> None:
    native = source()
    first = pending(native)
    a = first.complete(encoded(native.observe(first.request)))
    second = pending(native, provider_generation=identifier(99))
    b = second.complete(encoded(native.observe(second.request)))
    assert a.witness.payload.source_epoch == b.witness.payload.source_epoch
    assert a.witness.payload.authority_time_ms <= b.witness.payload.authority_time_ms
    assert a.epoch != b.epoch
    assert native.authority == scope().authority


@pytest.mark.parametrize(
    "field", [name for name in WitnessAuthorityScope.model_fields if name != "schema_version"]
)
def test_changed_authority_is_refused_before_observing_clock(field: str) -> None:
    clock = ControlledClock()
    native = source(clock)  # type: ignore[arg-type]
    before = clock.reads
    if field == "control_epoch":
        changed: Any = 2
    elif field.endswith("key_id") or field == "state_authority_id":
        changed = "sha256:" + "b" * 64
    else:
        changed = identifier(99)
    waiting = pending(native, **{field: changed})
    with pytest.raises(ResearchError) as error:
        native.observe(waiting.request)
    assert error.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert clock.reads == before


@pytest.mark.parametrize(
    "fault", ["wall_forward", "wall_backward", "counter", "suspend", "profile"]
)
def test_fault_latches_without_signing_a_new_epoch(fault: str) -> None:
    clock = ControlledClock()
    native = source(clock)  # type: ignore[arg-type]
    before = clock.reading
    first = pending(native)
    accepted = first.complete(encoded(native.observe(first.request)))
    changes = {
        "wall_forward": {"wall_ns": before.wall_ns + 2_000_000_000},
        "wall_backward": {"wall_ns": before.wall_ns - 2_000_000_000},
        "counter": {"continuous_ns": before.continuous_ns - 1},
        "suspend": {
            "continuous_ns": before.continuous_ns + 2_000_000_000,
            "wall_ns": before.wall_ns + 2_000_000_000,
        },
        "profile": {},
    }
    clock.reading = replace(before, **changes[fault])
    if fault == "profile":
        clock.profile = "unqualified-clock"
    with pytest.raises(ResearchError) as error:
        native.observe(pending(native).request)
    assert error.value.code == ErrorCode.STALE_FENCE
    clock.reading = before
    clock.profile = native.source.native_profile
    with pytest.raises(ResearchError, match="no longer qualified"):
        native.observe(pending(native, provider_generation=identifier(99)).request)
    assert accepted.witness.payload.source_epoch


def test_request_is_sampled_after_validation_not_from_a_cached_observation() -> None:
    clock = ControlledClock()
    native = source(clock)  # type: ignore[arg-type]
    first = native.observe(pending(native).request)
    old = clock.reading
    clock.reading = replace(
        old,
        wall_ns=old.wall_ns + 50_000_000,
        continuous_ns=old.continuous_ns + 50_000_000,
        awake_ns=old.awake_ns + 50_000_000,
    )
    second = native.observe(pending(native).request)
    assert second.payload.authority_time_ms - first.payload.authority_time_ms == 50
    assert second.payload.source_epoch == first.payload.source_epoch


def test_separate_native_processes_produce_distinct_clock_origins() -> None:
    native = source()
    program = """
import json, sys
from research_swarm.control.native_time_witnesses import NativeWitnessSource
from research_swarm.domain.time_witnesses import TimeWitnessRequest, WitnessAuthorityScope
data = json.load(sys.stdin)
source = NativeWitnessSource(
    WitnessAuthorityScope.model_validate(data['authority']),
    data['source_id'],
    bytes.fromhex(data['public_test_seed']),
)
print(source.observe(TimeWitnessRequest.model_validate(data['request'])).model_dump_json())
"""
    epochs = []
    for _ in range(2):
        waiting = pending(native)
        result = subprocess.run(
            [sys.executable, "-c", program],
            input=canonical_bytes(
                {
                    "authority": native.authority.model_dump(mode="json"),
                    "source_id": native.source.id,
                    "public_test_seed": SEED.hex(),
                    "request": waiting.request.model_dump(mode="json"),
                }
            ),
            capture_output=True,
            timeout=15,
        )
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        verified = waiting.complete(result.stdout)
        assert verified.witness.payload.native_profile == native.source.native_profile
        epochs.append(verified.witness.payload.source_epoch)
    assert len(set(epochs)) == 2


def test_unknown_native_profile_is_not_signed() -> None:
    clock = ControlledClock()
    clock.profile = "unqualified-clock"
    with pytest.raises(ResearchError, match="Unsupported native witness clock"):
        source(clock)  # type: ignore[arg-type]
