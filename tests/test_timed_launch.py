from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.results import PublishAttempt
from research_swarm.control.worker_outputs import WorkerOutputIntent
from research_swarm.control.workers import AttemptData
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.workers.clock_signatures import ClockVerifier
from research_swarm.workers.contracts import NativeCommand
from research_swarm.workers.journal import JournalEntry, JournalState, WorkerJournal
from research_swarm.workers.supervisor import run_attempt, spawn_supervisor
from research_swarm.workers.timed_launch import (
    Counter,
    TimingEvidence,
    capture_timing,
    verify_timing,
)
from tests.test_authority_clock import SimulatedClock, challenge, timed_worker
from tests.test_authority_clock import simulated as simulated
from tests.test_clock_estimates import advance, fixture
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_queries import claim
from tests.worker_support import dispatch


def command_for(grant: Any, tmp_path: Path) -> NativeCommand:
    payload = grant.assignment.assignment.payload
    stage = next(item for item in payload.spec.stages if item.id == payload.dispatch.stage_id)
    return NativeCommand(argv=stage.argv, cwd=str(tmp_path), timeout_ms=stage.timeout_ms)


def test_large_counters_round_trip_through_canonical_json_without_float_loss() -> None:
    estimate, grant, signer, _ = fixture()
    shift = 2**53 + 123
    estimate = replace(
        estimate, sent=advance(estimate.sent, shift), received=advance(estimate.received, shift)
    )
    verified = capture_timing(grant, estimate, advance(estimate.received, 1_000_000_000))
    encoded = canonical_bytes(verified.evidence.model_dump(mode="json"))
    record = TimingEvidence.model_validate(strict_json_loads(encoded))
    assert isinstance(strict_json_loads(encoded)["caps"]["launch_ns"], str)
    assert record.sent.reading() == estimate.sent
    assert int(record.caps.launch_ns) > 2**53
    restored = verify_timing(record, signer.authority, grant.grant, grant.assignment.assignment)
    assert restored.evidence == verified.evidence
    assert restored.estimate == estimate
    assert record.digest == verified.evidence.digest
    assert (
        record.digest == "sha256:2daab699556ec4c82f0dea873c6d4bc414d0fa38f8c6092c1ba3ca8aa8501cf7"
    )


@pytest.mark.parametrize("value", [1, 1.0, True, "-1", "01", "+1", "1e9", "1\n", str(2**64)])
def test_counter_wire_format_rejects_ambiguous_or_out_of_range_values(value: Any) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(Counter).validate_python(value)
    assert TypeAdapter(Counter).validate_python(str(2**64 - 1)) == str(2**64 - 1)


@pytest.mark.parametrize("changed", ["signature", "cap", "profile", "clock_epoch", "age"])
def test_historical_timing_rechecks_signatures_and_conservative_bounds(changed: str) -> None:
    estimate, grant, signer, _ = fixture()
    record = capture_timing(grant, estimate, estimate.received).evidence
    payload = record.model_dump(mode="json")
    if changed == "signature":
        payload["reply"]["signature"] = "0" * 128
    elif changed == "cap":
        payload["caps"]["hard_ns"] = str(int(record.caps.hard_ns) + 1)
    elif changed == "profile":
        payload["caps"]["origin_profile"] = "unqualified-clock"
    elif changed == "clock_epoch":
        payload["caps"]["clock_epoch"] = new_id()
    else:
        payload["qualified_at"]["continuous_ns"] = str(
            estimate.received.continuous_ns + 60_000_000_000
        )
        payload["qualified_at"]["awake_ns"] = str(estimate.received.awake_ns + 60_000_000_000)
        payload["qualified_at"]["wall_ns"] = str(estimate.received.wall_ns + 60_000_000_000)
    with pytest.raises(ResearchError):
        verify_timing(
            TimingEvidence.model_validate(payload),
            signer.authority,
            grant.grant,
            grant.assignment.assignment,
        )


def test_timed_journal_binds_once_under_race_and_cannot_downgrade(tmp_path: Path) -> None:
    estimate, grant, signer, _ = fixture()
    journal = WorkerJournal(tmp_path / "journal.sqlite")
    command = command_for(grant, tmp_path)
    received = journal.receive(grant, command)
    assert received.clock_mode == "bounded-continuous-v1"
    assert received.assignment_time == grant.grant
    assert journal.receive(grant, command) == received
    with pytest.raises(ResearchError, match="dispatch changed"):
        journal.receive(grant.assignment, command)
    verified = capture_timing(grant, estimate, estimate.received)
    with ThreadPoolExecutor(max_workers=4) as pool:
        entries = list(
            pool.map(
                lambda _: WorkerJournal(journal.path).bind_timing(
                    received.dispatch.attempt_id, verified
                ),
                range(4),
            )
        )
    assert all(entry == entries[0] for entry in entries)
    assert len(journal.pending()) == 2
    reopened = WorkerJournal(journal.path)
    entry = reopened.get(received.dispatch.attempt_id)
    assert entry.timing is not None
    assert (
        verify_timing(
            entry.timing, signer.authority, grant.grant, grant.assignment.assignment
        ).evidence
        == verified.evidence
    )
    with pytest.raises(ResearchError, match="live origin"):
        journal.claim_launch(entry.dispatch.attempt_id, new_id())
    with pytest.raises(ResearchError, match="live origin channel"):
        run_attempt(journal, entry.dispatch.attempt_id)
    child = spawn_supervisor(journal, entry.dispatch.attempt_id)
    assert child.wait(timeout=10) == 2
    assert journal.get(entry.dispatch.attempt_id).state == JournalState.RECEIVED
    assert not (tmp_path / "executions").exists()


def test_refresh_tightens_caps_and_cannot_transplant_a_new_origin(tmp_path: Path) -> None:
    estimate, grant, _, _ = fixture()
    journal = WorkerJournal(tmp_path / "journal.sqlite")
    entry = journal.receive(grant, command_for(grant, tmp_path))
    original = capture_timing(grant, estimate, estimate.received)
    journal.bind_timing(entry.dispatch.attempt_id, original)
    earlier = replace(
        original.evidence.caps.caps(),
        launch_ns=int(original.evidence.caps.launch_ns) - 1_000_000_000,
    )
    tightened = capture_timing(grant, estimate, estimate.received, prior=earlier)
    saved = journal.bind_timing(entry.dispatch.attempt_id, tightened)
    assert int(saved.timing.caps.launch_ns) == earlier.launch_ns
    with pytest.raises(ResearchError, match="cannot extend"):
        journal.bind_timing(entry.dispatch.attempt_id, original)
    new_origin = replace(estimate, origin_session_id=new_id())
    with pytest.raises(ResearchError, match="authority differs"):
        journal.bind_timing(
            entry.dispatch.attempt_id, capture_timing(grant, new_origin, new_origin.received)
        )
    assert journal.get(entry.dispatch.attempt_id) == saved
    journal.request_cancel(entry.dispatch.attempt_id, 1)
    ended = journal.settle_unstarted(entry.dispatch.attempt_id)
    assert ended.state == JournalState.NOT_STARTED and ended.timing == tightened.evidence
    assert journal.bind_timing(entry.dispatch.attempt_id, tightened) == ended


def test_timed_unstarted_settlement_never_uses_raw_wall_expiry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    estimate, grant, _, _ = fixture()
    journal = WorkerJournal(tmp_path / "journal.sqlite")
    entry = journal.receive(grant, command_for(grant, tmp_path))
    monkeypatch.setattr(
        "research_swarm.workers.journal.utc_now", lambda: "2999-01-01T00:00:00.000Z"
    )
    assert journal.settle_unstarted(entry.dispatch.attempt_id) == entry
    journal.request_cancel(entry.dispatch.attempt_id, 1)
    assert journal.settle_unstarted(entry.dispatch.attempt_id).state == JournalState.NOT_STARTED


@pytest.mark.parametrize("signed", [False, True])
def test_legacy_admission_digest_and_missing_clock_fields_replay_unchanged(
    tmp_path: Path,
    signed: bool,
) -> None:
    journal = WorkerJournal(tmp_path / "journal.sqlite")
    if signed:
        _, timed, _, _ = fixture()
        verified = timed.assignment
        command = command_for(timed, tmp_path)
    else:
        verified, command = dispatch(tmp_path)
    entry = journal.receive(verified, command)
    old_payload = {
        "dispatch": entry.dispatch.model_dump(mode="json"),
        "spec": entry.spec.model_dump(mode="json"),
        "command": command.model_dump(mode="json"),
    }
    if signed:
        old_payload["signed_assignment"] = entry.signed_assignment.model_dump(mode="json")
    with journal._connect() as connection:
        row = connection.execute("SELECT input_digest FROM attempts").fetchone()
        assert row["input_digest"] == record_digest("research.worker-input", old_payload)
        connection.execute(
            "UPDATE attempts SET payload=json_remove(payload,"
            "'$.clock_mode','$.assignment_time','$.timing')"
        )
    assert WorkerJournal(journal.path).receive(verified, command) == entry
    assert entry.clock_mode == "local-utc-v1"


def test_timed_mode_rejects_missing_binding_and_invented_supervisor(tmp_path: Path) -> None:
    _, grant, _, _ = fixture()
    journal = WorkerJournal(tmp_path / "journal.sqlite")
    entry = journal.receive(grant, command_for(grant, tmp_path))
    for changes in (
        {"assignment_time": None},
        {"clock_mode": "local-utc-v1"},
        {"supervisor_id": new_id()},
        {"state": "closed"},
        {"state": "launching"},
        {
            "native": {
                "backend": "posix_group",
                "pid": 123,
                "start_token": "synthetic",
                "scope_id": new_id(),
            }
        },
    ):
        with pytest.raises(ValidationError):
            JournalEntry.model_validate({**entry.model_dump(mode="json"), **changes})


def test_not_started_publication_retains_timed_grant_after_controller_clock_failure(
    pipeline: Pipeline,
    simulated: SimulatedClock,
    tmp_path: Path,
) -> None:
    original, worker, token = timed_worker(pipeline)
    runtime = pipeline.runtime
    principal = runtime.credentials.resolve(token)
    issued = runtime.credentials.issue(
        runtime.principal(),
        principal.model_copy(update={"scopes": principal.scopes | {"worker.publish"}}),
    )
    client = ResearchClient(
        LocalTransport(runtime, lambda: runtime.credentials.resolve(issued.token))
    )
    row = claim(original, worker, pipeline.ready())
    attempt = AttemptData.model_validate(row["data"])
    assert worker.assignment_authority is not None
    assert attempt.assignment_time is not None and attempt.signed_assignment is not None
    timed = ClockVerifier(worker.assignment_authority).binding(
        attempt.assignment_time, attempt.signed_assignment
    )
    journal = WorkerJournal(tmp_path / "timed.sqlite")
    journal.receive(timed, command_for(timed, tmp_path))
    journal.request_cancel(row["id"], 1)
    entry = journal.settle_unstarted(row["id"])
    observed = tmp_path / "journal.json"
    observed.write_bytes(canonical_bytes(entry.model_dump(mode="json")))
    old = simulated.reading
    simulated.reading = replace(old, wall_ns=old.wall_ns + 2_000_000_000)
    with pytest.raises(ResearchError):
        client.worker_clock(challenge(worker))
    stripped = tmp_path / "stripped-journal.json"
    stripped.write_bytes(
        canonical_bytes(
            entry.model_copy(
                update={"clock_mode": "local-utc-v1", "assignment_time": None}
            ).model_dump(mode="json")
        )
    )
    wrong_artifact = client.worker_upload_file(
        WorkerOutputIntent(
            worker_id=worker.binding.worker_id,
            session_id=worker.binding.worker_session_id,
            attempt_id=row["id"],
            role="journal",
        ),
        stripped,
        idempotency_key="stripped-closure-upload",
    )
    with pytest.raises(ResearchError) as downgrade:
        client.command(
            "worker.publish_attempt",
            PublishAttempt(attempt_id=row["id"], journal_artifact=wrong_artifact),
            idempotency_key="stripped-closure",
        )
    assert downgrade.value.code == ErrorCode.STALE_FENCE
    artifact = client.worker_upload_file(
        WorkerOutputIntent(
            worker_id=worker.binding.worker_id,
            session_id=worker.binding.worker_session_id,
            attempt_id=row["id"],
            role="journal",
        ),
        observed,
        idempotency_key="timed-closure-upload",
    )
    accepted = client.command(
        "worker.publish_attempt",
        PublishAttempt(attempt_id=row["id"], journal_artifact=artifact),
        idempotency_key="timed-closure",
    )
    assert accepted["result"]["state"] == "not_started"
    assert accepted["result"]["data"]["assignment_time"] == attempt.assignment_time.model_dump(
        mode="json"
    )
