from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.workers import clocks
from research_swarm.workers.clocks import (
    ClockReading,
    NativeClock,
    native_clock,
    require_continuity,
)
from research_swarm.workers.journal import JournalState, WorkerJournal
from research_swarm.workers.supervision import run_native
from research_swarm.workers.supervisor import run_attempt
from tests.test_supervision import command, require_closed
from tests.worker_support import dispatch


def test_native_clock_is_shared_with_an_independent_process() -> None:
    clock = native_clock()
    before = clock.sample()
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json; from dataclasses import asdict; "
            "from research_swarm.workers.clocks import native_clock; "
            "c=native_clock(); "
            "print(json.dumps({'profile':c.profile,'reading':asdict(c.sample())}))",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    child = json.loads(completed.stdout)
    after = clock.sample()
    assert child["profile"] == clock.profile
    assert before.continuous_ns <= child["reading"]["continuous_ns"] <= after.continuous_ns
    assert before.awake_ns <= child["reading"]["awake_ns"] <= after.awake_ns
    require_continuity(before, after)


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"continuous_ns": 999}, "regressed"),
        ({"awake_ns": 99}, "regressed"),
        ({"awake_ns": 101}, "suspend boundary"),
        ({"wall_ns": 999_000}, "wall-clock step"),
        ({"wall_ns": 999_999_000}, "wall-clock step"),
    ],
)
def test_discontinuities_invalidate_calibration_independently(
    change: dict[str, int],
    reason: str,
) -> None:
    before = ClockReading(
        continuous_ns=1000, awake_ns=100, wall_ns=1_000_000, read_uncertainty_ns=1
    )
    after = ClockReading(
        continuous_ns=2000, awake_ns=1100, wall_ns=1_001_000, read_uncertainty_ns=1
    )
    require_continuity(before, after, tolerance_ns=5)
    with pytest.raises(ResearchError, match=reason) as stale:
        require_continuity(before, replace(after, **change), tolerance_ns=5)
    assert stale.value.code == ErrorCode.STALE_FENCE


def test_sample_bounds_read_delay_and_rejects_a_counter_reversal() -> None:
    values = iter([100, 150, 200, 190])
    clock = NativeClock("synthetic", lambda: next(values), lambda: 5, 3)
    assert clock.sample().read_uncertainty_ns == 53
    with pytest.raises(ResearchError, match="regressed"):
        clock.sample()


def test_suspend_within_sampling_cannot_be_hidden_as_read_uncertainty() -> None:
    values = iter([1_000_000_000, 61_000_000_000])
    clock = NativeClock("synthetic-suspend", lambda: next(values), lambda: 1000, 1)
    with pytest.raises(ResearchError, match="interval is too wide"):
        clock.sample()
    before = ClockReading(1_000_000_000, 1000, 1_000_000_000, 1)
    after = ClockReading(61_000_000_000, 1000, 61_000_000_000, 60_000_000_000)
    with pytest.raises(ResearchError, match="uncertainty exceeds"):
        require_continuity(before, after)


def test_unsupported_platform_has_no_silent_clock_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    native_clock.cache_clear()
    with monkeypatch.context() as patch:
        patch.setattr(clocks, "sys", SimpleNamespace(platform="unqualified"))
        with pytest.raises(ResearchError) as missing:
            native_clock()
        assert missing.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert native_clock().profile != "unqualified"


def test_elapsed_clock_does_not_follow_wall_clock_adjustment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = native_clock()
    before = clock.sample()
    monkeypatch.setattr(clocks.time, "time_ns", lambda: before.wall_ns - 3_600_000_000_000)
    after = clock.sample()
    assert after.continuous_ns >= before.continuous_ns
    with pytest.raises(ResearchError, match="wall-clock step"):
        require_continuity(before, after)


def test_held_preparation_consumes_absolute_deadline_without_release(tmp_path: Path) -> None:
    marker = tmp_path / "must-not-run"
    cap = native_clock().continuous_ns() + 150_000_000

    def delayed_release(_: object) -> bool:
        time.sleep(0.25)
        return True

    result = run_native(
        command(
            tmp_path, f"from pathlib import Path; Path({str(marker)!r}).write_text('released')"
        ),
        tmp_path / "output",
        hard_deadline_ns=cap,
        on_held=delayed_release,
    )
    assert result.reason == "not_started" and result.released_at is None
    assert result.closure.scope_closed and not marker.exists()


def test_running_deadline_counts_suspend_even_during_a_blocked_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jumped = threading.Event()
    clock = clocks.continuous_time
    monkeypatch.setattr(
        "research_swarm.workers.supervision.continuous_time",
        lambda: clock() + (10 if jumped.is_set() else 0),
    )

    def released(_: str) -> None:
        jumped.set()
        time.sleep(0.2)

    started = time.monotonic()
    result = run_native(
        command(tmp_path, "import time; time.sleep(60)", timeout_ms=5000),
        tmp_path / "output",
        on_released=released,
    )
    assert time.monotonic() - started < 3
    assert result.reason == "deadline" and result.closure.scope_closed
    assert result.elapsed_ms >= 10_000
    require_closed(result.closure.identity.pid)


def test_expired_absolute_cap_never_becomes_a_new_positive_allowance(tmp_path: Path) -> None:
    result = run_native(
        command(tmp_path, "raise SystemExit(0)"),
        tmp_path / "output",
        hard_deadline_ns=0,
    )
    assert result.reason == "not_started" and result.released_at is None
    assert result.closure.scope_closed


def test_watchdog_is_armed_before_release_can_block(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from research_swarm.workers import supervision

    prepare = supervision._prepare
    stopped = threading.Event()

    def prepare_with_blocked_release(*args: Any) -> Any:
        child = prepare(*args)
        release, stop = child.release, child.stop

        def blocked_release() -> None:
            release()
            assert stopped.wait(2), "Released process had no active watchdog"

        def observed_stop(*, force: bool) -> None:
            stop(force=force)
            stopped.set()

        monkeypatch.setattr(child, "release", blocked_release)
        monkeypatch.setattr(child, "stop", observed_stop)
        return child

    monkeypatch.setattr(supervision, "_prepare", prepare_with_blocked_release)
    result = run_native(
        command(tmp_path, "import time; time.sleep(60)", timeout_ms=300), tmp_path / "output"
    )
    assert result.reason == "deadline" and result.closure.scope_closed
    require_closed(result.closure.identity.pid)


def test_watchdog_closes_a_held_child_while_authorization_is_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from research_swarm.workers import supervision

    prepare = supervision._prepare
    stopped = threading.Event()

    def tracked_child(*args: Any) -> Any:
        child = prepare(*args)
        stop = child.stop

        def observed_stop(*, force: bool) -> None:
            stop(force=force)
            stopped.set()

        monkeypatch.setattr(child, "stop", observed_stop)
        return child

    def blocked_authorization(_: object) -> bool:
        assert stopped.wait(3), "Held process had no active deadline watchdog"
        return True

    monkeypatch.setattr(supervision, "_prepare", tracked_child)
    result = run_native(
        command(tmp_path, "raise SystemExit('must never run')", timeout_ms=300),
        tmp_path / "output",
        on_held=blocked_authorization,
    )
    assert result.reason == "not_started" and result.released_at is None
    assert result.closure.scope_closed and stopped.is_set()
    require_closed(result.closure.identity.pid)


@pytest.mark.parametrize("eventually_available", [False, True])
def test_closed_scope_waits_for_leader_exit_within_original_cleanup_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, eventually_available: bool
) -> None:
    from research_swarm.workers import supervision

    prepare = supervision._prepare
    observations = []
    exit_reads = []
    watchdogs = []
    original_thread = threading.Thread

    class TrackedThread(original_thread):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            if kwargs.get("target").__name__ == "enforce_deadline":
                watchdogs.append(self)

    def delayed_exit(*args: Any) -> Any:
        child = prepare(*args)
        active, original = child.active, child.exit_code

        def delayed_active() -> bool:
            value = active()
            if not value:
                observations.append(value)
                if not eventually_available or len(observations) <= 3:
                    return True
            return value

        def exit_code() -> int | None:
            joined = bool(watchdogs) and all(not thread.is_alive() for thread in watchdogs)
            exit_reads.append(joined)
            assert joined, "Exit collection reaped the leader before joining its watchdog"
            return original()

        monkeypatch.setattr(child, "active", delayed_active)
        monkeypatch.setattr(child, "exit_code", exit_code)
        return child

    monkeypatch.setattr(supervision, "_prepare", delayed_exit)
    monkeypatch.setattr(supervision.threading, "Thread", TrackedThread)
    invocation = command(tmp_path, "raise SystemExit('held child must not run')").model_copy(
        update={"grace_ms": 10, "cleanup_ms": 200}
    )
    result = run_native(invocation, tmp_path / "output", on_held=lambda _: False)
    assert len(observations) > 3
    assert exit_reads == [True]
    assert result.reason == "not_started" and result.released_at is None
    assert result.closure.scope_closed == eventually_available
    assert (result.exit_code is not None) == eventually_available
    assert (
        "Incomplete native exit or output closure" in result.closure.findings
    ) != eventually_available
    require_closed(result.closure.identity.pid)


def test_missing_clock_leaves_unclaimed_journal_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verified, invocation = dispatch(tmp_path)
    journal = WorkerJournal(tmp_path / "journal.sqlite")
    initial = journal.receive(verified, invocation)

    def unavailable() -> Any:
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Synthetic clock unavailable")

    with monkeypatch.context() as patch:
        patch.setattr("research_swarm.workers.supervisor.native_clock", unavailable)
        with pytest.raises(ResearchError, match="clock unavailable"):
            run_attempt(journal, initial.dispatch.attempt_id)
    assert journal.get(initial.dispatch.attempt_id) == initial
    result = run_attempt(journal, initial.dispatch.attempt_id)
    assert result.state == JournalState.CLOSED
    assert result.outcome is not None and result.outcome.exit_code == 0
