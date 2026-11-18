from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, BinaryIO

import pytest

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.workers import clocks
from research_swarm.workers.clocks import (
    native_clock,
)
from research_swarm.workers.contracts import ProcessClosure
from research_swarm.workers.supervision import run_native
from tests.workers.test_supervision import command, require_closed


def test_native_clock_is_shared_with_an_independent_process() -> None:
    clock = native_clock()
    before = clock.continuous_ns()
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json; from dataclasses import asdict; "
            "from research_swarm.workers.clocks import native_clock; "
            "c=native_clock(); "
            "print(json.dumps({'profile':c.profile,'reading':c.continuous_ns()}))",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    child = json.loads(completed.stdout)
    after = clock.continuous_ns()
    assert child["profile"] == clock.profile
    assert before <= child["reading"] <= after


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
    before = clock.continuous_ns()
    monkeypatch.setattr(clocks.time, "time_ns", lambda: 0)
    assert clock.continuous_ns() >= before


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
    anchor = supervision.continuous_time()
    fixture_deadline = time.monotonic() + 5
    current = anchor
    phases: list[str] = []
    collecting_exit = False
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
        read, closure = child.read, child.closure
        closed = False
        eof: set[BinaryIO] = set()

        def observed_read(stream: BinaryIO, maximum: int) -> bytes | None:
            block = read(stream, maximum)
            if block == b"":
                eof.add(stream)
            return block

        def observed_closure() -> ProcessClosure:
            nonlocal closed
            result = closure()
            closed = result.scope_closed
            return result

        def delayed_active() -> bool:
            nonlocal current
            if time.monotonic() >= fixture_deadline:
                child.stop(force=True)
                raise OSError("Native fixture did not close within its outer guard")
            value = active()
            if value:
                return True
            if collecting_exit:
                return not eventually_available
            # Establish native closure before exercising the logical deadline.
            if not closed or child.stdout not in eof or child.stderr not in eof:
                return True
            if not phases:
                phases.append("before_deadline")
                current = anchor + 0.199
                return True
            if len(phases) == 1:
                if eventually_available:
                    phases.append("ready_before_deadline")
                    return False
                phases.append("expired")
                current = anchor + 0.200
                return True
            phases.append("extra_cleanup_poll")
            current = anchor + 5
            return not eventually_available

        def exit_code() -> int | None:
            nonlocal collecting_exit
            joined = bool(watchdogs) and all(not thread.is_alive() for thread in watchdogs)
            exit_reads.append(joined)
            assert joined, "Exit collection reaped the leader before joining its watchdog"
            collecting_exit = True
            return original()

        monkeypatch.setattr(child, "active", delayed_active)
        monkeypatch.setattr(child, "exit_code", exit_code)
        monkeypatch.setattr(child, "read", observed_read)
        monkeypatch.setattr(child, "closure", observed_closure)
        return child

    monkeypatch.setattr(supervision, "_prepare", delayed_exit)
    monkeypatch.setattr(supervision.threading, "Thread", TrackedThread)
    monkeypatch.setattr(supervision, "continuous_time", lambda: current)
    invocation = command(tmp_path, "raise SystemExit('held child must not run')").model_copy(
        update={"grace_ms": 10, "cleanup_ms": 200}
    )
    result = run_native(invocation, tmp_path / "output", on_held=lambda _: False)
    assert phases == [
        "before_deadline",
        "ready_before_deadline" if eventually_available else "expired",
    ]
    assert exit_reads == [True]
    assert result.reason == "not_started" and result.released_at is None
    assert result.closure.scope_closed == eventually_available
    assert (result.exit_code is not None) == eventually_available
    assert (
        "Incomplete native exit or output closure" in result.closure.findings
    ) != eventually_available
    require_closed(result.closure.identity.pid)
