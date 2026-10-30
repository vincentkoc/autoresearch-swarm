from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path

import psutil
import pytest

from research_swarm.workers.contracts import NativeCommand, NativeIdentity
from research_swarm.workers.supervision import base_environment, run_native


def command(tmp_path: Path, code: str, *, timeout_ms: int = 5000) -> NativeCommand:
    return NativeCommand(
        argv=(sys.executable, "-X", "utf8", "-c", code),
        cwd=str(tmp_path),
        timeout_ms=timeout_ms,
        grace_ms=100,
        cleanup_ms=3000,
        output_limit_bytes=4096,
    )


def require_closed(pid: int) -> None:
    with suppress(psutil.NoSuchProcess):
        assert psutil.Process(pid).status() == psutil.STATUS_ZOMBIE


def test_start_barrier_precedes_project_execution_and_preserves_arguments(tmp_path: Path) -> None:
    marker = tmp_path / "marker"
    received: list[NativeIdentity] = []

    def held(identity: NativeIdentity) -> bool:
        assert not marker.exists()
        received.append(identity)
        return True

    invocation = command(
        tmp_path,
        f"from pathlib import Path; import sys; Path({str(marker)!r}).touch(); print(sys.argv[1:])",
    )
    invocation = invocation.model_copy(
        update={"argv": (*invocation.argv, "two words", 'a"b', "猫")}
    )
    outcome = run_native(invocation, tmp_path / "output", on_held=held)
    assert outcome.reason == "exited"
    assert outcome.exit_code == 0 and outcome.closure.scope_closed
    assert outcome.closure.identity == received[0]
    assert marker.exists()
    text = (tmp_path / "output" / "stdout").read_text(encoding="utf-8")
    assert "two words" in text and "猫" in text and 'a"b' in text


def test_denied_barrier_never_executes_project_code(tmp_path: Path) -> None:
    marker = tmp_path / "marker"
    outcome = run_native(
        command(tmp_path, f"from pathlib import Path; Path({str(marker)!r}).touch()"),
        tmp_path / "output",
        on_held=lambda _: False,
    )
    assert outcome.reason == "not_started"
    assert outcome.released_at is None
    assert outcome.closure.scope_closed
    assert not marker.exists()


def test_failed_journal_callback_closes_held_native_scope(tmp_path: Path) -> None:
    def failed(_: NativeIdentity) -> bool:
        raise OSError("injected journal failure")

    outcome = run_native(
        command(tmp_path, "raise SystemExit(99)"), tmp_path / "out", on_held=failed
    )
    assert outcome.reason == "supervisor_error"
    assert outcome.released_at is None and outcome.closure.scope_closed
    require_closed(outcome.closure.identity.pid)


def test_both_output_streams_drain_after_retention_is_full(tmp_path: Path) -> None:
    size = 2 * 1024 * 1024
    outcome = run_native(
        command(
            tmp_path,
            f"import os; [(os.write(1,b'x'*65536),os.write(2,b'y'*65536)) "
            f"for _ in range({size // 65536})]",
        ),
        tmp_path / "out",
    )
    assert outcome.exit_code == 0 and outcome.closure.scope_closed
    for output in (outcome.stdout, outcome.stderr):
        assert output.retained_bytes == 4096
        assert output.observed_bytes == size
        assert output.eof
        assert (tmp_path / "out" / output.name).stat().st_size == 4096


def test_deadline_uses_monotonic_time_and_reports_native_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "research_swarm.workers.supervision.utc_now", lambda: "2000-01-01T00:00:00.000Z"
    )
    started = time.monotonic()
    outcome = run_native(
        command(tmp_path, "import time; time.sleep(60)", timeout_ms=300), tmp_path / "out"
    )
    assert outcome.reason == "deadline" and outcome.closure.scope_closed
    assert outcome.exit_code != 0
    assert 250 <= outcome.elapsed_ms < 4000
    assert time.monotonic() - started < 4


def test_cancellation_accounts_for_child_and_grandchild_without_touching_other_process(
    tmp_path: Path,
) -> None:
    child_path = tmp_path / "child.pid"
    grandchild_path = tmp_path / "grandchild.pid"
    grandchild = (
        f"import os,time; open({str(grandchild_path)!r},'w').write(str(os.getpid())); "
        "time.sleep(60)"
    )
    child = (
        f"import os,sys,subprocess,time; open({str(child_path)!r},'w').write(str(os.getpid())); "
        f"subprocess.Popen([sys.executable,'-c',{grandchild!r}]); time.sleep(60)"
    )
    parent = (
        f"import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',{child!r}]); "
        "time.sleep(60)"
    )
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        outcome = run_native(
            command(tmp_path, parent), tmp_path / "out", cancelled=grandchild_path.exists
        )
        assert outcome.reason == "cancelled" and outcome.closure.scope_closed
        require_closed(int(child_path.read_text()))
        require_closed(int(grandchild_path.read_text()))
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=5)


def test_primary_exit_does_not_hide_a_surviving_child(tmp_path: Path) -> None:
    marker = tmp_path / "child.pid"
    child = "import time; time.sleep(60)"
    parent = (
        f"import subprocess,sys; p=subprocess.Popen([sys.executable,'-c',{child!r}]); "
        f"open({str(marker)!r},'w').write(str(p.pid))"
    )
    outcome = run_native(command(tmp_path, parent), tmp_path / "out")
    assert outcome.reason == "descendants_remaining"
    assert outcome.exit_code == 0 and outcome.closure.scope_closed
    require_closed(int(marker.read_text()))


@pytest.mark.skipif(os.name != "posix", reason="POSIX group escape behavior")
def test_observed_group_escape_keeps_closure_unknown(tmp_path: Path) -> None:
    marker = tmp_path / "escaped.pid"
    child = "import time; time.sleep(60)"
    parent = (
        f"import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c',{child!r}],"
        f"start_new_session=True); open({str(marker)!r},'w').write(str(p.pid)); time.sleep(0.5)"
    )
    try:
        outcome = run_native(command(tmp_path, parent), tmp_path / "out")
        assert not outcome.closure.scope_closed
        assert int(marker.read_text()) in outcome.closure.observed_escaped_pids
    finally:
        if marker.exists():
            escaped = psutil.Process(int(marker.read_text()))
            escaped.kill()


def test_environment_excludes_controller_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RESEARCH_TOKEN", "test-secret")
    monkeypatch.setenv("OPENAI_API_KEY", "test-secret")
    assert "RESEARCH_TOKEN" not in base_environment()
    assert "OPENAI_API_KEY" not in base_environment()


def test_supervisor_loss_before_release_does_not_run_project(tmp_path: Path) -> None:
    marker = tmp_path / "project.marker"
    identity = tmp_path / "identity.json"
    invocation = command(tmp_path, f"from pathlib import Path; Path({str(marker)!r}).touch()")
    code = (
        "import os; from pathlib import Path; "
        "from research_swarm.workers.contracts import NativeCommand; "
        "from research_swarm.workers.supervision import run_native\n"
        "def held(ref):\n"
        f" Path({str(identity)!r}).write_text(ref.model_dump_json()); os._exit(77)\n"
        f"run_native(NativeCommand.model_validate_json({invocation.model_dump_json()!r}), "
        f"Path({str(tmp_path / 'out')!r}), on_held=held)"
    )
    supervisor = subprocess.Popen([sys.executable, "-c", code])
    assert supervisor.wait(timeout=10) == 77
    pid = int(json.loads(identity.read_text())["pid"])
    until = time.monotonic() + 5
    while time.monotonic() < until:
        try:
            require_closed(pid)
            break
        except AssertionError:
            time.sleep(0.02)
    require_closed(pid)
    assert not marker.exists()


def test_deadline_survives_a_blocked_release_callback(tmp_path: Path) -> None:
    marker = tmp_path / "too-late"
    invocation = command(
        tmp_path,
        f"import time; from pathlib import Path; time.sleep(0.7); Path({str(marker)!r}).touch()",
        timeout_ms=250,
    )
    outcome = run_native(invocation, tmp_path / "out", on_released=lambda _: time.sleep(1))
    assert outcome.reason == "deadline" and outcome.closure.scope_closed
    assert not marker.exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX executable paths and signal escalation")
def test_cancellation_escalates_for_a_sigterm_ignoring_child(tmp_path: Path) -> None:
    marker = tmp_path / "ready"
    invocation = command(
        tmp_path,
        "import signal,time; from pathlib import Path; "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        f"Path({str(marker)!r}).touch(); time.sleep(60)",
    )
    outcome = run_native(invocation, tmp_path / "out", cancelled=marker.exists)
    assert outcome.reason == "cancelled" and outcome.closure.scope_closed
    assert outcome.exit_code == -9
    with pytest.raises(ValueError, match="leave time"):
        NativeCommand.model_validate(invocation.model_copy(update={"grace_ms": 5000}))


@pytest.mark.skipif(os.name != "posix", reason="POSIX executable file fixture")
@pytest.mark.parametrize("executable", ["./program", "program"])
def test_relative_executables_resolve_from_the_project(tmp_path: Path, executable: str) -> None:
    program = tmp_path / "program"
    program.write_text("#!/bin/sh\nprintf 'project-directory'\n")
    program.chmod(0o700)
    invocation = NativeCommand(argv=(executable,), cwd=str(tmp_path), timeout_ms=3000)
    environment = {**base_environment(), "PATH": "."}
    outcome = run_native(invocation, tmp_path / "out", environment=environment)
    assert outcome.exit_code == 0 and outcome.closure.scope_closed
    assert (tmp_path / "out" / "stdout").read_text() == "project-directory"


def test_failed_deadline_signal_does_not_erase_independent_closure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from research_swarm.workers import supervision

    prepare = supervision._prepare

    def wrapped(command: NativeCommand, environment: dict[str, str]):
        child = prepare(command, environment)
        stop = child.stop

        def stop_with_failed_force(*, force: bool) -> None:
            if force:
                raise PermissionError("injected signal race")
            stop(force=force)

        child.stop = stop_with_failed_force
        return child

    monkeypatch.setattr(supervision, "_prepare", wrapped)
    outcome = run_native(
        command(tmp_path, "import time; time.sleep(60)", timeout_ms=250),
        tmp_path / "out",
        on_released=lambda _: time.sleep(0.5),
    )
    assert outcome.reason == "deadline" and outcome.closure.scope_closed
    assert any(
        "Deadline termination request failed" in finding for finding in outcome.closure.findings
    )


def test_primary_exit_allows_bounded_native_accounting_settlement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from research_swarm.workers import supervision

    prepare = supervision._prepare

    def wrapped(command: NativeCommand, environment: dict[str, str]):
        child = prepare(command, environment)
        closure = child.closure
        observations = 0

        def delayed_accounting():
            nonlocal observations
            observed = closure()
            if observed.scope_closed and observations < 2:
                observations += 1
                return observed.model_copy(update={"scope_closed": False})
            return observed

        child.closure = delayed_accounting
        return child

    monkeypatch.setattr(supervision, "_prepare", wrapped)
    outcome = run_native(command(tmp_path, "print('complete')"), tmp_path / "out")
    assert outcome.reason == "exited" and outcome.closure.scope_closed
    assert outcome.exit_code == 0
