"""Bounded native execution with a durable-caller start barrier and capped output."""

from __future__ import annotations

import hashlib
import os
import shutil
import sys
import threading
import time
from collections.abc import Callable
from contextlib import ExitStack, suppress
from pathlib import Path
from typing import BinaryIO, Literal, Protocol

from research_swarm.domain.types import utc_now
from research_swarm.workers.clocks import continuous_time
from research_swarm.workers.contracts import (
    CapturedOutput,
    NativeCommand,
    NativeIdentity,
    NativeOutcome,
    ProcessClosure,
)


class NativeChild(Protocol):
    identity: NativeIdentity
    stdout: BinaryIO
    stderr: BinaryIO

    def release(self) -> None: ...
    def read(self, stream: BinaryIO, maximum: int) -> bytes | None: ...
    def active(self) -> bool: ...
    def stop(self, *, force: bool) -> None: ...
    def closure(self) -> ProcessClosure: ...
    def exit_code(self) -> int | None: ...
    def close(self) -> None: ...


def base_environment() -> dict[str, str]:
    """Only operating-system necessities cross into a native workload by default."""
    allowed = {
        "PATH",
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "TEMP",
        "TMP",
        "TMPDIR",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "TZ",
    }
    return {key: value for key, value in os.environ.items() if key.upper() in allowed}


def _prepare(command: NativeCommand, environment: dict[str, str]) -> NativeChild:
    cwd = Path(command.cwd)
    name = command.argv[0]
    if os.path.dirname(name):
        name = str(cwd / name)
    path = os.pathsep.join(
        str(cwd / item) for item in environment.get("PATH", os.defpath).split(os.pathsep)
    )
    executable = shutil.which(name, path=path)
    if executable is None:
        raise FileNotFoundError("The execution profile cannot resolve the requested executable")
    command = command.model_copy(update={"argv": (executable, *command.argv[1:])})
    if sys.platform == "win32":
        from research_swarm.workers._windows import WindowsChild

        return WindowsChild(command, environment)
    if os.name == "posix":
        from research_swarm.workers._posix import PosixChild

        return PosixChild(command, environment)
    raise OSError("This operating system has no native process supervisor")


class _Capture:
    def __init__(self, name: Literal["stdout", "stderr"], output: BinaryIO, maximum: int) -> None:
        self.name = name
        self.output = output
        self.maximum = maximum
        self.retained = 0
        self.observed = 0
        self.eof = False
        self.digest = hashlib.sha256()

    def drain(self, child: NativeChild, stream: BinaryIO) -> bool:
        progress = False
        # Bound each turn so flooding one stream cannot starve cancellation or the other stream.
        until = continuous_time() + 0.008
        for _ in range(1024):
            if self.eof:
                break
            if continuous_time() >= until:
                break
            block = child.read(stream, 64 * 1024)
            if block is None:
                if not progress:
                    break
                # Give a backpressured writer a turn without restarting the native observation loop.
                time.sleep(0)
                continue
            if not block:
                self.eof = True
                break
            progress = True
            self.observed += len(block)
            retained = block[: max(0, self.maximum - self.retained)]
            self.output.write(retained)
            self.digest.update(retained)
            self.retained += len(retained)
        return progress

    def finish(self) -> CapturedOutput:
        self.output.flush()
        os.fsync(self.output.fileno())
        return CapturedOutput(
            name=self.name,
            retained_bytes=self.retained,
            observed_bytes=self.observed,
            digest="sha256:" + self.digest.hexdigest(),
            eof=self.eof,
        )


def run_native(
    command: NativeCommand,
    output_dir: Path,
    *,
    on_held: Callable[[NativeIdentity], bool] = lambda _: True,
    on_released: Callable[[str], None] = lambda _: None,
    cancelled: Callable[[], bool] = lambda: False,
    environment: dict[str, str] | None = None,
    hard_deadline_ns: int | None = None,
    launch_deadline_ns: int | None = None,
) -> NativeOutcome:
    """Run on a dedicated supervisor thread/process, never on the controller event loop.

    The caller commits native identity and release intent in on_held. A false result
    denies release. Callback failures terminate the owned scope and remain visible.
    This function never signals a process from a deserialized identity.
    """
    command = NativeCommand.model_validate(command)
    if hard_deadline_ns is not None and (type(hard_deadline_ns) is not int or hard_deadline_ns < 0):
        raise ValueError("Absolute deadline requires nonnegative continuous-clock nanoseconds")
    if launch_deadline_ns is not None and (
        type(launch_deadline_ns) is not int or launch_deadline_ns < 0
    ):
        raise ValueError("Launch deadline requires nonnegative continuous-clock nanoseconds")
    env = base_environment() if environment is None else dict(environment)
    if any("\0" in key + value or "=" in key or not key for key, value in env.items()):
        raise ValueError("Environment entries must be NUL-free and have valid names")
    output_dir.mkdir(mode=0o700)
    prepared_at = utc_now()
    started = continuous_time()
    deadline = started + command.timeout_ms / 1000
    if hard_deadline_ns is not None:
        deadline = min(deadline, hard_deadline_ns / 1_000_000_000)
    launch_deadline = (
        deadline
        if launch_deadline_ns is None
        else min(deadline, launch_deadline_ns / 1_000_000_000)
    )
    released_at = None
    reason: Literal[
        "exited",
        "cancelled",
        "deadline",
        "not_started",
        "supervisor_error",
        "descendants_remaining",
    ] = "not_started"
    findings: list[str] = []
    with ExitStack() as stack:
        stdout = _Capture(
            "stdout",
            stack.enter_context((output_dir / "stdout").open("xb")),
            command.output_limit_bytes,
        )
        stderr = _Capture(
            "stderr",
            stack.enter_context((output_dir / "stderr").open("xb")),
            command.output_limit_bytes,
        )
        child = _prepare(command, env)
        stack.callback(child.close)
        watchdog_stop = threading.Event()
        deadline_hit = threading.Event()
        watchdog_errors: list[str] = []
        watchdog: threading.Thread | None = None

        def enforce_deadline() -> None:
            # Condition waits may pause during suspend. Recheck the absolute
            # suspend-inclusive cap after each short wait instead of refunding it.
            while not watchdog_stop.is_set():
                remaining = deadline - continuous_time()
                if remaining <= 0:
                    deadline_hit.set()
                    try:
                        child.stop(force=True)
                    except OSError as error:
                        watchdog_errors.append(type(error).__name__)
                    return
                watchdog_stop.wait(min(remaining, 0.05))

        def stop_watchdog() -> None:
            watchdog_stop.set()
            if watchdog is not None and watchdog.ident is not None:
                # Join before reaping the leader or releasing native handles.
                watchdog.join()

        stack.callback(stop_watchdog)

        def drain() -> bool:
            left = stdout.drain(child, child.stdout)
            right = stderr.drain(child, child.stderr)
            return left or right

        try:
            watchdog = threading.Thread(target=enforce_deadline, daemon=True)
            watchdog.start()
            permitted = on_held(child.identity)
            if (
                permitted
                and not deadline_hit.is_set()
                and not cancelled()
                and continuous_time() < launch_deadline
            ):
                released_at = utc_now()
                child.release()
                on_released(released_at)
                reason = "exited"
                while child.active():
                    if cancelled():
                        reason = "cancelled"
                        break
                    if continuous_time() >= deadline:
                        reason = "deadline"
                        break
                    if not drain():
                        time.sleep(0.01)
                if deadline_hit.is_set() or continuous_time() >= deadline:
                    reason = "deadline"
            closure = child.closure()
            if reason == "exited" and not closure.scope_closed:
                # Native job accounting can lag a signaled primary process. Allow
                # bounded closure/drain progress before classifying surviving work.
                settle_until = min(deadline, continuous_time() + command.grace_ms / 1000)
                while not closure.scope_closed and continuous_time() < settle_until:
                    if cancelled():
                        reason = "cancelled"
                        break
                    drain()
                    time.sleep(0.01)
                    closure = child.closure()
                if deadline_hit.is_set() or continuous_time() >= deadline:
                    reason = "deadline"
            if reason == "exited" and not closure.scope_closed:
                reason = "descendants_remaining"
        except BaseException as error:
            reason = "supervisor_error"
            findings.append(f"Supervisor callback or observation failed: {type(error).__name__}")
            closure = ProcessClosure(
                identity=child.identity,
                scope_closed=False,
                scope="windows_job" if sys.platform == "win32" else "posix_process_group",
            )

        # Retain ownership through scope closure, exit readiness and pipe draining.
        cleanup_start = continuous_time()
        cleanup_deadline = cleanup_start + command.cleanup_ms / 1000
        force_at = cleanup_start + command.grace_ms / 1000
        forced = False

        def request_stop(*, force: bool) -> None:
            try:
                child.stop(force=force)
            except OSError as error:
                findings.append(f"Cleanup termination request failed: {type(error).__name__}")
                # A concurrent deadline signal can leave a zombie-only group.
                # Continue independent closure observation, draining, and reaping.

        try:
            if not closure.scope_closed:
                request_stop(force=False)
            while True:
                leader_active = child.active()  # Observe descendants without reaping the leader.
                progress = drain()
                closure = child.closure()
                if closure.scope_closed and stdout.eof and stderr.eof and not leader_active:
                    break
                now = continuous_time()
                if not closure.scope_closed and now >= force_at and not forced:
                    request_stop(force=True)
                    forced = True
                if now >= cleanup_deadline:
                    break
                if not progress:
                    time.sleep(0.01)
            stop_watchdog()
            exit_code = child.exit_code()
        except BaseException as error:
            findings.append(f"Native cleanup could not be established: {type(error).__name__}")
            closure = closure.model_copy(update={"scope_closed": False})
            exit_code = None
            with suppress(OSError):
                child.stop(force=True)
        stop_watchdog()
        if watchdog_errors:
            findings.append("Deadline termination request failed: " + ", ".join(watchdog_errors))
            # A signal error cannot erase an independently verified native closure.
        if not stdout.eof or not stderr.eof or exit_code is None:
            findings.append("Incomplete native exit or output closure")
            closure = closure.model_copy(update={"scope_closed": False})
        closure = closure.model_copy(update={"findings": (*closure.findings, *findings)})
        outcome = NativeOutcome(
            reason=reason,
            exit_code=exit_code,
            prepared_at=prepared_at,
            released_at=released_at,
            finished_at=utc_now(),
            elapsed_ms=int((continuous_time() - started) * 1000),
            closure=closure,
            stdout=stdout.finish(),
            stderr=stderr.finish(),
        )
    if sys.platform != "win32" and os.name == "posix":
        descriptor = os.open(output_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return outcome
