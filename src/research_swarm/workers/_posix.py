"""Owned POSIX process groups for trusted code; not hostile-code containment."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import BinaryIO, cast

import psutil

from research_swarm.domain.types import new_id
from research_swarm.workers.contracts import NativeCommand, NativeIdentity, ProcessClosure


def _nonblocking(descriptor: int) -> None:
    if sys.platform == "win32":
        raise OSError("POSIX supervision is unavailable")
    os.set_blocking(descriptor, False)


def _signal_group(pid: int, *, force: bool) -> None:
    if sys.platform == "win32":
        raise OSError("POSIX supervision is unavailable")
    os.killpg(pid, signal.SIGKILL if force else signal.SIGTERM)


def _group(pid: int) -> int:
    if sys.platform == "win32":
        raise OSError("POSIX supervision is unavailable")
    return os.getpgid(pid)


def start_token(process: psutil.Process) -> str:
    if sys.platform == "linux":
        # /proc start ticks and boot identity do not depend on the wall clock.
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        stat = Path(f"/proc/{process.pid}/stat").read_text()
        ticks = stat[stat.rfind(")") + 2 :].split()[19]
        return f"linux:{boot}:{ticks}"
    return f"native-created:{process.create_time().hex()}"


class PosixChild:
    def __init__(self, command: NativeCommand, environment: dict[str, str]) -> None:
        read_fd, write_fd = os.pipe()
        self.barrier: int | None = write_fd
        try:
            self.process = subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    str(Path(__file__).with_name("_bootstrap.py")),
                    str(read_fd),
                    "30",
                    *command.argv,
                ],
                cwd=command.cwd,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
                close_fds=True,
                pass_fds=(read_fd,),
            )
        except BaseException:
            os.close(write_fd)
            self.barrier = None
            raise
        finally:
            os.close(read_fd)
        self.stdout = cast(BinaryIO, self.process.stdout)
        self.stderr = cast(BinaryIO, self.process.stderr)
        try:
            self.native = psutil.Process(self.process.pid)
            self.identity = NativeIdentity(
                backend="posix_group",
                pid=self.process.pid,
                start_token=start_token(self.native),
                scope_id=new_id(),
            )
            _nonblocking(self.stdout.fileno())
            _nonblocking(self.stderr.fileno())
        except BaseException:
            with suppress(ProcessLookupError):
                _signal_group(self.process.pid, force=True)
            self.process.wait(timeout=5)
            self.close()
            raise
        self.descendants: dict[int, psutil.Process] = {}
        self.unobserved = False
        self.next_descendant_scan = 0.0

    def release(self) -> None:
        if self.barrier is None:
            raise RuntimeError("Process barrier is already closed")
        try:
            os.write(self.barrier, b"1")
        finally:
            os.close(self.barrier)
            self.barrier = None

    @staticmethod
    def read(stream: BinaryIO, maximum: int) -> bytes | None:
        try:
            return os.read(stream.fileno(), maximum)
        except BlockingIOError:
            return None

    def active(self) -> bool:
        # Do not reap yet: the owned leader PID prevents reuse of the group ID.
        try:
            alive = self.native.status() != psutil.STATUS_ZOMBIE
            if alive and time.monotonic() >= self.next_descendant_scan:
                for child in self.native.children(recursive=True):
                    self.descendants.setdefault(child.pid, child)
                self.next_descendant_scan = time.monotonic() + 0.2
            return alive
        except psutil.NoSuchProcess:
            return False
        except psutil.AccessDenied:
            self.unobserved = True
            return True

    def stop(self, *, force: bool) -> None:
        if self.barrier is not None:
            os.close(self.barrier)
            self.barrier = None
        with suppress(ProcessLookupError):
            _signal_group(self.process.pid, force=force)

    def closure(self) -> ProcessClosure:
        live_group = False
        escaped: list[int] = []
        inaccessible = self.unobserved
        for process in psutil.process_iter():
            try:
                if (
                    _group(process.pid) == self.process.pid
                    and process.status() != psutil.STATUS_ZOMBIE
                ):
                    live_group = True
            except (ProcessLookupError, psutil.NoSuchProcess):
                continue
            except (PermissionError, psutil.AccessDenied):
                # An unrelated inaccessible process is not evidence about our group.
                if process.pid in self.descendants or process.pid == self.process.pid:
                    inaccessible = True
        for process in self.descendants.values():
            try:
                if (
                    process.is_running()
                    and process.status() != psutil.STATUS_ZOMBIE
                    and _group(process.pid) != self.process.pid
                ):
                    escaped.append(process.pid)
            except (ProcessLookupError, psutil.NoSuchProcess):
                continue
            except (PermissionError, psutil.AccessDenied):
                inaccessible = True
        findings = ["POSIX group closure does not establish containment of unobserved escapees"]
        if inaccessible:
            findings.append("An owned process could not be observed")
        return ProcessClosure(
            identity=self.identity,
            scope_closed=not live_group and not escaped and not inaccessible,
            scope="posix_process_group",
            observed_escaped_pids=tuple(sorted(escaped)),
            findings=tuple(findings),
        )

    def exit_code(self) -> int | None:
        if self.active():
            return None
        return self.process.wait(timeout=0.1)

    def close(self) -> None:
        if self.barrier is not None:
            os.close(self.barrier)
            self.barrier = None
        self.stdout.close()
        self.stderr.close()
