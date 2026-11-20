"""Owned POSIX process groups for trusted code; not hostile-code containment."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import BinaryIO, cast

import psutil

from research_swarm.swarm.process import start_group, stop_group
from research_swarm.swarm.records import new_id
from research_swarm.swarm.workers.contracts import NativeCommand, NativeIdentity, ProcessClosure


def _nonblocking(descriptor: int) -> None:
    os.set_blocking(descriptor, False)


def _group(pid: int) -> int:
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
        self.process, self.owner = start_group(
            command.argv,
            cwd=command.cwd,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            observe_closure=True,
        )
        self.released = False
        self.status = bytearray()
        self.command_code: int | None = None
        self.stdout = cast(BinaryIO, self.process.stdout)
        self.stderr = cast(BinaryIO, self.process.stderr)
        try:
            self.owner.setblocking(False)
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
            self.close()
            raise
        self.descendants: dict[int, psutil.Process] = {}
        self.unobserved = False
        self.next_descendant_scan = 0.0

    def release(self) -> None:
        if self.released or self.owner.fileno() < 0:
            raise RuntimeError("Process barrier is already closed")
        self.owner.sendall(b"1")
        self.released = True

    def _read_status(self) -> None:
        if self.command_code is not None or self.owner.fileno() < 0:
            return
        with suppress(BlockingIOError, ConnectionResetError):
            self.status.extend(self.owner.recv(128))
            lines = self.status.split(b"\n")
            if len(lines) >= 2 and int(lines[0]):
                self.command_code = 127
            elif len(lines) >= 3:
                self.command_code = int(lines[1])

    @staticmethod
    def read(stream: BinaryIO, maximum: int) -> bytes | None:
        try:
            return os.read(stream.fileno(), maximum)
        except BlockingIOError:
            return None

    def active(self) -> bool:
        # Do not reap yet: the owned leader PID prevents reuse of the group ID.
        self._read_status()
        try:
            alive = self.native.status() != psutil.STATUS_ZOMBIE
            if alive and time.monotonic() >= self.next_descendant_scan:
                for child in self.native.children(recursive=True):
                    self.descendants.setdefault(child.pid, child)
                self.next_descendant_scan = time.monotonic() + 0.2
            # Before owner close, command status starts descendant settlement. After
            # close, cleanup must observe the guard's own exit before it can reap.
            return alive and (self.command_code is None or self.owner.fileno() < 0)
        except psutil.NoSuchProcess:
            return False
        except psutil.AccessDenied:
            self.unobserved = True
            return True

    def stop(self, *, force: bool) -> None:
        if not self.released:
            self.owner.close()
        stop_group(self.process, force=force)

    def closure(self) -> ProcessClosure:
        self._read_status()
        live_group = False
        escaped: list[int] = []
        inaccessible = self.unobserved
        for process in psutil.process_iter():
            try:
                if process.pid == self.process.pid and self.command_code is not None:
                    continue
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
        if not live_group and self.command_code is not None:
            self.owner.close()
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
        guard_code = self.process.poll()
        if guard_code is None:
            return None
        return guard_code if self.command_code is None else self.command_code

    def close(self) -> None:
        self.owner.close()
        try:
            self.process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            stop_group(self.process)
            self.process.wait(timeout=5)
        self.stdout.close()
        self.stderr.close()
