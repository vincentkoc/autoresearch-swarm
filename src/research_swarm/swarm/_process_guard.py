"""Own one command and its inherited process group until the caller closes the socket."""

from __future__ import annotations

import errno
import os
import select
import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from threading import Thread
from typing import BinaryIO


def kill_group() -> None:
    if os.getsid(0) != os.getpid():
        raise RuntimeError("Process guard requires its own session")
    os.killpg(os.getpid(), signal.SIGKILL)


def watch_worker(owner: socket.socket) -> None:
    # The worker holds the sole peer. Cancellation, normal close and SIGKILL all give EOF.
    try:
        owner.recv(1)
    finally:
        kill_group()


def supervise(
    argv: Sequence[str],
    *,
    owner: socket.socket | None = None,
    deadline: float | None = None,
    report: Callable[[str, bytes, bytes], None] | None = None,
    observe_closure: bool = False,
) -> int:
    """Linked commands close on owner EOF. Independent jobs retain their absolute deadline."""
    if os.getsid(0) != os.getpid():
        raise RuntimeError("Process guard requires its own session")
    if owner is None and (deadline is None or report is None):
        raise ValueError("Independent jobs require a deadline and terminal report")
    if observe_closure:
        # A graceful stop reaches the command first. Keep the owner-EOF watcher
        # alive until every remaining group member has closed or the owner forces stop.
        signal.signal(signal.SIGTERM, lambda signum, frame: None)
    try:
        with ExitStack() as outputs:
            if observe_closure and owner is not None and not select.select([owner], [], [], 30)[0]:
                return 1
            if owner is not None and (
                owner.recv(1) != b"1" or select.select([owner], [], [], 0)[0]
            ):
                return 1
            if deadline is not None and time.monotonic() >= deadline:
                assert report is not None
                report("timed_out", b"", b"")
                return 1
            # The direct command can exit while its descendants still hold stdout.
            # Stage an advisory response until its receipt deadline has been checked.
            out = outputs.enter_context(tempfile.TemporaryFile()) if report is not None else None
            err = outputs.enter_context(tempfile.TemporaryFile()) if report is not None else None
            try:
                child = subprocess.Popen(argv, stdout=out, stderr=err)
            except OSError as exc:
                if owner is not None:
                    owner.sendall(f"{exc.errno or errno.EIO}\n".encode())
                if report is not None:
                    report("failed", b"", b"")
                return 1
            if owner is not None:
                watcher = Thread(target=watch_worker, args=(owner,), daemon=True)
                watcher.start()
                owner.sendall(b"0\n")
            try:
                child.wait(
                    timeout=None if deadline is None else max(0, deadline - time.monotonic())
                )
            except subprocess.TimeoutExpired:
                assert report is not None
                report("timed_out", b"", b"")
                return 1
            if report is not None:
                if deadline is not None and (
                    time.monotonic() >= deadline or child.returncode == -signal.SIGALRM
                ):
                    report("timed_out", b"", b"")
                    return 1
                assert out is not None and err is not None
                report("ok" if child.returncode == 0 else "failed", _contents(out), _contents(err))
            if owner is not None:
                owner.sendall(f"{child.returncode}\n".encode())
                if observe_closure:
                    # Release the output pipe writers. The native observer inspects remaining
                    # members while this leader still prevents reuse of the group ID.
                    os.close(1)
                    os.close(2)
                    watcher.join()
            return child.returncode
    finally:
        # The leader retains its identity through terminal publication and group cleanup.
        kill_group()


def _contents(stream: BinaryIO) -> bytes:
    size = os.fstat(stream.fileno()).st_size
    stream.seek(0)
    return stream.read(size)


if __name__ == "__main__":
    with socket.socket(fileno=int(sys.argv[1])) as control:
        supervise(sys.argv[3:], owner=control, observe_closure=sys.argv[2] == "observe")
