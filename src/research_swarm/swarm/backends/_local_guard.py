"""Run one local command in a private POSIX group tied to its worker's socket."""

from __future__ import annotations

import errno
import os
import select
import signal
import socket
import subprocess
import sys
from threading import Thread


def kill_group() -> None:
    os.killpg(os.getpid(), signal.SIGKILL)


def watch_worker(owner: socket.socket) -> None:
    # The worker holds the sole peer. Cancellation, normal close and SIGKILL all give EOF.
    try:
        owner.recv(1)
    finally:
        kill_group()


def main() -> None:
    if os.getsid(0) != os.getpid():
        raise RuntimeError("Local command guard requires its own session")
    with socket.socket(fileno=0) as owner:
        try:
            if owner.recv(1) != b"1" or select.select([owner], [], [], 0)[0]:
                return
            try:
                child = subprocess.Popen(sys.argv[1:], stdin=subprocess.DEVNULL)
            except OSError as exc:
                owner.sendall(f"{exc.errno or errno.EIO}\n".encode())
                return
            Thread(target=watch_worker, args=(owner,), daemon=True).start()
            # Startup errno (zero on successful exec), then the real command's exit status.
            owner.sendall(b"0\n")
            owner.sendall(f"{child.wait()}\n".encode())
        finally:
            # The guard is still the group leader, including when the command exited first.
            kill_group()


if __name__ == "__main__":
    main()
