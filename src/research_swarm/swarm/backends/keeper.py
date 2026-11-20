"""Host-side idle shutdown for an explicitly enrolled Linux SSH pool host.

Install the supplied research-swarm-keeper.service and enable it at boot. The
controller configures instance-initiated shutdown to stop before resuming a host.
The service and the remote runner share the activity file under /run.
"""

from __future__ import annotations

import argparse
import fcntl
import math
import os
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

from research_swarm.swarm.process import run as run_command

ACTIVITY = Path("/run/research-swarm/activity")


@contextmanager
def active(lockfile: Path) -> Iterator[None]:
    # Only the keeper creates the file. Kernel lock release handles process death.
    with lockfile.open("r+") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if stream.read().strip() == "stopping":
            raise RuntimeError("host is stopping")
        try:
            yield
        finally:
            os.utime(stream.fileno(), None)


def keep(lockfile: Path, idle_seconds: float, poweroff: Sequence[str]) -> None:
    if not math.isfinite(idle_seconds) or idle_seconds <= 0:
        raise ValueError("idle seconds must be positive")
    with lockfile.open("a+") as stream:
        os.fchmod(stream.fileno(), 0o660)
        last_used = time.monotonic()
        stamp = os.fstat(stream.fileno()).st_mtime_ns
        while True:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                last_used = time.monotonic()
            else:
                try:
                    current = os.fstat(stream.fileno()).st_mtime_ns
                    if current != stamp:
                        last_used, stamp = time.monotonic(), current
                    if time.monotonic() - last_used >= idle_seconds:
                        stream.seek(0)
                        stream.truncate()
                        stream.write("stopping")
                        stream.flush()
                        try:
                            run_command(poweroff, check=True)
                        except BaseException:
                            stream.seek(0)
                            stream.truncate()
                            raise
                        return
                finally:
                    fcntl.flock(stream, fcntl.LOCK_UN)
            time.sleep(min(1.0, idle_seconds / 4))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--activity", type=Path, default=ACTIVITY)
    parser.add_argument("--idle-seconds", type=float, default=1800)
    args = parser.parse_args()
    keep(args.activity, args.idle_seconds, ["systemctl", "poweroff"])
