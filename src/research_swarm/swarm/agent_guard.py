"""Keep an advisory job's deadline independent of both its caller and its model."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def record_outcome(heartbeat: Path, status: str) -> None:
    """Publish one complete terminal record even if the caller and guard race at expiry."""
    job = json.loads(heartbeat.read_text())
    outcome = {"role": job["role"], "deadline": job["deadline"], "status": status}
    staging = heartbeat.with_name(f".outcome-{os.getpid()}.tmp")
    staging.write_text(json.dumps(outcome))
    try:
        os.link(staging, heartbeat.with_name("outcome.json"))
    except FileExistsError:
        pass
    finally:
        staging.unlink(missing_ok=True)


def _expire(heartbeat: Path) -> int:
    try:
        record_outcome(heartbeat, "timed_out")
    finally:
        # Includes this guard, the provider and descendants that inherited this group.
        os.killpg(os.getpid(), signal.SIGKILL)
    return 1  # Unreachable after SIGKILL.


def supervise(heartbeat: Path, deadline: float) -> int:
    # The caller must have launched THIS process in its own new session.
    if os.getsid(0) != os.getpid():
        raise RuntimeError("Advisory guard requires its own session")
    if time.monotonic() >= deadline:
        return _expire(heartbeat)
    try:
        provider = subprocess.Popen(
            [sys.executable, "-m", "research_swarm.swarm.agents", str(heartbeat), str(deadline)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError:
        record_outcome(heartbeat, "failed")
        raise
    try:
        stdout, stderr = provider.communicate(timeout=max(0, deadline - time.monotonic()))
    except subprocess.TimeoutExpired:
        return _expire(heartbeat)
    if provider.returncode == -signal.SIGALRM:
        return _expire(heartbeat)

    record_outcome(heartbeat, "ok" if provider.returncode == 0 else "failed")
    # An orphaned provider can finish successfully even if its original output pipe closed.
    try:
        sys.stdout.buffer.write(stdout)
        sys.stdout.buffer.flush()
        sys.stderr.buffer.write(stderr)
        sys.stderr.buffer.flush()
    except BrokenPipeError:
        pass
    return int(provider.returncode != 0)


if __name__ == "__main__":
    sys.exit(supervise(Path(sys.argv[1]), float(sys.argv[2])))
