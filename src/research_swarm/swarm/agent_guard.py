"""Keep an advisory job's deadline independent of both its caller and its model."""

from __future__ import annotations

import json
import math
import os
import sys
from pathlib import Path
from typing import Literal, get_args

Role = Literal["proposer", "diagnoser", "reporter"]

from research_swarm.swarm import _process_guard


def record_outcome(heartbeat: Path, status: str) -> None:
    """Publish one complete terminal record even if the caller and guard race at expiry."""
    try:
        try:
            metadata = heartbeat.with_name("job.json").read_bytes()
        except FileNotFoundError:
            # Jobs started by an older launcher kept metadata only in the heartbeat.
            metadata = heartbeat.read_bytes()
        job = json.loads(metadata.decode("utf-8"))
        role, deadline = job["role"], job["deadline"]
        if status == "timed_out" and (
            role not in get_args(Role)
            or type(deadline) not in (int, float)
            or not math.isfinite(deadline)
        ):
            raise ValueError("invalid launch metadata")
        outcome = {"role": role, "deadline": deadline, "status": status}
    except (
        FileNotFoundError,
        ValueError,
        TypeError,
        KeyError,
        OverflowError,
        RecursionError,
    ) as exc:
        if status != "timed_out":
            raise
        # Expiry is known from the monotonic clock. The launch fields are unknown.
        outcome = {
            "role": None,
            "deadline": None,
            "status": status,
            "error": (
                "missing_launch_metadata"
                if isinstance(exc, FileNotFoundError)
                else "invalid_launch_metadata"
            ),
        }
    staging = heartbeat.with_name(f".outcome-{os.getpid()}.tmp")
    staging.write_text(json.dumps(outcome))
    try:
        os.link(staging, heartbeat.with_name("outcome.json"))
    except FileExistsError:
        pass
    finally:
        staging.unlink(missing_ok=True)


def supervise(heartbeat: Path, deadline: float) -> int:
    def publish(status: str, stdout: bytes, stderr: bytes) -> None:
        record_outcome(heartbeat, status)
        # The journal survives a caller that closed its output pipes.
        try:
            sys.stdout.buffer.write(stdout)
            sys.stdout.buffer.flush()
            sys.stderr.buffer.write(stderr)
            sys.stderr.buffer.flush()
        except BrokenPipeError:
            pass

    return _process_guard.supervise(
        [sys.executable, "-m", "research_swarm.swarm.agents", str(heartbeat), str(deadline)],
        deadline=deadline,
        report=publish,
    )


if __name__ == "__main__":
    sys.exit(supervise(Path(sys.argv[1]), float(sys.argv[2])))
