"""JSON transport for one attempt on a preinstalled remote worker."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from research_swarm.swarm.backends.local import LocalBackend
from research_swarm.swarm.data_stage import hydrate, mount_readonly, stage_path
from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Result, Trial
from research_swarm.swarm.telemetry import observe_starts, workload_started

RESULT_PREFIX = "SWARM_RESULT "
START_PREFIX = "SWARM_STARTED "


@dataclass(frozen=True, kw_only=True)
class BootData:
    revision: str
    volume: str
    nvme: str
    mount_root: str
    threads: int = 16


async def _ssh_diagnostics(stream: asyncio.StreamReader, attempt_id: str) -> bytes:
    marker = f"\n{START_PREFIX}{attempt_id}\n".encode()
    tail = bytearray(b"\n")
    acknowledged = False
    while chunk := await stream.read(16384):
        tail.extend(chunk)
        if not acknowledged and marker in tail:
            workload_started(attempt_id)
            acknowledged = True
        del tail[:-4096]
    return bytes(tail)


async def command(
    argv: list[str],
    *,
    timeout: float,
    payload: bytes | None = None,
    started_attempt: str | None = None,
) -> str:
    process = await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.PIPE if payload is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(timeout):
            if started_attempt is None:
                stdout, stderr = await process.communicate(payload)
            else:
                assert process.stdin and process.stdout and process.stderr and payload is not None
                process.stdin.write(payload)
                process.stdin.close()
                stdout, stderr = await asyncio.gather(
                    process.stdout.read(), _ssh_diagnostics(process.stderr, started_attempt)
                )
                await process.wait()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    if process.returncode:
        message = stderr[-4096:].decode(errors="replace")
        raise RuntimeError(f"{argv[0]} exited with status {process.returncode}: {message}")
    return stdout.decode()


def request(spec: CampaignSpec, attempt: Attempt, task_dir: str, device: str) -> dict[str, Any]:
    return {
        "attempt_telemetry": True,
        "spec": {
            "campaign_id": spec.campaign_id,
            "command": list(spec.command),
            "task_dir": task_dir,
            "metric": spec.metric,
            "direction": spec.direction,
            "max_workers": 1,
            "max_trials": 1,
            "trial_timeout_seconds": spec.trial_timeout_seconds,
        },
        "attempt": asdict(attempt),
        "device": device,
    }


async def run(request_data: dict[str, Any]) -> None:
    mountpoint = None
    if request_data.get("data") is not None:
        data = BootData(**request_data["data"])
        campaign = request_data["spec"]["campaign_id"]
        local = await asyncio.to_thread(
            hydrate,
            Path(data.volume),
            Path(data.nvme),
            campaign,
            data.revision,
            threads=data.threads,
        )
        mountpoint = stage_path(Path(data.mount_root), campaign, data.revision)
        await asyncio.to_thread(mount_readonly, local, mountpoint)
    spec = CampaignSpec(**request_data["spec"])
    attempt_data = dict(request_data["attempt"])
    trial_data = dict(attempt_data.pop("trial"))
    trial_data["candidate"] = Candidate(**trial_data["candidate"])
    attempt = Attempt(trial=Trial(**trial_data), **attempt_data)

    def announce(attempt_id: str) -> None:
        print(START_PREFIX + attempt_id, file=sys.stderr, flush=True)

    with observe_starts(announce if request_data.get("acknowledge_start") else None):
        result = await LocalBackend(1, device=request_data["device"], data_dir=mountpoint).execute(
            spec, attempt
        )
    fields = asdict(result)
    del fields["attempt"]
    if not request_data.get("attempt_telemetry"):
        del fields["failure_class"]
    print(
        RESULT_PREFIX
        + json.dumps({"attempt_id": attempt.attempt_id, "result": fields}, allow_nan=False)
    )


def read_result(output: str, attempt: Attempt) -> Result:
    for line in reversed(output.splitlines()):
        # Sky prefixes each output line with its task name.
        _, found, body = line.partition(RESULT_PREFIX)
        if found:
            row = json.loads(body)
            if row["attempt_id"] != attempt.attempt_id:
                continue
            fields = row["result"]
            if fields["status"] not in {"ok", "failed", "lost"}:
                raise ValueError("unknown remote result status")
            fields["logs"] = tuple(fields.get("logs", ()))
            return Result(attempt=attempt, **fields)
    raise ValueError("remote output contains no result for this attempt")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path)
    args = parser.parse_args()
    payload = json.loads(args.request.read_bytes()) if args.request else json.load(sys.stdin)
    asyncio.run(run(payload))
