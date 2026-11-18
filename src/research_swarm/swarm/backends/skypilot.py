"""One managed SkyPilot job per claimed attempt, using the versioned CLI contract.

The remote image or setup command installs the same swarm worker package. Sky
syncs task_dir into its workdir. For staged data, every eligible location exposes
its prepared volume at the configured path and supports worker-local bind mounts.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import shlex
import shutil
import tempfile
from contextlib import suppress
from dataclasses import asdict
from pathlib import Path
from typing import Any

from research_swarm.swarm.backends import remote
from research_swarm.swarm.backends.capacity import Slots
from research_swarm.swarm.backends.jobs import Inventory, Job
from research_swarm.swarm.backends.remote import BootData
from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Result

TERMINAL = {
    "SUCCEEDED",
    "CANCELLED",
    "FAILED",
    "FAILED_SETUP",
    "FAILED_PRECHECKS",
    "FAILED_NO_RESOURCE",
    "FAILED_CONTROLLER",
}


class SkyCLI:
    def __init__(self, sky: str | None = None) -> None:
        installed = sky or shutil.which("sky")
        self.argv = [installed] if installed else ["uvx", "--from", "skypilot==0.14.3", "sky"]

    async def call(self, *args: str, timeout: float = 60) -> str:
        return await remote.command([*self.argv, "jobs", *args], timeout=timeout)

    async def queue(self) -> list[dict[str, Any]]:
        rows = json.loads(await self.call("queue", "--all", "-o", "json"))
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError("expected a managed-job queue array")
        return rows

    async def cancel(self, job: Job) -> bool:
        row = job.match(await self.queue())
        if row is None:
            return False
        if row["status"] not in TERMINAL | {"CANCELLING"}:
            await self.call("cancel", "-y", str(row["job_id"]))
        return True


class SkyPilotBackend:
    def __init__(
        self,
        *,
        state: Path,
        placements: list[str],
        max_workers: int,
        accelerators: str | None = None,
        idle_minutes: int = 10,
        sky: str | None = None,
        poll_seconds: float = 5.0,
        startup_timeout_seconds: float = 1800.0,
        setup: str | None = None,
        data: BootData | None = None,
        python: str = "python3",
        device: str | None = None,
    ) -> None:
        if not placements or any(not place for place in placements):
            raise ValueError("specify SkyPilot cloud/region or cloud/region/zone placements")
        if idle_minutes < 1 or any(
            not math.isfinite(x) or x <= 0 for x in (poll_seconds, startup_timeout_seconds)
        ):
            raise ValueError("idle, poll and startup intervals must be positive")
        self.inventory = Inventory(state)
        self.cli = SkyCLI(sky)
        self.placements = []
        aws_regions: set[str] = set()
        for placement in placements:
            cloud, _, location = placement.partition("/")
            if not re.fullmatch(r"[A-Za-z][\w-]*", cloud):
                raise ValueError("specify a named cloud for each SkyPilot placement")
            if cloud.lower() == "aws":
                region = location.split("/", 1)[0]
                if not re.fullmatch(r"[a-z0-9-]+", region):
                    raise ValueError("specify an AWS region so campaign gc can find its resources")
                aws_regions.add(region)
                placement = f"aws/{location}"
            self.placements.append(placement)
        self.aws_regions = sorted(aws_regions)
        self.accelerators, self.setup, self.data = accelerators, setup, data
        self.device = device or ("cuda" if accelerators else "cpu")
        self.python = python
        self.idle_minutes, self.poll_seconds = idle_minutes, poll_seconds
        self.startup_timeout_seconds = startup_timeout_seconds
        self._slots = Slots(max_workers)

    def free_slots(self, spec: CampaignSpec) -> int:
        return self._slots.free(spec)

    async def _launch(self, spec: CampaignSpec, attempt: Attempt, job: Job, spot: bool) -> int:
        # any_of optimizes by price; keeping each tier separate enforces spot-first.
        resources: dict[str, Any] = {
            "any_of": [{"infra": place, "use_spot": spot} for place in self.placements],
            "labels": job.labels,
            "job_recovery": {"strategy": "EAGER_NEXT_REGION", "max_restarts_on_errors": 0},
        }
        if self.accelerators:
            resources["accelerators"] = self.accelerators
        with tempfile.TemporaryDirectory(prefix="swarm-sky-") as temp:
            request_path, task = Path(temp) / "request.json", Path(temp) / "task.yaml"
            payload = remote.request(spec, attempt, ".", self.device)
            payload["data"] = asdict(self.data) if self.data else None
            request_path.write_text(json.dumps(payload, allow_nan=False), encoding="utf-8")
            remote_path = f"/tmp/{job.name}.json"
            definition = {
                "name": job.name,
                "resources": resources,
                "workdir": str(Path(spec.task_dir).resolve(strict=True)),
                "file_mounts": {remote_path: str(request_path)},
                "run": shlex.join(
                    [
                        self.python,
                        "-m",
                        "research_swarm.swarm.backends.remote",
                        "--request",
                        remote_path,
                    ]
                ),
            }
            if self.setup is not None:
                definition["setup"] = self.setup
            task.write_text(json.dumps(definition), encoding="utf-8")  # JSON is valid YAML.
            response = json.loads(
                await self.cli.call(
                    "launch",
                    "-y",
                    "-o",
                    "json",
                    "--config",
                    f"jobs.controller.autostop.idle_minutes={self.idle_minutes}",
                    "--config",
                    "jobs.controller.autostop.down=false",
                    "--config",
                    "aws.enforce_tags=[volume]",
                    str(task),
                    timeout=self.startup_timeout_seconds,
                )
            )
        ids = response.get("job_ids")
        if not isinstance(ids, list) or len(ids) != 1 or type(ids[0]) is not int or ids[0] < 1:
            raise ValueError("launch did not return exactly one managed job ID")
        return int(ids[0])

    async def _observe(self, job: Job, attempt: Attempt) -> Result:
        cancelling = False
        while True:
            row = job.match(await self.cli.queue())
            if row:
                job.job_id = row["job_id"]
                status = row["status"]
                if status in TERMINAL:
                    job.terminal = True
                    job.fallback |= status in {"FAILED_NO_RESOURCE", "FAILED_CONTROLLER"}
                    self.inventory.save(job)
                    if status == "SUCCEEDED":
                        logs = await self.cli.call("logs", "--no-follow", str(job.job_id))
                        return remote.read_result(logs, attempt)
                    lost = job.fallback or status == "CANCELLED"
                    return Result(
                        attempt=attempt,
                        status="lost" if lost else "failed",
                        error=f"managed job {status}",
                    )
                if not cancelling and (status == "RECOVERING" or row.get("recovery_count", 0)):
                    # Fence Sky's retry before handing the trial back to the queue.
                    job.fallback = True
                    self.inventory.save(job)
                    cancelling = await self.cli.cancel(job)
            await asyncio.sleep(self.poll_seconds)

    async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
        async with self._slots.claim(spec):
            job: Job | None = None
            try:
                async with asyncio.timeout(
                    self.startup_timeout_seconds + spec.trial_timeout_seconds
                ):
                    prior = self.inventory.jobs(spec.campaign_id, attempt.trial.trial_id)
                    for old in prior:
                        if old.attempt == attempt.attempt_id or old.terminal:
                            continue
                        await self.cli.cancel(old)
                        while True:
                            row = old.match(await self.cli.queue())
                            if row and row["status"] in TERMINAL:
                                old.terminal = True
                                old.fallback |= row["status"] in {
                                    "FAILED_NO_RESOURCE",
                                    "FAILED_CONTROLLER",
                                }
                                self.inventory.save(old)
                                break
                            if row and row["status"] != "CANCELLING":
                                await self.cli.cancel(old)
                            await asyncio.sleep(self.poll_seconds)
                    job, fresh = self.inventory.reserve(
                        spec.campaign_id,
                        attempt.trial.trial_id,
                        attempt.attempt_id,
                        self.aws_regions,
                    )
                    if fresh:
                        job.job_id = await self._launch(
                            spec, attempt, job, spot=not any(j.fallback for j in prior)
                        )
                        self.inventory.save(job)
                    return await self._observe(job, attempt)
            except asyncio.CancelledError:
                if job is not None:
                    with suppress(OSError, RuntimeError, ValueError, TimeoutError):
                        await self.cli.cancel(job)
                raise
            except (OSError, RuntimeError, ValueError, TimeoutError) as error:
                if job is not None and not job.terminal:
                    # Its durable name remains available for reconciliation and GC.
                    with suppress(OSError, RuntimeError, ValueError, TimeoutError):
                        await self.cli.cancel(job)
                return Result(
                    attempt=attempt, status="lost", error=str(error) or type(error).__name__
                )


def from_spec(spec: CampaignSpec) -> SkyPilotBackend:
    options = dict(getattr(spec, "options", {}).get("backend", {}))
    data = options.pop("data", None)
    return SkyPilotBackend(
        state=Path(options.pop("state")),
        max_workers=options.pop("max_workers", spec.max_workers),
        data=BootData(**data) if data else None,
        **options,
    )
