"""Inventory of the managed jobs this installation has requested."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from filelock import FileLock


def key(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


@dataclass
class Job:
    campaign: str
    trial: str
    attempt: str
    name: str
    job_id: int | None = None
    terminal: bool = False
    fallback: bool = False
    aws_regions: list[str] = field(default_factory=list)

    @property
    def labels(self) -> dict[str, str]:
        return {
            "swarm-managed": "true",
            "swarm-campaign": key(self.campaign)[:32],
            "swarm-job": self.name,
        }

    def match(self, queue: list[dict[str, Any]]) -> dict[str, Any] | None:
        rows = [
            row
            for row in queue
            if row.get("job_name") == self.name
            and (self.job_id is None or row.get("job_id") == self.job_id)
        ]
        if len(rows) > 1 or (
            rows and (type(rows[0].get("job_id")) is not int or rows[0]["job_id"] < 1)
        ):
            raise ValueError("ambiguous managed job identity")
        return rows[0] if rows else None


class Inventory:
    """Record a unique name before launch so GC can recover a lost CLI response."""

    def __init__(self, state: Path) -> None:
        self.root = state.resolve() / "sky-jobs"

    def path(self, job: Job) -> Path:
        return self.root / key(job.campaign) / key(job.trial) / (key(job.attempt) + ".json")

    def read(self, path: Path) -> Job:
        if path.is_symlink() or path.resolve() != path:
            raise ValueError("unexpected inventory path")
        job = Job(**json.loads(path.read_bytes()))
        if self.path(job) != path or not re.fullmatch(r"swarm-[0-9a-f]{24}", job.name):
            raise ValueError("invalid job ownership record")
        return job

    def jobs(self, campaign: str, trial: str | None = None) -> list[Job]:
        directory = self.root / key(campaign)
        paths = (
            directory.glob("*/*.json") if trial is None else (directory / key(trial)).glob("*.json")
        )
        return [self.read(path) for path in sorted(paths)]

    def reserve(
        self, campaign: str, trial: str, attempt: str, aws_regions: list[str]
    ) -> tuple[Job, bool]:
        new = Job(
            campaign, trial, attempt, f"swarm-{uuid.uuid4().hex[:24]}", aws_regions=aws_regions
        )
        path = self.path(new)
        path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(path) + ".lock"):
            if path.exists():
                return self.read(path), False
            self.save(new)
            return new, True

    def save(self, job: Job) -> None:
        path = self.path(job)
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as file:
            temp = Path(file.name)
            try:
                json.dump(asdict(job), file)
                file.flush()
                os.replace(temp, path)
            finally:
                temp.unlink(missing_ok=True)
