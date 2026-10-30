"""Preserve unresolved native probe ownership across callers and process restarts."""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from filelock import FileLock
from pydantic import Field

from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Name, VersionedRecord, new_id
from research_swarm.runtime import (
    default_state_directory,
    private_state_directory,
    read_private,
    write_private,
)
from research_swarm.workers.contracts import NativeIdentity, NativeOutcome


def default_probe_directory() -> Path:
    return default_state_directory().parent / "autoresearch-swarm-engine-probes"


class ProbeRecord(VersionedRecord):
    id: ID = Field(default_factory=new_id)
    profile: Name
    native: NativeIdentity | None = None
    outcome: NativeOutcome | None = None


class ProbeCustody:
    def __init__(self, root: Path, record: ProbeRecord) -> None:
        self.root, self.record = root, record
        self.work = root / record.id

    def _write(self) -> None:
        write_private(
            self.root / "pending.json", canonical_bytes(self.record.model_dump(mode="json"))
        )

    def held(self, identity: NativeIdentity) -> bool:
        self.record = self.record.model_copy(update={"native": identity})
        self._write()
        return True

    def observed(self, outcome: NativeOutcome) -> None:
        self.record = self.record.model_copy(update={"outcome": outcome})
        self._write()
        if not self.closed:
            raise self.unknown()

    @property
    def closed(self) -> bool:
        outcome = self.record.outcome
        return bool(
            outcome is not None
            and outcome.closure.scope_closed
            and self.record.native == outcome.closure.identity
        )

    def unknown(self) -> ResearchError:
        return ResearchError(
            ErrorCode.EXECUTION_UNKNOWN,
            "Probe lacks native closure; inspect its private custody record before recovery",
            details={"probe_id": self.record.id},
        )

    def cleanup(self) -> None:
        # The record's UUID is the only permitted child path. A closed receipt remains
        # until cleanup succeeds, so interrupted removal is retryable without relaunch.
        if self.work.exists():
            shutil.rmtree(self.work)
        (self.root / "pending.json").unlink()


@contextmanager
def probe_custody(profile: str, directory: Path | None) -> Iterator[ProbeCustody]:
    root = (directory or default_probe_directory()).expanduser().absolute()
    with private_state_directory(root), FileLock(root / "probe.lock"):
        pending = root / "pending.json"
        if pending.exists():
            try:
                prior = ProbeCustody(
                    root,
                    ProbeRecord.model_validate(
                        strict_json_loads(read_private(root, pending.name, limit=1024 * 1024))
                    ),
                )
            except ValueError:
                raise ResearchError(
                    ErrorCode.EXECUTION_UNKNOWN, "Private probe custody is invalid"
                ) from None
            if not prior.closed:
                raise prior.unknown()
            prior.cleanup()
        current = ProbeCustody(root, ProbeRecord(profile=profile))
        current._write()
        current.work.mkdir(mode=0o700)
        try:
            yield current
        finally:
            if current.closed:
                current.cleanup()
