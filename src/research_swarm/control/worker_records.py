"""Persisted worker and attempt records, independent of current admission authority."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from research_swarm.domain.assignments import AssignmentAuthority, SignedAssignment
from research_swarm.domain.clocks import ClockMode, SignedAssignmentTime
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import DispatchEnvelope
from research_swarm.domain.types import ID, Digest, Name, Timestamp, VersionedRecord, utc_now
from research_swarm.domain.workers import EngineCapabilities, WorkerBinding


class WorkerData(VersionedRecord):
    binding: WorkerBinding
    principal_id: ID
    name: Name
    profiles: tuple[EngineCapabilities, ...]
    max_concurrency: int
    assignment_authority: AssignmentAuthority | None = None
    project_ids: tuple[ID, ...] | None = None
    clock_mode: ClockMode = "local-utc-v1"
    environment_contracts: tuple[Digest, ...] | None = Field(
        default=None, min_length=1, max_length=128, exclude_if=lambda value: value is None
    )
    enrolled_at: Timestamp = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def matching_authority(self) -> WorkerData:
        if self.clock_mode == "bounded-continuous-v1" and self.assignment_authority is None:
            raise ValueError("Calibrated workers require signed assignment authority")
        if (
            self.assignment_authority is not None
            and self.assignment_authority.binding != self.binding
        ):
            raise ValueError("Worker signing authority differs from enrollment")
        return self


class AttemptData(VersionedRecord):
    dispatch: DispatchEnvelope
    profile: EngineCapabilities
    inputs: dict[Name, ArtifactRef]
    source_artifact: ArtifactRef
    source_tree_digest: Digest
    evaluation_request_id: ID | None = None
    signed_assignment: SignedAssignment | None = None
    assignment_time: SignedAssignmentTime | None = None
    outcome_artifact: ArtifactRef | None = None
    report_artifact: ArtifactRef | None = None
    result_digest: Digest | None = None
    publication_status: Literal["pending", "accepted"] = "pending"
    finding: str = ""
