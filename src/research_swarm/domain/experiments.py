"""Immutable research payloads and their separately issued dispatch authority."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import Field, JsonValue, model_validator

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.types import (
    ID,
    Amount,
    ByteCount,
    Digest,
    Dimension,
    Milliseconds,
    Name,
    Revision,
    StringSet,
    Timestamp,
    VersionedRecord,
    new_id,
)


class Direction(StrEnum):
    MINIMIZE = "minimize"
    MAXIMIZE = "maximize"


class MetricDefinition(VersionedRecord):
    name: Name
    unit: Name
    direction: Direction
    aggregation: Name = "evaluator_defined"


class Metric(VersionedRecord):
    name: Name
    value: float = Field(strict=True)
    unit: Name
    direction: Direction


class SourceRef(VersionedRecord):
    artifact_id: ID
    tree_digest: Digest
    base_revision: str | None = None


class ResourceRequirements(VersionedRecord):
    engine_profile: Name = "python-cpu"
    cpu_slots: int = Field(default=1, strict=True, gt=0)
    memory_bytes: ByteCount = 0
    scratch_bytes: ByteCount = 0
    accelerator_count: int = Field(default=0, strict=True, ge=0)
    accelerator_memory_bytes: ByteCount = 0
    exclusive_device: bool = True
    required_features: StringSet = frozenset()
    isolation: Literal["trusted_local", "restricted_native", "container", "strong"] = (
        "trusted_local"
    )


class StageSpec(VersionedRecord):
    id: Name
    kind: Literal["prepare", "smoke", "train", "evaluate", "policy_train"]
    argv: tuple[str, ...] = Field(min_length=1)
    dependencies: tuple[Name, ...] = ()
    required_artifacts: tuple[Name, ...] = ()
    output_names: tuple[Name, ...] = ()
    timeout_ms: Milliseconds = 300_000
    resources: ResourceRequirements = Field(default_factory=ResourceRequirements)

    @model_validator(mode="after")
    def command_valid(self) -> StageSpec:
        if self.timeout_ms == 0 or any("\x00" in value for value in self.argv):
            raise ValueError("Command requires a positive timeout and NUL-free arguments")
        return self


class ExperimentSpec(VersionedRecord):
    workspace_id: ID
    project_id: ID
    campaign_id: ID
    candidate_id: ID
    trial_id: ID = Field(default_factory=new_id)
    source: SourceRef
    protocol_digest: Digest
    comparison_class: Name
    environment_digest: Digest
    evaluator_contract_digest: Digest
    data_manifest_digest: Digest
    metrics: tuple[MetricDefinition, ...] = Field(min_length=1)
    stages: tuple[StageSpec, ...] = Field(min_length=1)
    seed: int = Field(strict=True, ge=0, le=9_007_199_254_740_991)
    initialization_digest: Digest | None = None
    policy_version: Name
    mutation_allow: tuple[str, ...] = ()
    mutation_deny: tuple[str, ...] = ()
    maximum_cost: dict[Dimension, Amount] = Field(default_factory=dict)
    extensions: dict[str, JsonValue] = Field(default_factory=dict)
    spec_digest: Digest | None = None

    def payload(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", exclude={"spec_digest"}, warnings=False)

    def sealed(self) -> ExperimentSpec:
        validated = ExperimentSpec.model_validate(self)
        validated.validate_graph()
        digest = record_digest("research.experiment", validated.payload())
        if self.spec_digest is not None and digest != self.spec_digest:
            raise ValueError("Experiment specification digest mismatch")
        return validated.model_copy(update={"spec_digest": digest})

    def verify(self) -> None:
        if self.spec_digest is None:
            raise ValueError("Experiment specification is not sealed")
        self.sealed()

    def validate_graph(self) -> None:
        stages = {stage.id: stage for stage in self.stages}
        if len(stages) != len(self.stages):
            raise ValueError("Duplicate stage ID")
        if len({metric.name for metric in self.metrics}) != len(self.metrics):
            raise ValueError("Duplicate metric name")
        visiting: set[str] = set()
        complete: set[str] = set()

        def visit(name: str) -> None:
            if name in visiting:
                raise ValueError("Stage graph contains a cycle")
            if name in complete:
                return
            if name not in stages:
                raise ValueError(f"Unknown stage dependency: {name}")
            visiting.add(name)
            for predecessor in stages[name].dependencies:
                visit(predecessor)
            visiting.remove(name)
            complete.add(name)

        for stage_id in stages:
            visit(stage_id)


class DispatchEnvelope(VersionedRecord):
    id: ID = Field(default_factory=new_id)
    workspace_id: ID
    controller_id: ID
    campaign_id: ID
    trial_id: ID
    job_id: ID
    attempt_id: ID
    worker_id: ID
    worker_session_id: ID
    spec_digest: Digest
    control_epoch: Revision
    fencing_generation: Revision
    reservation_ids: tuple[ID, ...]
    stage_id: Name
    expires_at: Timestamp
    hard_deadline_at: Timestamp
    launch_permit_digest: Digest
