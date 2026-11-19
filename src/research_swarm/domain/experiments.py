"""Stage, resource and metric contracts for registered projects."""

from __future__ import annotations

from enum import StrEnum
from graphlib import CycleError, TopologicalSorter
from typing import Literal

from pydantic import Field, model_validator

from research_swarm.domain.types import (
    ByteCount,
    Milliseconds,
    Name,
    StringSet,
    VersionedRecord,
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


def validate_stage_graph(stages: tuple[StageSpec, ...]) -> None:
    dependencies = {stage.id: stage.dependencies for stage in stages}
    if len(dependencies) != len(stages):
        raise ValueError("Duplicate stage ID")
    missing = {name for parents in dependencies.values() for name in parents} - dependencies.keys()
    if missing:
        raise ValueError(f"Unknown stage dependency: {sorted(missing)[0]}")
    try:
        TopologicalSorter(dependencies).prepare()
    except CycleError as error:
        raise ValueError("Stage graph contains a cycle") from error
