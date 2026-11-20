"""Portable wire records used by the campaign, project and engine interfaces."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from graphlib import CycleError, TopologicalSorter
from typing import Annotated, Any, Literal
from uuid import uuid4

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    StringConstraints,
    model_validator,
)

from research_swarm.swarm.canonical import canonical_bytes
from research_swarm.swarm.errors import ErrorCode, ResearchError

JsonObject = dict[str, Any]

ID = Annotated[
    str,
    StringConstraints(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
]
Digest = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]
Name = Annotated[str, StringConstraints(min_length=1, max_length=128)]
MediaType = Annotated[
    str,
    StringConstraints(
        pattern=r"^[A-Za-z0-9!#$%&'*+.^_`|~-]+/[A-Za-z0-9!#$%&'*+.^_`|~-]+$",
        min_length=3,
        max_length=128,
    ),
]
Milliseconds = Annotated[int, Field(strict=True, ge=0)]
ByteCount = Annotated[int, Field(strict=True, ge=0)]
SCHEMA_VERSION = "1.0"


def valid_timestamp(value: str) -> str:
    datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    return value


Timestamp = Annotated[
    str,
    StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"),
    AfterValidator(valid_timestamp),
]


def new_id() -> str:
    return str(uuid4())


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def stable_strings(values: frozenset[str]) -> list[str]:
    return sorted(values, key=canonical_bytes)


StringSet = Annotated[
    frozenset[str],
    PlainSerializer(stable_strings, return_type=list[str], when_used="json"),
    Field(json_schema_extra={"x-ordering": "canonical-json-bytes", "uniqueItems": True}),
]


class Record(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, allow_inf_nan=False, revalidate_instances="always"
    )


class VersionedRecord(Record):
    schema_version: str = Field(default=SCHEMA_VERSION, pattern=r"^1\.0$")


class ArtifactRef(VersionedRecord):
    id: ID
    digest: Digest
    size_bytes: ByteCount
    media_type: MediaType = "application/octet-stream"


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


class EngineCapabilities(VersionedRecord):
    name: Name
    backend: Name
    execution_fingerprint: Digest
    cpu_slots: int = Field(strict=True, ge=1)
    memory_bytes: ByteCount = 0
    scratch_bytes: ByteCount = 0
    accelerator_count: int = Field(default=0, strict=True, ge=0)
    accelerator_memory_bytes: ByteCount = 0
    features: StringSet = frozenset()
    isolation: StringSet = frozenset({"trusted_local"})
    network_policies: StringSet = frozenset({"inherit"})

    def require(self, resources: ResourceRequirements, network_policy: str) -> None:
        registered_mismatch = "registered-python-v1" in self.features and (
            resources.accelerator_count != self.accelerator_count
            or self.accelerator_count > 1
            or (self.accelerator_count > 0 and not resources.exclusive_device)
            or (
                "shared-host-device-memory" in self.features
                and resources.memory_bytes + resources.accelerator_memory_bytes > self.memory_bytes
            )
        )
        if (
            registered_mismatch
            or resources.engine_profile != self.name
            or resources.cpu_slots > self.cpu_slots
            or resources.memory_bytes > self.memory_bytes
            or resources.scratch_bytes > self.scratch_bytes
            or resources.accelerator_count > self.accelerator_count
            or resources.accelerator_memory_bytes > self.accelerator_memory_bytes
            or not resources.required_features <= self.features
            or resources.isolation not in self.isolation
            or network_policy not in self.network_policies
        ):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Worker profile cannot satisfy this stage"
            )
