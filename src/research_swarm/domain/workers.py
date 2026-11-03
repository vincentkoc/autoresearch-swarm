"""Worker identity and admitted engine capabilities, independent of execution transport."""

from typing import Self

from pydantic import Field, model_validator

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import ResourceRequirements
from research_swarm.domain.types import (
    ID,
    ByteCount,
    Digest,
    Name,
    Revision,
    StringSet,
    VersionedRecord,
)


class WorkerBinding(VersionedRecord):
    workspace_id: ID
    controller_id: ID
    worker_id: ID
    worker_session_id: ID
    control_epoch: Revision


class EngineCapabilities(VersionedRecord):
    name: Name
    backend: Name
    execution_fingerprint: Digest
    cpu_slots: int = Field(strict=True, ge=1)
    memory_bytes: ByteCount = 0
    scratch_bytes: ByteCount = 0
    accelerator_count: int = Field(default=0, strict=True, ge=0)
    accelerator_memory_bytes: ByteCount = 0
    shared_execution_slots: int = Field(
        default=0, strict=True, ge=0, le=8, exclude_if=lambda value: value == 0
    )
    features: StringSet = frozenset()
    isolation: StringSet = frozenset({"trusted_local"})
    network_policies: StringSet = frozenset({"inherit"})

    @model_validator(mode="after")
    def shared_admission(self) -> Self:
        if self.shared_execution_slots and (
            self.name != "pytorch-mps"
            or self.backend != "pytorch"
            or self.accelerator_count != 1
            or "mps-shared-execution-slots" not in self.features
        ):
            raise ValueError("Shared execution slots require explicit MPS capability")
        return self

    def require(self, resources: ResourceRequirements, network_policy: str) -> None:
        registered_mismatch = "registered-python-v1" in self.features and (
            resources.accelerator_count != self.accelerator_count
            or self.accelerator_count > 1
            or (
                self.accelerator_count > 0
                and not resources.exclusive_device
                and not (
                    self.name == "pytorch-mps"
                    and self.shared_execution_slots > 0
                    and "mps-shared-execution-slots" in self.features
                    and "mps-shared-execution-slots" in resources.required_features
                )
            )
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
