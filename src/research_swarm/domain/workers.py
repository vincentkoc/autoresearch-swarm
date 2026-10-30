"""Worker identity and admitted engine capabilities, independent of execution transport."""

from pydantic import Field

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
    features: StringSet = frozenset()
    isolation: StringSet = frozenset({"trusted_local"})
    network_policies: StringSet = frozenset({"inherit"})

    def require(self, resources: ResourceRequirements, network_policy: str) -> None:
        if (
            resources.engine_profile != self.name
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
