"""Public, path-free environment observations, distinct from workload qualification."""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, model_validator

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.types import ByteCount, Digest, Name, VersionedRecord

Observation = Annotated[
    str, StringConstraints(min_length=1, max_length=512, pattern=r"^[^\x00-\x1f]+$")
]
ProbeStatus = Literal[
    "available",
    "dependency_missing",
    "platform_unsupported",
    "backend_unavailable",
    "probe_failed",
    "timeout",
    "output_limit",
]


class EngineDefinition(VersionedRecord):
    name: Name
    framework: Literal["python", "pytorch", "mlx"]
    device_backend: Literal["cpu", "cuda", "rocm", "mps", "metal"]
    import_name: Name | None = None
    qualification: Literal["environment_probe"] = "environment_probe"


class PackageVersion(VersionedRecord):
    name: Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9.-]{0,127}$")]
    version: Observation


class DeviceObservation(VersionedRecord):
    index: int = Field(strict=True, ge=0)
    name: Observation
    architecture: Observation | None = None
    physical_id: Observation | None = None
    memory_bytes: ByteCount = 0
    unified_memory: bool = False


class EngineDescriptor(VersionedRecord):
    profile: Name
    framework: Literal["python", "pytorch", "mlx"]
    device_backend: Literal["cpu", "cuda", "rocm", "mps", "metal"]
    system: Observation
    release: Observation
    machine: Observation
    cpu_model: Observation
    cpu_count: int = Field(strict=True, ge=1)
    python_version: Observation
    python_implementation: Observation
    python_cache_tag: Observation
    packages: tuple[PackageVersion, ...] = Field(max_length=4096)
    framework_version: Observation | None = None
    runtime_version: Observation | None = None
    driver_version: Observation | None = None
    device: DeviceObservation | None = None

    @model_validator(mode="after")
    def consistent(self) -> Self:
        from research_swarm.engines.registry import engine_definition

        definition = engine_definition(self.profile)
        if (self.framework, self.device_backend) != (
            definition.framework,
            definition.device_backend,
        ):
            raise ValueError("Observed framework/backend differs from the selected profile")
        if (self.device_backend == "cpu") != (self.device is None):
            raise ValueError("Accelerator profiles require a selected device")
        if self.framework != "python" and self.framework_version is None:
            raise ValueError("Framework build version is absent")
        keys = [(package.name, package.version) for package in self.packages]
        if keys != sorted(set(keys)):
            raise ValueError("Package inventory must be sorted and unique")
        if len({package.name for package in self.packages}) != len(self.packages):
            raise ValueError("Ambiguous installed package versions")
        return self

    @property
    def fingerprint(self) -> str:
        return record_digest("research.engine-descriptor.v1", self.model_dump(mode="json"))


class ProbeReport(VersionedRecord):
    profile: Name
    status: ProbeStatus
    descriptor: EngineDescriptor | None = None
    descriptor_digest: Digest | None = None
    qualification: Literal["environment_probe"] = "environment_probe"

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if self.status == "available":
            if (
                self.descriptor is None
                or self.descriptor.profile != self.profile
                or self.descriptor_digest != self.descriptor.fingerprint
            ):
                raise ValueError("Successful probe requires an exact descriptor")
        elif self.descriptor is not None or self.descriptor_digest is not None:
            raise ValueError("Failed probe cannot advertise an available environment")
        return self
