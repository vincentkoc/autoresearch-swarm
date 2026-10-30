"""Immutable operator bindings to observed environments, without package installation."""

from __future__ import annotations

import hashlib
import os
import stat
import sys
from pathlib import Path
from typing import Literal, Self

from filelock import FileLock
from pydantic import Field, field_validator, model_validator

from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, ByteCount, Digest, VersionedRecord, new_id
from research_swarm.domain.workers import EngineCapabilities
from research_swarm.engines.adapter import current_adapter_digest
from research_swarm.engines.custody import default_probe_directory
from research_swarm.engines.models import EngineDescriptor
from research_swarm.engines.probe import InterpreterIdentity, observe_engine
from research_swarm.projects.native_files import SourceReader
from research_swarm.projects.workload import absolute_path
from research_swarm.runtime import private_state_directory, read_private, write_private


class ExecutionPolicy(VersionedRecord):
    # Precision and compilation are declared experiment semantics, not tensor shims.
    precision: Literal["project_declared", "fp32", "fp16", "bf16"] = "project_declared"
    compilation: Literal["project_declared", "eager", "cold", "warm"] = "project_declared"
    cpu_fallback: Literal["forbid"] = "forbid"
    python_search: Literal["isolated_probe_project_launch"] = "isolated_probe_project_launch"


class EnvironmentRegistration(VersionedRecord):
    id: ID = Field(default_factory=new_id)
    registration_version: Literal["observed-environment-v1"] = "observed-environment-v1"
    executable: str
    probe_directory: str
    executable_digest: Digest
    launch_adapter_digest: Digest | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    interpreter: InterpreterIdentity
    descriptor: EngineDescriptor
    environment_contracts: tuple[Digest, ...] = Field(min_length=1, max_length=128)
    cpu_slots: int = Field(strict=True, ge=1)
    memory_bytes: ByteCount = 0
    scratch_bytes: ByteCount = 0
    policy: ExecutionPolicy = Field(default_factory=ExecutionPolicy)
    qualification: Literal["environment_probe"] = "environment_probe"
    dependency_provenance: Literal["installed_names_versions"] = "installed_names_versions"
    lock_qualification: Literal["operator_authorized_unverified"] = "operator_authorized_unverified"
    _paths = field_validator("executable", "probe_directory")(absolute_path)

    @model_validator(mode="after")
    def coherent(self) -> Self:
        if self.cpu_slots > self.descriptor.cpu_count:
            raise ValueError("Requested CPU capacity exceeds observed capacity")
        if list(self.environment_contracts) != sorted(set(self.environment_contracts)):
            raise ValueError("Environment contract identities must be sorted and unique")
        return self

    @property
    def profile(self) -> EngineCapabilities:
        device = self.descriptor.device
        fingerprint = record_digest(
            "research.registered-execution-profile.v1",
            {
                "descriptor": self.descriptor.model_dump(mode="json"),
                "executable_digest": self.executable_digest,
                "policy": self.policy.model_dump(mode="json"),
                "cpu_slots": self.cpu_slots,
                "memory_bytes": self.memory_bytes,
                "scratch_bytes": self.scratch_bytes,
                **(
                    {"launch_adapter_digest": self.launch_adapter_digest}
                    if self.launch_adapter_digest is not None
                    else {}
                ),
            },
        )
        return EngineCapabilities(
            name=self.descriptor.profile,
            backend=self.descriptor.framework,
            execution_fingerprint=fingerprint,
            cpu_slots=self.cpu_slots,
            memory_bytes=self.memory_bytes,
            scratch_bytes=self.scratch_bytes,
            features=frozenset(
                {
                    "registered-python-v1",
                    *({"shared-host-device-memory"} if device and device.unified_memory else set()),
                }
            )
            if self.launch_adapter_digest is not None
            else frozenset(),
            accelerator_count=1 if device else 0,
            # Unified memory belongs to the same admitted host pool, never a second pool.
            accelerator_memory_bytes=(
                self.memory_bytes if device.unified_memory else device.memory_bytes
            )
            if device
            else 0,
        )

    @property
    def device_pool(self) -> str | None:
        device = self.descriptor.device
        if device is None:
            return None
        if device.physical_id is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Physical device identity is unresolved"
            )
        # A machine-local key. Framework, environment, worker, capacity, and ordinals
        # must not split one physical device into several supposedly exclusive pools.
        identity = record_digest("research.physical-device.v1", {"physical_id": device.physical_id})
        return "device:" + identity.removeprefix("sha256:")

    def require_environment(self, environment_digest: str) -> None:
        if environment_digest not in self.environment_contracts:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Environment contract is not authorized"
            )

    def public_description(self) -> dict[str, object]:
        return {
            "id": self.id,
            "profile": self.profile.model_dump(mode="json"),
            "descriptor": self.descriptor.model_dump(mode="json"),
            "policy": self.policy.model_dump(mode="json"),
            "environment_contracts": list(self.environment_contracts),
            "qualification": self.qualification,
            "dependency_provenance": self.dependency_provenance,
            "lock_qualification": self.lock_qualification,
            "launch_adapter_digest": self.launch_adapter_digest,
            "device_identity_resolved": self.descriptor.device is None
            or self.descriptor.device.physical_id is not None,
        }


def executable_digest(path: Path) -> str:
    digest = hashlib.sha256()
    target = path.resolve(strict=True)
    with SourceReader(target.parent) as reader, reader.open(target.name) as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > 1024 * 1024 * 1024:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Interpreter binary is not supported"
            )
        consumed = 0
        while chunk := stream.read(1024 * 1024):
            consumed += len(chunk)
            if consumed > 1024 * 1024 * 1024:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Interpreter binary exceeds its limit"
                )
            digest.update(chunk)
        after = os.fstat(stream.fileno())
    if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Interpreter changed while hashing")
    return "sha256:" + digest.hexdigest()


def read_registration(path: Path) -> EnvironmentRegistration:
    path = path.expanduser().absolute()
    try:
        return EnvironmentRegistration.model_validate(
            strict_json_loads(read_private(path.parent, path.name, limit=1024 * 1024))
        )
    except ValueError:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Private environment registration is invalid"
        ) from None


def verify_registration(registration: EnvironmentRegistration) -> None:
    registration = EnvironmentRegistration.model_validate(registration)
    path = Path(registration.executable)
    if executable_digest(path) != registration.executable_digest:
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Registered interpreter changed")
    observed = observe_engine(
        registration.descriptor.profile,
        executable=path,
        probe_directory=Path(registration.probe_directory),
        device_index=registration.descriptor.device.index if registration.descriptor.device else 0,
    )
    if (
        observed.report.descriptor != registration.descriptor
        or observed.identity != registration.interpreter
        or executable_digest(path) != registration.executable_digest
    ):
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Registered environment changed or is unavailable"
        )


def register_environment(
    destination: Path,
    *,
    profile: str,
    environment_contracts: tuple[str, ...],
    executable: Path | None = None,
    device_index: int = 0,
    cpu_slots: int = 1,
    memory_bytes: int = 0,
    scratch_bytes: int = 0,
    policy: ExecutionPolicy | None = None,
    probe_directory: Path | None = None,
) -> EnvironmentRegistration:
    destination = destination.expanduser().absolute()
    custody = (probe_directory or default_probe_directory()).expanduser().absolute()
    # Do not resolve this path: resolving a venv launcher can select the base environment.
    interpreter = (executable or Path(sys.executable)).expanduser().absolute()
    with private_state_directory(destination.parent), FileLock(str(destination) + ".lock"):
        if destination.exists():
            previous = read_registration(destination)
            if (
                previous.executable != str(interpreter)
                or previous.probe_directory != str(custody)
                or previous.descriptor.profile != profile
                or (previous.descriptor.device.index if previous.descriptor.device else 0)
                != device_index
                or previous.environment_contracts != tuple(sorted(set(environment_contracts)))
                or previous.cpu_slots != cpu_slots
                or previous.memory_bytes != memory_bytes
                or previous.scratch_bytes != scratch_bytes
                or previous.policy != (policy or ExecutionPolicy())
            ):
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Environment registration inputs changed"
                )
            verify_registration(previous)
            return previous
        digest = executable_digest(interpreter)
        observed = observe_engine(
            profile, executable=interpreter, device_index=device_index, probe_directory=custody
        )
        if observed.report.descriptor is None or observed.identity is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "Execution environment is unavailable",
                details={"probe_status": observed.report.status},
            )
        if executable_digest(interpreter) != digest:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Interpreter changed during probe"
            )
        registration = EnvironmentRegistration(
            executable=str(interpreter),
            probe_directory=str(custody),
            executable_digest=digest,
            launch_adapter_digest=current_adapter_digest(),
            interpreter=observed.identity,
            descriptor=observed.report.descriptor,
            environment_contracts=tuple(sorted(set(environment_contracts))),
            cpu_slots=cpu_slots,
            memory_bytes=memory_bytes,
            scratch_bytes=scratch_bytes,
            policy=policy or ExecutionPolicy(),
        )
        write_private(destination, canonical_bytes(registration.model_dump(mode="json")))
        return registration
