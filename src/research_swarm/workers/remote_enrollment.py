"""Explicit operator provisioning and resumable HTTP worker enrollment."""

from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Self, TypeVar

from filelock import FileLock
from pydantic import ConfigDict, Field, field_validator, model_validator

from research_swarm.client import LocalTransport, ResearchClient, controller_url
from research_swarm.control.worker_queries import WorkerRead
from research_swarm.control.workers import EnrollWorker, WorkerData
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import ID, Digest, Name, VersionedRecord, new_id
from research_swarm.domain.workers import EngineCapabilities
from research_swarm.engines.registration import EnvironmentRegistration, verify_registration
from research_swarm.engines.registry import engine_definition
from research_swarm.runtime import (
    default_state_directory,
    private_state_directory,
    read_private,
    write_private,
)
from research_swarm.state.identity import read_identity
from research_swarm.workers.clocks import NATIVE_CLOCK_PROFILES, native_clock
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.local_enrollment import cpu_profile

WORKER_SCOPES = frozenset(
    {"worker.query", "worker.claim", "worker.inputs", "worker.outputs", "worker.publish"}
)
MAX_ENROLLMENT_BYTES = 2 * 1024 * 1024
SecretRecord = TypeVar("SecretRecord", bound=VersionedRecord)


class WorkerDescription(VersionedRecord):
    worker_id: ID
    profile: EngineCapabilities
    clock_profile: Name
    environment_contracts: tuple[Digest, ...] | None = Field(
        default=None, min_length=1, max_length=128, exclude_if=lambda value: value is None
    )

    @property
    def max_concurrency(self) -> int:
        return 1 if self.profile.accelerator_count else self.profile.cpu_slots

    @model_validator(mode="after")
    def supported(self) -> Self:
        legacy = self.environment_contracts is None
        if self.clock_profile not in NATIVE_CLOCK_PROFILES or (
            legacy and (self.profile.name != "python-cpu" or self.profile.backend != "python")
        ):
            raise ValueError("Remote enrollment requires a supported CPU and native clock profile")
        if not legacy:
            definition = engine_definition(self.profile.name)
            if self.profile.backend != definition.framework or self.profile.accelerator_count != (
                0 if definition.device_backend == "cpu" else 1
            ):
                raise ValueError(
                    "Registered worker profile has inconsistent framework or device capacity"
                )
            if tuple(sorted(set(self.environment_contracts or ()))) != self.environment_contracts:
                raise ValueError("Worker environment contracts must be sorted and unique")
        return self


class RemoteWorkerBundle(VersionedRecord):
    model_config = ConfigDict(hide_input_in_errors=True)

    description: WorkerDescription
    controller_url: str = Field(max_length=2048)
    credential_id: ID
    token: str = Field(repr=False, pattern=r"^rsw_[A-Za-z0-9_-]{43}$")
    worker: WorkerData

    @field_validator("controller_url")
    @classmethod
    def destination(cls, value: str) -> str:
        return controller_url(value)

    @model_validator(mode="after")
    def matching_worker(self) -> Self:
        if (
            self.worker.binding.worker_id != self.description.worker_id
            or self.worker.profiles != (self.description.profile,)
            or self.worker.max_concurrency != self.description.max_concurrency
            or self.worker.environment_contracts != self.description.environment_contracts
            or self.worker.assignment_authority is None
            or not self.worker.project_ids
            or self.worker.clock_mode != "bounded-continuous-v1"
        ):
            raise ValueError("Remote bundle differs from its scoped calibrated worker")
        return self


class WorkerInstallation(VersionedRecord):
    model_config = ConfigDict(hide_input_in_errors=True)

    description: WorkerDescription
    machine_registry: str
    bundle: RemoteWorkerBundle | None = None
    initialized: bool = False
    journal_identity: ID | None = None
    registry_identity: ID | None = None
    registration: EnvironmentRegistration | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def matching_state(self) -> Self:
        if self.bundle is not None and self.bundle.description != self.description:
            raise ValueError("Worker installation description differs from its bundle")
        if self.initialized and (
            self.bundle is None or self.journal_identity is None or self.registry_identity is None
        ):
            raise ValueError("Initialized worker lacks its persistent authority or storage pins")
        if self.registration is not None and (
            self.registration.profile != self.description.profile
            or self.registration.environment_contracts != self.description.environment_contracts
        ):
            raise ValueError("Private registration differs from worker description")
        if self.registration is None and self.description.environment_contracts is not None:
            raise ValueError("Registered worker is missing its private environment binding")
        return self


class ProvisioningState(VersionedRecord):
    model_config = ConfigDict(hide_input_in_errors=True)

    description: WorkerDescription
    controller_url: str
    workspace_id: ID
    controller_id: ID
    control_epoch: int = Field(strict=True, ge=0)
    name: Name
    project_ids: tuple[ID, ...] = Field(min_length=1, max_length=128)
    session_id: ID
    principal_id: ID
    credential_id: ID
    token: str = Field(repr=False)
    worker: WorkerData | None = None


@dataclass(frozen=True)
class RemoteEnrollment:
    worker: LocalWorker
    bundle: RemoteWorkerBundle

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.worker.client.close()


def _read_secret(path: Path, model: type[SecretRecord]) -> SecretRecord:
    try:
        return model.model_validate(
            strict_json_loads(read_private(path.parent, path.name, limit=MAX_ENROLLMENT_BYTES))
        )
    except ValueError:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Private enrollment record is invalid"
        ) from None


def _installation(root: Path) -> WorkerInstallation:
    return _read_secret(root / "installation.json", WorkerInstallation)


def _write_installation(root: Path, state: WorkerInstallation) -> None:
    write_private(root / "installation.json", canonical_bytes(state.model_dump(mode="json")))


def _verify_machine(state: WorkerInstallation) -> None:
    description = state.description
    if (
        state.registration is None
        and description.profile
        != cpu_profile(
            cpu_slots=description.profile.cpu_slots, fallback_identity=description.worker_id
        )
    ) or description.clock_profile != native_clock().profile:
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Enrolled machine profile changed")
    if description.profile.cpu_slots > (os.cpu_count() or 1):
        raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Enrolled CPU capacity is unavailable")


def describe_worker(
    root: Path,
    *,
    cpu_slots: int = 1,
    machine_registry: Path | None = None,
    registration: EnvironmentRegistration | None = None,
) -> WorkerDescription:
    """Persist a machine request before emitting its non-secret provisioning description."""
    if type(cpu_slots) is not int or not 1 <= cpu_slots <= (os.cpu_count() or 1):
        raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Requested CPU capacity is unavailable")
    if registration is not None and registration.cpu_slots != cpu_slots:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "CPU capacity differs from registration"
        )
    root = root.expanduser().absolute()
    registry = (
        (
            machine_registry
            or default_state_directory().parent / "autoresearch-swarm-machine" / "resources.sqlite"
        )
        .expanduser()
        .absolute()
    )
    with private_state_directory(root), FileLock(root / "enrollment.lock"):
        if (root / "installation.json").exists():
            state = _installation(root)
            if (
                state.machine_registry != str(registry)
                or state.description.profile.cpu_slots != cpu_slots
                or state.registration != registration
            ):
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Worker description inputs changed"
                )
            _verify_machine(state)
            return state.description
        if (root / "execution").exists():
            raise ResearchError(ErrorCode.EXECUTION_UNKNOWN, "Worker installation is missing")
        identifier = new_id()
        if registration is not None:
            verify_registration(registration)
        state = WorkerInstallation(
            description=WorkerDescription(
                worker_id=identifier,
                profile=registration.profile
                if registration
                else cpu_profile(cpu_slots=cpu_slots, fallback_identity=identifier),
                clock_profile=native_clock().profile,
                environment_contracts=registration.environment_contracts if registration else None,
            ),
            machine_registry=str(registry),
            registration=registration,
        )
        _write_installation(root, state)
        return state.description


def provision_worker(
    client: ResearchClient,
    description: WorkerDescription,
    *,
    url: str,
    project_ids: tuple[str, ...],
    destination: Path,
    name: str = "Remote CPU worker",
) -> RemoteWorkerBundle:
    """Operator-local provisioning; exporting a bundle never sends it to another host."""
    if not isinstance(client.transport, LocalTransport):
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Provisioning requires local operator authority"
        )
    runtime, operator = client.transport.runtime, client.transport.resolve_principal()
    operator.require("worker.manage")
    operator.require("auth.manage")
    operator.require("read")
    description = WorkerDescription.model_validate(description)
    url = controller_url(url)
    projects = tuple(sorted(project_ids))
    if not projects or len(set(projects)) != len(projects):
        raise ValueError("Provisioning requires distinct admitted projects")
    for identifier in projects:
        client.get("project", identifier)
    root = runtime.root / "remote-workers" / description.worker_id
    with private_state_directory(root), FileLock(root / "provision.lock"):
        with runtime.database.connect() as connection:
            authority = connection.execute(
                "SELECT controller_id,control_epoch FROM workspaces WHERE id=?",
                (runtime.identity.workspace_id,),
            ).fetchone()
        expected = {
            "description": description.model_dump(mode="json"),
            "controller_url": url,
            "workspace_id": runtime.identity.workspace_id,
            "controller_id": authority["controller_id"],
            "control_epoch": authority["control_epoch"],
            "name": name,
            "project_ids": list(projects),
        }
        path = root / "provision.json"
        if path.exists():
            state = _read_secret(path, ProvisioningState)
            if state.model_dump(mode="json", include=set(expected)) != expected:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Provisioning authority or inputs changed"
                )
        else:
            state = ProvisioningState(
                **expected,
                session_id=new_id(),
                principal_id=new_id(),
                credential_id=new_id(),
                token="rsw_" + secrets.token_urlsafe(32),
            )
            write_private(path, canonical_bytes(state.model_dump(mode="json")))
        principal = Principal(
            id=state.principal_id, workspace_id=state.workspace_id, scopes=WORKER_SCOPES
        )
        if state.worker is None:
            runtime.credentials.install(
                operator, principal, identifier=state.credential_id, token=state.token
            )
        elif runtime.credentials.resolve(state.token) != principal:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Provisioned credential authority changed"
            )
        enrolled = client.command(
            "worker.enroll",
            EnrollWorker(
                worker_id=description.worker_id,
                session_id=state.session_id,
                principal_id=state.principal_id,
                name=state.name,
                profiles=(description.profile,),
                max_concurrency=description.max_concurrency,
                environment_contracts=description.environment_contracts,
                project_ids=projects,
                clock_mode="bounded-continuous-v1",
            ),
            idempotency_key="remote-enrollment:" + description.worker_id,
        )["result"]
        registered = WorkerData.model_validate(enrolled["data"])
        live = client.get("worker", description.worker_id)
        if (
            live["state"] != "active"
            or WorkerData.model_validate(live["data"]) != registered
            or registered.binding.control_epoch != state.control_epoch
            or registered.binding.controller_id != state.controller_id
            or (state.worker is not None and state.worker != registered)
        ):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Provisioned enrollment is no longer current"
            )
        bundle = RemoteWorkerBundle(
            description=description,
            controller_url=url,
            credential_id=state.credential_id,
            token=state.token,
            worker=registered,
        )
        state = state.model_copy(update={"worker": registered})
        write_private(path, canonical_bytes(state.model_dump(mode="json")))
        destination = destination.expanduser().absolute()
        with private_state_directory(destination.parent), FileLock(str(destination) + ".lock"):
            if destination.exists():
                existing = _read_secret(destination, RemoteWorkerBundle)
                if existing != bundle:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT,
                        "Bundle destination contains different authority",
                    )
            else:
                write_private(destination, canonical_bytes(bundle.model_dump(mode="json")))
        return bundle


def connect_worker(root: Path, *, bundle_path: Path | None = None) -> RemoteEnrollment:
    """Resume a pinned worker through HTTP without opening a local controller runtime."""
    root = root.expanduser().absolute()
    with private_state_directory(root), FileLock(root / "enrollment.lock"):
        state = _installation(root)
        _verify_machine(state)
        if bundle_path is not None:
            bundle_path = bundle_path.expanduser().absolute()
            incoming = _read_secret(bundle_path, RemoteWorkerBundle)
            if incoming.description != state.description or (
                state.bundle is not None and state.bundle != incoming
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Worker bundle changes pinned authority")
            if state.bundle is None:
                if (root / "execution").exists():
                    raise ResearchError(
                        ErrorCode.EXECUTION_UNKNOWN, "Unbound native worker state exists"
                    )
                state = state.model_copy(update={"bundle": incoming})
                _write_installation(root, state)
        if state.bundle is None:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Worker has no provisioned connection bundle"
            )
        bundle = state.bundle
        registry = Path(state.machine_registry)
        if state.initialized and (
            read_identity(root / "execution" / "journal.sqlite") != state.journal_identity
            or read_identity(registry) != state.registry_identity
        ):
            raise ResearchError(
                ErrorCode.EXECUTION_UNKNOWN,
                "Native storage identity changed; explicit recovery is required",
            )
        client = ResearchClient.connect(bundle.controller_url, bundle.token)
        try:
            binding = bundle.worker.binding
            row = client.worker_get(
                WorkerRead(
                    worker_id=binding.worker_id,
                    session_id=binding.worker_session_id,
                    kind="worker",
                    id=binding.worker_id,
                )
            )
            if row["state"] != "active" or WorkerData.model_validate(row["data"]) != bundle.worker:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Remote enrollment differs from pinned authority"
                )
            worker = LocalWorker(
                client,
                bundle.worker,
                root=root / "execution",
                machine_registry=registry,
                registration=state.registration,
            )
            if state.initialized and (
                worker.journal.identity != state.journal_identity
                or worker.resources.identity != state.registry_identity
            ):
                raise ResearchError(
                    ErrorCode.EXECUTION_UNKNOWN, "Native storage changed during connection"
                )
            if not state.initialized:
                state = state.model_copy(
                    update={
                        "initialized": True,
                        "journal_identity": worker.journal.identity,
                        "registry_identity": worker.resources.identity,
                    }
                )
                _write_installation(root, state)
            return RemoteEnrollment(worker, bundle)
        except BaseException:
            client.close()
            raise
