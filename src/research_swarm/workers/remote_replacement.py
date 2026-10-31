"""Resumable replacement provisioning and distinct native installation roots."""

from __future__ import annotations

import secrets
from pathlib import Path

from filelock import FileLock, Timeout
from pydantic import ConfigDict, Field

from research_swarm.client import LocalTransport, ResearchClient, controller_url
from research_swarm.control.worker_records import WorkerData
from research_swarm.control.worker_turnover import ReplaceWorkerSession, WorkerFence
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import ID, Revision, new_id
from research_swarm.runtime import private_state_directory, write_private
from research_swarm.state.identity import read_identity
from research_swarm.workers.remote_enrollment import (
    WORKER_SCOPES,
    ProvisioningState,
    RemoteWorkerBundle,
    ReplacementOrigin,
    WorkerDescription,
    WorkerInstallation,
    _installation,
    _read_secret,
    _write_bundle,
    _write_installation,
    connect_worker,
)


class ReplacementProvisioningState(ProvisioningState):
    model_config = ConfigDict(hide_input_in_errors=True)

    request: ReplaceWorkerSession
    expected_revision: Revision
    command_key: ID = Field(default_factory=new_id)
    destination: str


def provision_replacement(
    client: ResearchClient,
    description: WorkerDescription,
    *,
    fence_id: str,
    expected_revision: int,
    reason: str,
    url: str,
    destination: Path,
) -> RemoteWorkerBundle:
    if not isinstance(client.transport, LocalTransport):
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Provisioning requires local operator authority"
        )
    runtime, operator = client.transport.runtime, client.transport.resolve_principal()
    for scope in ("worker.manage", "auth.manage", "read"):
        operator.require(scope)
    description = WorkerDescription.model_validate(description)
    fence_row = client.get("worker_fence", fence_id)
    fence = WorkerFence.model_validate(fence_row["data"])
    archived = client.get("worker_session", fence.binding.worker_session_id)
    old = WorkerData.model_validate(archived["data"])
    if (
        fence_row["state"] != "immutable"
        or archived["state"] != "immutable"
        or old.binding != fence.binding
        or old.binding.worker_id != description.worker_id
        or old.profiles != (description.profile,)
        or old.environment_contracts != description.environment_contracts
        or old.clock_admission != description.clock_admission
        or old.max_concurrency != description.max_concurrency
        or old.clock_mode != "bounded-continuous-v1"
        or not old.project_ids
        or expected_revision != fence.worker_revision + 1
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Replacement description or fence differs")
    url = controller_url(url)
    destination = destination.expanduser().absolute()
    base = runtime.root / "remote-workers" / description.worker_id
    root = base / "replacements" / fence_id
    with (
        private_state_directory(base),
        FileLock(base / "provision.lock"),
        private_state_directory(root),
        private_state_directory(destination.parent),
        FileLock(str(destination) + ".lock"),
    ):
        path = root / "provision.json"
        state = _read_secret(path, ReplacementProvisioningState) if path.exists() else None
        if state is not None and (
            state.description != description
            or state.controller_url != url
            or state.destination != str(destination)
            or state.expected_revision != expected_revision
            or state.request.binding != old.binding
            or state.request.fence_id != fence_id
            or state.request.reason != reason
        ):
            raise ResearchError(
                ErrorCode.IDEMPOTENCY_CONFLICT, "Replacement provisioning inputs changed"
            )
        # Reject conflicting exports before issuing credentials or changing authority.
        if destination.exists():
            existing = _read_secret(destination, RemoteWorkerBundle)
            if state is None or state.worker is None or existing != _bundle(state):
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT,
                    "Replacement destination contains other authority",
                )
        if state is None:
            live = client.get("worker", description.worker_id)
            if (
                live["state"] != "fenced"
                or live["revision"] != expected_revision
                or WorkerData.model_validate(live["data"]) != old
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Original fenced enrollment changed")
            with runtime.database.connect() as connection:
                authority = connection.execute(
                    "SELECT controller_id,control_epoch FROM workspaces WHERE id=?",
                    (runtime.identity.workspace_id,),
                ).one()
            session_id, principal_id = new_id(), new_id()
            state = ReplacementProvisioningState(
                description=description,
                controller_url=url,
                workspace_id=runtime.identity.workspace_id,
                controller_id=authority["controller_id"],
                control_epoch=authority["control_epoch"],
                name=old.name,
                project_ids=old.project_ids,
                session_id=session_id,
                principal_id=principal_id,
                credential_id=new_id(),
                token="rsw_" + secrets.token_urlsafe(32),
                request=ReplaceWorkerSession(
                    binding=old.binding,
                    fence_id=fence_id,
                    expected_controller_id=authority["controller_id"],
                    expected_control_epoch=authority["control_epoch"],
                    session_id=session_id,
                    principal_id=principal_id,
                    reason=reason,
                ),
                expected_revision=expected_revision,
                destination=str(destination),
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
                ErrorCode.AUTHORIZATION_DENIED, "Replacement credential authority changed"
            )
        # Replay the persisted command before judging changed live state after a lost reply.
        result = client.command(
            "worker.replace_session",
            state.request,
            idempotency_key=state.command_key,
            expected_revision=state.expected_revision,
        )["result"]
        worker = WorkerData.model_validate(result["worker"]["data"])
        if state.worker is not None and state.worker != worker:
            raise ResearchError(ErrorCode.STALE_FENCE, "Replacement acknowledgment differs")
        state = state.model_copy(update={"worker": worker})
        write_private(path, canonical_bytes(state.model_dump(mode="json")))
        live = client.get("worker", description.worker_id)
        with runtime.database.connect() as connection:
            authority = connection.execute(
                "SELECT controller_id,control_epoch FROM workspaces WHERE id=?",
                (runtime.identity.workspace_id,),
            ).one()
        if (
            live["state"] != "active"
            or WorkerData.model_validate(live["data"]) != worker
            or worker.binding.controller_id != authority["controller_id"]
            or worker.binding.control_epoch != authority["control_epoch"]
        ):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Provisioned replacement is no longer current"
            )
        if runtime.credentials.resolve(state.token) != principal:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Replacement credential authority changed"
            )
        bundle = _bundle(state)
        _write_bundle(bundle, destination)
        return bundle


def _bundle(state: ReplacementProvisioningState) -> RemoteWorkerBundle:
    assert state.worker is not None
    return RemoteWorkerBundle(
        description=state.description,
        controller_url=state.controller_url,
        credential_id=state.credential_id,
        token=state.token,
        worker=state.worker,
        replaces=state.request.binding,
    )


def install_replacement(
    original_root: Path, replacement_root: Path, bundle_path: Path
) -> WorkerData:
    original_root = original_root.expanduser().absolute()
    replacement_root = replacement_root.expanduser().absolute()
    original_real, replacement_real = original_root.resolve(), replacement_root.resolve()
    if (
        original_real == replacement_real
        or original_real in replacement_real.parents
        or replacement_real in original_real.parents
        or replacement_root.is_symlink()
    ):
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID,
            "Replacement requires distinct non-nested installation roots",
        )
    # Resolve the source before creating a replacement directory or accepting credentials.
    _installation(original_root)
    try:
        with FileLock(original_root / "enrollment.lock", timeout=0):
            original = _installation(original_root)
            if not original.initialized or original.bundle is None:
                raise ResearchError(
                    ErrorCode.EXECUTION_UNKNOWN, "Original installation is incomplete"
                )
            assert original.journal_identity is not None and original.registry_identity is not None
            if (
                read_identity(original_root / "execution/journal.sqlite")
                != original.journal_identity
                or read_identity(Path(original.machine_registry)) != original.registry_identity
            ):
                raise ResearchError(ErrorCode.EXECUTION_UNKNOWN, "Original native custody differs")
            bundle = _read_secret(bundle_path.expanduser().absolute(), RemoteWorkerBundle)
            if (
                bundle.replaces != original.bundle.worker.binding
                or bundle.description != original.description
                or bundle.worker.principal_id == original.bundle.worker.principal_id
                or bundle.credential_id == original.bundle.credential_id
            ):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Bundle does not replace this original installation"
                )
            origin = ReplacementOrigin(
                original_worker=original.bundle.worker,
                installation_digest=record_digest(
                    "research.original-installation", original.model_dump(mode="json")
                ),
                journal_identity=original.journal_identity,
                registry_identity=original.registry_identity,
            )
            pending = WorkerInstallation(
                description=original.description,
                machine_registry=original.machine_registry,
                registration=original.registration,
                bundle=bundle,
                replacement=origin,
            )
            with (
                private_state_directory(replacement_root),
                FileLock(replacement_root / "replacement.lock", timeout=0),
            ):
                with FileLock(replacement_root / "enrollment.lock", timeout=0):
                    path = replacement_root / "installation.json"
                    if path.exists():
                        previous = _installation(replacement_root)
                        omitted = {"initialized", "journal_identity", "registry_identity"}
                        if previous.model_dump(exclude=omitted) != pending.model_dump(
                            exclude=omitted
                        ):
                            raise ResearchError(
                                ErrorCode.IDEMPOTENCY_CONFLICT,
                                "Replacement installation inputs changed",
                            )
                    else:
                        if (replacement_root / "execution").exists():
                            raise ResearchError(
                                ErrorCode.EXECUTION_UNKNOWN, "Replacement has unbound native state"
                            )
                        _write_installation(replacement_root, pending)
                with connect_worker(replacement_root) as connected:
                    return connected.bundle.worker
    except Timeout:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE, "Original or replacement installation is owned"
        ) from None
