"""Persist local worker/evaluator identities before provisioning their authority."""

from __future__ import annotations

import os
import platform
import secrets
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from filelock import FileLock
from pydantic import Field

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.workers import EngineCapabilities, EnrollWorker, WorkerData
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import ID, VersionedRecord, new_id
from research_swarm.runtime import (
    LocalRuntime,
    default_state_directory,
    private_state_directory,
    read_private,
    write_private,
)
from research_swarm.workers.local import LocalWorker


class EnrollmentState(VersionedRecord):
    initialized: bool = False
    worker_id: ID
    session_id: ID
    worker_principal_id: ID
    worker_credential_id: ID
    worker_token: str = Field(repr=False)
    evaluator_principal_id: ID
    evaluator_credential_id: ID
    evaluator_token: str = Field(repr=False)
    profile: EngineCapabilities
    machine_registry: str


@dataclass(frozen=True)
class LocalEnrollment:
    worker: LocalWorker
    evaluator: ResearchClient
    evaluator_id: str


def cpu_profile(*, cpu_slots: int, fallback_identity: str) -> EngineCapabilities:
    model = platform.processor()
    try:
        if sys.platform == "darwin":
            probe = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if probe.returncode == 0:
                model = probe.stdout.strip()
        elif sys.platform.startswith("linux"):
            with Path("/proc/cpuinfo").open(encoding="utf-8") as stream:
                for line in stream.read(65536).splitlines():
                    if line.partition(":")[0].strip() in {"model name", "Hardware"}:
                        model = line.partition(":")[2].strip()
                        break
    except (OSError, subprocess.SubprocessError):
        pass
    model = model or "unknown:" + fallback_identity
    fingerprint = record_digest(
        "research.python-cpu-profile",
        {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "cpu_model": model,
            "python": platform.python_version(),
            "implementation": sys.implementation.name,
            "cache_tag": sys.implementation.cache_tag,
            "admitted_cpu_slots": cpu_slots,
        },
    )
    return EngineCapabilities(
        name="python-cpu", backend="python", execution_fingerprint=fingerprint, cpu_slots=cpu_slots
    )


def ensure_local_enrollment(
    runtime: LocalRuntime,
    *,
    cpu_slots: int = 1,
    machine_registry: Path | None = None,
    principal: Principal | None = None,
) -> LocalEnrollment:
    if type(cpu_slots) is not int or not 1 <= cpu_slots <= (os.cpu_count() or 1):
        raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Requested CPU capacity is unavailable")
    operator = principal or runtime.principal()
    operator.require("worker.manage")
    operator.require("auth.manage")
    operator.require("read")
    root = runtime.root / "local-worker"
    registry = (
        (
            machine_registry
            or default_state_directory().parent / "autoresearch-swarm-machine" / "resources.sqlite"
        )
        .expanduser()
        .absolute()
    )
    with private_state_directory(root), FileLock(root / "enrollment.lock"):
        path = root / "enrollment.json"
        if path.exists():
            state = EnrollmentState.model_validate(
                strict_json_loads(read_private(root, path.name, limit=65536))
            )
            if state.machine_registry != str(registry) or state.profile != cpu_profile(
                cpu_slots=cpu_slots, fallback_identity=state.worker_id
            ):
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE,
                    "Local enrollment inputs changed; explicit recovery is required",
                )
        else:
            if (root / "execution").exists():
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED,
                    "Local enrollment is missing from existing execution state",
                )
            identifier = new_id()
            state = EnrollmentState(
                worker_id=identifier,
                session_id=new_id(),
                worker_principal_id=new_id(),
                worker_credential_id=new_id(),
                worker_token="rsw_" + secrets.token_urlsafe(32),
                evaluator_principal_id=new_id(),
                evaluator_credential_id=new_id(),
                evaluator_token="rsw_" + secrets.token_urlsafe(32),
                profile=cpu_profile(cpu_slots=cpu_slots, fallback_identity=identifier),
                machine_registry=str(registry),
            )
            write_private(path, canonical_bytes(state.model_dump(mode="json")))
        worker_principal = Principal(
            id=state.worker_principal_id,
            workspace_id=runtime.identity.workspace_id,
            scopes=frozenset({"read", "artifact.upload", "worker.claim", "worker.publish"}),
        )
        evaluator_principal = Principal(
            id=state.evaluator_principal_id,
            workspace_id=runtime.identity.workspace_id,
            scopes=frozenset({"read", "evaluation.publish"}),
        )
        if state.initialized and (
            runtime.credentials.resolve(state.worker_token) != worker_principal
            or runtime.credentials.resolve(state.evaluator_token) != evaluator_principal
        ):
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Local enrollment credentials differ"
            )
        runtime.credentials.install(
            operator,
            worker_principal,
            identifier=state.worker_credential_id,
            token=state.worker_token,
        )
        runtime.credentials.install(
            operator,
            evaluator_principal,
            identifier=state.evaluator_credential_id,
            token=state.evaluator_token,
        )
        client = ResearchClient(LocalTransport(runtime, lambda: operator))
        if not state.initialized:
            client.command(
                "worker.enroll",
                EnrollWorker(
                    worker_id=state.worker_id,
                    session_id=state.session_id,
                    principal_id=state.worker_principal_id,
                    name="Local CPU worker",
                    profiles=(state.profile,),
                    max_concurrency=cpu_slots,
                ),
                idempotency_key="local-enrollment:" + state.worker_id,
            )
        row = client.get("worker", state.worker_id)
        registered = WorkerData.model_validate(row["data"])
        with runtime.database.connect() as connection:
            authority = connection.execute(
                "SELECT controller_id,control_epoch FROM workspaces WHERE id=?",
                (runtime.identity.workspace_id,),
            ).fetchone()
        if (
            row["state"] != "active"
            or registered.binding.controller_id != authority["controller_id"]
            or registered.binding.control_epoch != authority["control_epoch"]
            or registered.binding.worker_session_id != state.session_id
            or registered.principal_id != state.worker_principal_id
            or registered.profiles != (state.profile,)
            or registered.max_concurrency != cpu_slots
        ):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Local worker enrollment is no longer current"
            )
        worker_client = ResearchClient(
            LocalTransport(runtime, lambda: runtime.credentials.resolve(state.worker_token))
        )
        evaluator = ResearchClient(
            LocalTransport(runtime, lambda: runtime.credentials.resolve(state.evaluator_token))
        )
        worker = LocalWorker(
            worker_client,
            registered,
            key=read_private(runtime.root, "dispatch.key", limit=32),
            root=root / "execution",
            machine_registry=registry,
        )
        if not state.initialized:
            state = state.model_copy(update={"initialized": True})
            write_private(path, canonical_bytes(state.model_dump(mode="json")))
        return LocalEnrollment(
            worker=worker, evaluator=evaluator, evaluator_id=state.evaluator_principal_id
        )
