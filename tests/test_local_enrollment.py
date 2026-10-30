from __future__ import annotations

import secrets
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.auth import CredentialStore
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime, read_private
from research_swarm.workers import local_enrollment
from research_swarm.workers.journal import WorkerJournal
from research_swarm.workers.local_enrollment import EnrollmentState, ensure_local_enrollment
from research_swarm.workers.resources import MachineResources


def stored(runtime: LocalRuntime) -> EnrollmentState:
    return EnrollmentState.model_validate_json(
        read_private(runtime.root / "local-worker", "enrollment.json", limit=65536)
    )


def test_concurrent_enrollment_and_restart_preserve_identities(tmp_path: Path) -> None:
    root, registry = tmp_path / "runtime", tmp_path / "machine" / "resources.sqlite"
    runtime = LocalRuntime.open(root)
    with ThreadPoolExecutor(max_workers=3) as pool:
        enrolled = list(
            pool.map(
                lambda _: ensure_local_enrollment(runtime, machine_registry=registry), range(3)
            )
        )
    restarted = ensure_local_enrollment(LocalRuntime.open(root), machine_registry=registry)
    assert all(item.worker.worker == restarted.worker.worker for item in enrolled)
    assert all(item.evaluator_id == restarted.evaluator_id for item in enrolled)
    state = stored(runtime)
    assert state.initialized
    assert state.worker_token not in repr(state) and state.evaluator_token not in repr(state)
    with runtime.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM credentials").fetchone()[0] == 3
        assert (
            connection.execute("SELECT COUNT(*) FROM records WHERE kind='worker'").fetchone()[0]
            == 1
        )
    worker = runtime.credentials.resolve(state.worker_token)
    evaluator = runtime.credentials.resolve(state.evaluator_token)
    assert worker.id != evaluator.id
    with pytest.raises(ResearchError):
        worker.require("evaluation.publish")
    with pytest.raises(ResearchError):
        evaluator.require("worker.claim")
    assert restarted.worker.resources.availability() == {"cpu": 1}


def test_interrupted_credential_install_resumes_persisted_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = LocalRuntime.open(tmp_path / "runtime")
    registry = tmp_path / "machine" / "resources.sqlite"
    install = CredentialStore.install
    calls = 0

    def interrupted(self: CredentialStore, *args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("Synthetic provisioning interruption")
        return install(self, *args, **kwargs)

    monkeypatch.setattr(CredentialStore, "install", interrupted)
    with pytest.raises(OSError, match="interruption"):
        ensure_local_enrollment(runtime, machine_registry=registry)
    pending = stored(runtime)
    assert not pending.initialized
    assert runtime.credentials.resolve(pending.worker_token).id == pending.worker_principal_id
    monkeypatch.setattr(CredentialStore, "install", install)
    ensure_local_enrollment(LocalRuntime.open(runtime.root), machine_registry=registry)
    resumed = stored(runtime)
    assert resumed.journal_identity is not None and resumed.registry_identity is not None
    assert resumed.model_dump(
        exclude={"initialized", "journal_identity", "registry_identity"}
    ) == pending.model_dump(exclude={"initialized", "journal_identity", "registry_identity"})


@pytest.mark.parametrize("kind", ["journal", "registry"])
@pytest.mark.parametrize("replace", [False, True])
def test_missing_or_replaced_native_storage_requires_recovery(
    tmp_path: Path, kind: str, replace: bool
) -> None:
    runtime = LocalRuntime.open(tmp_path / "runtime")
    registry = tmp_path / "machine" / "resources.sqlite"
    enrollment = ensure_local_enrollment(runtime, machine_registry=registry)
    path = enrollment.worker.journal.path if kind == "journal" else registry
    path.unlink()
    if replace:
        (WorkerJournal if kind == "journal" else MachineResources)(path)
    with pytest.raises(ResearchError) as failure:
        ensure_local_enrollment(runtime, machine_registry=registry)
    assert failure.value.code == ErrorCode.EXECUTION_UNKNOWN
    assert path.exists() is replace


@pytest.mark.parametrize("damage", ["revoked", "missing", "epoch", "enrollment"])
def test_enrollment_does_not_recreate_lost_or_revoked_authority(
    tmp_path: Path, damage: str
) -> None:
    runtime = LocalRuntime.open(tmp_path / "runtime")
    registry = tmp_path / "machine" / "resources.sqlite"
    enrollment = ensure_local_enrollment(runtime, machine_registry=registry)
    state = stored(runtime)
    if damage == "revoked":
        runtime.credentials.revoke(runtime.principal(), state.worker_credential_id)
        with pytest.raises(ResearchError):
            enrollment.worker.client.capabilities()
    elif damage == "missing":
        with runtime.database.connect() as connection:
            connection.execute("DELETE FROM credentials WHERE id=?", (state.worker_credential_id,))
    elif damage == "epoch":
        with runtime.database.connect() as connection:
            connection.execute("UPDATE workspaces SET control_epoch=control_epoch+1")
    else:
        (runtime.root / "local-worker" / "enrollment.json").unlink()
    with pytest.raises(ResearchError) as failure:
        ensure_local_enrollment(runtime, machine_registry=registry)
    assert failure.value.code in {ErrorCode.AUTHORIZATION_DENIED, ErrorCode.STALE_FENCE}
    if damage != "enrollment":
        assert stored(runtime) == state


def test_changed_profile_and_other_controller_cannot_reenroll_native_pool(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = LocalRuntime.open(tmp_path / "first")
    registry = tmp_path / "machine" / "resources.sqlite"
    enrolled = ensure_local_enrollment(runtime, machine_registry=registry)
    profile = enrolled.worker.profile
    monkeypatch.setattr(
        local_enrollment,
        "cpu_profile",
        lambda **_: profile.model_copy(update={"execution_fingerprint": "sha256:" + "a" * 64}),
    )
    with pytest.raises(ResearchError) as mismatch:
        ensure_local_enrollment(runtime, machine_registry=registry)
    assert mismatch.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    second = LocalRuntime.open(tmp_path / "second")
    with pytest.raises(ResearchError) as contention:
        ensure_local_enrollment(second, machine_registry=registry)
    assert contention.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert enrolled.worker.resources.availability() == {"cpu": 1}


def test_credential_install_is_atomic_and_never_rotates(tmp_path: Path) -> None:
    runtime = LocalRuntime.open(tmp_path / "runtime")
    issuer = runtime.principal()
    actor = Principal(id=new_id(), workspace_id=issuer.workspace_id, scopes=frozenset({"read"}))
    identifier, token = new_id(), "rsw_" + secrets.token_urlsafe(32)
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(
            pool.map(
                lambda _: runtime.credentials.install(
                    issuer, actor, identifier=identifier, token=token
                ),
                range(3),
            )
        )
    assert all(result == results[0] for result in results)
    for changed_id, changed_token, changed_actor in (
        (new_id(), token, actor),
        (identifier, "rsw_" + secrets.token_urlsafe(32), actor),
        (identifier, token, actor.model_copy(update={"scopes": frozenset({"*"})})),
    ):
        with pytest.raises(ResearchError):
            runtime.credentials.install(
                issuer, changed_actor, identifier=changed_id, token=changed_token
            )
    runtime.credentials.revoke(issuer, identifier)
    with pytest.raises(ResearchError):
        runtime.credentials.install(issuer, actor, identifier=identifier, token=token)
    with runtime.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM credentials").fetchone()[0] == 2
