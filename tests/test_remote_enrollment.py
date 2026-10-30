from __future__ import annotations

import json
import subprocess
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.control.auth import CredentialStore
from research_swarm.control.worker_queries import WorkerQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.runtime import LocalRuntime, read_private, write_private
from research_swarm.state.identity import read_identity
from research_swarm.workers import remote_enrollment
from research_swarm.workers.remote_enrollment import (
    WORKER_SCOPES,
    ProvisioningState,
    WorkerDescription,
    WorkerInstallation,
    connect_worker,
    describe_worker,
    provision_worker,
)
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline


def describe(tmp_path: Path) -> WorkerDescription:
    return describe_worker(
        tmp_path / "remote", machine_registry=tmp_path / "remote-machine" / "resources.sqlite"
    )


def provision(pipeline: Pipeline, description: WorkerDescription, tmp_path: Path, url: str) -> Any:
    return provision_worker(
        pipeline.client,
        description,
        url=url,
        project_ids=(pipeline.campaign["data"]["contract"]["project_id"],),
        destination=tmp_path / "bundles" / "worker.json",
    )


def installation(tmp_path: Path) -> WorkerInstallation:
    return WorkerInstallation.model_validate_json(
        read_private(tmp_path / "remote", "installation.json", limit=128 * 1024)
    )


def provisioning(pipeline: Pipeline, description: WorkerDescription) -> ProvisioningState:
    return ProvisioningState.model_validate_json(
        read_private(
            pipeline.runtime.root / "remote-workers" / description.worker_id,
            "provision.json",
            limit=128 * 1024,
        )
    )


def test_parallel_description_and_provisioning_reuse_one_scoped_identity(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    with ThreadPoolExecutor(max_workers=3) as pool:
        descriptions = list(pool.map(lambda _: describe(tmp_path), range(3)))
        bundles = list(
            pool.map(
                lambda _: provision(
                    pipeline, descriptions[0], tmp_path, "https://controller.example"
                ),
                range(3),
            )
        )
    assert all(item == descriptions[0] for item in descriptions)
    assert all(item == bundles[0] for item in bundles)
    bundle = bundles[0]
    assert pipeline.runtime.credentials.resolve(bundle.token).scopes == WORKER_SCOPES
    assert bundle.token not in repr(bundle)
    assert bundle.token not in repr(provisioning(pipeline, descriptions[0]))
    assert bundle.worker.clock_mode == "bounded-continuous-v1"
    with pipeline.runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM credentials WHERE id=?", (bundle.credential_id,)
            ).fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='worker' AND id=?",
                (descriptions[0].worker_id,),
            ).fetchone()[0]
            == 1
        )


def test_connected_http_worker_executes_and_resumes_without_local_controller_authority(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    description = describe(tmp_path)
    with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:
        bundle = provision(pipeline, description, tmp_path, url)

        def forbidden(*_: Any, **__: Any) -> Any:
            raise AssertionError("Remote worker opened a local controller")

        monkeypatch.setattr(LocalRuntime, "open", forbidden)
        with connect_worker(
            tmp_path / "remote", bundle_path=tmp_path / "bundles" / "worker.json"
        ) as enrolled:
            worker = enrolled.worker
            with pytest.raises(ResearchError) as denied:
                worker.client.get("trial", pipeline.trial["id"])
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
            principal = pipeline.runtime.credentials.resolve(bundle.token)
            for scope in ("evaluation.publish", "worker.manage", "auth.manage", "read", "*"):
                with pytest.raises(ResearchError):
                    principal.require(scope)
            offers = worker.client.worker_list(
                WorkerQuery(
                    worker_id=bundle.worker.binding.worker_id,
                    session_id=bundle.worker.binding.worker_session_id,
                    kind="offers",
                )
            )
            attempt = worker.claim(offers["items"][0])
            completed = worker.execute(attempt["id"])
            assert completed["state"] == "succeeded"
            native = worker.journal.get(attempt["id"])
            assert native.timing is not None and native.outcome is not None
        first = installation(tmp_path)
        assert first.initialized and bundle.token not in repr(first)
        with connect_worker(tmp_path / "remote") as resumed:
            assert resumed.worker.execute(attempt["id"]) == completed
            assert resumed.worker.journal.get(attempt["id"]) == native
        assert installation(tmp_path) == first
    assert not (tmp_path / "remote" / "operator.token").exists()
    assert not (tmp_path / "remote" / "controller.sqlite").exists()


@pytest.mark.parametrize("boundary", ["credential", "enroll", "bundle"])
def test_provisioning_interruption_keeps_original_token_session_and_enrollment(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    description = describe(tmp_path)
    with monkeypatch.context() as patch:
        if boundary == "credential":
            install = CredentialStore.install

            def installed_then_lost(self: CredentialStore, *args: Any, **kwargs: Any) -> Any:
                install(self, *args, **kwargs)
                raise OSError("Synthetic lost credential installation reply")

            patch.setattr(CredentialStore, "install", installed_then_lost)
        elif boundary == "enroll":
            command = pipeline.client.command

            def enrolled_then_lost(*args: Any, **kwargs: Any) -> Any:
                command(*args, **kwargs)
                raise OSError("Synthetic lost enrollment acknowledgment")

            patch.setattr(pipeline.client, "command", enrolled_then_lost)
        else:
            write = remote_enrollment.write_private

            def failed_export(path: Path, payload: bytes) -> None:
                if path.name == "worker.json":
                    raise OSError("Synthetic bundle export failure")
                write(path, payload)

            patch.setattr(remote_enrollment, "write_private", failed_export)
        with pytest.raises(OSError, match="Synthetic"):
            provision(pipeline, description, tmp_path, "https://controller.example")
    prior = provisioning(pipeline, description)
    bundle = provision(pipeline, description, tmp_path, "https://controller.example")
    assert bundle.token == prior.token and bundle.credential_id == prior.credential_id
    assert bundle.worker.binding.worker_session_id == prior.session_id
    assert bundle.worker.principal_id == prior.principal_id


def test_connection_interruption_recovers_the_same_native_storage(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    description = describe(tmp_path)
    with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:
        provision(pipeline, description, tmp_path, url)
        original = remote_enrollment._write_installation

        def fail_final_write(root: Path, state: WorkerInstallation) -> None:
            if state.initialized:
                raise OSError("Synthetic final installation failure")
            original(root, state)

        with monkeypatch.context() as patch:
            patch.setattr(remote_enrollment, "_write_installation", fail_final_write)
            with pytest.raises(OSError, match="final installation"):
                connect_worker(
                    tmp_path / "remote", bundle_path=tmp_path / "bundles" / "worker.json"
                )
        pending = installation(tmp_path)
        assert not pending.initialized and pending.bundle is not None
        journal = read_identity(tmp_path / "remote" / "execution" / "journal.sqlite")
        registry = read_identity(Path(pending.machine_registry))
        with connect_worker(tmp_path / "remote") as resumed:
            assert resumed.worker.journal.identity == journal
            assert resumed.worker.resources.identity == registry
        assert installation(tmp_path).initialized


@pytest.mark.parametrize("change", ["token", "url", "profile", "journal", "registry", "revoke"])
def test_connection_rejects_changed_authority_or_missing_native_state(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    description = describe(tmp_path)
    with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:
        bundle = provision(pipeline, description, tmp_path, url)
        bundle_path = tmp_path / "bundles" / "worker.json"
        with connect_worker(tmp_path / "remote", bundle_path=bundle_path):
            pass
        prior = installation(tmp_path)
        if change in {"token", "url"}:
            payload = bundle.model_dump(mode="json")
            payload["token" if change == "token" else "controller_url"] = (
                "rsw_" + "x" * 43 if change == "token" else "https://different.example"
            )
            write_private(bundle_path, json.dumps(payload).encode())
        elif change == "profile":
            monkeypatch.setattr(
                remote_enrollment,
                "cpu_profile",
                lambda **_: description.profile.model_copy(
                    update={"execution_fingerprint": "sha256:" + "0" * 64}
                ),
            )
        elif change in {"journal", "registry"}:
            path = (
                tmp_path / "remote" / "execution" / "journal.sqlite"
                if change == "journal"
                else Path(prior.machine_registry)
            )
            path.unlink()
        else:
            pipeline.runtime.credentials.revoke(pipeline.runtime.principal(), bundle.credential_id)
        with pytest.raises(ResearchError):
            connect_worker(tmp_path / "remote", bundle_path=bundle_path)
        assert installation(tmp_path) == prior
        if change in {"journal", "registry"}:
            assert not path.exists()


def test_missing_installation_cannot_be_replaced_over_native_state(tmp_path: Path) -> None:
    describe(tmp_path)
    (tmp_path / "remote" / "installation.json").unlink()
    (tmp_path / "remote" / "execution").mkdir()
    with pytest.raises(ResearchError, match="installation is missing"):
        describe(tmp_path)


def test_insecure_destination_fails_before_credential_or_enrollment_mutation(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    description = describe(tmp_path)
    with pytest.raises(ValueError, match="HTTPS"):
        provision(pipeline, description, tmp_path, "http://controller.example")
    assert not (pipeline.runtime.root / "remote-workers" / description.worker_id).exists()


@pytest.mark.parametrize("change", ["missing", "revoked"])
def test_completed_provisioning_cannot_restore_a_credential(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    description = describe(tmp_path)
    bundle = provision(pipeline, description, tmp_path, "https://controller.example")
    if change == "missing":
        with pipeline.runtime.database.connect() as connection:
            connection.execute("DELETE FROM credentials WHERE id=?", (bundle.credential_id,))
    else:
        pipeline.runtime.credentials.revoke(pipeline.runtime.principal(), bundle.credential_id)

    def forbidden(*_: Any, **__: Any) -> Any:
        raise AssertionError("Completed provisioning attempted to reinstall credentials")

    monkeypatch.setattr(CredentialStore, "install", forbidden)
    with pytest.raises(ResearchError):
        provision(pipeline, description, tmp_path, "https://controller.example")


@pytest.mark.parametrize("source", ["bundle", "installation", "provision"])
def test_private_record_validation_never_renders_its_token_in_exception_chain(
    pipeline: Pipeline, tmp_path: Path, source: str
) -> None:
    description = describe(tmp_path)
    bundle = provision(pipeline, description, tmp_path, "https://controller.example")
    path = {
        "bundle": tmp_path / "bundles" / "worker.json",
        "installation": tmp_path / "remote" / "installation.json",
        "provision": pipeline.runtime.root
        / "remote-workers"
        / description.worker_id
        / "provision.json",
    }[source]
    payload = json.loads(read_private(path.parent, path.name, limit=128 * 1024))
    payload["unexpected_secret"] = bundle.token
    write_private(path, json.dumps(payload).encode())
    with pytest.raises(ResearchError, match="Private enrollment record is invalid") as failed:
        if source == "provision":
            provision(pipeline, description, tmp_path, "https://controller.example")
        else:
            connect_worker(tmp_path / "remote", bundle_path=tmp_path / "bundles" / "worker.json")
    rendered = "".join(traceback.format_exception(failed.value))
    assert bundle.token not in rendered
    assert bundle.token not in repr(failed.value.as_dict())


def test_cli_describe_provision_connect_never_prints_private_bundle(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    root = tmp_path / "remote"
    description_path = tmp_path / "description.json"
    registry = tmp_path / "remote-machine" / "resources.sqlite"

    def cli(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "research_swarm.cli", *args],
            text=True,
            capture_output=True,
            timeout=15,
            check=True,
        )

    described = cli(
        "worker", "describe", "--worker-dir", str(root), "--machine-registry", str(registry)
    )
    description = WorkerDescription.model_validate_json(described.stdout)
    description_path.write_text(described.stdout, encoding="utf-8")
    bundle_path = tmp_path / "bundles" / "worker.json"
    with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:
        provisioned = cli(
            "--state-dir",
            str(pipeline.runtime.root),
            "worker",
            "provision",
            "--description",
            str(description_path),
            "--url",
            url,
            "--project",
            pipeline.campaign["data"]["contract"]["project_id"],
            "--bundle",
            str(bundle_path),
        )
        connected = cli(
            "worker", "connect", "--worker-dir", str(root), "--bundle", str(bundle_path)
        )
        resumed = cli("worker", "connect", "--worker-dir", str(root))
        assert connected.stdout == resumed.stdout
    state = installation(tmp_path)
    assert state.bundle is not None
    for output in (described, provisioned, connected, resumed):
        assert state.bundle.token not in output.stdout + output.stderr
    assert json.loads(connected.stdout)["worker"]["binding"]["worker_id"] == description.worker_id
