from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.runtime import read_private, write_private
from research_swarm.state.identity import read_identity
from research_swarm.workers import remote_enrollment, remote_replacement
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.remote_enrollment import (
    RemoteWorkerBundle,
    WorkerInstallation,
    connect_worker,
)
from research_swarm.workers.remote_replacement import (
    ReplacementProvisioningState,
    install_replacement,
    provision_replacement,
)
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_recovery import Recoverable
from tests.test_worker_recovery import recoverable as recoverable
from tests.test_worker_turnover import fence


def provision(
    pipeline: Pipeline, original: Recoverable, fenced: dict[str, Any], destination: Path
) -> RemoteWorkerBundle:
    bundle = original.enrollment.bundle
    return provision_replacement(
        pipeline.client,
        bundle.description,
        fence_id=fenced["fence"]["id"],
        expected_revision=fenced["worker"]["revision"],
        reason="Install replacement fixture",
        url=bundle.controller_url,
        destination=destination,
    )


def pending(
    pipeline: Pipeline, original: Recoverable, fenced: dict[str, Any]
) -> ReplacementProvisioningState:
    root = (
        pipeline.runtime.root
        / "remote-workers"
        / original.enrollment.bundle.description.worker_id
        / "replacements"
        / fenced["fence"]["id"]
    )
    return ReplacementProvisioningState.model_validate_json(
        read_private(root, "provision.json", limit=2 * 1024 * 1024)
    )


def installed(root: Path) -> WorkerInstallation:
    return WorkerInstallation.model_validate_json(
        read_private(root, "installation.json", limit=2 * 1024 * 1024)
    )


def test_replacement_installs_distinct_native_state_and_preserves_original_custody(
    pipeline: Pipeline, recoverable: Recoverable, tmp_path: Path
) -> None:
    before = read_private(recoverable.root, "installation.json", limit=2 * 1024 * 1024)
    journal = recoverable.enrollment.worker.journal.get(recoverable.attempt_id)
    fenced = fence(pipeline, recoverable.enrollment.bundle.worker)
    destination = tmp_path / "bundles" / "replacement-bundle.json"
    bundle = provision(pipeline, recoverable, fenced, destination)
    root = tmp_path / "replacement"
    assert install_replacement(recoverable.root, root, destination) == bundle.worker
    first = installed(root)
    assert first.initialized and first.replacement is not None
    assert first.journal_identity != installed(recoverable.root).journal_identity
    assert first.registry_identity == installed(recoverable.root).registry_identity
    assert first.machine_registry == installed(recoverable.root).machine_registry
    assert (
        first.bundle == bundle and bundle.replaces == recoverable.enrollment.bundle.worker.binding
    )
    assert install_replacement(recoverable.root, root, destination) == bundle.worker
    assert installed(root) == first
    assert read_private(recoverable.root, "installation.json", limit=2 * 1024 * 1024) == before
    assert recoverable.enrollment.worker.journal.get(recoverable.attempt_id) == journal
    with connect_worker(root) as new:
        assert new.worker.resources.availability() == {"cpu": 0}
        assert new.worker.journal.unresolved() == ()
    assert (
        pipeline.client.get("worker_admission", bundle.worker.binding.worker_id)["state"]
        == "draining"
    )
    assert not (root / "execution/driver").exists()
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1

    from research_swarm.control.commands import SubmitTrial
    from research_swarm.control.worker_lifecycle import SetWorkerAdmission
    from research_swarm.workers.recovery import recover_worker_attempt
    from tests.test_closure import close_operationally

    recovered = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert recovered.delivery_state == "quarantined"
    close_operationally(
        pipeline, pipeline.client.get("attempt_submission", recovered.submission_id)
    )
    admission = pipeline.client.get("worker_admission", bundle.worker.binding.worker_id)
    pipeline.client.command(
        "worker.set_admission",
        SetWorkerAdmission(
            binding=bundle.worker.binding, admission="active", reason="Installed replacement ready"
        ),
        idempotency_key=new_id(),
        expected_revision=admission["revision"],
    )
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=pipeline.trial["data"]["spec"]["candidate_id"],
            validation_id=pipeline.trial["data"]["validation_id"],
            seed=19,
        ),
        idempotency_key=new_id(),
        expected_revision=campaign["revision"],
    )
    with connect_worker(root) as new:
        attempt = new.worker.claim(pipeline.ready())
        assert new.worker.execute(attempt["id"])["state"] == "succeeded"
        assert new.worker.resources.availability() == {"cpu": 1}
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 2


@pytest.mark.parametrize("boundary", ["credential", "command", "accepted_state", "export"])
def test_replacement_provisioning_replays_each_interruption_without_new_authority(
    pipeline: Pipeline,
    recoverable: Recoverable,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    fenced = fence(pipeline, recoverable.enrollment.bundle.worker)
    destination = tmp_path / "bundles" / "replacement-bundle.json"
    with monkeypatch.context() as patch:
        if boundary == "credential":
            original = pipeline.runtime.credentials.install

            def fail(*args: Any, **kwargs: Any) -> Any:
                original(*args, **kwargs)
                raise OSError("Synthetic credential acknowledgment loss")

            patch.setattr(pipeline.runtime.credentials, "install", fail)
        elif boundary == "command":
            command = pipeline.client.command

            def fail(*args: Any, **kwargs: Any) -> Any:
                command(*args, **kwargs)
                raise OSError("Synthetic replacement acknowledgment loss")

            patch.setattr(pipeline.client, "command", fail)
        elif boundary == "accepted_state":
            write = remote_replacement.write_private

            def fail(path: Path, content: bytes) -> Any:
                write(path, content)
                if json.loads(content).get("worker") is not None:
                    raise OSError("Synthetic accepted state acknowledgment loss")

            patch.setattr(remote_replacement, "write_private", fail)
        else:
            export = remote_replacement._write_bundle

            def fail(*args: Any, **kwargs: Any) -> Any:
                export(*args, **kwargs)
                raise OSError("Synthetic export acknowledgment loss")

            patch.setattr(remote_replacement, "_write_bundle", fail)
        with pytest.raises(OSError, match="Synthetic"):
            provision(pipeline, recoverable, fenced, destination)
    first = pending(pipeline, recoverable, fenced)
    bundle = provision(pipeline, recoverable, fenced, destination)
    second = pending(pipeline, recoverable, fenced)
    assert second.session_id == first.session_id and second.principal_id == first.principal_id
    assert second.credential_id == first.credential_id and second.token == first.token
    assert second.request == first.request and second.command_key == first.command_key
    assert bundle.worker == second.worker
    assert provision(pipeline, recoverable, fenced, destination) == bundle
    assert len(pipeline.client.list(RecordQuery(kind="worker_replacement"))["items"]) == 1


def test_conflicting_export_fails_before_credential_or_session_mutation(
    pipeline: Pipeline, recoverable: Recoverable
) -> None:
    fenced = fence(pipeline, recoverable.enrollment.bundle.worker)
    with pipeline.runtime.database.connect() as connection:
        before = connection.execute("SELECT COUNT(*) FROM credentials").fetchone()[0]
    with pytest.raises(ResearchError) as conflict:
        provision(pipeline, recoverable, fenced, recoverable.root / "bundle.json")
    assert conflict.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert (
        pipeline.client.get("worker", recoverable.enrollment.bundle.worker.binding.worker_id)
        == fenced["worker"]
    )
    with pipeline.runtime.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM credentials").fetchone()[0] == before


def test_changed_inputs_or_revoked_completed_credentials_do_not_rotate_identity(
    pipeline: Pipeline, recoverable: Recoverable, tmp_path: Path
) -> None:
    fenced = fence(pipeline, recoverable.enrollment.bundle.worker)
    destination = tmp_path / "bundles" / "replacement-bundle.json"
    bundle = provision(pipeline, recoverable, fenced, destination)
    before = pending(pipeline, recoverable, fenced)
    with pytest.raises(ResearchError) as changed:
        provision(pipeline, recoverable, fenced, tmp_path / "bundles" / "different-bundle.json")
    assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    pipeline.runtime.credentials.revoke(pipeline.runtime.principal(), bundle.credential_id)
    with pytest.raises(ResearchError) as revoked:
        provision(pipeline, recoverable, fenced, destination)
    assert revoked.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert pending(pipeline, recoverable, fenced) == before


def test_interrupted_native_installation_resumes_the_same_journal(
    pipeline: Pipeline, recoverable: Recoverable, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fenced = fence(pipeline, recoverable.enrollment.bundle.worker)
    destination, root = tmp_path / "bundles" / "replacement-bundle.json", tmp_path / "replacement"
    provision(pipeline, recoverable, fenced, destination)
    original = remote_enrollment._write_installation

    def fail(path: Path, state: WorkerInstallation) -> None:
        if state.initialized:
            raise OSError("Synthetic final installation interruption")
        original(path, state)

    with monkeypatch.context() as patch:
        patch.setattr(remote_enrollment, "_write_installation", fail)
        with pytest.raises(OSError, match="Synthetic"):
            install_replacement(recoverable.root, root, destination)
    assert not installed(root).initialized
    identity = read_identity(root / "execution/journal.sqlite")
    install_replacement(recoverable.root, root, destination)
    assert installed(root).journal_identity == identity
    assert recoverable.enrollment.worker.resources.availability() == {"cpu": 0}


def test_missing_registry_between_preflight_and_constructor_is_never_recreated(
    pipeline: Pipeline, recoverable: Recoverable, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fenced = fence(pipeline, recoverable.enrollment.bundle.worker)
    destination, root = tmp_path / "bundles" / "replacement-bundle.json", tmp_path / "replacement"
    provision(pipeline, recoverable, fenced, destination)
    registry = recoverable.enrollment.worker.resources.path
    saved = registry.with_suffix(".original")
    original = LocalWorker.__init__

    def remove(self: LocalWorker, *args: Any, **kwargs: Any) -> None:
        registry.rename(saved)
        original(self, *args, **kwargs)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(LocalWorker, "__init__", remove)
            with pytest.raises(ResearchError) as missing:
                install_replacement(recoverable.root, root, destination)
        assert missing.value.code == ErrorCode.EXECUTION_UNKNOWN and not registry.exists()
    finally:
        if saved.exists():
            saved.rename(registry)
    install_replacement(recoverable.root, root, destination)
    assert installed(root).registry_identity == installed(recoverable.root).registry_identity
    assert recoverable.enrollment.worker.resources.availability() == {"cpu": 0}


@pytest.mark.parametrize("target", ["same", "nested", "parent"])
def test_replacement_rejects_overlapping_native_roots(
    pipeline: Pipeline, recoverable: Recoverable, tmp_path: Path, target: str
) -> None:
    fenced = fence(pipeline, recoverable.enrollment.bundle.worker)
    destination = tmp_path / "bundles" / "replacement-bundle.json"
    provision(pipeline, recoverable, fenced, destination)
    root = (
        recoverable.root
        if target == "same"
        else recoverable.root / "new"
        if target == "nested"
        else recoverable.root.parent
    )
    with pytest.raises(ResearchError) as refused:
        install_replacement(recoverable.root, root, destination)
    assert refused.value.code == ErrorCode.PROJECT_CONTRACT_INVALID


def test_real_cli_installs_without_printing_private_bundle(
    pipeline: Pipeline, recoverable: Recoverable, tmp_path: Path
) -> None:
    from tests.test_cli_mcp import cli

    fenced = fence(pipeline, recoverable.enrollment.bundle.worker)
    description = tmp_path / "description.json"
    write_private(
        description,
        canonical_bytes(recoverable.enrollment.bundle.description.model_dump(mode="json")),
    )
    destination, root = tmp_path / "bundles" / "replacement-bundle.json", tmp_path / "replacement"
    receipt = cli(
        pipeline.runtime.root,
        "worker",
        "provision-replacement",
        "--description",
        str(description),
        "--fence",
        fenced["fence"]["id"],
        "--revision",
        str(fenced["worker"]["revision"]),
        "--reason",
        "CLI fixture",
        "--url",
        recoverable.enrollment.bundle.controller_url,
        "--bundle",
        str(destination),
    )
    assert receipt["state"] == "provisioned"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_swarm.cli",
            "worker",
            "install-replacement",
            "--original-worker-dir",
            str(recoverable.root),
            "--worker-dir",
            str(root),
            "--bundle",
            str(destination),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    output = json.loads(result.stdout)
    assert output["state"] == "installed" and output["worker"] == receipt["worker"]
    bundle = RemoteWorkerBundle.model_validate_json(
        read_private(destination.parent, destination.name, limit=2 * 1024 * 1024)
    )
    assert bundle.token not in result.stdout + result.stderr
    assert str(root) not in result.stdout + result.stderr


@pytest.mark.parametrize("fault", ["wrong_previous", "unavailable"])
def test_replacement_receipt_is_verified_before_native_initialization(
    pipeline: Pipeline,
    recoverable: Recoverable,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    from research_swarm.client import ResearchClient

    fenced = fence(pipeline, recoverable.enrollment.bundle.worker)
    destination, root = tmp_path / "bundles" / "replacement-bundle.json", tmp_path / "replacement"
    provision(pipeline, recoverable, fenced, destination)
    original = ResearchClient.worker_get

    def wrong(self: ResearchClient, request: Any) -> Any:
        row = original(self, request)
        if request.kind == "worker_replacement":
            if fault == "unavailable":
                raise ResearchError(ErrorCode.NOT_FOUND, "Synthetic missing receipt")
            row["data"]["previous_binding"]["worker_session_id"] = new_id()
        return row

    monkeypatch.setattr(ResearchClient, "worker_get", wrong)
    with pytest.raises(ResearchError) as rejected:
        install_replacement(recoverable.root, root, destination)
    assert rejected.value.code in {ErrorCode.STALE_FENCE, ErrorCode.NOT_FOUND}
    assert not (root / "execution").exists()
    assert recoverable.enrollment.worker.resources.availability() == {"cpu": 0}


def test_registered_replacement_preserves_environment_without_launching_a_probe(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from research_swarm.api.http import create_app
    from research_swarm.workers.remote_enrollment import describe_worker, provision_worker
    from tests.test_http_delivery import serving
    from tests.test_registered_execution import registration

    registered = registration(pipeline, tmp_path, isolated=True)
    original_root, replacement_root = (
        tmp_path / "registered-original",
        tmp_path / "registered-replacement",
    )
    description = describe_worker(
        original_root,
        registration=registered,
        machine_registry=tmp_path / "registered-machine/resources.sqlite",
    )
    with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:
        initial_path = original_root / "bundle.json"
        original = provision_worker(
            pipeline.client,
            description,
            url=url,
            project_ids=(pipeline.trial["data"]["spec"]["project_id"],),
            destination=initial_path,
        )
        with connect_worker(original_root, bundle_path=initial_path):
            pass
        fenced = fence(pipeline, original.worker)
        replacement_path = tmp_path / "bundles" / "replacement.json"
        bundle = provision_replacement(
            pipeline.client,
            description,
            fence_id=fenced["fence"]["id"],
            expected_revision=fenced["worker"]["revision"],
            reason="Registered replacement",
            url=url,
            destination=replacement_path,
        )

        def forbidden(*_: Any, **__: Any) -> Any:
            raise AssertionError("Replacement installation launched a process")

        monkeypatch.setattr(subprocess, "Popen", forbidden)
        assert (
            install_replacement(original_root, replacement_root, replacement_path) == bundle.worker
        )
        assert installed(replacement_root).registration == registered
        assert installed(replacement_root).registration == installed(original_root).registration


def test_two_fenced_workers_cannot_claim_one_bundle_destination(
    pipeline: Pipeline, recoverable: Recoverable, tmp_path: Path
) -> None:
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from tests.test_scoped_driver import enrollment

    with enrollment(
        pipeline, tmp_path / "other", recoverable.enrollment.bundle.controller_url
    ) as other:
        originals = [recoverable, Recoverable(tmp_path / "other", other, new_id())]
        fences = [fence(pipeline, item.enrollment.bundle.worker) for item in originals]
        with pipeline.runtime.database.connect() as connection:
            before = connection.execute("SELECT COUNT(*) FROM credentials").fetchone()[0]
        barrier = threading.Barrier(2)

        def compete(index: int) -> Any:
            barrier.wait(timeout=5)
            try:
                return provision(
                    pipeline,
                    originals[index],
                    fences[index],
                    tmp_path / "bundles" / "one-destination.json",
                )
            except ResearchError as error:
                return error

        with ThreadPoolExecutor(max_workers=2) as pool:
            replies = list(pool.map(compete, range(2)))
        winners = [i for i, item in enumerate(replies) if isinstance(item, RemoteWorkerBundle)]
        assert len(winners) == 1
        loser = 1 - winners[0]
        assert isinstance(replies[loser], ResearchError)
        assert replies[loser].code == ErrorCode.IDEMPOTENCY_CONFLICT
        assert (
            pipeline.client.get(
                "worker", originals[loser].enrollment.bundle.worker.binding.worker_id
            )
            == fences[loser]["worker"]
        )
        with pipeline.runtime.database.connect() as connection:
            assert (
                connection.execute("SELECT COUNT(*) FROM credentials").fetchone()[0] == before + 1
            )
        assert len(pipeline.client.list(RecordQuery(kind="worker_replacement"))["items"]) == 1


def test_credential_revoked_during_replacement_is_not_exported_or_regenerated(
    pipeline: Pipeline, recoverable: Recoverable, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fenced = fence(pipeline, recoverable.enrollment.bundle.worker)
    destination = tmp_path / "bundles" / "replacement-bundle.json"
    command = pipeline.client.command

    def revoke(*args: Any, **kwargs: Any) -> Any:
        reply = command(*args, **kwargs)
        state = pending(pipeline, recoverable, fenced)
        pipeline.runtime.credentials.revoke(pipeline.runtime.principal(), state.credential_id)
        return reply

    with monkeypatch.context() as patch:
        patch.setattr(pipeline.client, "command", revoke)
        with pytest.raises(ResearchError) as rejected:
            provision(pipeline, recoverable, fenced, destination)
    assert rejected.value.code == ErrorCode.AUTHORIZATION_DENIED
    first = pending(pipeline, recoverable, fenced)
    assert first.worker is not None and not destination.exists()
    with pytest.raises(ResearchError) as retry:
        provision(pipeline, recoverable, fenced, destination)
    assert retry.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert pending(pipeline, recoverable, fenced) == first
    assert not destination.exists()


def test_original_resumed_connection_also_opens_only_its_pinned_registry(
    recoverable: Recoverable, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = recoverable.enrollment.worker.resources.path
    saved = registry.with_suffix(".original")
    original = LocalWorker.__init__

    def remove(self: LocalWorker, *args: Any, **kwargs: Any) -> None:
        registry.rename(saved)
        original(self, *args, **kwargs)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(LocalWorker, "__init__", remove)
            with pytest.raises(ResearchError) as missing:
                connect_worker(recoverable.root)
        assert missing.value.code == ErrorCode.EXECUTION_UNKNOWN
        assert not registry.exists()
    finally:
        if saved.exists():
            saved.rename(registry)
    with connect_worker(recoverable.root) as restored:
        assert restored.worker.resources.availability() == {"cpu": 0}
