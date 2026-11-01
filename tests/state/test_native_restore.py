"""Inactive restore preserves evidence and liabilities across native interruption."""

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

from research_swarm import ResearchClient
from research_swarm.checkpoints import restore as restore_module
from research_swarm.checkpoints.backup import export_backup
from research_swarm.checkpoints.restore import restore_inactive
from research_swarm.cli import main
from research_swarm.control.commands import RegisterProject
from research_swarm.control.worker_records import WorkerData
from research_swarm.control.workers import EngineCapabilities, EnrollWorker
from research_swarm.domain.canonical import content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from tests.projects.test_workspace_seal import seal_request
from tests.state.test_native_backup import sealed_runtime as sealed_runtime
from tests.support import ResearchFixture
from tests.workers.test_worker_control import ready_job, trial
from tests.workers.test_worker_control import request as claim_request


@pytest.mark.parametrize("legacy_seal", [False, True])
def test_restore_reopens_inactive_with_original_bytes_and_exact_ack(
    sealed_runtime, tmp_path, legacy_seal
):
    runtime, context, seal, artifact = sealed_runtime
    if legacy_seal:
        # Older signed native v1 bundles have a gate but no separate seal history.
        runtime.database.store.write(
            lambda sql: sql.execute("DELETE FROM records WHERE kind='workspace_seal'")
        )
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    manifest = export_backup(runtime, context, bundle, backup_id=seal.backup_id)
    identifier = new_id()
    receipt = restore_inactive(
        bundle, target, restore_id=identifier, expected_digest=manifest.digest
    )
    assert receipt.target_control_epoch == receipt.source_control_epoch + 1
    restored = LocalRuntime.open(target)
    assert restored.identity == runtime.identity
    assert restored.database.snapshot(context)["control_epoch"] == receipt.target_control_epoch
    assert (
        restored.database.read_record(context, "workspace_gate", receipt.workspace_id)["state"]
        == "restoring"
    )
    with restored.artifacts.store.reader(receipt.workspace_id, manifest.artifacts[0]) as stream:
        assert stream.read() == b"checkpoint bytes"
    with pytest.raises(ResearchError) as refused, restored.artifacts.reader(context, artifact):
        pass
    assert refused.value.code == ErrorCode.STALE_FENCE
    original_seal = context.model_copy(update={"idempotency_key": "seal", "expected_revision": 0})
    assert restored.service.seal_workspace(original_seal, seal) == runtime.service.seal_workspace(
        original_seal, seal
    )
    assert restore_inactive(bundle, target, restore_id=identifier) == receipt
    assert restored.database.snapshot(context)["control_epoch"] == receipt.target_control_epoch
    assert not (target / "worker-journal.sqlite").exists()
    assert not (target / "machine-resources.sqlite").exists()
    assert not (target / "backup-custody.json").exists()
    assert not (target / "recovery.pending").exists()
    assert (target / "restore-custody.json").is_file()
    with ResearchClient.local(target) as client:
        assert client.get("restoration", identifier)["state"] == "inactive"


@pytest.mark.parametrize("after_commit", [False, True])
def test_interrupted_restore_requires_same_intent_and_advances_epoch_once(
    sealed_runtime, tmp_path, monkeypatch, after_commit
):
    runtime, context, seal, _ = sealed_runtime
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    manifest = export_backup(runtime, context, bundle, backup_id=seal.backup_id)
    identifier = new_id()
    original = restore_module._transform

    def interrupt(*args):
        if after_commit:
            original(*args)
        raise OSError("Fixture interrupted restore")

    with monkeypatch.context() as patch:
        patch.setattr(restore_module, "_transform", interrupt)
        with pytest.raises(OSError):
            restore_inactive(bundle, target, restore_id=identifier)
    assert (target / "recovery.pending").is_file()
    with pytest.raises(ResearchError, match="restore is incomplete"):
        LocalRuntime.open(target)
    with pytest.raises(ResearchError, match="identity differs"):
        restore_inactive(bundle, target, restore_id=new_id())
    receipt = restore_inactive(bundle, target, restore_id=identifier)
    assert receipt.target_control_epoch == manifest.seal.control_epoch + 1
    restored = LocalRuntime.open(target)
    with restored.database.connect() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM records WHERE kind='restoration'").one()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM events "
                "WHERE json_extract(payload,'$.type')='workspace.restored_inactive'"
            ).one()[0]
            == 1
        )


def test_corrupted_bundle_never_creates_destination(sealed_runtime, tmp_path):
    runtime, context, seal, _ = sealed_runtime
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    manifest = export_backup(runtime, context, bundle, backup_id=seal.backup_id)
    (bundle / "blobs" / manifest.artifacts[0].generation).unlink()
    with pytest.raises(ResearchError):
        restore_inactive(bundle, target, restore_id=new_id())
    assert not target.exists()


def test_completed_restore_rechecks_materialized_generations(sealed_runtime, tmp_path):
    runtime, context, seal, _ = sealed_runtime
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    manifest = export_backup(runtime, context, bundle, backup_id=seal.backup_id)
    identifier = new_id()
    receipt = restore_inactive(bundle, target, restore_id=identifier)
    restored = LocalRuntime.open(target)
    restored.artifacts.store.delete(receipt.workspace_id, manifest.artifacts[0])
    with pytest.raises(ResearchError):
        restore_inactive(bundle, target, restore_id=identifier)
    assert restored.database.snapshot(context)["control_epoch"] == receipt.target_control_epoch


def test_restore_preserves_unresolved_attempt_and_budget_while_archiving_worker(research, tmp_path):
    runtime = LocalRuntime.open(tmp_path / "source-runtime")

    def context(key=None, revision=None):
        return CommandContext(
            principal=runtime.principal(),
            idempotency_key=key or new_id(),
            expected_revision=revision,
        )

    fixture = ResearchFixture(
        runtime.service,
        research.source,
        context,
        {},
        ArtifactRef(id=new_id(), digest="sha256:" + "0" * 64, size_bytes=0),
    )
    fixture.artifact = fixture.capture()
    fixture.project = runtime.service.register_project(
        context(), RegisterProject(source_artifact=fixture.artifact)
    )["result"]["project"]
    enrollment = EnrollWorker(
        worker_id=new_id(),
        session_id=new_id(),
        principal_id=runtime.principal().id,
        name="Restore fixture worker",
        max_concurrency=1,
        profiles=(
            EngineCapabilities(
                name="python-cpu",
                backend="python",
                cpu_slots=1,
                execution_fingerprint=content_digest(b"restore-fixture"),
            ),
        ),
    )
    worker = WorkerData.model_validate(
        runtime.service.enroll_worker(context(), enrollment)["result"]["data"]
    )
    job = ready_job(fixture, trial(fixture, fixture.campaign()))
    claim_context = context("original-claim", job["revision"])
    claim = runtime.service.claim_job(claim_context, claim_request(worker, job))
    attempt_id = claim["result"]["id"]
    seal_context = context("seal", 0)
    seal = seal_request(runtime.database, seal_context)
    runtime.service.seal_workspace(seal_context, seal)
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    export_backup(runtime, context(), bundle, backup_id=seal.backup_id)
    receipt = restore_inactive(bundle, target, restore_id=new_id())
    restored = LocalRuntime.open(target)
    assert receipt.unresolved_attempt_ids == (attempt_id,)
    original_attempt = runtime.database.read_record(context(), "attempt", attempt_id)
    assert restored.database.read_record(context(), "attempt", attempt_id) == original_attempt
    assert restored.service.claim_job(claim_context, claim_request(worker, job)) == claim
    with pytest.raises(ResearchError) as refused:
        restored.service.claim_job(
            context("new-claim", job["revision"]), claim_request(worker, job)
        )
    assert refused.value.code == ErrorCode.STALE_FENCE
    archived = restored.database.read_record(
        context(), "worker_session", worker.binding.worker_session_id
    )
    assert WorkerData.model_validate(archived["data"]) == worker
    current = restored.database.read_record(context(), "worker", worker.binding.worker_id)
    assert current["state"] == "fenced" and WorkerData.model_validate(current["data"]) == worker
    for table in (
        "budgets",
        "budget_reservations",
        "budget_entries",
        "budget_settlements",
        "commands",
    ):
        with runtime.database.connect() as source, restored.database.connect() as copied:
            before = [tuple(row) for row in source.execute("SELECT * FROM " + table).fetchall()]
            after = [tuple(row) for row in copied.execute("SELECT * FROM " + table).fetchall()]
        assert after == before
    assert runtime.database.snapshot(context())["control_epoch"] == receipt.source_control_epoch
    with restored.database.connect() as connection:
        connection.execute("UPDATE budget_reservations SET state='released'")
    with pytest.raises(ResearchError, match="preserve its source"):
        restore_inactive(bundle, target, restore_id=receipt.restore_id)


def test_restore_cli_is_explicit_and_repeatable(sealed_runtime, tmp_path, capsys):
    runtime, context, seal, _ = sealed_runtime
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    export_backup(runtime, context, bundle, backup_id=seal.backup_id)
    argv = [
        "backup",
        "restore",
        str(bundle),
        "--destination",
        str(target),
        "--restore-id",
        new_id(),
    ]
    assert main(argv) == 0
    receipt = strict_json_loads(capsys.readouterr().out)
    assert main(argv) == 0
    assert strict_json_loads(capsys.readouterr().out) == receipt


@pytest.mark.parametrize("phase", ["before-marker", "staged-file", "transaction"])
def test_native_exit_preserves_admission_and_exact_resume(sealed_runtime, tmp_path, phase):
    runtime, context, seal, _ = sealed_runtime
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    manifest = export_backup(runtime, context, bundle, backup_id=seal.backup_id)
    identifier = new_id()
    code = """
import os, sys
from pathlib import Path
from research_swarm.checkpoints import restore
from research_swarm.checkpoints import files
from research_swarm.state import private_files
from research_swarm.state.database import Transaction
phase = sys.argv[4]
original_event = Transaction.event
def event(self, event_type, *args, **kwargs):
    result = original_event(self, event_type, *args, **kwargs)
    if phase == 'transaction' and event_type == 'workspace.restored_inactive':
        os._exit(73)
    return result
Transaction.event = event
original_write = restore.write_private
def write(path, content):
    if phase == 'before-marker' and path.name == 'recovery.pending':
        os._exit(73)
    return original_write(path, content)
restore.write_private = write
original_replace = private_files.replace_private
def replace(source, destination):
    if phase == 'staged-file' and Path(destination).name == 'identity.json':
        os._exit(73)
    return original_replace(source, destination)
private_files.replace_private = replace
files.replace_private = replace
restore.restore_inactive(Path(sys.argv[1]), Path(sys.argv[2]), restore_id=sys.argv[3])
"""
    exited = subprocess.run(
        [sys.executable, "-c", code, str(bundle), str(target), identifier, phase],
        capture_output=True,
        timeout=20,
        check=False,
    )
    assert exited.returncode == 73, exited.stderr.decode()
    with pytest.raises(ResearchError, match="restore is incomplete"):
        LocalRuntime.open(target)
    alias = target.with_name(target.name.upper())
    if alias.exists() and alias.samefile(target):
        with pytest.raises(ResearchError, match="restore is incomplete"):
            LocalRuntime.open(alias)
    receipt = restore_inactive(bundle, target, restore_id=identifier)
    assert receipt.target_control_epoch == manifest.seal.control_epoch + 1
    assert restore_inactive(bundle, target, restore_id=identifier) == receipt
    assert not list(target.glob("*.pending"))


def test_concurrent_exact_restore_has_one_custody_key_and_epoch(sealed_runtime, tmp_path):
    runtime, context, seal, _ = sealed_runtime
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    export_backup(runtime, context, bundle, backup_id=seal.backup_id)
    identifier = new_id()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda _: restore_inactive(bundle, target, restore_id=identifier), range(2))
        )
    assert results[0] == results[1]


@pytest.mark.parametrize("after_commit", [False, True])
def test_resume_rejects_lost_command_acknowledgments(
    sealed_runtime, tmp_path, monkeypatch, after_commit
):
    runtime, context, seal, _ = sealed_runtime
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    export_backup(runtime, context, bundle, backup_id=seal.backup_id)
    identifier = new_id()
    original = restore_module._transform

    def interrupt(*args):
        if after_commit:
            original(*args)
        raise OSError("Fixture interrupted restore")

    with monkeypatch.context() as patch:
        patch.setattr(restore_module, "_transform", interrupt)
        with pytest.raises(OSError):
            restore_inactive(bundle, target, restore_id=identifier)
    from research_swarm.state.database import Database

    damaged = Database(target / "research.sqlite")
    with damaged.connect() as connection:
        connection.execute("DELETE FROM commands")
    with pytest.raises(ResearchError, match="preserve its source"):
        restore_inactive(bundle, target, restore_id=identifier)
    assert (target / "recovery.pending").is_file()
