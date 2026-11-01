"""An authority seal freezes mutations, not the original native execution."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event

import pytest

from research_swarm import ResearchClient
from research_swarm.api.http import create_app
from research_swarm.control.backup import BackupControl
from research_swarm.control.commands import CreateCampaign
from research_swarm.control.service import ResearchService
from research_swarm.domain.backup import SealWorkspace
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime, read_private
from research_swarm.state.artifacts import BeginUpload
from research_swarm.state.database import Database
from research_swarm.workers.local_enrollment import ensure_local_enrollment
from tests.orchestration.test_http_delivery import serving
from tests.support import LIMITS
from tests.workers.test_worker_control import KEY, enroll, ready_job, request, trial


def seal_request(database, context):
    snapshot = database.snapshot(context)
    with database.connect() as connection:
        authority = connection.execute(
            "SELECT controller_id FROM workspaces WHERE id=?", (context.principal.workspace_id,)
        ).one()
    return SealWorkspace(
        backup_id=new_id(),
        expected_controller_id=authority["controller_id"],
        expected_control_epoch=snapshot["control_epoch"],
        reason="Fixture sealed checkpoint",
    )


def test_seal_blocks_sdk_http_credentials_and_rejection_hooks_but_preserves_replay(
    research, tmp_path
):
    runtime = LocalRuntime.open(tmp_path / "runtime")
    issued = runtime.credentials.issue(runtime.principal(), runtime.principal())
    with ResearchClient.local(runtime.root) as local:
        registered = local.register_project(research.source, idempotency_key="source")["result"]
        create = CreateCampaign(
            project_id=registered["project"]["id"],
            name="Backup fixture",
            primary_metric="val_bpb",
            limits=LIMITS,
        )
        accepted = local.command("campaign.create", create, idempotency_key="create")
    context = CommandContext(
        principal=runtime.principal(), idempotency_key="seal", expected_revision=0
    )
    intent = seal_request(runtime.database, context)
    token = read_private(runtime.root, "operator.token", limit=256).decode()
    with (
        serving(create_app(runtime.service, runtime.credentials)) as url,
        ResearchClient.connect(url, token) as remote,
    ):
        sealed = remote.command(
            "workspace.seal", intent, idempotency_key="seal", expected_revision=0
        )
        assert sealed["result"]["data"]["unresolved_attempt_ids"] == []
        assert (
            remote.command("workspace.seal", intent, idempotency_key="seal", expected_revision=0)
            == sealed
        )
        assert remote.command("campaign.create", create, idempotency_key="create") == accepted
        with pytest.raises(ResearchError) as refused:
            remote.command("campaign.create", create, idempotency_key="new-create")
        assert refused.value.code == ErrorCode.STALE_FENCE
        assert remote.get("workspace_gate", runtime.identity.workspace_id)["state"] == "sealed"
    before = runtime.database.snapshot(context)
    touched = []
    with pytest.raises(ResearchError):
        runtime.database.execute(
            context.model_copy(update={"idempotency_key": "hook"}),
            "fixture.reject",
            {},
            "write",
            lambda transaction: touched.append("handler"),
            on_rejection=lambda transaction, error: touched.append("hook"),
        )
    assert touched == [] and runtime.database.snapshot(context) == before
    with pytest.raises(ResearchError):
        runtime.credentials.issue(runtime.principal(), runtime.principal())
    with pytest.raises(ResearchError):
        runtime.credentials.revoke(runtime.principal(), issued.id)
    assert runtime.credentials.resolve(issued.token) == issued.principal
    notifications = runtime.database.pending_notifications()
    with pytest.raises(ResearchError):
        runtime.database.acknowledge_notification(notifications[0].id)
    assert runtime.database.pending_notifications() == notifications
    with pytest.raises(ResearchError):
        ensure_local_enrollment(runtime, machine_registry=tmp_path / "must-not-exist.sqlite")
    assert not (runtime.root / "local-worker").exists()
    assert not (tmp_path / "must-not-exist.sqlite").exists()
    with ResearchClient.local(runtime.root) as reopened:
        assert reopened.command("campaign.create", create, idempotency_key="create") == accepted
        with pytest.raises(ResearchError):
            reopened.command("campaign.create", create, idempotency_key="after-reopen")


def test_pending_upload_and_live_reader_block_seal_without_releasing_storage(tmp_path):
    runtime = LocalRuntime.open(tmp_path / "runtime")
    actor = runtime.principal()

    def context():
        return CommandContext(principal=actor, idempotency_key=new_id(), expected_revision=0)

    upload = runtime.artifacts.begin_upload(
        context(), BeginUpload(digest=content_digest(b"abc"), size_bytes=3)
    )["result"]
    request = seal_request(runtime.database, context())
    with pytest.raises(ResearchError, match="Artifact operations"):
        runtime.service.seal_workspace(context(), request)
    runtime.artifacts.upload_part(context(), upload["id"], 0, b"abc")
    result = runtime.artifacts.finalize_upload(context(), upload["id"])
    artifact = runtime.artifacts.get(context(), result["result"]["id"])
    with runtime.artifacts.reader(context(), artifact.reference) as stream:
        assert stream.read() == b"abc"
        with pytest.raises(ResearchError, match="Artifact operations"):
            runtime.service.seal_workspace(context(), request)
    assert runtime.service.seal_workspace(context(), request)["result"]["state"] == "sealed"
    assert runtime.artifacts.occupancy(context())["retained_bytes"] == 3


def test_claim_racing_seal_is_in_the_cut_or_loses_admission(research):
    worker = enroll(research)
    campaign = research.campaign()
    job = ready_job(research, trial(research, campaign))
    seal_context = research.context(revision=0)
    intent = seal_request(research.service.database, seal_context)
    second = ResearchService(
        Database(research.service.database.path), research.service.artifacts, dispatch_key=KEY
    )
    barrier = Barrier(2)

    def claim():
        barrier.wait(timeout=10)
        try:
            return second.claim_job(
                research.context(revision=job["revision"]), request(worker, job)
            )["result"]
        except ResearchError as error:
            assert error.code == ErrorCode.STALE_FENCE
            return None

    def seal():
        barrier.wait(timeout=10)
        return BackupControl(research.service.database).seal(seal_context, intent)["result"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        claimed, sealed = pool.submit(claim), pool.submit(seal)
        attempt, checkpoint = claimed.result(), sealed.result()
    assert checkpoint["data"]["worker_ids"] == [worker.binding.worker_id]
    assert checkpoint["data"]["unresolved_attempt_ids"] == ([attempt["id"]] if attempt else [])
    with research.service.database.connect() as connection:
        reservations = connection.execute(
            "SELECT COUNT(*) FROM budget_reservations WHERE state='reserved'"
        ).one()[0]
    assert reservations == (1 if attempt else 0)


def test_deletion_replays_after_seal_without_touching_storage(tmp_path, monkeypatch):
    runtime = LocalRuntime.open(tmp_path / "runtime")
    context = CommandContext(principal=runtime.principal(), idempotency_key="bytes")
    reference = runtime.artifacts.publish_bytes(
        context, b"backup deletion fixture", purpose="seal fixture"
    )
    deletion = CommandContext(
        principal=Principal(
            id=new_id(),
            workspace_id=runtime.identity.workspace_id,
            scopes=frozenset({"artifact.delete"}),
        ),
        idempotency_key="delete",
        expected_revision=1,
    )
    entered, release = Event(), Event()
    calls = []
    original_delete = runtime.artifacts.store.delete

    def delete(workspace_id, blob):
        calls.append(blob)
        entered.set()
        assert release.wait(timeout=10)
        original_delete(workspace_id, blob)

    monkeypatch.setattr(runtime.artifacts.store, "delete", delete)
    seal_context = context.model_copy(update={"idempotency_key": "seal", "expected_revision": 0})
    intent = seal_request(runtime.database, seal_context)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(runtime.artifacts.delete, deletion, reference.id, "fixture")
        assert entered.wait(timeout=10)
        duplicate = pool.submit(runtime.artifacts.delete, deletion, reference.id, "fixture")
        try:
            with pytest.raises(ResearchError, match="Artifact operations"):
                runtime.service.seal_workspace(seal_context, intent)
        finally:
            release.set()
        assert first.result() == duplicate.result()
    assert len(calls) == 1
    runtime.service.seal_workspace(
        seal_context.model_copy(update={"idempotency_key": "seal-after-closure"}), intent
    )
    assert runtime.artifacts.delete(deletion, reference.id, "fixture") == first.result()
    assert len(calls) == 1


def test_finalize_ack_survives_aborted_duplicate_cleanup_and_seal(tmp_path, monkeypatch):
    runtime = LocalRuntime.open(tmp_path / "runtime")
    context = CommandContext(principal=runtime.principal(), idempotency_key="original")
    runtime.artifacts.publish_bytes(context, b"same bytes", purpose="fixture")
    upload = runtime.artifacts.begin_upload(
        context.model_copy(update={"idempotency_key": "duplicate"}),
        BeginUpload(digest=content_digest(b"same bytes"), size_bytes=10),
    )["result"]
    runtime.artifacts.upload_part(context, upload["id"], 0, b"same bytes")
    finalize = context.model_copy(update={"idempotency_key": "finalize"})

    def interrupted(*args):
        raise OSError("Fixture interrupted duplicate cleanup")

    with monkeypatch.context() as patch:
        patch.setattr(runtime.artifacts.store, "delete", interrupted)
        with pytest.raises(OSError):
            runtime.artifacts.finalize_upload(finalize, upload["id"])
    previous = runtime.database.replay(
        finalize, "artifact.finalize_upload", {"upload_id": upload["id"]}, "artifact.upload"
    )
    assert previous is not None
    runtime.artifacts.abort_upload(
        context.model_copy(update={"idempotency_key": "abort"}), upload["id"]
    )
    seal = context.model_copy(update={"idempotency_key": "seal", "expected_revision": 0})
    runtime.service.seal_workspace(seal, seal_request(runtime.database, seal))
    monkeypatch.setattr(runtime.artifacts.store, "delete", interrupted)
    assert runtime.artifacts.finalize_upload(finalize, upload["id"]) == previous


def test_upload_status_holds_pending_state_until_storage_read_closes(tmp_path, monkeypatch):
    runtime = LocalRuntime.open(tmp_path / "runtime")
    context = CommandContext(principal=runtime.principal(), idempotency_key="upload")
    upload = runtime.artifacts.begin_upload(
        context, BeginUpload(digest=content_digest(b"abc"), size_bytes=3)
    )["result"]
    runtime.artifacts.upload_part(context, upload["id"], 0, b"abc")
    entered, release = Event(), Event()
    original = runtime.artifacts.store.progress

    def progress(upload):
        entered.set()
        assert release.wait(timeout=10)
        return original(upload)

    monkeypatch.setattr(runtime.artifacts.store, "progress", progress)
    seal = context.model_copy(update={"idempotency_key": "seal", "expected_revision": 0})
    with ThreadPoolExecutor(max_workers=2) as pool:
        status = pool.submit(runtime.artifacts.upload_status, context, upload["id"])
        assert entered.wait(timeout=10)
        finalized = pool.submit(
            runtime.artifacts.finalize_upload,
            context.model_copy(update={"idempotency_key": "finalize"}),
            upload["id"],
        )
        try:
            with pytest.raises(ResearchError, match="Artifact operations"):
                runtime.service.seal_workspace(seal, seal_request(runtime.database, seal))
        finally:
            release.set()
        assert status.result()["state"] == "pending"
        assert finalized.result()["result"]["state"] == "verified"
    runtime.service.seal_workspace(
        seal.model_copy(update={"idempotency_key": "seal-after-read"}),
        seal_request(runtime.database, seal),
    )
    entered.clear()
    assert runtime.artifacts.upload_status(context, upload["id"])["transfer"] is None
    assert not entered.is_set()
