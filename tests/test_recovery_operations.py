"""An elected target receives original journals without admitting new research."""

from dataclasses import dataclass, field

import pytest

from research_swarm.api.http import create_app
from research_swarm.checkpoints.admission import admit_recovery
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.models import TrialData
from research_swarm.control.observations import PublishAttempt
from research_swarm.control.reconciliation import ReconcileAttempt
from research_swarm.control.worker_artifacts import WorkerAttemptTarget
from research_swarm.control.worker_outputs import BeginWorkerUpload
from research_swarm.control.workers import AttemptData
from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.artifacts import BeginUpload
from research_swarm.workers.journal_records import JournalEntry, JournalState
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.supervisor import run_attempt
from tests.test_http_delivery import serving
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_recovery_admission import copy_pipeline
from tests.test_worker_outputs import begin, intent, output_client


@dataclass
class Recovery:
    runtime: LocalRuntime
    operator: ResearchClient
    worker: ResearchClient
    native: LocalWorker
    target: WorkerAttemptTarget
    token: str = field(repr=False)
    campaign_id: str


@pytest.fixture
def recovery(pipeline, tmp_path):
    client, worker, credential, target = output_client(pipeline)
    native = LocalWorker(
        client,
        worker,
        root=tmp_path / "worker",
        machine_registry=tmp_path / "machine" / "resources.sqlite",
    )
    attempt = pipeline.client.get("attempt", target.attempt_id)
    native._prepare(
        AttemptData.model_validate(attempt["data"]),
        TrialData.model_validate(pipeline.trial["data"]),
    )
    runtime, context, bundle, claim = copy_pipeline(pipeline, tmp_path)
    admit_recovery(runtime, context, bundle, claim)
    yield Recovery(
        runtime,
        ResearchClient(LocalTransport(runtime)),
        ResearchClient(
            LocalTransport(runtime, lambda: runtime.credentials.resolve(credential.token))
        ),
        native,
        target,
        credential.token,
        pipeline.campaign["id"],
    )


def upload(
    recovery: Recovery, entry: JournalEntry, key: str = "journal"
) -> tuple[ArtifactRef, dict]:
    path = recovery.native.root / (key + ".json")
    path.write_bytes(canonical_bytes(entry.model_dump(mode="json")))
    reference = recovery.worker.worker_upload_file(
        intent(recovery.target, role="journal"), path, idempotency_key=key
    )
    request = PublishAttempt(attempt_id=recovery.target.attempt_id, journal_artifact=reference)
    reply = recovery.worker.command(
        "worker.submit_evidence", request, idempotency_key=key + "-submit"
    )
    return reference, reply["result"]


def reconcile(recovery: Recovery, submission: dict, key: str = "close") -> dict:
    authority = recovery.operator.context(recovery.campaign_id)["authority"]
    request = ReconcileAttempt(
        attempt_id=recovery.target.attempt_id,
        submission_id=submission["id"],
        expected_controller_id=authority["controller_id"],
        expected_control_epoch=authority["control_epoch"],
        reason="Original journal proves native closure",
    )
    return recovery.operator.command(
        "attempt.reconcile_closure",
        request,
        idempotency_key=key,
        expected_revision=recovery.operator.get("attempt", recovery.target.attempt_id)["revision"],
    )


def test_http_journal_after_restart_closes_once_without_scientific_acceptance(recovery):
    entry = run_attempt(recovery.native.journal, recovery.target.attempt_id)
    assert entry.state == JournalState.CLOSED and entry.outcome.closure.scope_closed
    runtime = LocalRuntime.open(recovery.runtime.root)
    with (
        serving(create_app(runtime.service, runtime.credentials)) as url,
        ResearchClient.connect(url, recovery.token) as client,
    ):
        recovery.worker = client
        reference, submission = upload(recovery, entry)
        # A second transfer exercises canonical deduplication and derived pin cleanup.
        duplicate, same = upload(recovery, entry, "duplicate")
        assert duplicate == reference and same == submission
        before = recovery.operator.budget(recovery.campaign_id)
        result = reconcile(recovery, submission)
        assert result["result"]["attempt"]["state"] == "closed_unaccepted"
        after = recovery.operator.budget(recovery.campaign_id)
        assert after != before
        assert all(item["reserved"] == item["uncertain"] == "0" for item in after["accounts"])
        assert reconcile(recovery, submission, "close-again")["result"] == result["result"]
        assert recovery.operator.budget(recovery.campaign_id) == after
        assert upload(recovery, entry)[1] == submission  # Exact upload/submission acknowledgments.
        request = PublishAttempt(attempt_id=entry.dispatch.attempt_id, journal_artifact=reference)
        with pytest.raises(ResearchError):
            client.command("worker.publish_attempt", request, idempotency_key=new_id())
        with pytest.raises(ResearchError):
            recovery.operator.command(
                "attempt.reconcile",
                ReconcileAttempt(
                    attempt_id=entry.dispatch.attempt_id,
                    submission_id=submission["id"],
                    expected_controller_id=runtime.identity.controller_id,
                    expected_control_epoch=entry.dispatch.control_epoch + 1,
                    reason="Forbidden scientific acceptance",
                ),
                idempotency_key=new_id(),
                expected_revision=result["result"]["attempt"]["revision"],
            )
    with runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind IN ('run_result','evaluation_receipt')"
            ).one()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='upload' "
                "AND state IN ('pending','cleanup','aborting')"
            ).one()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' AND state='retained' "
                "AND json_extract(payload,'$.retention.owner_kind')='artifact_read'"
            ).one()[0]
            == 0
        )
    assert (
        recovery.operator.get("workspace_gate", runtime.identity.workspace_id)["state"]
        == "recovering"
    )
    assert recovery.operator.get("campaign", recovery.campaign_id)["state"] == "paused"


@pytest.mark.parametrize("state", [JournalState.RECEIVED, JournalState.UNKNOWN])
def test_unfinished_journals_are_quarantined_without_settlement(recovery, state):
    entry = recovery.native.journal.get(recovery.target.attempt_id).model_copy(
        update={"state": state}
    )
    before = recovery.operator.budget(recovery.campaign_id)
    _, submission = upload(recovery, entry)
    assert submission["state"] == "quarantined"
    with pytest.raises(ResearchError) as refused:
        reconcile(recovery, submission)
    assert refused.value.code in {ErrorCode.INVALID_TRANSITION, ErrorCode.EXECUTION_UNKNOWN}
    assert recovery.operator.budget(recovery.campaign_id) == before
    assert recovery.operator.get("campaign", recovery.campaign_id)["state"] == "pausing"


def test_original_unstarted_grant_closes_without_consumption(recovery):
    recovery.native.journal.request_local_cancel(recovery.target.attempt_id)
    entry = recovery.native.journal.settle_unstarted(recovery.target.attempt_id)
    assert entry.state == JournalState.NOT_STARTED and entry.native is None
    _, submission = upload(recovery, entry)
    result = reconcile(recovery, submission)
    assert result["result"]["closure"]["data"]["not_started"]
    accounts = recovery.operator.budget(recovery.campaign_id)["accounts"]
    assert all(
        item["reserved"] == item["uncertain"] == item["consumed"] == "0" for item in accounts
    )


def test_partial_transfer_survives_restart_and_bad_journal_can_be_aborted(recovery):
    content = b"not a journal"
    target = begin(recovery.worker, recovery.target, content)
    context = CommandContext(
        principal=recovery.runtime.credentials.resolve(recovery.token), idempotency_key=new_id()
    )
    assert recovery.runtime.service.worker_outputs.append(context, target, 0, content[:3]) == 3
    restarted = LocalRuntime.open(recovery.runtime.root)
    operator = CommandContext(principal=restarted.principal(), idempotency_key=new_id())
    assert (
        restarted.service.worker_outputs.status(context, target)["transfer"]["received_bytes"] == 3
    )
    assert restarted.service.worker_outputs.append(context, target, 3, content[3:]) == len(content)
    with pytest.raises(ResearchError, match="journal is invalid"):
        recovery.worker.command("worker.finalize_upload", target, idempotency_key="bad-finalize")
    assert (
        restarted.database.read_record(operator, "upload", target.upload_id)["state"] == "pending"
    )
    reply = recovery.worker.command("worker.abort_upload", target, idempotency_key="abort-bad")
    assert (
        restarted.database.read_record(operator, "upload", target.upload_id)["state"] == "aborted"
    )
    assert recovery.operator.get("worker_upload", target.upload_id)["state"] == "aborted"
    assert (
        recovery.worker.command("worker.abort_upload", target, idempotency_key="abort-bad") == reply
    )


def test_pending_recovery_publication_cannot_omit_prepared_journal(recovery, monkeypatch):
    entry = recovery.native.journal.get(recovery.target.attempt_id)
    content = canonical_bytes(entry.model_dump(mode="json"))
    target = begin(recovery.worker, recovery.target, content)
    recovery.worker.transport.worker_append(target, 0, content)
    with monkeypatch.context() as missing:
        missing.setattr(
            recovery.runtime.service.worker_outputs, "_validate_journal", lambda *args: None
        )
        with pytest.raises(ResearchError, match="journal proof is absent"):
            recovery.worker.command(
                "worker.finalize_upload", target, idempotency_key="missing-proof"
            )
    assert recovery.operator.get("worker_upload", target.upload_id)["state"] == "pending"
    accepted = recovery.worker.command(
        "worker.finalize_upload", target, idempotency_key="checked-proof"
    )
    assert accepted["result"]["upload_id"] == target.upload_id
    assert recovery.operator.get("worker_upload", target.upload_id)["state"] == "finalized"


@pytest.mark.parametrize(
    "role,name,size",
    [("report", None, 5), ("output", "checkpoint", 5), ("journal", None, 1024 * 1024 + 1)],
)
def test_recovery_does_not_open_general_output_uploads(recovery, role, name, size):
    request = BeginWorkerUpload(
        **intent(recovery.target, role=role, output_name=name).model_dump(),
        content=BeginUpload(digest=content_digest(b"x"), size_bytes=size),
    )
    with pytest.raises(ResearchError, match="bounded journals"):
        recovery.worker.command("worker.begin_upload", request, idempotency_key=new_id())


def test_worker_guard_is_not_a_general_command_or_wire_capability(recovery):
    from pydantic import ValidationError

    context = CommandContext(
        principal=recovery.runtime.credentials.resolve(recovery.token), idempotency_key=new_id()
    )
    with pytest.raises(ValidationError):
        CommandContext.model_validate(
            {
                **context.model_dump(),
                "_recovery_operation": {"attempt_id": recovery.target.attempt_id},
            }
        )
    with pytest.raises(ResearchError):
        recovery.runtime.database.execute(
            context,
            "worker.begin_upload",
            {},
            "worker.outputs",
            lambda transaction: {"bypassed": True},
        )
    for wrong in (
        recovery.target.model_copy(update={"attempt_id": new_id()}),
        recovery.target.model_copy(update={"session_id": new_id()}),
    ):
        with pytest.raises(ResearchError):
            begin(recovery.worker, wrong, b"no")


def test_altered_native_grant_cannot_be_published_and_remains_abortable(recovery):
    entry = recovery.native.journal.get(recovery.target.attempt_id)
    bad = entry.model_copy(
        update={
            "command": entry.command.model_copy(update={"timeout_ms": entry.command.timeout_ms + 1})
        }
    )
    content = canonical_bytes(bad.model_dump(mode="json"))
    target = begin(recovery.worker, recovery.target, content)
    context = CommandContext(
        principal=recovery.runtime.credentials.resolve(recovery.token), idempotency_key=new_id()
    )
    recovery.runtime.service.worker_outputs.append(context, target, 0, content)
    with pytest.raises(ResearchError, match="original grant"):
        recovery.worker.command("worker.finalize_upload", target, idempotency_key="bad-grant")
    recovery.worker.command("worker.abort_upload", target, idempotency_key="abort-grant")
    assert recovery.operator.get("worker_upload", target.upload_id)["state"] == "aborted"


@pytest.mark.parametrize("completion", ["worker.abort_upload", "worker.cleanup_upload"])
def test_abort_after_native_settlement_retries_failed_physical_cleanup(
    recovery, monkeypatch, completion
):
    target = begin(recovery.worker, recovery.target, b"unfinished")
    _, submission = upload(
        recovery, run_attempt(recovery.native.journal, recovery.target.attempt_id)
    )
    reconcile(recovery, submission)
    store = recovery.runtime.artifacts.store
    original = store.delete
    with monkeypatch.context() as patch:

        def fail(*args):
            raise OSError("Fixture cleanup interrupted")

        patch.setattr(store, "delete", fail)
        with pytest.raises(OSError, match="cleanup interrupted"):
            recovery.worker.command(
                "worker.abort_upload", target, idempotency_key="abort-after-close"
            )
    assert store.delete == original
    key = "abort-after-close" if completion == "worker.abort_upload" else "cleanup-after-close"
    reply = recovery.worker.command(completion, target, idempotency_key=key)
    assert reply["result"]["state"] == (
        "aborting" if completion == "worker.abort_upload" else "aborted"
    )
    original = recovery.worker.command(
        "worker.abort_upload", target, idempotency_key="abort-after-close"
    )
    assert original["result"]["state"] == "aborting"
    assert recovery.operator.get("worker_upload", target.upload_id)["state"] == "aborted"


@pytest.mark.parametrize("changed", ["epoch", "gate", "admission"])
def test_recovery_proof_is_rechecked_in_write_transaction(recovery, changed):
    from research_swarm.control.recovery_operations import recovery_context

    request = BeginWorkerUpload(
        **intent(recovery.target, role="journal").model_dump(),
        content=BeginUpload(digest=content_digest(b"x"), size_bytes=1),
    )
    context = CommandContext(
        principal=recovery.runtime.credentials.resolve(recovery.token), idempotency_key=new_id()
    )
    guarded = recovery_context(
        recovery.runtime.database,
        context,
        "worker.begin_upload",
        request.model_dump(mode="json"),
        attempt_id=request.attempt_id,
    )
    with recovery.runtime.database.connect() as connection:
        if changed == "epoch":
            connection.execute("UPDATE workspaces SET control_epoch=control_epoch+1")
        elif changed == "gate":
            connection.execute("UPDATE records SET revision=revision+1 WHERE kind='workspace_gate'")
        else:
            connection.execute(
                "UPDATE records SET payload=json_set(payload,'$.target_signature',?) "
                "WHERE kind='recovery_admission'",
                ("0" * 128,),
            )
    called = []
    with pytest.raises(ResearchError):
        recovery.runtime.database.execute(
            guarded,
            "worker.begin_upload",
            request.model_dump(mode="json"),
            "worker.outputs",
            lambda transaction: called.append(True) or {},
        )
    assert not called


def test_derived_pin_cannot_open_foreign_artifact(recovery):
    from research_swarm.control.recovery_operations import recovery_context

    entry = recovery.native.journal.get(recovery.target.attempt_id)
    reference, _ = upload(recovery, entry)
    request = PublishAttempt(attempt_id=entry.dispatch.attempt_id, journal_artifact=reference)
    context = CommandContext(
        principal=recovery.runtime.credentials.resolve(recovery.token), idempotency_key=new_id()
    )
    guarded = recovery_context(
        recovery.runtime.database,
        context,
        "worker.submit_evidence",
        request.model_dump(mode="json"),
        attempt_id=request.attempt_id,
        artifact_id=reference.id,
    )
    other = ArtifactRef.model_validate(
        recovery.operator.get("attempt", request.attempt_id)["data"]["source_artifact"]
    )
    with (
        pytest.raises(ResearchError, match="cannot derive"),
        recovery.runtime.artifacts.pinned_authorized(
            guarded, other, scope="worker.publish", authorize=lambda transaction, reference: None
        ),
    ):
        pytest.fail("Foreign bytes were opened")


def test_generic_byte_route_cannot_bypass_recovery_upload_guard(recovery):
    target = begin(recovery.worker, recovery.target, b"abc")
    principal = recovery.runtime.credentials.resolve(recovery.token).model_copy(
        update={"scopes": frozenset({"*"})}
    )
    context = CommandContext(principal=principal, idempotency_key=new_id())
    with pytest.raises(ResearchError):
        recovery.runtime.artifacts.upload_part(context, target.upload_id, 0, b"abc")
    assert (
        recovery.runtime.service.worker_outputs.status(context, target)["transfer"][
            "received_bytes"
        ]
        == 0
    )
