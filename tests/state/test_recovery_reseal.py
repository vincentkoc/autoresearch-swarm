"""An activated target can resume explicitly and become the next independent backup source."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from research_swarm.checkpoints.activation import activate_recovery
from research_swarm.checkpoints.admission import admit_recovery
from research_swarm.checkpoints.backup import export_backup, source_custody, verify_backup
from research_swarm.checkpoints.claims import claim_restore, request_restore_claim
from research_swarm.checkpoints.lifecycle import verify_activated
from research_swarm.checkpoints.restore import restore_inactive
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import CampaignTarget
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.workers import ClaimJob, EnrollWorker, WorkerData
from research_swarm.domain.backup import RestoreReceipt
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from research_swarm.workers.local import LocalWorker
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.projects.test_workspace_seal import seal_request
from tests.state.test_recovery_activation import closed, intent
from tests.state.test_recovery_admission import copy_pipeline
from tests.state.test_recovery_operations import recovery as recovery


def reseal(runtime, bundle):
    context = CommandContext(principal=runtime.principal(), idempotency_key="next-seal")
    previous = runtime.database.read_record(
        context, "workspace_gate", runtime.identity.workspace_id
    )
    context = context.model_copy(update={"expected_revision": previous["revision"]})
    request = seal_request(runtime.database, context)
    reply = runtime.service.seal_workspace(context, request)
    assert reply["result"]["revision"] == previous["revision"] + 1
    exported = export_backup(runtime, context, bundle, backup_id=request.backup_id)
    verified = verify_backup(bundle)
    assert verified.manifest == exported
    assert verified.manifest.seal.backup_id == request.backup_id
    return verified


def test_activated_target_reseals_exports_and_elects_next_epoch(recovery, tmp_path):
    closed(recovery, not_started=True)
    runtime = recovery.runtime
    context, request = intent(runtime)
    activate_recovery(runtime, context, request)
    original = RestoreReceipt.model_validate_json((runtime.root / "restore.json").read_bytes())
    assert not (runtime.root / "backup-custody.json").exists()
    first_history = runtime.database.read_record(context, "workspace_gate", original.workspace_id)[
        "data"
    ]
    verified = reseal(runtime, tmp_path / "next-bundle")
    assert verified.manifest.seal.control_epoch == original.target_control_epoch
    assert verified.manifest.source_public_key not in {
        original.source_public_key,
        original.target_public_key,
    }
    assert verified.manifest.source_custody_id != original.source_custody_id
    assert (
        runtime.database.read_record(context, "workspace_seal", original.backup_id)["data"]
        == first_history
    )
    assert verify_activated(runtime.database, original)
    assert (
        restore_inactive(tmp_path / "bundle", runtime.root, restore_id=original.restore_id)
        == original
    )

    next_target = tmp_path / "next-target"
    next_receipt = restore_inactive(verified.root, next_target, restore_id=new_id())
    assert next_receipt.source_control_epoch == original.target_control_epoch
    assert next_receipt.target_control_epoch == original.target_control_epoch + 1
    claim_request = request_restore_claim(
        verified.root, next_target, restore_id=next_receipt.restore_id
    )
    claim = claim_restore(runtime, context, verified.root, claim_request)
    successor = LocalRuntime.open(next_target)
    admission = CommandContext(principal=successor.principal(), idempotency_key="admit-next")
    admit_recovery(successor, admission, verified.root, claim)
    next_context, next_request = intent(successor)
    activate_recovery(successor, next_context, next_request)
    assert verify_activated(successor.database, next_receipt)
    for restore_id in (original.restore_id, next_receipt.restore_id):
        assert (
            successor.database.read_record(next_context, "recovery_activation", restore_id)["state"]
            == "immutable"
        )
    # Neither ancestor regains execution authority after electing its successor.
    assert (
        runtime.database.read_record(context, "workspace_gate", original.workspace_id)["state"]
        == "sealed"
    )
    assert source_custody(runtime).claim == claim


def test_two_new_seals_compare_and_swap_one_active_gate(recovery):
    closed(recovery, not_started=True)
    runtime = recovery.runtime
    context, request = intent(runtime)
    activate_recovery(runtime, context, request)
    gate = runtime.database.read_record(context, "workspace_gate", runtime.identity.workspace_id)
    barrier = Barrier(2)

    def choose(_):
        reopened = LocalRuntime.open(runtime.root)
        command = CommandContext(
            principal=reopened.principal(),
            idempotency_key=new_id(),
            expected_revision=gate["revision"],
        )
        candidate = seal_request(reopened.database, command)
        barrier.wait(timeout=10)
        try:
            return reopened.service.seal_workspace(command, candidate)
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(choose, range(2)))
    assert sum(isinstance(reply, dict) for reply in replies) == 1
    assert ErrorCode.REVISION_CONFLICT in replies


def test_reseal_failure_rolls_back_new_history_and_keeps_active_gate(recovery, monkeypatch):
    closed(recovery, not_started=True)
    runtime = recovery.runtime
    context, request = intent(runtime)
    activate_recovery(runtime, context, request)
    gate = runtime.database.read_record(context, "workspace_gate", runtime.identity.workspace_id)
    context = context.model_copy(
        update={"idempotency_key": "failed-seal", "expected_revision": gate["revision"]}
    )
    candidate = seal_request(runtime.database, context)
    original = Transaction.event

    def fail(self, name, *args, **kwargs):
        if name == "workspace.sealed":
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Fixture seal failure")
        return original(self, name, *args, **kwargs)

    monkeypatch.setattr(Transaction, "event", fail)
    with pytest.raises(ResearchError, match="Fixture"):
        runtime.service.seal_workspace(context, candidate)
    assert (
        runtime.database.read_record(context, "workspace_gate", runtime.identity.workspace_id)
        == gate
    )
    with pytest.raises(ResearchError) as missing:
        runtime.database.read_record(context, "workspace_seal", candidate.backup_id)
    assert missing.value.code == ErrorCode.NOT_FOUND


def test_explicit_resume_uses_new_worker_and_preserves_fence_on_original(pipeline, tmp_path):
    training = pipeline.worker.claim(pipeline.ready())
    pipeline.worker.execute(training["id"])
    runtime, context, bundle, claim = copy_pipeline(pipeline, tmp_path)
    admit_recovery(runtime, context, bundle, claim)
    activation_context, activation = intent(runtime)
    activate_recovery(runtime, activation_context, activation)
    client = ResearchClient(LocalTransport(runtime))
    campaign = client.get("campaign", pipeline.campaign["id"])
    client.command(
        "campaign.start",
        CampaignTarget(campaign_id=campaign["id"]),
        idempotency_key="explicit-resume",
        expected_revision=campaign["revision"],
    )
    scoring_id = pipeline.ready()["id"]
    scoring = client.get("job", scoring_id)
    authorization = client.command(
        "evaluation.authorize",
        AuthorizeEvaluation(job_id=scoring_id, evaluator_id=runtime.identity.operator_id),
        idempotency_key="independent-evaluation",
        expected_revision=scoring["revision"],
    )["result"]
    old = pipeline.worker.worker
    original_client = ResearchClient(
        LocalTransport(runtime, pipeline.worker.client.transport.resolve_principal)
    )
    with pytest.raises(ResearchError) as fenced:
        original_client.command(
            "worker.claim_job",
            ClaimJob(
                worker_id=old.binding.worker_id,
                session_id=old.binding.worker_session_id,
                job_id=scoring_id,
            ),
            idempotency_key="old-worker-refused",
            expected_revision=client.get("job", scoring_id)["revision"],
        )
    assert fenced.value.code == ErrorCode.STALE_FENCE
    principal = Principal(
        id=new_id(),
        workspace_id=runtime.identity.workspace_id,
        scopes=frozenset({"read", "artifact.upload", "worker.claim", "worker.publish"}),
    )
    credential = runtime.credentials.issue(runtime.principal(), principal)
    enrolled = client.command(
        "worker.enroll",
        EnrollWorker(
            worker_id=new_id(),
            session_id=new_id(),
            principal_id=principal.id,
            name="Successor CPU worker",
            profiles=old.profiles,
            max_concurrency=1,
        ),
        idempotency_key="successor-worker",
    )["result"]
    worker_client = ResearchClient(
        LocalTransport(runtime, lambda: runtime.credentials.resolve(credential.token))
    )
    worker = LocalWorker(
        worker_client,
        WorkerData.model_validate(enrolled["data"]),
        root=tmp_path / "successor-worker",
        machine_registry=tmp_path / "successor-machine" / "resources.sqlite",
    )
    attempt = worker.claim(client.get("job", scoring_id))
    assert worker.execute(attempt["id"])["state"] == "succeeded"
    evaluation = client.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authorization["id"], attempt_id=attempt["id"]),
        idempotency_key="successor-evaluation",
    )["result"]
    assert evaluation["state"] == "valid"
    assert client.get("trial", pipeline.trial["id"])["state"] == "succeeded"
    assert client.get("worker", old.binding.worker_id)["state"] == "fenced"
    assert worker.worker.binding.control_epoch == activation.expected_control_epoch
    reseal(runtime, tmp_path / "resumed-bundle")
