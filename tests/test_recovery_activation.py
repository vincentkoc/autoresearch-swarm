"""Activation closes original obligations while preserving queued scientific work."""

import subprocess
import sys

import pytest
from pydantic import ValidationError

from research_swarm.checkpoints.activation import activate_recovery
from research_swarm.checkpoints.admission import admit_recovery
from research_swarm.checkpoints.lifecycle import verify_activated
from research_swarm.checkpoints.restore import restore_inactive
from research_swarm.cli import main
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.confirmation_models import ConfirmationData
from research_swarm.control.dispatch import COMMANDS
from research_swarm.control.evaluations import AuthorizeEvaluation
from research_swarm.control.recovery_proofs import verify_activation
from research_swarm.domain.artifacts import ReaderSessionTarget
from research_swarm.domain.backup import (
    ActivateRecovery,
    RecoveryActivation,
    RecoveryAdmission,
    RestoreReceipt,
)
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from research_swarm.state.private_files import write_private
from research_swarm.workers.journal import JournalState
from research_swarm.workers.supervisor import run_attempt
from tests.test_child_campaigns import OPTIONS, create
from tests.test_confirmation import create_plan, options, setup_plan
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_recovery_admission import copy_pipeline
from tests.test_recovery_operations import reconcile, upload
from tests.test_recovery_operations import recovery as recovery
from tests.test_recovery_readers import crashed as crashed
from tests.test_recovery_turns import close
from tests.test_recovery_turns import saved as saved


def intent(runtime):
    receipt = RestoreReceipt.model_validate_json((runtime.root / "restore.json").read_bytes())
    context = CommandContext(
        principal=runtime.principal(), idempotency_key="activate:" + receipt.restore_id
    )
    gate = runtime.database.read_record(context, "workspace_gate", receipt.workspace_id)
    context = context.model_copy(update={"expected_revision": gate["revision"]})
    return context, ActivateRecovery(
        restore_id=receipt.restore_id,
        expected_control_epoch=receipt.target_control_epoch,
        reason="Original execution and custody are reconciled",
    )


def closed(recovery, *, not_started=False):
    if not_started:
        recovery.native.journal.request_local_cancel(recovery.target.attempt_id)
        entry = recovery.native.journal.settle_unstarted(recovery.target.attempt_id)
    else:
        entry = run_attempt(recovery.native.journal, recovery.target.attempt_id)
    _, submission = upload(recovery, entry, "closure-journal")
    return reconcile(recovery, submission)


@pytest.mark.parametrize("not_started", [False, True])
def test_closed_original_activates_once_without_resuming_or_accepting(recovery, not_started):
    closed(recovery, not_started=not_started)
    runtime = recovery.runtime
    context, request = intent(runtime)
    budget = recovery.operator.budget(recovery.campaign_id)
    reply = activate_recovery(runtime, context, request)
    receipt = RestoreReceipt.model_validate_json((runtime.root / "restore.json").read_bytes())
    activation = RecoveryActivation.model_validate(reply["result"]["data"])
    admission = RecoveryAdmission.model_validate(
        runtime.database.read_record(context, "recovery_admission", receipt.restore_id)["data"]
    )
    verify_activation(activation, admission, receipt)
    assert verify_activated(runtime.database, receipt)
    assert runtime.database.snapshot(context)["control_epoch"] == receipt.target_control_epoch
    assert recovery.operator.get("workspace_gate", receipt.workspace_id)["state"] == "active"
    assert recovery.operator.get("campaign", recovery.campaign_id)["state"] == "paused"
    assert (
        recovery.operator.get("attempt", recovery.target.attempt_id)["state"] == "closed_unaccepted"
    )
    assert recovery.operator.budget(recovery.campaign_id) == budget
    assert (
        restore_inactive(
            runtime.root.parent / "bundle", runtime.root, restore_id=receipt.restore_id
        )
        == receipt
    )
    (runtime.root / "restore-custody.json").unlink()
    restarted = LocalRuntime.open(runtime.root)
    assert activate_recovery(restarted, context, request) == reply
    with runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='recovery_activation'"
            ).one()[0]
            == 1
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM records WHERE kind='run_result'").one()[0] == 0
        )


@pytest.mark.parametrize("state", [None, JournalState.RECEIVED, JournalState.UNKNOWN])
def test_issued_or_unknown_execution_blocks_then_same_intent_can_activate(recovery, state):
    runtime = recovery.runtime
    context, request = intent(runtime)
    if state is not None:
        entry = recovery.native.journal.get(recovery.target.attempt_id).model_copy(
            update={"state": state}
        )
        upload(recovery, entry, "unfinished")
    before = runtime.database.snapshot(context)
    with pytest.raises(ResearchError, match="execution must close"):
        activate_recovery(runtime, context, request)
    assert runtime.database.snapshot(context) == before
    closed(recovery, not_started=True)
    assert activate_recovery(runtime, context, request)["disposition"] == "accepted"


@pytest.mark.parametrize("corruption", ["settlement", "account", "closure", "lease"])
def test_inconsistent_closure_or_accounting_cannot_activate(recovery, corruption):
    closed(recovery)
    runtime = recovery.runtime
    context, request = intent(runtime)
    with runtime.database.connect() as connection:
        if corruption == "settlement":
            connection.execute("DELETE FROM budget_settlements")
        elif corruption == "account":
            connection.execute(
                "UPDATE budgets SET payload=json_set(payload,'$.consumed','91') "
                "WHERE dimension='attempts'"
            )
        elif corruption == "closure":
            connection.execute("UPDATE records SET state='mutable' WHERE kind='attempt_closure'")
        else:
            connection.execute("UPDATE records SET state='active' WHERE kind='acceptance_lease'")
    with pytest.raises(ResearchError):
        activate_recovery(runtime, context, request)
    assert (
        recovery.operator.get("workspace_gate", runtime.identity.workspace_id)["state"]
        == "recovering"
    )


@pytest.mark.parametrize("legacy_lease", [False, True])
def test_accepted_training_with_queued_evaluation_preserves_work_and_funding(
    pipeline, tmp_path, legacy_lease
):
    attempt = pipeline.worker.claim(pipeline.ready())
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    if legacy_lease:
        # Pre-lease source history has accepted results and settlements but no lease row.
        with pipeline.runtime.database.connect() as connection:
            connection.execute("DELETE FROM records WHERE kind='acceptance_lease'")
    trial = pipeline.client.get("trial", pipeline.trial["id"])
    assert trial["state"] == "running" and trial["data"]["resource_status"] == "active"
    queued = pipeline.client.get("job", pipeline.ready()["id"])
    runtime, context, bundle, claim = copy_pipeline(pipeline, tmp_path)
    admit_recovery(runtime, context, bundle, claim)
    client = ResearchClient(LocalTransport(runtime))
    assert client.get("campaign", pipeline.campaign["id"])["state"] == "pausing"
    budget = client.budget(pipeline.campaign["id"])
    context, request = intent(runtime)
    activate_recovery(runtime, context, request)
    assert client.get("campaign", pipeline.campaign["id"])["state"] == "paused"
    assert client.get("job", queued["id"]) == queued
    assert client.get("trial", trial["id"]) == trial
    assert client.budget(pipeline.campaign["id"])["accounts"] == budget["accounts"]
    assert (
        restore_inactive(bundle, runtime.root, restore_id=request.restore_id).restore_id
        == request.restore_id
    )


def test_pending_original_turn_blocks_and_retirement_keeps_driver_fenced(saved):
    context, request = intent(saved.runtime)
    with pytest.raises(ResearchError, match="driver is not closed"):
        activate_recovery(saved.runtime, context, request)
    close(saved)
    activate_recovery(saved.runtime, context, request)
    assert saved.client.get("orchestration_binding", saved.campaign_id)["state"] == "fenced"


def test_activation_event_failure_rolls_back_gate_and_receipt(recovery, monkeypatch):
    closed(recovery)
    context, request = intent(recovery.runtime)
    before = recovery.runtime.database.snapshot(context)
    original = Transaction.event

    def fail(self, name, *args, **kwargs):
        if name == "workspace.recovery_activated":
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Fixture commit failure")
        return original(self, name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "event", fail)
        with pytest.raises(ResearchError, match="Fixture"):
            activate_recovery(recovery.runtime, context, request)
    assert recovery.runtime.database.snapshot(context) == before
    assert activate_recovery(recovery.runtime, context, request)["disposition"] == "accepted"


@pytest.mark.parametrize("change", ["private", "record", "upload"])
def test_changes_during_byte_verification_reject_entire_activation(recovery, monkeypatch, change):
    closed(recovery)
    runtime = recovery.runtime
    context, request = intent(runtime)
    original = runtime.artifacts.store.verify
    changed = False

    def verify(workspace, blob):
        nonlocal changed
        result = original(workspace, blob)
        if not changed:
            changed = True
            if change == "private":
                write_private(runtime.root / "dispatch.key", b"x" * 32)
            else:
                with LocalRuntime.open(runtime.root).database.connect() as connection:
                    if change == "record":
                        connection.execute(
                            "UPDATE records SET revision=revision+1 WHERE kind='trial'"
                        )
                    else:
                        connection.execute("UPDATE records SET state='cleanup' WHERE kind='upload'")
        return result

    monkeypatch.setattr(runtime.artifacts.store, "verify", verify)
    with pytest.raises(ResearchError):
        activate_recovery(runtime, context, request)
    assert (
        recovery.operator.get("workspace_gate", runtime.identity.workspace_id)["state"]
        == "recovering"
    )
    with runtime.database.connect() as connection:
        assert (
            connection.execute("SELECT 1 FROM records WHERE kind='recovery_activation'").fetchone()
            is None
        )


def test_bare_and_wire_activation_cannot_mint_native_proof(recovery):
    context, request = intent(recovery.runtime)
    assert "workspace.activate_recovery" not in COMMANDS
    touched = []
    with pytest.raises(ResearchError, match="Native activation proof"):
        recovery.runtime.database.execute(
            context,
            "workspace.activate_recovery",
            request.model_dump(mode="json"),
            "*",
            lambda transaction: touched.append(transaction),
        )
    assert not touched
    with pytest.raises(ValidationError):
        CommandContext.model_validate(
            {**context.model_dump(mode="json"), "_recovery_activation": {}}
        )


def test_cli_uses_explicit_native_target(recovery, tmp_path, capsys, monkeypatch):
    closed(recovery)
    context, request = intent(recovery.runtime)
    payload = tmp_path / "activation.json"
    payload.write_bytes(canonical_bytes(request.model_dump(mode="json")))
    monkeypatch.setattr(
        "research_swarm.cli._client", lambda args: pytest.fail("Native CLI fell through")
    )
    assert (
        main(
            [
                "backup",
                "activate",
                "--file",
                str(payload),
                "--revision",
                str(context.expected_revision),
            ]
        )
        == 2
    )
    capsys.readouterr()
    assert (
        main(
            [
                "--state-dir",
                str(recovery.runtime.root),
                "backup",
                "activate",
                "--file",
                str(payload),
                "--revision",
                str(context.expected_revision),
            ]
        )
        == 0
    )
    assert '"disposition":"accepted"' in capsys.readouterr().out


def test_dead_reader_pins_block_until_exact_reader_custody_is_reconciled(recovery, crashed):
    closed(recovery, not_started=True)
    context, request = intent(recovery.runtime)
    with pytest.raises(ResearchError, match="Artifact operations"):
        activate_recovery(recovery.runtime, context, request)
    recovery.operator.command(
        "artifact.reconcile_reader_session",
        ReaderSessionTarget(session_id=crashed),
        idempotency_key="close-original-reader",
    )
    activate_recovery(recovery.runtime, context, request)
    assert (
        recovery.operator.get("workspace_gate", recovery.runtime.identity.workspace_id)["state"]
        == "active"
    )


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_open_child_funding_survives_activation(pipeline, tmp_path):
    created = create(pipeline)
    allocation = pipeline.client.get("funding_projection", created["campaign"]["id"])
    runtime, context, bundle, claim = copy_pipeline(pipeline, tmp_path)
    admit_recovery(runtime, context, bundle, claim)
    client = ResearchClient(LocalTransport(runtime))
    budget = client.budget(pipeline.campaign["id"])["accounts"]
    context, request = intent(runtime)
    activate_recovery(runtime, context, request)
    assert client.get("funding_projection", created["campaign"]["id"]) == allocation
    assert client.budget(pipeline.campaign["id"])["accounts"] == budget
    assert any(item["reserved"] != "0" for item in budget)


@pytest.mark.parametrize("pipeline", [options()], indirect=True)
@pytest.mark.parametrize("scored", [False, True])
def test_queued_and_consumed_confirmation_exposures_keep_exact_accounts(pipeline, tmp_path, scored):
    plan_request, campaign = setup_plan(pipeline)
    plan = ConfirmationData.model_validate(
        create_plan(pipeline, plan_request, campaign)["data"]
    ).plan
    if scored:
        member = plan.bindings[0]
        trial = pipeline.client.get("trial", member.trial_id)
        train = pipeline.client.get("job", trial["data"]["job_ids"][0])
        pipeline.worker.execute(pipeline.worker.claim(train)["id"])
        job = pipeline.client.get("job", member.scoring_job_id)
        pipeline.client.command(
            "evaluation.authorize",
            AuthorizeEvaluation(job_id=job["id"], evaluator_id=plan_request.evaluator_id),
            idempotency_key="scoring",
            expected_revision=job["revision"],
        )
        job = pipeline.client.get("job", job["id"])
        pipeline.worker.execute(pipeline.worker.claim(job)["id"])
    runtime, context, bundle, claim = copy_pipeline(pipeline, tmp_path)
    admit_recovery(runtime, context, bundle, claim)
    client = ResearchClient(LocalTransport(runtime))
    budget = client.budget(pipeline.campaign["id"])["accounts"]
    context, request = intent(runtime)
    activate_recovery(runtime, context, request)
    assert client.budget(pipeline.campaign["id"])["accounts"] == budget
    exposure = next(item for item in budget if item["dimension"] == "evaluation_exposures")
    assert exposure["reserved"] == ("9" if scored else "10")
    assert exposure["consumed"] == ("1" if scored else "0")


@pytest.mark.parametrize("phase", ["transaction", "committed"])
def test_real_process_exit_at_activation_has_one_recoverable_commit(recovery, phase):
    closed(recovery, not_started=True)
    runtime = recovery.runtime
    context, request = intent(runtime)
    script = """
import os, sys
from pathlib import Path
from research_swarm.checkpoints import activation
from research_swarm.domain.backup import ActivateRecovery
from research_swarm.domain.identity import CommandContext
from research_swarm.runtime import LocalRuntime
runtime = LocalRuntime.open(Path(sys.argv[1]))
context = CommandContext.model_validate_json(sys.argv[2])
request = ActivateRecovery.model_validate_json(sys.argv[3])
if sys.argv[4] == 'transaction':
    original = activation._activate
    def interrupt(transaction, record):
        original(transaction, record)
        os._exit(71)
    activation._activate = interrupt
activation.activate_recovery(runtime, context, request)
os._exit(72)
"""
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(runtime.root),
            context.model_dump_json(),
            request.model_dump_json(),
            phase,
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == (71 if phase == "transaction" else 72), completed.stderr
    restarted = LocalRuntime.open(runtime.root)
    gate = restarted.database.read_record(context, "workspace_gate", runtime.identity.workspace_id)
    assert gate["state"] == ("recovering" if phase == "transaction" else "active")
    reply = activate_recovery(restarted, context, request)
    assert activate_recovery(restarted, context, request) == reply
    with restarted.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='recovery_activation'"
            ).one()[0]
            == 1
        )
