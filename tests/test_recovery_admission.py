"""One verified native recovery transition preserves unresolved research custody."""

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import ValidationError

from research_swarm.api.http import create_app
from research_swarm.checkpoints import admission as admission_module
from research_swarm.checkpoints.admission import admit_recovery, verify_admission
from research_swarm.checkpoints.backup import export_backup
from research_swarm.checkpoints.claims import claim_restore, request_restore_claim
from research_swarm.checkpoints.restore import restore_inactive
from research_swarm.cli import main
from research_swarm.control.dispatch import COMMANDS
from research_swarm.control.models import TrialData
from research_swarm.control.results import PublishAttempt
from research_swarm.control.worker_records import AttemptData
from research_swarm.domain.backup import RecoveryAdmission, RestoreReceipt
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.private_files import read_private, write_private
from research_swarm.workers.journal import JournalState
from tests.test_drive_closures import report_action
from tests.test_http_delivery import serving
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_native_backup import sealed_runtime as sealed_runtime
from tests.test_native_restore_claim import election as election
from tests.test_orchestration_turns import bind, prepare
from tests.test_orchestration_waits import record, report
from tests.test_workspace_seal import seal_request


def prepared(election):
    source, context, bundle, destination, _, request = election
    claim = claim_restore(source, context, bundle, request)
    runtime = LocalRuntime.open(destination)
    context = CommandContext(principal=runtime.principal(), idempotency_key="admission")
    return runtime, context, bundle, claim


def copy_pipeline(pipeline, tmp_path):
    runtime = pipeline.runtime
    context = CommandContext(
        principal=runtime.principal(), idempotency_key="seal", expected_revision=0
    )
    seal = seal_request(runtime.database, context)
    runtime.service.seal_workspace(context, seal)
    bundle = tmp_path / "bundle"
    export_backup(runtime, context, bundle, backup_id=seal.backup_id)
    target = tmp_path / "target"
    receipt = restore_inactive(bundle, target, restore_id=new_id())
    request = request_restore_claim(bundle, target, restore_id=receipt.restore_id)
    claim = claim_restore(runtime, context, bundle, request)
    destination = LocalRuntime.open(target)
    return (
        destination,
        CommandContext(principal=destination.principal(), idempotency_key="admission"),
        bundle,
        claim,
    )


def test_admission_is_immutable_inactive_and_replayable_after_bundle_loss(election):
    runtime, context, bundle, claim = prepared(election)
    receipt = RestoreReceipt.model_validate_json((runtime.root / "restore.json").read_bytes())
    reply = admit_recovery(runtime, context, bundle, claim)
    admitted = RecoveryAdmission.model_validate(reply["result"]["data"])
    verify_admission(admitted, receipt)
    assert admitted.signed_claim == claim
    assert runtime.database.snapshot(context)["control_epoch"] == receipt.target_control_epoch
    assert (
        runtime.database.read_record(context, "workspace_gate", receipt.workspace_id)["state"]
        == "recovering"
    )
    assert restore_inactive(bundle, runtime.root, restore_id=receipt.restore_id) == receipt
    (bundle / "manifest.json").unlink()
    assert admit_recovery(LocalRuntime.open(runtime.root), context, bundle, claim) == reply
    with pytest.raises(ResearchError):
        runtime.credentials.issue(runtime.principal(), runtime.principal())
    with pytest.raises(ResearchError):
        runtime.database.acknowledge_notification(runtime.database.pending_notifications()[0].id)


def test_concurrent_exact_admission_returns_one_receipt(election):
    runtime, context, bundle, claim = prepared(election)
    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(
            pool.map(
                lambda _: admit_recovery(LocalRuntime.open(runtime.root), context, bundle, claim),
                range(2),
            )
        )
    assert replies[0] == replies[1]
    with runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) AS n FROM records WHERE kind='recovery_admission'"
            ).one()["n"]
            == 1
        )


@pytest.mark.parametrize("corruption", ["custody", "source_signature", "ledger"])
def test_invalid_preflight_preserves_inactive_target(election, corruption):
    runtime, context, bundle, claim = prepared(election)
    if corruption == "custody":
        path = runtime.root / "restore-custody.json"
        data = strict_json_loads(path.read_bytes())
        data["private_key"] = Ed25519PrivateKey.generate().private_bytes_raw().hex()
        write_private(path, canonical_bytes(data))
    elif corruption == "source_signature":
        claim = claim.model_copy(update={"signature": "0" * 128})
    else:
        with runtime.database.connect() as connection:
            connection.execute("DELETE FROM commands WHERE idempotency_key='seal'")
    before = runtime.database.snapshot(context)
    with pytest.raises(ResearchError):
        admit_recovery(runtime, context, bundle, claim)
    assert runtime.database.snapshot(context) == before


def test_other_inactive_target_cannot_admit_elected_claim(election, tmp_path):
    _, context, bundle, claim = prepared(election)
    target = tmp_path / "other"
    restore_inactive(bundle, target, restore_id=new_id())
    runtime = LocalRuntime.open(target)
    with pytest.raises(ResearchError):
        admit_recovery(runtime, context, bundle, claim)
    assert (
        runtime.database.read_record(context, "workspace_gate", runtime.identity.workspace_id)[
            "state"
        ]
        == "restoring"
    )


def test_bare_operation_and_wire_context_cannot_bypass_native_verification(election):
    runtime, context, _, claim = prepared(election)
    assert "workspace.begin_recovery" not in COMMANDS
    touched = []
    with pytest.raises(ResearchError) as error:
        runtime.database.execute(
            context,
            "workspace.begin_recovery",
            claim.model_dump(mode="json"),
            "*",
            lambda tx: touched.append(tx),
        )
    assert error.value.code == ErrorCode.AUTHORIZATION_DENIED and touched == []
    with pytest.raises(ValidationError):
        CommandContext.model_validate(
            {**context.model_dump(mode="json"), "_recovery_guard": {"watermark": 0}}
        )


def test_http_cannot_invoke_native_admission(election):
    runtime, context, _, claim = prepared(election)
    before = runtime.database.snapshot(context)
    token = read_private(runtime.root, "operator.token", limit=256).decode()
    with (
        serving(create_app(runtime.service, runtime.credentials)) as url,
        httpx.Client(base_url=url, headers={"Authorization": "Bearer " + token}) as client,
    ):
        refused = client.post(
            "/v1/commands/workspace.begin_recovery",
            json=claim.model_dump(mode="json"),
            headers={"Idempotency-Key": "wire-bypass"},
        )
        assert refused.status_code == 404
        assert refused.json() == {"detail": "Not Found"}
    assert runtime.database.snapshot(context) == before


@pytest.mark.parametrize("ready", [False, True])
def test_recovery_fences_waits_and_drains_without_resuming_provider(pipeline, tmp_path, ready):
    drivers = bind(pipeline)
    wait = record(pipeline, drivers)["wait"]
    if ready:
        report(pipeline)
    runtime, context, bundle, claim = copy_pipeline(pipeline, tmp_path)
    owner = runtime.database.read_record(context, "orchestration_owner", drivers.actor.id)
    admit_recovery(runtime, context, bundle, claim)
    campaign = pipeline.campaign["id"]
    assert runtime.database.read_record(context, "campaign", campaign)["state"] == "paused"
    assert (
        runtime.database.read_record(context, "orchestration_binding", campaign)["state"]
        == "fenced"
    )
    assert runtime.database.read_record(context, "orchestration_owner", drivers.actor.id) == owner
    waited = runtime.database.read_record(context, "orchestration_wait", wait["id"])
    assert waited["state"] == "superseded"
    if not ready:
        with runtime.database.connect() as connection:
            rows = connection.execute(
                "SELECT payload FROM records WHERE kind='wait_readiness'"
            ).fetchall()
        assert rows and strict_json_loads(rows[-1]["payload"])["cause"] == "fenced"


@pytest.mark.parametrize("failure", [OSError, ResearchError])
def test_admission_preserves_pending_turn_and_rolls_back_all_fences(
    pipeline, tmp_path, monkeypatch, failure
):
    drivers = bind(pipeline)
    turn = prepare(pipeline, drivers.control, report_action(pipeline))
    runtime, context, bundle, claim = copy_pipeline(pipeline, tmp_path)
    before = runtime.database.snapshot(context)
    original = admission_module.request_drain_pause

    def fail(transaction, row):
        original(transaction, row)
        if failure is ResearchError:
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Fixture admission interruption")
        raise OSError("Fixture admission interruption")

    with monkeypatch.context() as patch:
        patch.setattr(admission_module, "request_drain_pause", fail)
        with pytest.raises(failure, match="interruption"):
            admit_recovery(runtime, context, bundle, claim)
    assert runtime.database.snapshot(context) == before
    admit_recovery(runtime, context, bundle, claim)
    assert runtime.database.read_record(context, "drive_turn", turn["id"]) == turn
    binding = runtime.database.read_record(
        context, "orchestration_binding", pipeline.campaign["id"]
    )
    assert binding["state"] == "fenced" and binding["data"]["pending_turn_id"] == turn["id"]


def test_rejected_driver_admission_does_not_poison_operator_retry(pipeline, tmp_path):
    drivers = bind(pipeline)
    runtime, context, bundle, claim = copy_pipeline(pipeline, tmp_path)
    before = runtime.database.snapshot(context)
    driver = CommandContext(principal=drivers.actor, idempotency_key="driver-admission")
    with pytest.raises(ResearchError, match="Dedicated drivers"):
        admit_recovery(runtime, driver, bundle, claim)
    assert runtime.database.snapshot(context) == before
    with runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM commands WHERE idempotency_key=?",
                (driver.idempotency_key,),
            ).fetchone()
            is None
        )
    assert admit_recovery(runtime, context, bundle, claim)["disposition"] == "accepted"


@pytest.mark.parametrize("unknown", [False, True])
def test_unresolved_native_attempt_keeps_budget_and_campaign_pausing(pipeline, tmp_path, unknown):
    bind(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    if unknown:
        journal = pipeline.worker._prepare(
            AttemptData.model_validate(attempt["data"]),
            TrialData.model_validate(pipeline.trial["data"]),
        )
        path = tmp_path / "unknown.json"
        path.write_text(
            journal.model_copy(update={"state": JournalState.UNKNOWN}).model_dump_json()
        )
        reference = pipeline.worker.client.upload_file(path, idempotency_key="unknown-artifact")
        pipeline.worker.client.command(
            "worker.publish_attempt",
            PublishAttempt(attempt_id=attempt["id"], journal_artifact=reference),
            idempotency_key="unknown",
        )
    runtime, context, bundle, claim = copy_pipeline(pipeline, tmp_path)
    original_attempt = runtime.database.read_record(context, "attempt", attempt["id"])
    with runtime.database.connect() as connection:
        reservations = [
            tuple(row)
            for row in connection.execute("SELECT * FROM budget_reservations ORDER BY id")
        ]
    admit_recovery(runtime, context, bundle, claim)
    assert (
        runtime.database.read_record(context, "campaign", pipeline.campaign["id"])["state"]
        == "pausing"
    )
    assert runtime.database.read_record(context, "attempt", attempt["id"]) == original_attempt
    with runtime.database.connect() as connection:
        assert [
            tuple(row)
            for row in connection.execute("SELECT * FROM budget_reservations ORDER BY id")
        ] == reservations


def test_cli_admission_requires_existing_target_and_replays(election, tmp_path, capsys):
    runtime, _, bundle, claim = prepared(election)
    path = tmp_path / "claim.json"
    path.write_text(claim.model_dump_json())
    args = ["backup", "admit", str(bundle), "--claim-file", str(path)]
    assert main(args) == 2
    assert "explicit restored target" in capsys.readouterr().err
    args = ["--state-dir", str(runtime.root), *args]
    assert main(args) == 0
    first = strict_json_loads(capsys.readouterr().out)
    assert main(args) == 0
    assert strict_json_loads(capsys.readouterr().out) == first


@pytest.mark.parametrize("phase", ["transaction", "committed"])
def test_process_exit_at_admission_recovers_exact_transition(election, phase):
    runtime, _, bundle, claim = prepared(election)
    script = """
import os, sys
from pathlib import Path
from research_swarm.checkpoints.admission import admit_recovery
from research_swarm.checkpoints import admission
from research_swarm.domain.backup import SignedRestoreClaim
from research_swarm.domain.identity import CommandContext
from research_swarm.runtime import LocalRuntime
runtime = LocalRuntime.open(Path(sys.argv[1]))
if sys.argv[4] == 'transaction':
    original = admission._admit
    def interrupt(transaction, record):
        original(transaction, record)
        os._exit(73)
    admission._admit = interrupt
admit_recovery(runtime, CommandContext(principal=runtime.principal(), idempotency_key='admission'),
    Path(sys.argv[2]), SignedRestoreClaim.model_validate_json(sys.argv[3]))
os._exit(73)
"""
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(runtime.root),
            str(bundle),
            claim.model_dump_json(),
            phase,
        ],
        capture_output=True,
        timeout=15,
    )
    assert child.returncode == 73, child.stderr.decode(errors="replace")
    if phase == "committed":
        (bundle / "manifest.json").unlink()
    else:
        assert (
            runtime.database.read_record(
                CommandContext(principal=runtime.principal(), idempotency_key="inspect"),
                "workspace_gate",
                runtime.identity.workspace_id,
            )["state"]
            == "restoring"
        )
    context = CommandContext(principal=runtime.principal(), idempotency_key="admission")
    reply = admit_recovery(runtime, context, bundle, claim)
    assert reply["result"]["id"] == claim.claim.request.restore_id


@pytest.mark.parametrize("defect", ["authority", "request", "watermark"])
def test_native_proof_is_bound_to_exact_authority_request_and_cut(election, monkeypatch, defect):
    runtime, context, bundle, claim = prepared(election)
    original = runtime.database.execute
    before = runtime.database.snapshot(context)

    def stale(verified, operation, payload, scope, handler, **kwargs):
        proof = verified._recovery_guard
        assert proof is not None and operation == "workspace.begin_recovery"
        if defect == "authority":
            verified._recovery_guard = replace(proof, authority=("other", "other"))
        elif defect == "request":
            verified._recovery_guard = replace(proof, request_digest="sha256:" + "0" * 64)
        else:
            with runtime.database.connect() as connection:
                connection.execute("UPDATE workspaces SET next_sequence=next_sequence+1")
        return original(verified, operation, payload, scope, handler, **kwargs)

    monkeypatch.setattr(runtime.database, "execute", stale)
    with pytest.raises(ResearchError):
        admit_recovery(runtime, context, bundle, claim)
    assert runtime.database.snapshot(context)["records"] == before["records"]
