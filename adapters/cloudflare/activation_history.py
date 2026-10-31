"""Verify completed target history without freezing current research/provider state."""

from dataclasses import dataclass

from research_swarm.control.hosted_activation_proofs import verify_hosted_activation
from research_swarm.domain.backup import RecoveryAdmission, RestoreReceipt, WorkspaceSeal
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.recovery_runtime import (
    HostedActivationRuntime,
    HostedRecoveryActivation,
    RecoveryRuntimePlan,
    RuntimePreparationReceipt,
)
from research_swarm.state.recovery_elections import retained_election

if __package__:
    from .checkpoint_anchor import read_anchor_marker
    from .checkpoint_history import history_record, saved_import
else:
    from checkpoint_anchor import read_anchor_marker
    from checkpoint_history import history_record, saved_import


@dataclass(frozen=True)
class CompletedActivation:
    activation: HostedRecoveryActivation
    restoration: RestoreReceipt
    runtime: HostedActivationRuntime


def require_completed_activation(sql, manifest, binding, expected_digest, *, crypto):
    if binding is None:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Protected recovery binding is required"
        )
    intent, config = binding.intent, manifest.configuration
    scope = config.authority
    if (
        intent.target_backend != "cloudflare-do"
        or intent.target_location != config.store_location
        or intent.target_deployment_digest != manifest.digest
        or intent.operator_id != manifest.provisioning_actor_id
        or (scope.workspace_id, scope.controller_id, scope.control_epoch)
        != (
            binding.expected_source.workspace_id,
            binding.expected_source.controller_id,
            binding.expected_source.control_epoch + 1,
        )
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Completed recovery deployment differs")
    receipt, _ = saved_import(sql, intent)
    workspace, restore_id = receipt.workspace_id, receipt.restore_id
    if (workspace, receipt.controller_id, receipt.target_control_epoch) != (
        scope.workspace_id,
        scope.controller_id,
        scope.control_epoch,
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Completed recovery authority differs")
    original = RestoreReceipt.model_validate_json(
        history_record(sql, workspace, "restoration", restore_id, state="inactive")["payload"]
    )
    admission = RecoveryAdmission.model_validate_json(
        history_record(sql, workspace, "recovery_admission", restore_id)["payload"]
    )
    activation = HostedRecoveryActivation.model_validate_json(
        history_record(sql, workspace, "recovery_activation", restore_id)["payload"]
    )
    plan = RecoveryRuntimePlan.model_validate_json(
        history_record(sql, workspace, "recovery_runtime_plan", restore_id)["payload"]
    )
    prepared = RuntimePreparationReceipt.model_validate_json(
        history_record(sql, workspace, "recovery_runtime_preparation", restore_id)["payload"]
    )
    expected_plan = RecoveryRuntimePlan(
        restore_id=restore_id,
        deployment_digest=manifest.digest,
        configuration_digest=config.digest,
        authority=scope,
        source=config.source,
        credentials=tuple(item.model_dump(mode="json") for item in manifest.credentials),
    )
    if (
        original != receipt
        or activation.digest != expected_digest
        or plan != expected_plan
        or prepared.plan_digest != plan.digest
        or prepared.restoration_digest != receipt.digest
        or prepared.admission_digest
        != record_digest(
            "research.recovery-admission-envelope.v1", admission.model_dump(mode="json")
        )
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Completed recovery evidence differs")
    runtime = HostedActivationRuntime(
        plan=plan,
        preparation_digest=prepared.digest,
        provider_revision=prepared.provider_revision,
        provider=prepared.provider,
    )
    anchor = read_anchor_marker(sql, workspace_id=workspace, custody_id=receipt.source_custody_id)
    elected = retained_election(sql, receipt, admission)
    if (
        anchor is None
        or elected is None
        or anchor.source_authority != binding.expected_source
        or anchor.custody_pin != binding.expected_custody
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Completed recovery source custody differs")
    verify_hosted_activation(
        activation,
        admission,
        receipt,
        elected,
        runtime=runtime,
        expected_intent=intent,
        expected_source=anchor,
        crypto_provider=crypto,
    )
    current = sql.execute(
        "SELECT controller_id,control_epoch FROM workspaces WHERE id=? "
        "AND typeof(controller_id)='text' AND length(CAST(controller_id AS BLOB))=36 "
        "AND typeof(control_epoch)='integer' AND control_epoch BETWEEN 1 AND 9007199254740991",
        (workspace,),
    ).fetchone()
    gate = sql.execute(
        "SELECT state,revision,payload FROM records WHERE workspace_id=? AND kind='workspace_gate' "
        "AND id=? AND state IN ('active','sealed') AND typeof(revision)='integer' "
        "AND typeof(payload)='text' AND length(CAST(payload AS BLOB))<=1048576",
        (workspace, workspace),
    ).fetchone()
    if (
        gate is None
        or current is None
        or (current[0], current[1]) != (scope.controller_id, scope.control_epoch)
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Completed recovery phase differs")
    seal = WorkspaceSeal.model_validate_json(gate["payload"])
    original_seal = WorkspaceSeal.model_validate_json(
        history_record(sql, workspace, "workspace_seal", receipt.backup_id)["payload"]
    )
    if (
        original_seal.backup_id != receipt.backup_id
        or (original_seal.workspace_id, original_seal.controller_id, original_seal.control_epoch)
        != (workspace, receipt.controller_id, receipt.source_control_epoch)
        or (
            gate["state"] == "active"
            and (gate["revision"] != activation.gate_revision + 1 or seal != original_seal)
        )
        or (
            gate["state"] == "sealed"
            and (
                gate["revision"] <= activation.gate_revision + 1
                or seal.control_epoch != scope.control_epoch
                or seal.workspace_id != workspace
                or seal.controller_id != scope.controller_id
            )
        )
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Completed recovery seal history differs")
    if gate["state"] == "sealed":
        recorded_seal = WorkspaceSeal.model_validate_json(
            history_record(sql, workspace, "workspace_seal", seal.backup_id)["payload"]
        )
        if recorded_seal != seal:
            raise ResearchError(ErrorCode.STALE_FENCE, "Completed recovery current seal differs")
    retired = activation.retired_credential_ids
    for offset in range(0, len(retired), 128):
        identifiers = retired[offset : offset + 128]
        placeholders = ",".join("?" for _ in identifiers)
        rows = sql.execute(
            f"SELECT id FROM credentials WHERE workspace_id=? AND id IN ({placeholders}) "
            "AND typeof(revoked)='integer' AND revoked=1",
            (workspace, *identifiers),
        ).fetchall()
        if {row[0] for row in rows} != set(identifiers):
            raise ResearchError(ErrorCode.STALE_FENCE, "Retired recovery credential changed")
    return CompletedActivation(activation, receipt, runtime)
