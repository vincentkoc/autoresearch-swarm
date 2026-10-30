"""Recheck exact recovery capabilities at the transaction and byte-I/O boundaries."""

from collections.abc import Mapping
from typing import Any

from research_swarm.domain.backup import RecoveryAdmission, RestoreReceipt, WorkspaceSeal
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.state.sql import SqlSession, StoreIdentity

ROOT_OPERATIONS = frozenset(
    {
        "worker.begin_upload",
        "worker.output",
        "worker.upload_part",
        "worker.finalize_upload",
        "worker.cleanup_upload",
        "worker.abort_upload",
        "worker.submit_evidence",
        "attempt.reconcile_closure",
    }
)


def require_recovery_operation(
    connection: SqlSession,
    context: CommandContext,
    authority: StoreIdentity,
    operation: str,
    payload: Mapping[str, Any],
    request_digest: str,
) -> None:
    proof = context._recovery_operation
    if (
        proof is None
        or proof.authority != (authority.backend, authority.location)
        or proof.actor_id != context.principal.id
        or context._drive_guard is not None
        or proof.root_operation not in ROOT_OPERATIONS
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery operation proof is required")
    workspace = context.principal.workspace_id

    def record(kind: str, identifier: str) -> tuple[str, int, dict[str, Any]]:
        row = connection.execute(
            "SELECT state,revision,payload FROM records WHERE workspace_id=? AND kind=? AND id=?",
            (workspace, kind, identifier),
        ).fetchone()
        if row is None:
            raise ResearchError(ErrorCode.STALE_FENCE, "Recovery record is absent")
        return row["state"], row["revision"], dict(strict_json_loads(row["payload"]))

    state, revision, gate_data = record("workspace_gate", workspace)
    restored_state, _, restored_data = record("restoration", proof.restore_id)
    admitted_state, _, admitted_data = record("recovery_admission", proof.restore_id)
    receipt = RestoreReceipt.model_validate(restored_data)
    admitted = RecoveryAdmission.model_validate(admitted_data)
    seal = WorkspaceSeal.model_validate(gate_data)
    current = connection.execute(
        "SELECT controller_id,control_epoch FROM workspaces WHERE id=?", (workspace,)
    ).one()
    if (
        state != "recovering"
        or revision != proof.gate_revision
        or restored_state != "inactive"
        or admitted_state != "immutable"
        or receipt.digest != proof.restoration_digest
        or record_digest(
            "research.recovery-admission-envelope.v1", admitted.model_dump(mode="json")
        )
        != proof.admission_digest
        or admitted.restoration_digest != receipt.digest
        or (receipt.workspace_id, receipt.controller_id, receipt.target_control_epoch)
        != (workspace, current["controller_id"], current["control_epoch"])
        or current["control_epoch"] != proof.control_epoch
        or (seal.backup_id, seal.control_epoch) != (receipt.backup_id, receipt.source_control_epoch)
        or proof.attempt_id not in receipt.unresolved_attempt_ids
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Recovery evidence authority changed")
    owner = connection.execute(
        "SELECT 1 FROM records WHERE workspace_id=? AND kind='orchestration_owner' AND id=?",
        (workspace, context.principal.id),
    ).fetchone()
    if owner is not None:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Drivers cannot submit recovery evidence"
        )
    session_state, _, session = record("worker_session", proof.session_id)
    _, _, attempt = record("attempt", proof.attempt_id)
    binding = session["binding"]
    if (
        session_state != "immutable"
        or session["principal_id"] != proof.worker_principal_id
        or binding["workspace_id"] != workspace
        or binding["worker_id"] != proof.worker_id
        or binding["worker_session_id"] != proof.session_id
        or any(attempt["dispatch"].get(key) != value for key, value in binding.items())
        or attempt["dispatch"]["attempt_id"] != proof.attempt_id
        or (
            proof.root_operation != "attempt.reconcile_closure"
            and proof.actor_id != proof.worker_principal_id
        )
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery session binding differs")
    if proof.derived_request_digest is None:
        if operation != proof.root_operation or request_digest != proof.root_request_digest:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery request differs")
        return
    if request_digest != proof.derived_request_digest:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Derived recovery request differs")
    if operation == "artifact.unpin" and proof.cleanup_pin_id is not None:
        _, _, retained = record("artifact_reference", str(payload.get("reference_id")))
        retention = retained["retention"]
        if (
            retained["actor_id"] == proof.actor_id
            and retention["owner_kind"] == "artifact_read"
            and retention["owner_id"] == proof.cleanup_pin_id
        ):
            return
    if proof.cleanup_pin_id is not None:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery cleanup pin differs")
    artifact_id = proof.artifact_id
    if proof.upload_id is not None:
        _, _, upload = record("worker_upload", proof.upload_id)
        if (
            upload["attempt_id"] != proof.attempt_id
            or upload["binding"] != binding
            or upload["role"] != "journal"
            or upload["output_name"] is not None
        ):
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery upload differs")
        if upload["artifact"] is not None:
            artifact_id = upload["artifact"]["id"]
    if operation == "artifact.pin" and artifact_id is not None:
        reference = payload.get("reference")
        if isinstance(reference, Mapping) and reference.get("id") == artifact_id:
            return
    if payload.get("upload_id") == proof.upload_id and proof.upload_id is not None:
        if (
            operation == "worker.finish_upload_abort"
            and proof.root_operation == "worker.abort_upload"
        ):
            return
        if operation == "worker.finish_upload_cleanup" and proof.root_operation in {
            "worker.finalize_upload",
            "worker.cleanup_upload",
        }:
            return
    raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery cannot derive this operation")
