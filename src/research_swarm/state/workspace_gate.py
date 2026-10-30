"""Shared authority admission, including callers that do not use an HTTP server."""

from collections.abc import Mapping
from typing import Any

from research_swarm.domain.backup import BackupCustodyPin, RestoreReceipt, WorkspaceSeal
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.state.sql import SqlSession, StoreIdentity


def require_recovery_admission(
    connection: SqlSession, context: CommandContext, authority: StoreIdentity, request_digest: str
) -> None:
    proof = context._recovery_guard
    if (
        proof is None
        or proof.authority != (authority.backend, authority.location)
        or proof.request_digest != request_digest
    ):
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Native recovery verification is required"
        )
    workspace = context.principal.workspace_id
    current = connection.execute(
        "SELECT controller_id,control_epoch,next_sequence FROM workspaces WHERE id=?",
        (workspace,),
    ).one()
    gate = connection.execute(
        "SELECT state,revision,payload FROM records WHERE workspace_id=? "
        "AND kind='workspace_gate' AND id=?",
        (workspace, workspace),
    ).one()
    restored = connection.execute(
        "SELECT state,payload FROM records WHERE workspace_id=? AND kind='restoration' AND id=?",
        (workspace, proof.restore_id),
    ).one()
    receipt = RestoreReceipt.model_validate_json(restored["payload"])
    seal = WorkspaceSeal.model_validate_json(gate["payload"])
    pin_row = connection.execute(
        "SELECT state,payload FROM records WHERE workspace_id=? AND kind='backup_custody' AND id=?",
        (workspace, receipt.source_custody_id),
    ).one()
    pin = BackupCustodyPin.model_validate_json(pin_row["payload"])
    if (
        gate["state"] != "restoring"
        or gate["revision"] != proof.gate_revision
        or restored["state"] != "inactive"
        or receipt.digest != proof.restoration_digest
        or receipt.target_public_key != proof.target_public_key
        or current["control_epoch"] != proof.control_epoch
        or current["next_sequence"] - 1 != proof.watermark
        or (receipt.workspace_id, receipt.controller_id, receipt.target_control_epoch)
        != (workspace, current["controller_id"], proof.control_epoch)
        or (seal.workspace_id, seal.controller_id, seal.backup_id, seal.control_epoch)
        != (workspace, receipt.controller_id, receipt.backup_id, receipt.source_control_epoch)
        or pin_row["state"] != "immutable"
        or record_digest("research.backup-custody-pin.v1", pin.model_dump(mode="json"))
        != proof.custody_pin_digest
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Verified recovery cut changed")


def require_workspace_mutation(
    connection: SqlSession,
    workspace_id: str,
    operation: str,
    *,
    context: CommandContext | None = None,
    authority: StoreIdentity | None = None,
    payload: Mapping[str, Any] | None = None,
    request_digest: str | None = None,
) -> None:
    row = connection.execute(
        "SELECT state,payload FROM records WHERE workspace_id=? AND kind='workspace_gate' AND id=?",
        (workspace_id, workspace_id),
    ).fetchone()
    if row is None or row["state"] == "active":
        return
    if (
        row["state"] == "recovering"
        and context is not None
        and authority is not None
        and payload is not None
        and request_digest is not None
    ):
        if context._recovery_turn is not None:
            from research_swarm.state.recovery_turns import require_recovery_turn

            require_recovery_turn(
                connection, context, authority, operation, payload, request_digest
            )
            return
        if context._recovery_reader is not None:
            from research_swarm.state.recovery_readers import require_recovery_reader

            require_recovery_reader(
                connection, context, authority, operation, payload, request_digest
            )
            return
        from research_swarm.state.recovery_guard import require_recovery_operation

        require_recovery_operation(
            connection, context, authority, operation, payload, request_digest
        )
        return
    seal = WorkspaceSeal.model_validate(strict_json_loads(row["payload"]))
    if seal.workspace_id != workspace_id:
        raise ResearchError(ErrorCode.STALE_FENCE, "Workspace recovery identity differs")
    if row["state"] == "sealed" and operation in {
        "workspace.seal",
        "workspace.register_backup_custody",
    }:
        pinned = connection.execute(
            "SELECT 1 FROM records WHERE workspace_id=? AND kind='backup_custody' "
            "AND json_extract(payload,'$.control_epoch')=? LIMIT 1",
            (workspace_id, seal.control_epoch),
        ).fetchone()
        if pinned is not None:
            raise ResearchError(ErrorCode.STALE_FENCE, "Backup custody finalized the sealed cut")
        return  # The command still verifies the exact original seal and current revision.
    raise ResearchError(
        ErrorCode.STALE_FENCE,
        "Workspace authority is inactive",
        details={"phase": row["state"], "backup_id": seal.backup_id},
    )
