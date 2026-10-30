"""Verify activated restore history without requiring its old research to remain paused."""

from research_swarm.control.recovery_proofs import verify_activation
from research_swarm.domain.backup import (
    RecoveryActivation,
    RecoveryAdmission,
    RestoreReceipt,
    WorkspaceSeal,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Database
from research_swarm.state.sql import SqlSession


def verify_activated(database: Database, receipt: RestoreReceipt) -> bool:
    def read(connection: SqlSession) -> tuple[RecoveryActivation, RecoveryAdmission] | None:
        row = connection.execute(
            "SELECT state,payload FROM records WHERE workspace_id=? "
            "AND kind='recovery_activation' AND id=?",
            (receipt.workspace_id, receipt.restore_id),
        ).fetchone()
        if row is None:
            return None
        activation = RecoveryActivation.model_validate_json(row["payload"])
        admission = connection.execute(
            "SELECT state,payload FROM records WHERE workspace_id=? "
            "AND kind='recovery_admission' AND id=?",
            (receipt.workspace_id, receipt.restore_id),
        ).one()
        original = connection.execute(
            "SELECT state,payload FROM records WHERE workspace_id=? "
            "AND kind='restoration' AND id=?",
            (receipt.workspace_id, receipt.restore_id),
        ).one()
        gate = connection.execute(
            "SELECT state,revision,payload FROM records WHERE workspace_id=? "
            "AND kind='workspace_gate' AND id=?",
            (receipt.workspace_id, receipt.workspace_id),
        ).one()
        authority = connection.execute(
            "SELECT controller_id,control_epoch FROM workspaces WHERE id=?",
            (receipt.workspace_id,),
        ).one()
        seal = WorkspaceSeal.model_validate_json(gate["payload"])
        if (
            row["state"] != "immutable"
            or admission["state"] != "immutable"
            or original["state"] != "inactive"
            or RestoreReceipt.model_validate_json(original["payload"]) != receipt
            or (authority["controller_id"], authority["control_epoch"])
            != (receipt.controller_id, receipt.target_control_epoch)
            or gate["state"] not in {"active", "sealed"}
            or (
                gate["state"] == "active"
                and (
                    gate["revision"] != activation.gate_revision + 1
                    or (seal.backup_id, seal.control_epoch)
                    != (receipt.backup_id, receipt.source_control_epoch)
                )
            )
            or (
                gate["state"] == "sealed"
                and (
                    gate["revision"] <= activation.gate_revision + 1
                    or seal.control_epoch != receipt.target_control_epoch
                )
            )
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Activated restore lifecycle differs")
        return activation, RecoveryAdmission.model_validate_json(admission["payload"])

    result = database.store.read(read)
    if result is None:
        return False
    verify_activation(*result, receipt)
    return True
