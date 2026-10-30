"""Bind maintenance to an original fenced turn and its actual target acknowledgment."""

from collections.abc import Mapping
from typing import Any

from research_swarm.domain.backup import RecoveryAdmission
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.orchestration import DriveTurn, OrchestrationBinding, OrchestrationOwner
from research_swarm.state.database import JsonObject, Transaction
from research_swarm.state.drive_closures import target_acknowledgment
from research_swarm.state.recovery_guard import require_recovery_authority
from research_swarm.state.sql import SqlSession, StoreIdentity

OPERATIONS = frozenset({"orchestration.reconcile_turn", "orchestration.close_turn"})


def turn_scope(transaction: Transaction, restore_id: str, turn_id: str) -> JsonObject:
    row = transaction.get("drive_turn", turn_id)
    turn = DriveTurn.model_validate(row["data"])
    binding_row = transaction.get("orchestration_binding", turn.campaign_id)
    binding = OrchestrationBinding.model_validate(binding_row["data"])
    owner_row = transaction.get("orchestration_owner", turn.actor_id)
    owner = OrchestrationOwner.model_validate(owner_row["data"])
    admitted = RecoveryAdmission.model_validate(
        transaction.get("recovery_admission", restore_id)["data"]
    )
    if (
        turn.campaign_id not in admitted.orchestration_campaign_ids
        or binding_row["state"] != "fenced"
        or owner_row["state"] != "assigned"
        or binding.workspace_id != transaction.workspace_id
        or binding.campaign_id != turn.campaign_id
        or binding.epoch != turn.epoch
        or binding.configuration_digest != turn.configuration_digest
        or binding.routes.get(turn.route) != turn.actor_id
        or (owner.campaign_id, owner.epoch, owner.route, owner.actor_id)
        != (turn.campaign_id, turn.epoch, turn.route, turn.actor_id)
        or row["state"] not in {"prepared", "settled", "closed_unacknowledged"}
        or (row["state"] == "prepared" and binding.pending_turn_id != turn_id)
        or (row["state"] != "prepared" and binding.pending_turn_id == turn_id)
    ):
        raise ResearchError(
            ErrorCode.STALE_FENCE, "Recovery turn differs from its original fenced owner"
        )
    return {
        "turn": row,
        "binding": binding_row,
        "owner": owner_row,
        "acknowledgment": target_acknowledgment(transaction, turn),
    }


def scope_digest(scope: JsonObject) -> str:
    return record_digest("research.recovery-turn-scope.v1", scope)


def require_recovery_turn(
    connection: SqlSession,
    context: CommandContext,
    authority: StoreIdentity,
    operation: str,
    payload: Mapping[str, Any],
    request_digest: str,
) -> None:
    guard = context._recovery_turn
    if (
        guard is None
        or operation not in OPERATIONS
        or operation != guard.operation
        or guard.request_digest != request_digest
        or payload.get("turn_id") != guard.turn_id
        or context._recovery_operation is not None
        or context._recovery_reader is not None
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery turn proof is required")
    require_recovery_authority(connection, context, authority, guard)
    scope = turn_scope(Transaction(connection, context), guard.restore_id, guard.turn_id)
    if scope_digest(scope) != guard.scope_digest:
        raise ResearchError(ErrorCode.STALE_FENCE, "Recovery turn or target acknowledgment changed")
