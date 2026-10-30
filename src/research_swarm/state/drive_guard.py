"""Transaction-time fencing for dedicated orchestration principals."""

from collections.abc import Mapping
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import DriveGuard, guarded_context
from research_swarm.domain.orchestration import DriveTurn, OrchestrationBinding, OrchestrationOwner
from research_swarm.state.database import Transaction

DRIVER_COMMANDS = {
    "stream.create_snapshot",
    "stream.release_snapshot",
    "orchestration.record_wait",
    "orchestration.reconcile_wait",
    "orchestration.acknowledge_wakeup",
    "orchestration.prepare_turn",
    "orchestration.execute_turn",
    "orchestration.reconcile_turn",
}


def require_reserved_command_key(transaction: Transaction, request_digest: str) -> None:
    """Reject key collisions before creating an acknowledgment under a reserved target key."""
    context = transaction.context
    identifier = str(
        uuid5(
            NAMESPACE_URL,
            record_digest(
                "research.drive-command-key-v1",
                {
                    "workspace_id": transaction.workspace_id,
                    "actor_id": context.principal.id,
                    "key": context.idempotency_key,
                },
            ),
        )
    )
    row = transaction.connection.execute(
        "SELECT payload FROM records WHERE workspace_id=? AND kind='drive_command_key' AND id=?",
        (transaction.workspace_id, identifier),
    ).fetchone()
    if row is None:
        return
    turn_id = str(strict_json_loads(row["payload"])["turn_id"])
    turn = DriveTurn.model_validate(transaction.get("drive_turn", turn_id)["data"])
    if request_digest != turn.action.request_digest:
        raise ResearchError(
            ErrorCode.IDEMPOTENCY_CONFLICT, "Key is reserved for another saved action"
        )
    if context._drive_guard != DriveGuard(turn_id, request_digest):
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Reserved action requires its execution route"
        )


def require_storage_authority(transaction: Transaction, upload_id: str) -> None:
    """Fence bare driver calls before admitting private staging I/O, without a storage bypass."""
    context = transaction.context
    owner = transaction.connection.execute(
        "SELECT 1 FROM records WHERE workspace_id=? AND kind='orchestration_owner' AND id=?",
        (transaction.workspace_id, context.principal.id),
    ).fetchone()
    if owner is None:
        return
    guard = context._drive_guard
    if guard is None:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Dedicated drivers cannot mutate uploads directly"
        )
    if transaction.get("upload", upload_id)["data"]["actor_id"] != context.principal.id:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Turn cannot mutate another actor's upload"
        )
    turn = DriveTurn.model_validate(transaction.get("drive_turn", guard.turn_id)["data"])
    root = guarded_context(
        context.model_copy(
            update={
                "idempotency_key": turn.action.idempotency_key,
                "expected_revision": turn.action.expected_revision,
            }
        ),
        DriveGuard(guard.turn_id, guard.root_request_digest),
    )
    require_drive_authority(
        Transaction(transaction.connection, root),
        turn.action.operation,
        turn.action.payload,
        turn.action.request_digest,
    )


def require_drive_authority(
    transaction: Transaction,
    operation: str,
    payload: Mapping[str, Any],
    request_digest: str,
) -> None:
    context = transaction.context
    row = transaction.connection.execute(
        "SELECT payload FROM records WHERE workspace_id=? AND kind='orchestration_owner' AND id=?",
        (transaction.workspace_id, context.principal.id),
    ).fetchone()
    if row is None:
        if context._drive_guard is not None:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Turn actor has no ownership record"
            )
        return
    if operation in DRIVER_COMMANDS and context._drive_guard is None:
        return  # Each lifecycle handler performs its own actor and binding checks.
    guard = context._drive_guard
    if guard is None:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Dedicated drivers must execute saved turns"
        )
    owner = OrchestrationOwner.model_validate_json(row["payload"])
    turn_row = transaction.get("drive_turn", guard.turn_id)
    turn = DriveTurn.model_validate(turn_row["data"])
    if (turn.campaign_id, turn.epoch, turn.route, turn.actor_id) != (
        owner.campaign_id,
        owner.epoch,
        owner.route,
        owner.actor_id,
    ) or guard.root_request_digest != turn.action.request_digest:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Turn capability differs from its owner"
        )
    if guard.derived_request_digest is None:
        if (
            operation != turn.action.operation
            or request_digest != turn.action.request_digest
            or context.idempotency_key != turn.action.idempotency_key
        ):
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Command differs from saved turn")
    elif request_digest != guard.derived_request_digest:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Derived operation differs from its capability"
        )
    if guard.cleanup_pin_id is not None:
        if operation != "artifact.unpin":
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid cleanup capability")
        retained = transaction.get("artifact_reference", str(payload.get("reference_id")))["data"]
        retention = retained["retention"]
        if (
            retained["actor_id"] != owner.actor_id
            or retention["owner_kind"] != "artifact_read"
            or retention["owner_id"] != guard.cleanup_pin_id
        ):
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Cleanup cannot release another pin"
            )
        return
    binding_row = transaction.get("orchestration_binding", owner.campaign_id)
    binding = OrchestrationBinding.model_validate(binding_row["data"])
    if (
        binding_row["state"] != "active"
        or binding.epoch != owner.epoch
        or binding.routes.get(owner.route) != owner.actor_id
        or binding.pending_turn_id != guard.turn_id
        or turn_row["state"] != "prepared"
    ):
        raise ResearchError(
            ErrorCode.STALE_FENCE, "Orchestration ownership no longer admits this turn"
        )
