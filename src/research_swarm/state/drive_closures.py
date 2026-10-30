"""Absence of a committed action differs from a rejected scientific command."""

from __future__ import annotations

from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import (
    DriveClosure,
    DriveReceipt,
    DriveTurn,
    OrchestrationBinding,
)
from research_swarm.domain.types import utc_now
from research_swarm.state.database import JsonObject, Transaction


def target_acknowledgment(transaction: Transaction, turn: DriveTurn) -> JsonObject | None:
    saved = transaction.connection.execute(
        "SELECT request_digest,response FROM commands "
        "WHERE workspace_id=? AND actor_id=? AND idempotency_key=?",
        (transaction.workspace_id, turn.actor_id, turn.action.idempotency_key),
    ).fetchone()
    if saved is None:
        return None
    if saved["request_digest"] != turn.action.request_digest:
        raise ResearchError(
            ErrorCode.IDEMPOTENCY_CONFLICT, "Target command key has another request"
        )
    return dict(strict_json_loads(saved["response"]))


def closure_outcome(transaction: Transaction, row: JsonObject) -> JsonObject:
    return {
        "turn": row,
        "closure": transaction.get("drive_closure", row["id"]),
        "receipt": None,
        "response": None,
    }


def close_unacknowledged(
    transaction: Transaction, row: JsonObject, *, epoch: int, finding: str, request_digest: str
) -> JsonObject:
    turn = DriveTurn.model_validate(row["data"])
    if row["state"] == "closed_unacknowledged":
        outcome = closure_outcome(transaction, row)
        if outcome["closure"]["data"]["closure_request_digest"] != request_digest:
            raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Turn closure has another intent")
        return outcome
    if row["revision"] != transaction.context.expected_revision:
        raise ResearchError(ErrorCode.REVISION_CONFLICT, "Drive turn revision changed")
    owner = transaction.get("orchestration_binding", turn.campaign_id)
    binding = OrchestrationBinding.model_validate(owner["data"])
    if (
        owner["state"] != "fenced"
        or epoch != binding.epoch
        or epoch != turn.epoch
        or binding.pending_turn_id != row["id"]
        or row["state"] != "prepared"
    ):
        raise ResearchError(
            ErrorCode.STALE_FENCE, "Closure requires the original fenced pending turn"
        )
    if target_acknowledgment(transaction, turn) is not None:
        raise ResearchError(
            ErrorCode.INVALID_TRANSITION,
            "Original action has an acknowledgment; reconcile it instead",
        )
    receipt = transaction.insert(
        "drive_closure",
        row["id"],
        DriveClosure(
            turn_id=row["id"],
            epoch=epoch,
            finding=finding,
            target_request_digest=turn.action.request_digest,
            closure_request_digest=request_digest,
            closed_by=transaction.context.principal.id,
            closed_at=utc_now(),
        ).model_dump(mode="json"),
        state="immutable",
        campaign_id=turn.campaign_id,
    )
    row = transaction.update(
        "drive_turn",
        row["id"],
        expected_revision=row["revision"],
        data=row["data"],
        state="closed_unacknowledged",
    )
    transaction.update(
        "orchestration_binding",
        turn.campaign_id,
        expected_revision=owner["revision"],
        state="fenced",
        data=binding.model_copy(update={"pending_turn_id": None}).model_dump(mode="json"),
    )
    transaction.event("orchestration.turn_closed_unacknowledged", receipt, {"turn_id": row["id"]})
    return closure_outcome(transaction, row)


def settle_acknowledged(transaction: Transaction, row: JsonObject) -> JsonObject:
    turn = DriveTurn.model_validate(row["data"])
    response = target_acknowledgment(transaction, turn)
    if response is None:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE, "Turn has no durable target acknowledgment"
        )
    if row["state"] == "settled":
        receipt = transaction.get("drive_receipt", row["id"])
    else:
        receipt = transaction.insert(
            "drive_receipt",
            row["id"],
            DriveReceipt(
                turn_id=row["id"],
                target_command_id=response["command_id"],
                target_request_digest=turn.action.request_digest,
                target_response_digest=record_digest("research.drive-response-v1", response),
                disposition=response["disposition"],
                reconciled_by=transaction.context.principal.id,
                reconciled_at=utc_now(),
            ).model_dump(mode="json"),
            state="immutable",
            campaign_id=turn.campaign_id,
        )
        row = transaction.update(
            "drive_turn",
            row["id"],
            expected_revision=row["revision"],
            data=row["data"],
            state="settled",
        )
        owner = transaction.get("orchestration_binding", turn.campaign_id)
        binding = OrchestrationBinding.model_validate(owner["data"])
        if binding.epoch == turn.epoch and binding.pending_turn_id == row["id"]:
            transaction.update(
                "orchestration_binding",
                turn.campaign_id,
                expected_revision=owner["revision"],
                state=owner["state"],
                data=binding.model_copy(update={"pending_turn_id": None}).model_dump(mode="json"),
            )
        transaction.event(
            "orchestration.turn_settled", receipt, {"disposition": response["disposition"]}
        )
    return {"turn": row, "receipt": receipt, "response": response}
