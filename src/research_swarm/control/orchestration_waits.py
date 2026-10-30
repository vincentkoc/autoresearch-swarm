"""Register durable waits and acknowledge notification delivery without consuming evidence."""

from __future__ import annotations

from research_swarm.control.wait_models import (
    AcknowledgeWakeup,
    OrchestrationWaitTarget,
    RecordOrchestrationWait,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.orchestration import OrchestrationBinding, OrchestrationOwner
from research_swarm.domain.orchestration_waits import (
    OrchestrationWait,
    WakeupAcknowledgment,
    WakeupDelivery,
)
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.waits import consume_ready_wait, reconcile_condition, watermark


class _WaitPending(Exception):
    """No durable acknowledgment exists until the condition is ready."""


def wait_outcome(transaction: Transaction, row: JsonObject) -> JsonObject:
    if row["state"] == "waiting":
        return {"wait": row, "readiness": None, "delivery": None}
    return {
        "wait": row,
        "readiness": transaction.get("wait_readiness", row["id"]),
        "delivery": transaction.get("wakeup_delivery", row["id"]),
    }


class OrchestrationWaitControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def record(self, context: CommandContext, intent: RecordOrchestrationWait) -> JsonObject:
        def record(transaction: Transaction) -> JsonObject:
            owner = OrchestrationOwner.model_validate(
                transaction.get("orchestration_owner", context.principal.id)["data"]
            )
            if (owner.campaign_id, owner.epoch) != (intent.campaign_id, intent.epoch):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Driver does not own this wait destination"
                )
            wait = OrchestrationWait(
                campaign_id=intent.campaign_id,
                epoch=intent.epoch,
                route=owner.route,
                actor_id=owner.actor_id,
                configuration_digest=intent.configuration_digest,
                condition=intent.condition,
                recorded_at=utc_now(),
            )
            try:
                previous = transaction.get("orchestration_wait", intent.wait_id)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
            else:
                prior = OrchestrationWait.model_validate(previous["data"])
                if prior.model_dump(exclude={"recorded_at"}) != wait.model_dump(
                    exclude={"recorded_at"}
                ):
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Wait identity has different conditions"
                    )
                return wait_outcome(transaction, previous)
            row = transaction.get("orchestration_binding", intent.campaign_id)
            binding = OrchestrationBinding.model_validate(row["data"])
            if (
                row["state"] != "active"
                or binding.epoch != intent.epoch
                or binding.routes.get(owner.route) != owner.actor_id
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Orchestration binding is fenced")
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Orchestration binding changed")
            if binding.configuration_digest != intent.configuration_digest:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Wait configuration changed"
                )
            if binding.pending_turn_id is not None:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Resolve the pending turn before waiting"
                )
            if intent.condition.observed_watermark > watermark(transaction):
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Wait watermark is in the future"
                )
            consume_ready_wait(transaction, binding, intent.condition.observed_watermark)
            saved = transaction.insert(
                "orchestration_wait",
                intent.wait_id,
                wait.model_dump(mode="json"),
                state="waiting",
                campaign_id=intent.campaign_id,
                parent_id=intent.campaign_id,
            )
            transaction.update(
                "orchestration_binding",
                intent.campaign_id,
                expected_revision=row["revision"],
                state="active",
                data=binding.model_copy(update={"active_wait_id": intent.wait_id}).model_dump(
                    mode="json"
                ),
            )
            transaction.event(
                "orchestration.wait_recorded",
                saved,
                {"observed_watermark": intent.condition.observed_watermark},
            )
            return wait_outcome(transaction, reconcile_condition(transaction, saved))

        return self.database.execute(
            context,
            "orchestration.record_wait",
            intent.model_dump(mode="json"),
            "orchestration.drive",
            record,
        )

    def reconcile(self, context: CommandContext, intent: OrchestrationWaitTarget) -> JsonObject:
        def reconcile(transaction: Transaction) -> JsonObject:
            row = transaction.get("orchestration_wait", intent.wait_id)
            wait = OrchestrationWait.model_validate(row["data"])
            if wait.actor_id != context.principal.id:
                context.principal.require("orchestration.manage")
            updated = reconcile_condition(transaction, row)
            if updated["state"] == "waiting":
                raise _WaitPending
            return wait_outcome(transaction, updated)

        try:
            return self.database.execute(
                context,
                "orchestration.reconcile_wait",
                intent.model_dump(mode="json"),
                "orchestration.drive",
                reconcile,
            )
        except _WaitPending:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Wait condition is not ready", retryable=True
            ) from None

    def acknowledge(self, context: CommandContext, intent: AcknowledgeWakeup) -> JsonObject:
        def acknowledge(transaction: Transaction) -> JsonObject:
            row = transaction.get("wakeup_delivery", intent.delivery_id)
            delivery = WakeupDelivery.model_validate(row["data"])
            if (delivery.campaign_id, delivery.epoch) != (intent.campaign_id, intent.epoch):
                raise ResearchError(ErrorCode.STALE_FENCE, "Wakeup belongs to another destination")
            if context.principal.id != delivery.actor_id:
                context.principal.require("orchestration.deliver")
            if row["state"] == "delivered":
                return transaction.get("wakeup_acknowledgment", row["id"])
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Wakeup delivery changed")
            receipt = transaction.insert(
                "wakeup_acknowledgment",
                row["id"],
                WakeupAcknowledgment(
                    delivery_id=row["id"],
                    wait_id=delivery.wait_id,
                    campaign_id=delivery.campaign_id,
                    epoch=delivery.epoch,
                    acknowledged_by=context.principal.id,
                    acknowledged_at=utc_now(),
                ).model_dump(mode="json"),
                state="immutable",
                campaign_id=delivery.campaign_id,
                parent_id=delivery.wait_id,
            )
            updated = transaction.update(
                "wakeup_delivery",
                row["id"],
                expected_revision=row["revision"],
                data=row["data"],
                state="delivered",
            )
            transaction.event(
                "orchestration.wakeup_delivered", updated, {"wait_id": delivery.wait_id}
            )
            return receipt

        return self.database.execute(
            context,
            "orchestration.acknowledge_wakeup",
            intent.model_dump(mode="json"),
            "orchestration.drive",
            acknowledge,
        )
