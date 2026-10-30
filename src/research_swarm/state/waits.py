"""Wake waits in the event transaction; delivery never consumes research observations."""

from __future__ import annotations

from typing import Literal

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import Event
from research_swarm.domain.orchestration import OrchestrationBinding
from research_swarm.domain.orchestration_waits import (
    OrchestrationWait,
    WaitReadiness,
    WakeupDelivery,
    wakeup_destination_id,
)
from research_swarm.domain.types import utc_now
from research_swarm.state.database import JsonObject, Transaction


def watermark(transaction: Transaction) -> int:
    return int(
        transaction.connection.execute(
            "SELECT next_sequence-1 FROM workspaces WHERE id=?",
            (transaction.workspace_id,),
        ).fetchone()[0]
    )


def wake_wait(
    transaction: Transaction,
    row: JsonObject,
    cause: Literal["event", "timer", "fenced"],
    event: Event | None = None,
) -> JsonObject:
    if row["state"] != "waiting":
        return row
    wait = OrchestrationWait.model_validate(row["data"])
    binding = OrchestrationBinding.model_validate(
        transaction.get("orchestration_binding", wait.campaign_id)["data"]
    )
    if (
        binding.epoch != wait.epoch
        or binding.routes.get(wait.route) != wait.actor_id
        or binding.active_wait_id != row["id"]
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Wait no longer belongs to its destination")
    ready = WaitReadiness(
        wait_id=row["id"],
        cause=cause,
        watermark=event.sequence if event else watermark(transaction),
        event_id=event.id if event else None,
        ready_at=utc_now(),
    )
    transaction.insert(
        "wait_readiness",
        row["id"],
        ready.model_dump(mode="json"),
        state="immutable",
        campaign_id=wait.campaign_id,
    )
    delivery = transaction.insert(
        "wakeup_delivery",
        row["id"],
        WakeupDelivery(
            wait_id=row["id"],
            campaign_id=wait.campaign_id,
            epoch=wait.epoch,
            route=wait.route,
            actor_id=wait.actor_id,
            adapter=binding.adapter,
            readiness=ready,
        ).model_dump(mode="json"),
        state="pending",
        campaign_id=wait.campaign_id,
        parent_id=wakeup_destination_id(wait.campaign_id, wait.epoch, wait.actor_id),
    )
    updated = transaction.update(
        "orchestration_wait",
        row["id"],
        expected_revision=row["revision"],
        data=row["data"],
        state="superseded" if cause == "fenced" else "ready",
    )
    transaction.event("orchestration.wait_ready", delivery, {"cause": cause, "wait_id": row["id"]})
    return updated


def consider_event(transaction: Transaction, event: Event) -> None:
    if event.type.startswith("orchestration.") or event.campaign_id is None:
        return
    rows = transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='orchestration_wait' "
        "AND campaign_id=? AND state='waiting'",
        (transaction.workspace_id, event.campaign_id),
    ).fetchall()
    for candidate in rows:
        row = transaction.get("orchestration_wait", candidate["id"])
        condition = OrchestrationWait.model_validate(row["data"]).condition
        if event.sequence > condition.observed_watermark and (
            not condition.event_types or event.type in condition.event_types
        ):
            wake_wait(transaction, row, "event", event)


def reconcile_condition(transaction: Transaction, row: JsonObject) -> JsonObject:
    if row["state"] != "waiting":
        return row
    wait = OrchestrationWait.model_validate(row["data"])
    condition = wait.condition
    query = (
        "SELECT payload FROM events WHERE workspace_id=? AND sequence>? "
        "AND json_extract(payload,'$.campaign_id')=? "
        "AND json_extract(payload,'$.type') NOT GLOB 'orchestration.*'"
    )
    parameters: list[str | int] = [
        transaction.workspace_id,
        condition.observed_watermark,
        wait.campaign_id,
    ]
    if condition.event_types:
        query += (
            " AND json_extract(payload,'$.type') IN ("
            + ",".join("?" for _ in condition.event_types)
            + ")"
        )
        parameters.extend(condition.event_types)
    event = transaction.connection.execute(
        query + " ORDER BY sequence LIMIT 1", parameters
    ).fetchone()
    if event is not None:
        return wake_wait(transaction, row, "event", Event.model_validate_json(event["payload"]))
    if condition.wake_at is not None and utc_now() >= condition.wake_at:
        return wake_wait(transaction, row, "timer")
    return row


def consume_ready_wait(
    transaction: Transaction, binding: OrchestrationBinding, observed_watermark: int
) -> None:
    if binding.active_wait_id is None:
        return
    row = transaction.get("orchestration_wait", binding.active_wait_id)
    if row["state"] != "ready":
        raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Wait has no ready condition")
    readiness = WaitReadiness.model_validate(transaction.get("wait_readiness", row["id"])["data"])
    if observed_watermark < readiness.watermark:
        raise ResearchError(
            ErrorCode.REVISION_CONFLICT, "Planning has not observed the wakeup watermark"
        )
    transaction.update(
        "orchestration_wait",
        row["id"],
        expected_revision=row["revision"],
        data=row["data"],
        state="consumed",
    )


def fence_wait(transaction: Transaction, binding: OrchestrationBinding) -> None:
    if binding.active_wait_id is None:
        return
    row = transaction.get("orchestration_wait", binding.active_wait_id)
    if row["state"] == "waiting":
        wake_wait(transaction, row, "fenced")
    elif row["state"] == "ready":
        transaction.update(
            "orchestration_wait",
            row["id"],
            expected_revision=row["revision"],
            data=row["data"],
            state="superseded",
        )
