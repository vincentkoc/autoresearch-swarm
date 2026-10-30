"""Local async wait recovery and callback delivery over the shared research API."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import partial
from time import monotonic

from research_swarm.client import ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.control.wait_models import AcknowledgeWakeup, OrchestrationWaitTarget
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration_waits import WakeupDelivery, wakeup_destination_id
from research_swarm.orchestration.custody import run_owned_step
from research_swarm.state.database import JsonObject


class DurableWaitDriver:
    def __init__(
        self, client: ResearchClient, campaign_id: str, *, epoch: int, actor_id: str
    ) -> None:
        self.client, self.campaign_id = client, campaign_id
        self.epoch, self.actor_id = epoch, actor_id

    def reconcile(self, wait_id: str) -> JsonObject:
        row = self.client.get("orchestration_wait", wait_id)
        data = row["data"]
        if (data["campaign_id"], data["epoch"], data["actor_id"]) != (
            self.campaign_id,
            self.epoch,
            self.actor_id,
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Wait belongs to another destination")
        return dict(
            self.client.command(
                "orchestration.reconcile_wait",
                OrchestrationWaitTarget(wait_id=wait_id),
                idempotency_key=record_digest("research.reconcile-wait-v1", {"wait_id": wait_id}),
            )["result"]
        )

    async def wait_ready(
        self, wait_id: str, *, timeout_seconds: float = 30, poll_seconds: float = 0.25
    ) -> JsonObject | None:
        if not 0 < timeout_seconds <= 3600 or not 0 < poll_seconds <= 60:
            raise ValueError("Wait polling requires bounded positive durations")
        deadline = monotonic() + timeout_seconds
        while True:
            try:
                return await run_owned_step(partial(self.reconcile, wait_id))
            except ResearchError as error:
                if error.code != ErrorCode.RESOURCE_UNAVAILABLE:
                    raise
            remaining = deadline - monotonic()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(poll_seconds, remaining))

    async def deliver_once(
        self,
        notify: Callable[[WakeupDelivery], Awaitable[None]],
        *,
        limit: int = 16,
    ) -> tuple[str, ...]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Delivery batch must contain 1 to 100 wakeups")
        page = await run_owned_step(
            partial(
                self.client.list,
                RecordQuery(
                    kind="wakeup_delivery",
                    campaign_id=self.campaign_id,
                    parent_id=wakeup_destination_id(self.campaign_id, self.epoch, self.actor_id),
                    state="pending",
                    limit=limit,
                ),
            )
        )
        delivered = []
        for row in page["items"]:
            wakeup = WakeupDelivery.model_validate(row["data"])
            if (wakeup.epoch, wakeup.actor_id) != (self.epoch, self.actor_id):
                continue
            await notify(wakeup)
            await run_owned_step(
                partial(
                    self.client.command,
                    "orchestration.acknowledge_wakeup",
                    AcknowledgeWakeup(
                        delivery_id=row["id"], campaign_id=self.campaign_id, epoch=self.epoch
                    ),
                    idempotency_key=record_digest(
                        "research.wakeup-delivery-v1", {"delivery_id": row["id"]}
                    ),
                    expected_revision=row["revision"],
                )
            )
            delivered.append(row["id"])
        return tuple(delivered)
