"""Durable wait conditions and destination-bound, at-least-once wakeup delivery."""

from __future__ import annotations

from typing import Literal
from uuid import NAMESPACE_URL, uuid5

from pydantic import Field, model_validator

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.orchestration import DriverRoute
from research_swarm.domain.types import ID, Digest, Name, Revision, Timestamp, VersionedRecord


class WaitCondition(VersionedRecord):
    observed_watermark: Revision
    event_types: tuple[Name, ...] = Field(default=(), max_length=32)
    wake_at: Timestamp | None = None

    @model_validator(mode="after")
    def distinct_domain_events(self) -> WaitCondition:
        if len(set(self.event_types)) != len(self.event_types):
            raise ValueError("Wait event types must be unique")
        if any(event.startswith("orchestration.") for event in self.event_types):
            raise ValueError("Orchestration bookkeeping cannot wake itself")
        return self


class OrchestrationWait(VersionedRecord):
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    route: DriverRoute
    actor_id: ID
    configuration_digest: Digest
    condition: WaitCondition
    recorded_at: Timestamp


class WaitReadiness(VersionedRecord):
    wait_id: ID
    cause: Literal["event", "timer", "fenced"]
    watermark: Revision
    event_id: ID | None = None
    ready_at: Timestamp


class WakeupDelivery(VersionedRecord):
    wait_id: ID
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    route: DriverRoute
    actor_id: ID
    adapter: Name
    readiness: WaitReadiness


class WakeupAcknowledgment(VersionedRecord):
    delivery_id: ID
    wait_id: ID
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    acknowledged_by: ID
    acknowledged_at: Timestamp


def wakeup_destination_id(campaign_id: str, epoch: int, actor_id: str) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            record_digest(
                "research.wakeup-destination-v1",
                {
                    "campaign_id": campaign_id,
                    "epoch": epoch,
                    "actor_id": actor_id,
                },
            ),
        )
    )
