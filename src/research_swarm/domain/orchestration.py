"""Provider-neutral ownership and immutable, single-command orchestration turns."""

from __future__ import annotations

from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid5

from pydantic import Field, model_validator

from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.types import ID, Digest, Name, Revision, Timestamp, VersionedRecord

DriverRoute = Literal["control", "evaluator"]


class DriveAction(VersionedRecord):
    operation: Name
    payload: dict[str, Any]
    idempotency_key: Name
    expected_revision: Revision | None = None

    @model_validator(mode="after")
    def bounded(self) -> DriveAction:
        if len(canonical_bytes(self.model_dump(mode="json"))) > 64 * 1024:
            raise ValueError("Drive action exceeds 64 KiB")
        return self

    @property
    def request_digest(self) -> str:
        return record_digest(
            "research.command",
            {
                "operation": self.operation,
                "expected_revision": self.expected_revision,
                "payload": self.payload,
            },
        )


class OrchestrationBinding(VersionedRecord):
    workspace_id: ID
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    adapter: Name
    configuration_digest: Digest
    routes: dict[DriverRoute, ID] = Field(min_length=1, max_length=2)
    next_ordinal: int = Field(default=1, strict=True, ge=1)
    pending_turn_id: ID | None = None
    bound_by: ID
    bound_at: Timestamp

    @model_validator(mode="after")
    def distinct_routes(self) -> OrchestrationBinding:
        if "control" not in self.routes or len(set(self.routes.values())) != len(self.routes):
            raise ValueError("A binding requires a control route and distinct dedicated actors")
        return self


class OrchestrationOwner(VersionedRecord):
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    route: DriverRoute
    actor_id: ID


class DriveTurn(VersionedRecord):
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    ordinal: int = Field(strict=True, ge=1)
    route: DriverRoute
    actor_id: ID
    configuration_digest: Digest
    planning_watermark: Revision
    action: DriveAction
    prepared_at: Timestamp


class DriveReceipt(VersionedRecord):
    turn_id: ID
    target_command_id: ID
    target_request_digest: Digest
    target_response_digest: Digest
    disposition: Literal["accepted", "rejected"]
    reconciled_by: ID
    reconciled_at: Timestamp


def drive_turn_id(workspace_id: str, campaign_id: str, epoch: int, ordinal: int) -> str:
    identity = record_digest(
        "research.drive-turn-v1",
        {
            "workspace_id": workspace_id,
            "campaign_id": campaign_id,
            "epoch": epoch,
            "ordinal": ordinal,
        },
    )
    return str(uuid5(NAMESPACE_URL, "research-swarm:drive-turn:" + identity))
