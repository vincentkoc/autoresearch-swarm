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
    active_wait_id: ID | None = Field(default=None, exclude_if=lambda value: value is None)
    start_ids: dict[DriverRoute, ID] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
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


class DeliveryRegistration(VersionedRecord):
    workspace_id: ID
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    configuration_digest: Digest
    gateway_credential_id: ID
    gateway_actor_id: ID
    route_credentials: dict[DriverRoute, ID] = Field(min_length=1, max_length=2)
    routes: dict[DriverRoute, ID] = Field(min_length=1, max_length=2)
    registered_by: ID
    registered_at: Timestamp

    @model_validator(mode="after")
    def distinct_routes(self) -> DeliveryRegistration:
        actors = [self.gateway_actor_id, *self.routes.values()]
        credentials = [self.gateway_credential_id, *self.route_credentials.values()]
        if (
            "control" not in self.routes
            or set(self.routes) != set(self.route_credentials)
            or len(set(actors)) != len(actors)
            or len(set(credentials)) != len(credentials)
        ):
            raise ValueError("Separate gateway and exact distinct driver routes are required")
        return self


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


class DriveClosure(VersionedRecord):
    turn_id: ID
    epoch: int = Field(strict=True, ge=1)
    reason: Literal["fenced_without_target_ack"] = "fenced_without_target_ack"
    finding: str = Field(min_length=1, max_length=2048)
    target_request_digest: Digest
    closure_request_digest: Digest
    closed_by: ID
    closed_at: Timestamp


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
