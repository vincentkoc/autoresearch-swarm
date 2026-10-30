"""Versioned, bounded wire references for remote delivery adapters."""

from dataclasses import asdict
from typing import Literal

from pydantic import Field, model_validator

from research_swarm.domain.orchestration import DriverRoute
from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.orchestration.delivery_models import (
    BindingReference,
    StartupRequest,
    WorkReference,
)
from research_swarm.state.sql import MAX_SQL_COUNTER


class GatewayBinding(VersionedRecord):
    workspace_id: ID
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1, le=MAX_SQL_COUNTER)
    configuration_digest: Digest
    actors: dict[DriverRoute, ID] = Field(min_length=1, max_length=2)

    @model_validator(mode="after")
    def registered_routes(self) -> "GatewayBinding":
        if "control" not in self.actors or len(set(self.actors.values())) != len(self.actors):
            raise ValueError("Distinct registered actors and a control route are required")
        return self

    def reference(self) -> BindingReference:
        return BindingReference(
            self.workspace_id,
            self.campaign_id,
            self.epoch,
            self.configuration_digest,
            {route: actor for route, actor in self.actors.items()},
        )

    @classmethod
    def from_reference(cls, reference: BindingReference) -> "GatewayBinding":
        return cls.model_validate(asdict(reference))


class GatewayStartup(VersionedRecord):
    binding: GatewayBinding
    recovery_id: ID

    def reference(self) -> StartupRequest:
        return StartupRequest(self.binding.reference(), self.recovery_id)


class GatewayWork(VersionedRecord):
    binding: GatewayBinding
    kind: Literal["turn", "wait"]
    record_id: ID
    actor_id: ID

    @model_validator(mode="after")
    def registered_actor(self) -> "GatewayWork":
        if self.actor_id not in self.binding.actors.values():
            raise ValueError("Work actor must be a registered route")
        return self

    def reference(self) -> WorkReference:
        return WorkReference(self.binding.reference(), self.kind, self.record_id, self.actor_id)

    @classmethod
    def from_reference(cls, reference: WorkReference) -> "GatewayWork":
        return cls.model_validate(asdict(reference))
