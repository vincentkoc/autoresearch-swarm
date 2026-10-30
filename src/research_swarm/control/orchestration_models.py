"""Commands for durable driver ownership; they do not control native processes."""

from typing import Self

from pydantic import Field, model_validator

from research_swarm.domain.orchestration import DriveAction, DriverRoute
from research_swarm.domain.types import ID, Digest, Name, Revision, VersionedRecord


class BindOrchestration(VersionedRecord):
    campaign_id: ID
    adapter: Name = "local"
    configuration_digest: Digest
    routes: dict[DriverRoute, ID] = Field(min_length=1, max_length=2)

    @model_validator(mode="after")
    def distinct_routes(self) -> Self:
        if "control" not in self.routes or len(set(self.routes.values())) != len(self.routes):
            raise ValueError("A binding requires a control route and distinct dedicated actors")
        return self


class PrepareDriveTurn(VersionedRecord):
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    ordinal: int = Field(strict=True, ge=1)
    configuration_digest: Digest
    planning_watermark: Revision
    action: DriveAction


class DriveTurnTarget(VersionedRecord):
    turn_id: ID


class CloseDriveTurn(DriveTurnTarget):
    epoch: int = Field(strict=True, ge=1)
    finding: str = Field(min_length=1, max_length=2048)


class TransferOrchestration(BindOrchestration):
    transfer_id: ID
    epoch: int = Field(strict=True, ge=1)
    campaign_revision: Revision
    reason: str = Field(min_length=1, max_length=2048)


class AcknowledgeDriverStart(VersionedRecord):
    start_id: ID
    checkpoint_digest: Digest
    observed_watermark: Revision


class FenceOrchestration(VersionedRecord):
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    reason: str = Field(min_length=1, max_length=2048)
