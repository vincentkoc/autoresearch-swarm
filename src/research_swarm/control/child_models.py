"""Child intent exposes funding and policy choices, not evaluator or dataset overrides."""

from __future__ import annotations

from pydantic import Field, model_validator

from research_swarm.control.models import SearchConfiguration
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.funding import MAX_FUNDING_DEPTH
from research_swarm.domain.types import ID, Name, VersionedRecord


class CreateChildCampaign(VersionedRecord):
    parent_campaign_id: ID
    name: Name
    limits: tuple[BudgetLimit, ...] = Field(min_length=1, max_length=128)
    search: SearchConfiguration | None = None
    max_trials: int | None = Field(default=None, strict=True, ge=1, le=100_000)
    remaining_depth: int = Field(default=0, strict=True, ge=0, lt=MAX_FUNDING_DEPTH)

    @model_validator(mode="after")
    def distinct_dimensions(self) -> CreateChildCampaign:
        if len({limit.dimension for limit in self.limits}) != len(self.limits):
            raise ValueError("Child funding dimensions must be unique")
        return self


class SettleChildCampaign(VersionedRecord):
    child_campaign_id: ID


class CancelChildCampaign(SettleChildCampaign):
    reason: str = Field(min_length=1, max_length=4096)
