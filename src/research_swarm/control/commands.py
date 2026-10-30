"""Public command inputs contain stable intent, not eagerly generated server IDs."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from research_swarm.control.models import (
    DelegationConfiguration,
    PromotionConfiguration,
    SearchConfiguration,
    StoppingRule,
)
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import ID, Name, VersionedRecord


class RegisterProject(VersionedRecord):
    source_artifact: ArtifactRef
    manifest_path: str = "research.toml"
    project_id: ID | None = None


class CreateCampaign(VersionedRecord):
    project_id: ID
    project_revision_id: ID | None = None
    name: Name
    primary_metric: Name
    limits: tuple[BudgetLimit, ...] = Field(min_length=1)
    search: SearchConfiguration = Field(default_factory=SearchConfiguration)
    promotion: PromotionConfiguration = Field(default_factory=PromotionConfiguration)
    stopping: StoppingRule = Field(default_factory=StoppingRule)
    input_artifacts: dict[Name, ArtifactRef] = Field(default_factory=dict)
    delegation: DelegationConfiguration | None = Field(
        default=None, exclude_if=lambda value: value is None
    )


class CampaignTarget(VersionedRecord):
    campaign_id: ID


class PauseCampaign(CampaignTarget):
    mode: Literal["drain", "checkpoint", "terminate"] = "drain"


class ProposeCandidate(CampaignTarget):
    source_artifact: ArtifactRef
    parent_ids: tuple[ID, ...] = ()
    hypothesis: str = Field(min_length=1, max_length=8192)


class ValidateCandidate(VersionedRecord):
    candidate_id: ID


class SubmitTrial(CampaignTarget):
    candidate_id: ID
    validation_id: ID
    seed: int = Field(strict=True, ge=0, le=9_007_199_254_740_991)


class CompleteCampaign(CampaignTarget):
    reason: str = Field(min_length=1, max_length=4096)
