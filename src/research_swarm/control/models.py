"""Application aggregates bind immutable project contracts to research state."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, model_validator

from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import ExperimentSpec, StageSpec
from research_swarm.domain.types import (
    ID,
    Digest,
    Name,
    Revision,
    Timestamp,
    VersionedRecord,
)
from research_swarm.projects.manifest import ResolvedProject
from research_swarm.projects.source import SourceManifest

Seed = Annotated[int, Field(strict=True, ge=0, le=9_007_199_254_740_991)]


class ProjectData(VersionedRecord):
    slug: str
    name: Name
    description: str
    current_revision_id: ID


class ProjectRevisionData(VersionedRecord):
    project_id: ID
    source_artifact: ArtifactRef
    resolved: ResolvedProject


class SearchConfiguration(VersionedRecord):
    policy: Literal["greedy", "portfolio", "external"] = "greedy"
    version: Name = "greedy-v1"
    max_parallel_trials: int = Field(default=1, strict=True, ge=1, le=1024)


class PromotionConfiguration(VersionedRecord):
    mode: Literal["observed", "paired_confirmation"] = "observed"
    version: Name = "observed-v1"
    minimum_effect: float = Field(default=0.0, strict=True, ge=0)
    confirmation_seeds: tuple[Seed, ...] = ()
    confidence: float = Field(default=0.95, strict=True, gt=0.5, lt=1)

    @model_validator(mode="after")
    def fixed_panel(self) -> PromotionConfiguration:
        if len(set(self.confirmation_seeds)) != len(self.confirmation_seeds) or any(
            type(seed) is not int or not 0 <= seed <= 9_007_199_254_740_991
            for seed in self.confirmation_seeds
        ):
            raise ValueError("Confirmation requires distinct nonnegative integer seeds")
        if self.mode == "paired_confirmation" and len(self.confirmation_seeds) < 3:
            raise ValueError("Paired confirmation requires at least three declared seeds")
        return self


class StoppingRule(VersionedRecord):
    max_trials: int = Field(default=10, strict=True, ge=1, le=100_000)
    target_metric: float | None = Field(default=None, strict=True)
    permit_manual_completion: bool = True


class CampaignContract(VersionedRecord):
    project_id: ID
    project_revision_id: ID
    project_manifest_digest: Digest
    protocol_digest: Digest
    comparison_class: Name
    primary_metric: Name
    search: SearchConfiguration = Field(default_factory=SearchConfiguration)
    promotion: PromotionConfiguration = Field(default_factory=PromotionConfiguration)
    stopping: StoppingRule = Field(default_factory=StoppingRule)
    limits: tuple[BudgetLimit, ...] = Field(min_length=1)
    input_artifacts: dict[Name, ArtifactRef] = Field(default_factory=dict)

    @model_validator(mode="after")
    def distinct_limits(self) -> CampaignContract:
        if len({limit.dimension for limit in self.limits}) != len(self.limits):
            raise ValueError("Campaign budget dimensions must be unique")
        return self


class CampaignData(VersionedRecord):
    name: Name
    contract: CampaignContract
    contract_digest: Digest | None = None
    sealed_at: Timestamp | None = None
    incumbent_candidate_id: ID | None = None
    incumbent_revision: Revision = 0
    stopping_reason: str | None = None
    final_report_id: ID | None = None
    parent_campaign_id: ID | None = None


class CandidateData(VersionedRecord):
    project_id: ID
    campaign_id: ID
    source_artifact: ArtifactRef
    source: SourceManifest
    parent_ids: tuple[ID, ...] = ()
    hypothesis: str = Field(min_length=1, max_length=8192)
    proposer_id: ID


class CandidateValidation(VersionedRecord):
    candidate_id: ID
    source_tree_digest: Digest
    campaign_contract_digest: Digest
    validator_version: Name = "source-contract-v1"
    valid: bool
    changed_paths: tuple[str, ...] = ()
    findings: tuple[str, ...] = ()


class TrialData(VersionedRecord):
    candidate_id: ID
    validation_id: ID
    spec: ExperimentSpec
    job_ids: tuple[ID, ...]
    execution_outcome: Literal["pending", "succeeded", "failed", "cancelled", "unknown"] = "pending"
    evaluation_status: Literal["pending", "valid", "invalid", "incomplete"] = "pending"
    resource_status: Literal["unallocated", "active", "closed", "unknown"] = "unallocated"


class JobData(VersionedRecord):
    trial_id: ID
    stage: StageSpec
    predecessor_ids: tuple[ID, ...] = ()
    input_sources: dict[Name, ID] = Field(default_factory=dict)
    inputs: dict[Name, ArtifactRef] = Field(default_factory=dict)
    current_attempt_id: ID | None = None
    evaluation_request_id: ID | None = None
    outputs: dict[Name, ArtifactRef] = Field(default_factory=dict)
