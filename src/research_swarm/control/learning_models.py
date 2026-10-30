"""Controller-owned learning plans and one-step decision commitments."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import Field, model_validator

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.learning import CategoricalPolicy, DecisionTrace, RewardSpec, Split
from research_swarm.domain.types import ID, Digest, Name, Revision, Timestamp, VersionedRecord


class PlanEpisode(VersionedRecord):
    request_id: ID
    split: Split


class FreezeLearningPlan(VersionedRecord):
    plan_id: ID
    policy: CategoricalPolicy
    reward_spec: RewardSpec
    action_artifacts: dict[Name, ArtifactRef] = Field(min_length=2, max_length=32)
    episodes: tuple[PlanEpisode, ...] = Field(min_length=1, max_length=256)
    comparison_policy: Literal["same-seed-v1"] = "same-seed-v1"

    @model_validator(mode="after")
    def identities(self) -> Self:
        if len({item.request_id for item in self.episodes}) != len(self.episodes):
            raise ValueError("Every planned episode requires a distinct proposal request")
        if set(self.action_artifacts) != {action.id for action in self.policy.actions} or any(
            self.action_artifacts[action.id].digest != action.source_digest
            for action in self.policy.actions
        ):
            raise ValueError("Policy action sources must match the exact artifact byte digests")
        return self


class FrozenEpisode(VersionedRecord):
    # The first profile has exactly one decision per proposal; their IDs coincide.
    episode_id: ID
    campaign_id: ID
    split: Split
    proposal_revision: Revision
    proposal_digest: Digest
    proposer_id: ID
    context_digest: Digest
    context_watermark: Revision
    seed: int = Field(strict=True, ge=0, le=9_007_199_254_740_991)
    campaign_contract_digest: Digest
    controller_id: ID
    control_epoch: Revision
    incumbent_revision: Revision
    comparator_candidate_id: ID
    comparator_evaluation_id: ID
    comparison_digest: Digest


class LearningPlan(VersionedRecord):
    request: FreezeLearningPlan
    request_digest: Digest
    episodes: tuple[FrozenEpisode, ...] = Field(min_length=1, max_length=256)
    evidence_through: Revision
    actor_id: ID
    recorded_at: Timestamp

    @property
    def digest(self) -> str:
        return record_digest("research.learning-plan.v1", self.model_dump(mode="json"))


class EpisodeBinding(VersionedRecord):
    plan_id: ID
    plan_digest: Digest
    episode: FrozenEpisode


class CommitPolicyDecision(VersionedRecord):
    request_id: ID


class CommittedPolicyDecision(VersionedRecord):
    plan_id: ID
    plan_digest: Digest
    decision: DecisionTrace
    source_artifact: ArtifactRef
    actor_id: ID
    recorded_at: Timestamp
