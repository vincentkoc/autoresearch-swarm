"""Learning data contracts; parsing a dataset never grants evidence authority."""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import Direction
from research_swarm.domain.types import (
    ID,
    BalanceAmount,
    Digest,
    Dimension,
    Name,
    Revision,
    VersionedRecord,
)

FiniteNumber = Annotated[float, Field(strict=True, allow_inf_nan=False)]
Probability = Annotated[float, Field(strict=True, gt=0, le=1, allow_inf_nan=False)]
Logit = Annotated[float, Field(strict=True, ge=-40, le=40, allow_inf_nan=False)]
LearningRate = Annotated[float, Field(strict=True, gt=0, le=10, allow_inf_nan=False)]
Split = Literal["development", "held_out"]


class PolicyAction(VersionedRecord):
    id: Name
    source_digest: Digest


class CategoricalPolicy(VersionedRecord):
    profile: Literal["categorical-softmax-v1"] = "categorical-softmax-v1"
    actions: tuple[PolicyAction, ...] = Field(min_length=2, max_length=32)
    logits: tuple[Logit, ...] = Field(min_length=2, max_length=32)
    parent_digest: Digest | None = None

    @model_validator(mode="after")
    def dimensions(self) -> Self:
        if len(self.logits) != len(self.actions) or len({item.id for item in self.actions}) != len(
            self.actions
        ):
            raise ValueError("Policy logits require distinct ordered actions")
        if max(self.logits) - min(self.logits) > 16:
            raise ValueError("This sampling profile limits the logit spread to 16")
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.categorical-policy.v1", self.model_dump(mode="json"))


class RewardSpec(VersionedRecord):
    rule: Literal["comparable-metric-delta-v1"] = "comparable-metric-delta-v1"
    metric_name: Name
    direction: Direction
    scale: FiniteNumber = Field(gt=0)
    clip_magnitude: FiniteNumber = Field(default=1, gt=0, le=100)
    execution_ms_penalty: FiniteNumber = Field(default=0, ge=0)
    invalid_or_missing: Literal["block_dataset"] = "block_dataset"
    credit: Literal["one_step"] = "one_step"

    @property
    def digest(self) -> str:
        return record_digest("research.reward-spec.v1", self.model_dump(mode="json"))


class DecisionTrace(VersionedRecord):
    episode_id: ID
    campaign_id: ID
    proposal_request_id: ID
    context_digest: Digest
    behavior_policy_digest: Digest
    sampling: Literal["sha256-categorical-v1"] = "sha256-categorical-v1"
    seed: int = Field(strict=True, ge=0, le=9_007_199_254_740_991)
    draw: FiniteNumber = Field(ge=0, lt=1)
    probabilities: tuple[Probability, ...] = Field(min_length=2, max_length=32)
    action_id: Name
    source_digest: Digest

    @property
    def digest(self) -> str:
        return record_digest("research.policy-decision.v1", self.model_dump(mode="json"))


class LearningEvidence(VersionedRecord):
    kind: Literal[
        "proposal_request",
        "proposal_response",
        "policy_transition",
        "trial",
        "search_observation",
        "evaluation_receipt",
        "evaluation_request",
        "candidate",
        "candidate_validation",
        "job",
        "attempt",
        "run_result",
        "attempt_closure",
        "attempt_submission",
        "learning_plan",
        "learning_episode",
        "learning_decision",
    ]
    id: ID
    revision: Revision
    digest: Digest


class BudgetSettlementEvidence(VersionedRecord):
    kind: Literal["budget_settlement"] = "budget_settlement"
    id: ID
    request_digest: Digest
    response_digest: Digest


TrajectoryEvidence = Annotated[
    LearningEvidence | BudgetSettlementEvidence, Field(discriminator="kind")
]


class Trajectory(VersionedRecord):
    id: ID
    decision: DecisionTrace
    split: Split
    trial_id: ID | None
    outcome: Literal[
        "measurement",
        "execution_failed",
        "not_started",
        "cancelled",
        "execution_unknown",
        "closed_unaccepted",
        "evaluation_invalid",
        "evaluation_incomplete",
        "evidence_invalidated",
        "unrankable",
        "proposal_failed",
        "not_admitted",
    ]
    metric_name: Name | None = None
    comparator_value: FiniteNumber | None = None
    candidate_value: FiniteNumber | None = None
    comparison_digest: Digest | None = None
    comparator_evaluation_id: ID | None = None
    candidate_evaluation_id: ID | None = None
    consumed: dict[Dimension, BalanceAmount] = Field(max_length=16)
    evidence: tuple[TrajectoryEvidence, ...] = Field(min_length=1, max_length=128)
    reward: FiniteNumber | None

    @model_validator(mode="after")
    def measured_outcome(self) -> Self:
        measurements = (
            self.metric_name,
            self.comparator_value,
            self.candidate_value,
            self.comparison_digest,
            self.comparator_evaluation_id,
            self.candidate_evaluation_id,
        )
        if self.outcome == "measurement":
            if (
                self.trial_id is None
                or self.reward is None
                or any(value is None for value in measurements)
            ):
                raise ValueError("Measurement trajectories require paired evidence and reward")
        elif self.reward is not None or any(value is not None for value in measurements):
            raise ValueError("Unqualified outcomes retain missing scores and reward")
        if len({(item.kind, item.id) for item in self.evidence}) != len(self.evidence):
            raise ValueError("Trajectory evidence contains duplicate records")
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.trajectory.v1", self.model_dump(mode="json"))


class TrajectoryDataset(VersionedRecord):
    id: ID
    plan_digest: Digest
    behavior_policy_digest: Digest
    reward_spec: RewardSpec
    evidence_through: Revision
    planned_episode_ids: tuple[ID, ...] = Field(min_length=1, max_length=256)
    trajectories: tuple[Trajectory, ...] = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def complete_batch(self) -> Self:
        episodes = tuple(row.decision.episode_id for row in self.trajectories)
        trials = [row.trial_id for row in self.trajectories if row.trial_id is not None]
        evaluations = [
            row.candidate_evaluation_id
            for row in self.trajectories
            if row.candidate_evaluation_id is not None
        ]
        if (
            len(set(self.planned_episode_ids)) != len(self.planned_episode_ids)
            or episodes != self.planned_episode_ids
            or len({row.id for row in self.trajectories}) != len(self.trajectories)
            or len({row.decision.proposal_request_id for row in self.trajectories})
            != len(self.trajectories)
            or len(set(trials)) != len(trials)
            or len(set(evaluations)) != len(evaluations)
        ):
            raise ValueError("Dataset must preserve the complete ordered episode batch")
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.trajectory-dataset.v1", self.model_dump(mode="json"))


class PolicyUpdate(VersionedRecord):
    algorithm: Literal["one-step-reinforce-v1"] = "one-step-reinforce-v1"
    behavior_policy_digest: Digest
    dataset_digest: Digest
    reward_spec_digest: Digest
    learning_rate: LearningRate
    sample_count: int = Field(strict=True, ge=1, le=256)
    gradient: tuple[FiniteNumber, ...] = Field(min_length=2, max_length=32)
    mean_reward: FiniteNumber
    policy: CategoricalPolicy
    epochs: Literal[1] = 1

    @model_validator(mode="after")
    def checkpoint_lineage(self) -> Self:
        if (
            len(self.gradient) != len(self.policy.actions)
            or self.policy.parent_digest != self.behavior_policy_digest
        ):
            raise ValueError("Updated checkpoint must retain behavior-policy lineage")
        return self


class TrainingJob(VersionedRecord):
    """Learning receipt over ordinary execution records, not another job state machine."""

    campaign_id: ID
    trial_id: ID
    job_id: ID
    attempt_id: ID
    dataset_digest: Digest
    behavior_policy_digest: Digest
    algorithm: Literal["one-step-reinforce-v1"] = "one-step-reinforce-v1"
    policy_artifact: ArtifactRef
    update_report: ArtifactRef
    evaluator_receipt_id: ID
