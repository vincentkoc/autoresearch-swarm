"""Pre-fitting commitment for one categorical policy's held-out seed panel."""

from typing import Literal, Self

from pydantic import Field, model_validator

from research_swarm.control.models import CampaignContract
from research_swarm.domain.budgets import BudgetKind, BudgetLimit
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.learning import CategoricalPolicy, RewardSpec
from research_swarm.domain.types import ID, Digest, Revision, Timestamp, VersionedRecord
from research_swarm.evaluation.statistics import FixedPairedPolicy


class FreezePolicyBenchmark(VersionedRecord):
    benchmark_id: ID
    training_admission_id: ID
    inputs_id: ID
    profile_id: ID
    gate: FixedPairedPolicy
    per_arm_limits: tuple[BudgetLimit, ...] = Field(min_length=4, max_length=4)
    objective: Literal["episode_reward"] = "episode_reward"
    incomplete: Literal["block_assessment"] = "block_assessment"

    @model_validator(mode="after")
    def equal_arm_contract(self) -> Self:
        limits = {item.dimension: item for item in self.per_arm_limits}
        if (
            set(limits) != {"attempts", "execution_ms", "proposal_requests", "trial_admissions"}
            or any(
                item.kind != BudgetKind.CUMULATIVE or item.limit <= 0 for item in limits.values()
            )
            or any(
                item.unit != ("ms" if name == "execution_ms" else "count")
                for name, item in limits.items()
            )
            or limits["proposal_requests"].limit != 1
            or limits["trial_admissions"].limit != 2
        ):
            raise ValueError(
                "Each benchmark arm needs explicit equal caps for one proposal and two trials"
            )
        return self


class PolicyBenchmark(VersionedRecord):
    profile: Literal["categorical-seed-holdout-v1"] = "categorical-seed-holdout-v1"
    request: FreezePolicyBenchmark
    learning_plan_digest: Digest
    inputs_digest: Digest
    dataset_digest: Digest
    training_profile_digest: Digest
    research_contract: CampaignContract
    initial_source: ArtifactRef
    comparison_digest: Digest
    baseline_policy: CategoricalPolicy
    reward_spec: RewardSpec
    evidence_through: Revision
    actor_id: ID
    recorded_at: Timestamp

    @property
    def digest(self) -> str:
        return record_digest("research.policy-benchmark.v1", self.model_dump(mode="json"))


class BenchmarkReservation(VersionedRecord):
    benchmark_id: ID
    benchmark_digest: Digest


class BenchmarkSeed(BenchmarkReservation):
    protocol_digest: Digest
    seed: int = Field(strict=True, ge=0, le=9_007_199_254_740_991)
