"""Immutable arm inventory and fixed trial slots for a policy benchmark."""

from typing import Literal

from pydantic import Field

from research_swarm.domain.learning import CategoricalPolicy
from research_swarm.domain.types import ID, Digest, Timestamp, VersionedRecord


class StartPolicyBenchmark(VersionedRecord):
    benchmark_id: ID
    proposer_id: ID


class BenchmarkExecution(VersionedRecord):
    request: StartPolicyBenchmark
    benchmark_digest: Digest
    training_publication_digest: Digest
    learned_policy: CategoricalPolicy
    arm_ids: tuple[ID, ...] = Field(min_length=2, max_length=256)
    actor_id: ID
    recorded_at: Timestamp


class BenchmarkArm(VersionedRecord):
    benchmark_id: ID
    benchmark_digest: Digest
    campaign_id: ID
    campaign_contract_digest: Digest
    side: Literal["baseline", "challenger"]
    seed: int = Field(strict=True, ge=0, le=9_007_199_254_740_991)
    policy_digest: Digest
    initial_candidate_id: ID
    initial_validation_id: ID
    comparator_trial_id: ID
    candidate_trial_id: ID
    proposal_request_id: ID
    learning_plan_id: ID
    proposer_id: ID


class BenchmarkTrialSlot(VersionedRecord):
    benchmark_id: ID
    arm_id: ID
    trial_id: ID
    role: Literal["comparator", "candidate"]


class RequestBenchmarkProposal(VersionedRecord):
    arm_id: ID


class BenchmarkProposal(VersionedRecord):
    request: RequestBenchmarkProposal
    benchmark_id: ID
    execution_digest: Digest
    proposal_request_id: ID
    learning_plan_id: ID
    learning_plan_digest: Digest
    actor_id: ID
    recorded_at: Timestamp
