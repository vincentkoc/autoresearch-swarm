"""Private, role-specific admission for fixed benchmark trial slots."""

from dataclasses import dataclass
from typing import Literal

from research_swarm.control.benchmark_execution_models import (
    BenchmarkArm,
    BenchmarkExecution,
    BenchmarkTrialSlot,
)
from research_swarm.control.benchmark_guard import benchmark_seed_key
from research_swarm.control.benchmark_models import BenchmarkSeed, PolicyBenchmark
from research_swarm.control.commands import SubmitTrial
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_models import CommittedPolicyDecision
from research_swarm.control.models import CampaignData, CandidateData
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction


@dataclass(frozen=True)
class BenchmarkTrialAuthority:
    arm_id: str
    role: Literal["comparator", "candidate"]
    execution_digest: str
    proposal_id: str | None = None


def execution_digest(execution: BenchmarkExecution) -> str:
    return record_digest("research.benchmark-execution.v1", execution.model_dump(mode="json"))


def require_trial_slot(
    transaction: Transaction,
    campaign: CampaignData,
    request: SubmitTrial,
    authority: BenchmarkTrialAuthority | None,
    consuming_proposal_id: str | None = None,
) -> str | None:
    arm_row = optional_record(transaction, "benchmark_arm", request.campaign_id)
    case_row = optional_record(
        transaction,
        "benchmark_seed",
        benchmark_seed_key(campaign.contract.protocol_digest, request.seed),
    )
    if arm_row is None and case_row is None and authority is None:
        return None
    if arm_row is None or authority is None or case_row is None:
        raise ResearchError(
            ErrorCode.COMPARISON_INELIGIBLE,
            "Held-out trial requires its exact benchmark arm authority",
        )
    arm = BenchmarkArm.model_validate(arm_row["data"])
    benchmark_row = transaction.get("policy_benchmark", arm.benchmark_id)
    benchmark = PolicyBenchmark.model_validate(benchmark_row["data"])
    execution_row = transaction.get("benchmark_execution", arm.benchmark_id)
    execution = BenchmarkExecution.model_validate(execution_row["data"])
    case = BenchmarkSeed.model_validate(case_row["data"])
    trial_id = arm.comparator_trial_id if authority.role == "comparator" else arm.candidate_trial_id
    slot_row = transaction.get("benchmark_trial_slot", trial_id)
    slot = BenchmarkTrialSlot.model_validate(slot_row["data"])
    candidate = CandidateData.model_validate(
        transaction.get("candidate", request.candidate_id)["data"]
    )
    if (
        arm_row["state"] != "immutable"
        or execution_row["state"] != "immutable"
        or benchmark_row["state"] != "immutable"
        or case_row["state"] != "reserved"
        or slot_row["state"] != "reserved"
        or authority.arm_id != request.campaign_id
        or authority.arm_id != arm.campaign_id
        or authority.arm_id not in execution.arm_ids
        or authority.execution_digest != execution_digest(execution)
        or execution.request.benchmark_id != arm.benchmark_id
        or execution.benchmark_digest != benchmark.digest
        or arm.benchmark_digest != benchmark.digest
        or case.benchmark_digest != benchmark.digest
        or case.benchmark_id != arm.benchmark_id
        or case.protocol_digest != campaign.contract.protocol_digest
        or case.seed != arm.seed
        or request.seed != arm.seed
        or campaign.contract_digest != arm.campaign_contract_digest
        or slot.benchmark_id != arm.benchmark_id
        or slot.arm_id != authority.arm_id
        or slot.trial_id != trial_id
        or slot.role != authority.role
    ):
        raise ResearchError(
            ErrorCode.COMPARISON_INELIGIBLE, "Benchmark trial slot or authority differs"
        )
    if authority.role == "comparator":
        valid = (
            request.candidate_id == arm.initial_candidate_id
            and request.validation_id == arm.initial_validation_id
            and candidate.source_artifact == benchmark.initial_source
            and authority.proposal_id is None
            and consuming_proposal_id is None
        )
    else:
        committed = CommittedPolicyDecision.model_validate(
            transaction.get("learning_decision", arm.proposal_request_id)["data"]
        )
        valid = (
            authority.role == "candidate"
            and authority.proposal_id == arm.proposal_request_id
            and consuming_proposal_id == arm.proposal_request_id
            and candidate.parent_ids == (arm.initial_candidate_id,)
            and candidate.proposer_id == arm.proposer_id
            and candidate.source_artifact == committed.source_artifact
            and committed.plan_id == arm.learning_plan_id
        )
    if not valid:
        raise ResearchError(
            ErrorCode.COMPARISON_INELIGIBLE, "Benchmark trial role or candidate differs"
        )
    return slot.trial_id


def consume_trial_slot(transaction: Transaction, trial_id: str) -> None:
    row = transaction.get("benchmark_trial_slot", trial_id)
    if row["state"] != "reserved":
        raise ResearchError(
            ErrorCode.REVISION_CONFLICT, "Benchmark trial slot was already consumed"
        )
    updated = transaction.update(
        "benchmark_trial_slot",
        trial_id,
        expected_revision=row["revision"],
        data=row["data"],
        state="consumed",
    )
    transaction.event("learning.benchmark_trial_admitted", updated, {"trial_id": trial_id})


def require_ordinary_proposal(transaction: Transaction, campaign_id: str) -> None:
    if optional_record(transaction, "benchmark_arm", campaign_id) is not None:
        raise ResearchError(
            ErrorCode.COMPARISON_INELIGIBLE, "Benchmark arm requires its planned held-out proposal"
        )
