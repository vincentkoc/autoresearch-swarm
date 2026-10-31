"""Derive candidate-slot authority only from the arm's committed agent response."""

from research_swarm.control.benchmark_execution_models import BenchmarkProposal
from research_swarm.control.benchmark_proposals import current_arm
from research_swarm.control.benchmark_trials import BenchmarkTrialAuthority, execution_digest
from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_models import (
    CommittedPolicyDecision,
    EpisodeBinding,
    LearningPlan,
)
from research_swarm.control.models import CampaignData
from research_swarm.control.search_models import ProposalRequest, ProposalResponse
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction


def candidate_authority(
    transaction: Transaction,
    request_id: str,
    request: ProposalRequest,
    response: ProposalResponse,
) -> BenchmarkTrialAuthority | None:
    if optional_record(transaction, "benchmark_arm", request.campaign_id) is None:
        return None
    arm, execution, benchmark = current_arm(transaction, request.campaign_id)
    prepared_row = transaction.get("benchmark_proposal", arm.campaign_id)
    prepared = BenchmarkProposal.model_validate(prepared_row["data"])
    plan = LearningPlan.model_validate(
        transaction.get("learning_plan", arm.learning_plan_id)["data"]
    )
    binding = EpisodeBinding.model_validate(transaction.get("learning_episode", request_id)["data"])
    episode = binding.episode
    campaign = CampaignData.model_validate(transaction.get("campaign", request.campaign_id)["data"])
    policy = benchmark.baseline_policy if arm.side == "baseline" else execution.learned_policy
    if (
        prepared_row["state"] != "immutable"
        or prepared.request.arm_id != arm.campaign_id
        or prepared.benchmark_id != arm.benchmark_id
        or prepared.execution_digest != execution_digest(execution)
        or prepared.proposal_request_id != request_id
        or request_id != arm.proposal_request_id
        or prepared.learning_plan_id != arm.learning_plan_id
        or prepared.learning_plan_digest != plan.digest
        or binding.plan_id != arm.learning_plan_id
        or binding.plan_digest != plan.digest
        or plan.request.plan_id != arm.learning_plan_id
        or plan.request.policy != policy
        or plan.request.reward_spec != benchmark.reward_spec
        or plan.episodes != (episode,)
        or episode.episode_id != request_id
        or episode.split != "held_out"
        or episode.campaign_id != arm.campaign_id
        or episode.seed != arm.seed
        or request.seed != arm.seed
        or episode.proposer_id != arm.proposer_id
        or request.proposer_id != arm.proposer_id
        or episode.comparator_candidate_id != arm.initial_candidate_id
        or request.parent_candidate_id != arm.initial_candidate_id
        or episode.comparator_evaluation_id != campaign.incumbent_evaluation_id
        or episode.comparator_evaluation_id != request.context["incumbent_evaluation_id"]
        or episode.comparison_digest != benchmark.comparison_digest
        or episode.campaign_contract_digest != arm.campaign_contract_digest
        or campaign.contract_digest != arm.campaign_contract_digest
        or episode.context_digest != request.context_digest
        or episode.proposal_digest
        != record_digest("research.learning-proposal.v1", request.model_dump(mode="json"))
    ):
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Benchmark proposal binding differs")
    comparator = eligible_evaluation(
        transaction, episode.comparator_evaluation_id, arm.campaign_id, campaign
    )
    if (
        comparator.trial.spec.trial_id != arm.comparator_trial_id
        or comparator.trial.spec.seed != arm.seed
        or comparator.comparison_digest != benchmark.comparison_digest
    ):
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Benchmark comparator changed")
    if response.request.outcome == "failed":
        return None
    committed = CommittedPolicyDecision.model_validate(
        transaction.get("learning_decision", request_id)["data"]
    )
    trace = committed.decision
    if (
        committed.plan_id != arm.learning_plan_id
        or committed.plan_digest != plan.digest
        or committed.actor_id != arm.proposer_id
        or trace.behavior_policy_digest != arm.policy_digest
        or trace.episode_id != request_id
        or trace.proposal_request_id != request_id
        or trace.campaign_id != arm.campaign_id
        or trace.seed != arm.seed
        or trace.context_digest != episode.context_digest
        or plan.request.action_artifacts.get(trace.action_id) != committed.source_artifact
        or trace.source_digest != committed.source_artifact.digest
        or response.request.source_artifact != committed.source_artifact
    ):
        raise ResearchError(
            ErrorCode.COMPARISON_INELIGIBLE, "Benchmark response differs from its committed action"
        )
    return BenchmarkTrialAuthority(
        arm_id=arm.campaign_id,
        role="candidate",
        execution_digest=execution_digest(execution),
        proposal_id=request_id,
    )
