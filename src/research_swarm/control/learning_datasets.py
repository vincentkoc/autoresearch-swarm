"""Qualify complete development batches from actual admission, evaluation and cost."""

from __future__ import annotations

from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.learning_costs import episode_cost
from research_swarm.control.learning_evidence import capture_record
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_models import (
    CommittedPolicyDecision,
    EpisodeBinding,
    FrozenEpisode,
    LearningPlan,
    QualifiedTrajectoryDataset,
    QualifyTrajectoryDataset,
)
from research_swarm.control.models import (
    CampaignData,
    CandidateData,
    CandidateValidation,
    TrialData,
)
from research_swarm.control.search_models import (
    ProposalAdmission,
    ProposalRequest,
    ProposalResponse,
)
from research_swarm.control.search_observations import trial_facts
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.learning import LearningEvidence, Trajectory, TrajectoryDataset
from research_swarm.domain.types import utc_now
from research_swarm.learning.reinforce import reward, validate_batch
from research_swarm.state.database import Database, JsonObject, Transaction

MAX_DATASET_BYTES = 8 * 1024 * 1024


def blocked(episode_id: str, outcome: str) -> None:
    raise ResearchError(
        ErrorCode.COMPARISON_INELIGIBLE,
        "Every planned development episode requires complete qualified evidence",
        details={"episode_id": episode_id, "outcome": outcome},
    )


def qualified_episode(
    transaction: Transaction, plan: LearningPlan, episode: FrozenEpisode
) -> Trajectory:
    evidence: list[LearningEvidence] = []
    request_id, campaign_id = episode.episode_id, episode.campaign_id
    capture_record(transaction, "learning_plan", plan.request.plan_id, evidence)
    binding = EpisodeBinding.model_validate(
        capture_record(transaction, "learning_episode", request_id, evidence)["data"]
    )
    if (
        binding.plan_id != plan.request.plan_id
        or binding.plan_digest != plan.digest
        or binding.episode != episode
    ):
        blocked(request_id, "binding_changed")
    decision_row = optional_record(transaction, "learning_decision", request_id)
    if decision_row is None:
        blocked(request_id, "decision_missing")
    committed = CommittedPolicyDecision.model_validate(
        capture_record(transaction, "learning_decision", request_id, evidence)["data"]
    )
    trace = committed.decision
    request = ProposalRequest.model_validate(
        capture_record(transaction, "proposal_request", request_id, evidence)["data"]
    )
    campaign = CampaignData.model_validate(transaction.get("campaign", campaign_id)["data"])
    if (
        committed.plan_id != plan.request.plan_id
        or committed.plan_digest != plan.digest
        or committed.actor_id != episode.proposer_id
        or trace.episode_id != request_id
        or trace.proposal_request_id != request_id
        or trace.campaign_id != campaign_id
        or trace.context_digest != episode.context_digest
        or trace.seed != episode.seed
        or trace.behavior_policy_digest != plan.request.policy.digest
        or record_digest("research.learning-proposal.v1", request.model_dump(mode="json"))
        != episode.proposal_digest
        or request.proposer_id != episode.proposer_id
        or request.campaign_id != campaign_id
        or campaign.contract_digest != episode.campaign_contract_digest
        or request.context["incumbent_evaluation_id"] != episode.comparator_evaluation_id
        or committed.source_artifact.digest != trace.source_digest
        or plan.request.action_artifacts.get(trace.action_id) != committed.source_artifact
    ):
        blocked(request_id, "decision_or_context_changed")
    if optional_record(transaction, "proposal_response", request_id) is None:
        blocked(request_id, "response_missing")
    response = ProposalResponse.model_validate(
        capture_record(transaction, "proposal_response", request_id, evidence)["data"]
    )
    if response.request.outcome == "failed":
        blocked(request_id, "proposal_failed")
    if (
        response.request.request_id != request_id
        or response.responder_id != episode.proposer_id
        or response.campaign_id != campaign_id
        or response.response_digest
        != record_digest("research.proposal-response", response.request.model_dump(mode="json"))
        or response.request.source_artifact != committed.source_artifact
    ):
        blocked(request_id, "response_changed")
    if optional_record(transaction, "policy_transition", request_id) is None:
        blocked(request_id, "not_admitted")
    admission = ProposalAdmission.model_validate(
        capture_record(transaction, "policy_transition", request_id, evidence)["data"]
    )
    if (
        admission.request_id != request_id
        or admission.campaign_id != campaign_id
        or admission.response_digest != response.response_digest
        or admission.outcome != "admitted"
        or admission.trial_id is None
        or admission.candidate_id is None
        or admission.validation_id is None
    ):
        blocked(request_id, "not_admitted")
    assert (
        admission.trial_id is not None
        and admission.candidate_id is not None
        and admission.validation_id is not None
    )
    candidate = CandidateData.model_validate(
        capture_record(transaction, "candidate", admission.candidate_id, evidence)["data"]
    )
    validation = CandidateValidation.model_validate(
        capture_record(transaction, "candidate_validation", admission.validation_id, evidence)[
            "data"
        ]
    )
    trial_row = capture_record(transaction, "trial", admission.trial_id, evidence)
    trial = TrialData.model_validate(trial_row["data"])
    trial.spec.verify()
    if (
        candidate.campaign_id != campaign_id
        or candidate.proposer_id != episode.proposer_id
        or candidate.source_artifact != committed.source_artifact
        or candidate.parent_ids != (episode.comparator_candidate_id,)
        or trial.candidate_id != admission.candidate_id
        or trial.validation_id != admission.validation_id
        or trial.spec.campaign_id != campaign_id
        or trial.spec.seed != episode.seed
        or trial.spec.source.artifact_id != candidate.source_artifact.id
        or trial.spec.source.tree_digest != candidate.source.tree_digest
        or trial.spec.extensions.get("campaign_contract_digest") != episode.campaign_contract_digest
        or not validation.valid
        or validation.candidate_id != admission.candidate_id
        or validation.source_tree_digest != candidate.source.tree_digest
        or validation.campaign_contract_digest != episode.campaign_contract_digest
    ):
        blocked(request_id, "admission_identity_changed")
    facts = trial_facts(transaction, trial_row, campaign)
    if facts.outcome != "measurement" or facts.evaluation_id is None:
        blocked(request_id, facts.outcome)
    assert facts.evaluation_id is not None
    comparator = eligible_evaluation(
        transaction, episode.comparator_evaluation_id, campaign_id, campaign
    )
    evaluated = eligible_evaluation(transaction, facts.evaluation_id, campaign_id, campaign)
    spec = plan.request.reward_spec
    if (
        comparator.trial.candidate_id != episode.comparator_candidate_id
        or evaluated.trial.candidate_id != admission.candidate_id
        or comparator.trial.spec.seed != episode.seed
        or evaluated.trial.spec.seed != episode.seed
        or comparator.comparison_digest != episode.comparison_digest
        or evaluated.comparison_digest != episode.comparison_digest
        or comparator.metric.name != spec.metric_name
        or evaluated.metric.name != spec.metric_name
        or comparator.metric.direction != spec.direction
        or evaluated.metric.direction != spec.direction
    ):
        blocked(request_id, "comparison_changed")
    for evaluated_side in (comparator, evaluated):
        for dependency in evaluated_side.dependencies:
            capture_record(transaction, dependency.kind, dependency.id, evidence)
        capture_record(
            transaction, "evaluation_request", evaluated_side.authorization.request.id, evidence
        )
    consumed, settlements = episode_cost(
        transaction, request_id, request, admission.trial_id, trial, evidence
    )
    if len(evidence) + len(settlements) > 128:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Trajectory evidence exceeds its bound"
        )
    trajectory = Trajectory(
        id=request_id,
        decision=trace,
        split=episode.split,
        trial_id=admission.trial_id,
        outcome="measurement",
        metric_name=spec.metric_name,
        comparator_value=comparator.metric.value,
        candidate_value=evaluated.metric.value,
        comparison_digest=episode.comparison_digest,
        comparator_evaluation_id=episode.comparator_evaluation_id,
        candidate_evaluation_id=facts.evaluation_id,
        consumed=consumed,
        evidence=(*evidence, *settlements),
        reward=0.0,
    )
    return trajectory.model_copy(update={"reward": reward(spec, trajectory)})


def _build_dataset(
    transaction: Transaction, plan: LearningPlan, identifier: str, evidence_through: int
) -> TrajectoryDataset:
    episodes = tuple(item for item in plan.episodes if item.split == "development")
    if not episodes:
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Plan has no development episodes")
    data = TrajectoryDataset(
        id=identifier,
        plan_digest=plan.digest,
        behavior_policy_digest=plan.request.policy.digest,
        reward_spec=plan.request.reward_spec,
        evidence_through=evidence_through,
        planned_episode_ids=tuple(item.episode_id for item in episodes),
        trajectories=tuple(qualified_episode(transaction, plan, item) for item in episodes),
    )
    validate_batch(plan.request.policy, data)
    if len(canonical_bytes(data.model_dump(mode="json"))) > MAX_DATASET_BYTES:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Learning dataset exceeds its byte bound"
        )
    return data


def build_dataset(
    transaction: Transaction, plan: LearningPlan, identifier: str, evidence_through: int
) -> TrajectoryDataset:
    try:
        return _build_dataset(transaction, plan, identifier, evidence_through)
    except ValueError as error:
        raise ResearchError(
            ErrorCode.EVIDENCE_INVALIDATED, "Recorded learning evidence violates its contract"
        ) from error


def require_current_dataset(
    transaction: Transaction, identifier: str
) -> QualifiedTrajectoryDataset:
    row = transaction.get("learning_dataset", identifier)
    qualified = QualifiedTrajectoryDataset.model_validate(row["data"])
    plan = LearningPlan.model_validate(transaction.get("learning_plan", qualified.plan_id)["data"])
    if row["state"] != "qualified" or plan.digest != qualified.plan_digest:
        raise ResearchError(ErrorCode.STALE_FENCE, "Qualified learning plan changed")
    current = build_dataset(transaction, plan, identifier, qualified.dataset.evidence_through)
    if current.digest != qualified.dataset.digest:
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Qualified learning evidence changed")
    return qualified


class LearningDatasetControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def qualify(self, context: CommandContext, intent: QualifyTrajectoryDataset) -> JsonObject:
        def commit(transaction: Transaction) -> JsonObject:
            previous = optional_record(transaction, "learning_dataset", intent.dataset_id)
            if previous is not None:
                if previous["data"]["plan_id"] != intent.plan_id:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Dataset belongs to another plan"
                    )
                return previous
            plan = LearningPlan.model_validate(
                transaction.get("learning_plan", intent.plan_id)["data"]
            )
            watermark = transaction.connection.execute(
                "SELECT next_sequence-1 FROM workspaces WHERE id=?", (transaction.workspace_id,)
            ).one()[0]
            data = build_dataset(transaction, plan, intent.dataset_id, watermark)
            qualified = QualifiedTrajectoryDataset(
                plan_id=intent.plan_id,
                plan_digest=plan.digest,
                dataset=data,
                actor_id=context.principal.id,
                recorded_at=utc_now(),
            )
            row = transaction.insert(
                "learning_dataset",
                intent.dataset_id,
                qualified.model_dump(mode="json"),
                state="qualified",
                parent_id=intent.plan_id,
            )
            transaction.event(
                "learning.dataset_qualified", row, {"episode_count": len(data.trajectories)}
            )
            return row

        return self.database.execute(
            context,
            "learning.qualify_dataset",
            intent.model_dump(mode="json"),
            "learning.manage",
            commit,
        )
