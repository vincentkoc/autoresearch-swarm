"""Freeze complete episode plans before sampling or observing candidate outcomes."""

from __future__ import annotations

from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_models import (
    CommitPolicyDecision,
    CommittedPolicyDecision,
    EpisodeBinding,
    FreezeLearningPlan,
    FrozenEpisode,
    LearningPlan,
    PlanEpisode,
)
from research_swarm.control.models import CampaignData
from research_swarm.control.proposals import ProposalControl, search_state
from research_swarm.control.search_models import ProposalRequest, SearchState
from research_swarm.control.workers import authority
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.learning.reinforce import decision
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Database, JsonObject, Transaction


def live_proposal(transaction: Transaction, identifier: str) -> tuple[JsonObject, ProposalRequest]:
    row = transaction.get("proposal_request", identifier)
    request = ProposalRequest.model_validate(row["data"])
    campaign_row = transaction.get("campaign", request.campaign_id)
    campaign = CampaignData.model_validate(campaign_row["data"])
    state = SearchState.model_validate(
        search_state(transaction, request.campaign_id, campaign)["data"]
    )
    if (
        row["state"] != "pending"
        or identifier not in state.pending_request_ids
        or optional_record(transaction, "proposal_response", identifier) is not None
        or optional_record(transaction, "proposal_cancellation", identifier) is not None
        or campaign_row["state"] != "running"
    ):
        raise ResearchError(
            ErrorCode.INVALID_TRANSITION, "Learning requires a pending live proposal"
        )
    if (
        authority(transaction) != (request.controller_id, request.control_epoch)
        or campaign.contract_digest != request.campaign_contract_digest
        or campaign.incumbent_revision != request.incumbent_revision
        or campaign.incumbent_candidate_id != request.parent_candidate_id
        or campaign.incumbent_evaluation_id != request.context["incumbent_evaluation_id"]
        or campaign.incumbent_eligibility != "eligible"
        or record_digest("research.proposal-context", request.context) != request.context_digest
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Learning proposal context is no longer current")
    return row, request


def freeze_episode(
    transaction: Transaction, episode: PlanEpisode, intent: FreezeLearningPlan
) -> FrozenEpisode:
    row, request = live_proposal(transaction, episode.request_id)
    if optional_record(transaction, "learning_episode", episode.request_id) is not None:
        raise ResearchError(
            ErrorCode.IDEMPOTENCY_CONFLICT, "Proposal already belongs to a learning plan"
        )
    campaign = CampaignData.model_validate(transaction.get("campaign", request.campaign_id)["data"])
    comparator_id = request.context["incumbent_evaluation_id"]
    comparator = eligible_evaluation(transaction, comparator_id, request.campaign_id, campaign)
    if (
        comparator.metric.name != intent.reward_spec.metric_name
        or comparator.metric.direction != intent.reward_spec.direction
        or comparator.comparison_digest != request.context["comparison_digest"]
        or comparator.trial.spec.seed != request.seed
    ):
        raise ResearchError(
            ErrorCode.COMPARISON_INELIGIBLE,
            "Learning reward requires the frozen same-seed comparator",
        )
    return FrozenEpisode(
        episode_id=episode.request_id,
        campaign_id=request.campaign_id,
        split=episode.split,
        proposal_revision=row["revision"],
        proposal_digest=record_digest("research.learning-proposal.v1", row["data"]),
        proposer_id=request.proposer_id,
        context_digest=request.context_digest,
        context_watermark=request.context_watermark,
        seed=request.seed,
        campaign_contract_digest=request.campaign_contract_digest,
        controller_id=request.controller_id,
        control_epoch=request.control_epoch,
        incumbent_revision=request.incumbent_revision,
        comparator_candidate_id=request.parent_candidate_id,
        comparator_evaluation_id=comparator_id,
        comparison_digest=comparator.comparison_digest,
    )


class LearningControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def freeze(self, context: CommandContext, intent: FreezeLearningPlan) -> JsonObject:
        def commit(transaction: Transaction) -> JsonObject:
            digest = record_digest(
                "research.freeze-learning-plan.v1", intent.model_dump(mode="json")
            )
            previous = optional_record(transaction, "learning_plan", intent.plan_id)
            if previous is not None:
                if LearningPlan.model_validate(previous["data"]).request_digest != digest:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Learning plan is immutable"
                    )
                return previous
            episodes = tuple(freeze_episode(transaction, item, intent) for item in intent.episodes)
            for action, artifact in intent.action_artifacts.items():
                ArtifactCatalog.available(transaction, artifact)
                ArtifactCatalog.retain(
                    transaction,
                    ArtifactRetention(
                        artifact_id=artifact.id,
                        owner_kind="learning_plan",
                        owner_id=intent.plan_id,
                        purpose=action,
                    ),
                )
            watermark = transaction.connection.execute(
                "SELECT next_sequence-1 FROM workspaces WHERE id=?", (transaction.workspace_id,)
            ).one()[0]
            plan = LearningPlan(
                request=intent,
                request_digest=digest,
                episodes=episodes,
                evidence_through=watermark,
                actor_id=context.principal.id,
                recorded_at=utc_now(),
            )
            result = transaction.insert(
                "learning_plan", intent.plan_id, plan.model_dump(mode="json"), state="immutable"
            )
            for episode in episodes:
                binding = transaction.insert(
                    "learning_episode",
                    episode.episode_id,
                    EpisodeBinding(
                        plan_id=intent.plan_id, plan_digest=plan.digest, episode=episode
                    ).model_dump(mode="json"),
                    state="immutable",
                    campaign_id=episode.campaign_id,
                    parent_id=intent.plan_id,
                )
                transaction.event("learning.episode_bound", binding, {"plan_id": intent.plan_id})
            transaction.event("learning.plan_frozen", result, {"episode_count": len(episodes)})
            return result

        return self.database.execute(
            context,
            "learning.freeze_plan",
            intent.model_dump(mode="json"),
            "learning.manage",
            commit,
        )

    def commit_decision(self, context: CommandContext, intent: CommitPolicyDecision) -> JsonObject:
        def commit(transaction: Transaction) -> JsonObject:
            ProposalControl._owned(transaction, intent.request_id)
            existing = optional_record(transaction, "learning_decision", intent.request_id)
            if existing is not None:
                return existing
            binding = EpisodeBinding.model_validate(
                transaction.get("learning_episode", intent.request_id)["data"]
            )
            plan = LearningPlan.model_validate(
                transaction.get("learning_plan", binding.plan_id)["data"]
            )
            if binding.plan_digest != plan.digest:
                raise ResearchError(ErrorCode.STALE_FENCE, "Learning plan binding differs")
            row, request = live_proposal(transaction, intent.request_id)
            episode = binding.episode
            if (
                row["revision"] != episode.proposal_revision
                or record_digest("research.learning-proposal.v1", row["data"])
                != episode.proposal_digest
                or episode not in plan.episodes
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Learning proposal binding changed")
            campaign = CampaignData.model_validate(
                transaction.get("campaign", request.campaign_id)["data"]
            )
            comparator = eligible_evaluation(
                transaction, episode.comparator_evaluation_id, request.campaign_id, campaign
            )
            if comparator.comparison_digest != episode.comparison_digest:
                raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Learning comparator changed")
            trace = decision(
                plan.request.policy,
                episode_id=episode.episode_id,
                campaign_id=episode.campaign_id,
                proposal_request_id=intent.request_id,
                context_digest=episode.context_digest,
                seed=episode.seed,
            )
            result = transaction.insert(
                "learning_decision",
                intent.request_id,
                CommittedPolicyDecision(
                    plan_id=binding.plan_id,
                    plan_digest=binding.plan_digest,
                    decision=trace,
                    source_artifact=plan.request.action_artifacts[trace.action_id],
                    actor_id=context.principal.id,
                    recorded_at=utc_now(),
                ).model_dump(mode="json"),
                state="immutable",
                campaign_id=episode.campaign_id,
                parent_id=intent.request_id,
            )
            transaction.event("learning.decision_committed", result, {"plan_id": binding.plan_id})
            return result

        return self.database.execute(
            context,
            "learning.commit_decision",
            intent.model_dump(mode="json"),
            "proposal.respond",
            commit,
        )
