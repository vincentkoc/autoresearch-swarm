"""Sealed portfolio admission and one durable owner for scientific selection."""

from __future__ import annotations

from typing import TYPE_CHECKING

from research_swarm.control.models import CampaignData, PortfolioConfiguration
from research_swarm.control.observation_models import (
    ObservationSelection,
    TrialObservation,
    observation_selection_id,
)
from research_swarm.control.search_models import RequestProposal, SearchState
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction

if TYPE_CHECKING:
    from research_swarm.control.promotions import PromotionAssessment


def portfolio_contract(campaign: CampaignData) -> PortfolioConfiguration | None:
    configuration = campaign.contract.search
    if configuration.policy != "portfolio":
        if configuration.portfolio is not None:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID,
                "Portfolio configuration requires its policy",
            )
        return None
    if (
        configuration.version != "portfolio-v1"
        or configuration.portfolio is None
        or configuration.max_parallel_trials > 16
        or campaign.contract.promotion.mode != "observed"
    ):
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE,
            "portfolio-v1 requires sealed themes, at most 16 slots, and observed promotion",
        )
    return configuration.portfolio


def validate_portfolio_request(
    transaction: Transaction,
    campaign: CampaignData,
    state: SearchState,
    request: RequestProposal,
) -> None:
    portfolio = portfolio_contract(campaign)
    if portfolio is None:
        if request.theme_id is not None:
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Policy has no sealed theme")
        return
    from research_swarm.control.campaigns import target_met

    if target_met(transaction, request.campaign_id, campaign):
        raise ResearchError(ErrorCode.INVALID_TRANSITION, "Declared research target is met")
    theme = portfolio.themes[state.request_count % len(portfolio.themes)]
    if (
        request.theme_id != theme.theme_id
        or request.proposer_id != theme.proposer_id
        or request.instructions != theme.instructions
        or request.seed != portfolio.first_seed + state.request_count
    ):
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Request differs from sealed theme")
    require_portfolio_trial_allowance(
        transaction,
        request.campaign_id,
        campaign,
        len(state.pending_request_ids),
    )


def require_portfolio_trial_allowance(
    transaction: Transaction,
    campaign_id: str,
    campaign: CampaignData,
    pending_count: int,
) -> None:
    trials = transaction.connection.execute(
        "SELECT COUNT(*) FROM records WHERE workspace_id=? AND campaign_id=? AND kind='trial'",
        (transaction.workspace_id, campaign_id),
    ).fetchone()[0]
    if trials + pending_count >= campaign.contract.stopping.max_trials:
        raise ResearchError(ErrorCode.BUDGET_EXHAUSTED, "No unpromised trial allowance remains")


def require_portfolio_selection(
    transaction: Transaction,
    campaign: CampaignData,
    assessment: PromotionAssessment,
) -> None:
    if portfolio_contract(campaign) is None:
        return
    from research_swarm.control.proposals import search_state
    from research_swarm.control.search_observations import trial_facts

    decision = assessment.decision
    state = SearchState.model_validate(
        search_state(transaction, decision.campaign_id, campaign)["data"]
    )
    owner = state.selection_trial_id
    if campaign.incumbent_candidate_id is None or assessment.replaces_suspension_id is not None:
        if owner is not None:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Resolve the existing selection first"
            )
        return
    if owner is None:
        raise ResearchError(
            ErrorCode.INVALID_TRANSITION, "Portfolio promotion requires a selection owner"
        )
    selected = ObservationSelection.model_validate(
        transaction.get(
            "policy_transition",
            observation_selection_id(transaction.workspace_id, decision.campaign_id, owner),
        )["data"]
    )
    row = transaction.get("search_observation", selected.observation_id)
    observed = TrialObservation.model_validate(row["data"])
    facts = observed.facts
    if (
        facts.trial_id != owner
        or facts.candidate_id != decision.candidate_id
        or facts.evaluation_id != assessment.candidate_evaluation_id
        or facts.comparison_digest != assessment.comparison_digest
        or observed.comparator_id != decision.comparator_id
        or observed.comparator_evaluation_id != assessment.comparator_evaluation_id
        or observed.incumbent_revision != decision.expected_incumbent_revision
        or selected.observation_digest
        != record_digest("research.selected-observation", row["data"])
    ):
        raise ResearchError(
            ErrorCode.COMPARISON_INELIGIBLE, "Promotion differs from the owned selection"
        )
    current = trial_facts(transaction, transaction.get("trial", owner), campaign)
    if (
        record_digest("research.trial-facts", current.model_dump(mode="json"))
        != observed.facts_digest
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Selected trial facts changed")
