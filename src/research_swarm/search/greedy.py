"""A pure greedy planner; all effects pass through the research command service."""

from __future__ import annotations

from pydantic import Field

from research_swarm.control.models import CampaignData
from research_swarm.control.search_models import SearchState
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.search.common import (
    SearchAction as SearchAction,
)
from research_swarm.search.common import (
    SearchSnapshot as SearchSnapshot,
)
from research_swarm.search.common import (
    SearchWait as SearchWait,
)
from research_swarm.search.common import (
    _action,
    _decision,
    plan_baseline,
    plan_response_admission,
    plan_trial_selection,
    proposal_allowance,
)


class GreedyConfiguration(VersionedRecord):
    proposer_id: ID
    first_seed: int = Field(default=0, strict=True, ge=0, le=9_007_199_254_740_991)
    instructions: str = Field(
        default="Propose one focused change from the eligible incumbent.", max_length=8192
    )
    baseline_trial_id: ID | None = None


class GreedyPolicy:
    @staticmethod
    def plan(snapshot: SearchSnapshot, config: GreedyConfiguration) -> SearchAction | SearchWait:
        campaign_id = snapshot.campaign["id"]
        campaign = CampaignData.model_validate(snapshot.campaign["data"])
        search = SearchState.model_validate(snapshot.search["data"]) if snapshot.search else None
        revision = snapshot.search["revision"] if snapshot.search else 0
        authority = snapshot.context["authority"]
        fence = {
            "expected_controller_id": authority["controller_id"],
            "expected_control_epoch": authority["control_epoch"],
        }
        if (campaign.contract.search.policy, campaign.contract.search.version) != (
            "greedy",
            "greedy-v1",
        ):
            return SearchWait(
                state="needs_review", reason="This driver requires the sealed greedy-v1 policy"
            )
        if not snapshot.scan_complete:
            return SearchWait(
                state="needs_review",
                reason="Bounded evidence scan cannot establish the next action",
            )
        phase = snapshot.campaign["state"]
        if phase in {"completed", "failed", "cancelled"}:
            return SearchWait(state="terminal", reason="Campaign is terminal")
        decision = _decision(snapshot)
        if phase != "running" and not (
            decision and decision["state"] == "committed" and snapshot.pin
        ):
            return SearchWait(state="paused", reason="Campaign does not admit fresh search actions")
        if campaign.incumbent_eligibility != "eligible":
            return SearchWait(state="needs_review", reason="Incumbent evidence is suspended")
        if campaign.incumbent_candidate_id is None:
            return plan_baseline(snapshot)
        if search and search.active_trial_ids:
            if len(search.active_trial_ids) != 1 or snapshot.trial is None:
                return SearchWait(
                    state="needs_review",
                    reason="Greedy search requires one identified active trial",
                )
            return plan_trial_selection(snapshot)
        if snapshot.context.get("stopping_status", {}).get("target_met"):
            return SearchWait(state="target_reached", reason="Declared incumbent target is met")
        if search and search.pending_request_ids:
            if len(search.pending_request_ids) != 1 or snapshot.request is None:
                return SearchWait(
                    state="needs_review",
                    reason="Greedy search requires one identified pending request",
                )
            if snapshot.response is None:
                return SearchWait(
                    state="awaiting_proposal",
                    reason="Assigned agent has not responded",
                    evidence_ids=(snapshot.request["id"],),
                )
            return plan_response_admission(snapshot, snapshot.request)
        if (
            campaign.contract.promotion.mode == "paired_confirmation"
            and snapshot.confirmation is not None
        ):
            return SearchWait(
                state="budget_exhausted",
                reason="The campaign's single confirmation panel is already allocated",
            )
        allowance = proposal_allowance(snapshot)
        if allowance is not None:
            return allowance
        seed = config.first_seed + (search.request_count if search else 0)
        if seed > 9_007_199_254_740_991 or seed in campaign.contract.promotion.confirmation_seeds:
            return SearchWait(
                state="needs_review",
                reason="Next exploration seed exceeds its range or is reserved for confirmation",
            )
        return _action(
            "search.request_proposal",
            {
                "campaign_id": campaign_id,
                "proposer_id": config.proposer_id,
                "seed": seed,
                "expected_incumbent_revision": campaign.incumbent_revision,
                **fence,
                "instructions": config.instructions,
            },
            revision,
        )
