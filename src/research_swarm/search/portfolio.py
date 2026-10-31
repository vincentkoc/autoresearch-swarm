"""Asynchronous portfolio planning without a generation barrier or mutable local state."""

from __future__ import annotations

from dataclasses import dataclass

from research_swarm.control.models import CampaignData
from research_swarm.control.search_models import SearchState
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.search.common import (
    SearchAction,
    SearchSnapshot,
    SearchWait,
    _action,
    _decision,
    plan_baseline,
    plan_response_admission,
    plan_trial_selection,
    proposal_allowance,
)
from research_swarm.state.database import JsonObject


class PortfolioDriverConfiguration(VersionedRecord):
    baseline_trial_id: ID | None = None


@dataclass(frozen=True)
class RequestView:
    request: JsonObject
    response: JsonObject | None = None


@dataclass(frozen=True)
class PortfolioSnapshot:
    header: SearchSnapshot
    requests: tuple[RequestView, ...] = ()
    trials: tuple[SearchSnapshot, ...] = ()


def _idle(waits: list[SearchWait], fallback: SearchWait) -> SearchWait:
    if not waits:
        return fallback
    chosen = next((item for item in waits if item.state == "needs_review"), waits[0])
    return SearchWait(
        state=chosen.state,
        reason=chosen.reason,
        evidence_ids=tuple(
            dict.fromkeys(identifier for item in waits for identifier in item.evidence_ids)
        ),
    )


class PortfolioPolicy:
    @staticmethod
    def plan(snapshot: PortfolioSnapshot) -> SearchAction | SearchWait:
        header = snapshot.header
        campaign = CampaignData.model_validate(header.campaign["data"])
        configuration = campaign.contract.search
        portfolio = configuration.portfolio
        if (
            (configuration.policy, configuration.version) != ("portfolio", "portfolio-v1")
            or portfolio is None
            or configuration.max_parallel_trials > 16
            or campaign.contract.promotion.mode != "observed"
        ):
            return SearchWait(
                state="needs_review",
                reason="This driver requires sealed portfolio-v1 themes and observed promotion",
            )
        state = SearchState.model_validate(header.search["data"]) if header.search else None
        active = list(state.active_trial_ids) if state else []
        pending = list(state.pending_request_ids) if state else []
        if (
            [item.trial["id"] if item.trial else None for item in snapshot.trials] != active
            or [item.request["id"] for item in snapshot.requests] != pending
            or len(active) + len(pending) > configuration.max_parallel_trials
        ):
            return SearchWait(
                state="needs_review", reason="Portfolio snapshot does not identify every owned slot"
            )
        phase = header.campaign["state"]
        if phase in {"completed", "failed", "cancelled"}:
            return SearchWait(state="terminal", reason="Campaign is terminal")
        if campaign.incumbent_eligibility != "eligible":
            return SearchWait(state="needs_review", reason="Incumbent evidence is suspended")
        owner_id = state.selection_trial_id if state else None
        owner = next(
            (item for item in snapshot.trials if item.trial and item.trial["id"] == owner_id), None
        )
        if phase != "running":
            decision = _decision(owner) if owner else None
            if owner and owner.pin and decision and decision["state"] == "committed":
                return plan_trial_selection(owner)
            return SearchWait(state="paused", reason="Campaign does not admit fresh search actions")
        if campaign.incumbent_candidate_id is None:
            if not header.scan_complete:
                return SearchWait(
                    state="needs_review", reason="Bounded baseline evidence is incomplete"
                )
            return plan_baseline(header)

        waits: list[SearchWait] = []
        if owner is not None:
            result = (
                plan_trial_selection(owner)
                if owner.pin is not None
                else SearchWait(
                    state="needs_review",
                    reason="Selection owner has no original pin",
                    evidence_ids=(owner.trial["id"],) if owner.trial else (),
                )
            )
            if isinstance(result, SearchAction):
                return result
            waits.append(result)

        authority = header.context["authority"]
        revision = header.search["revision"] if header.search else 0
        for view in snapshot.requests:
            request = view.request["data"]
            if (
                request["incumbent_revision"] != campaign.incumbent_revision
                or (request.get("comparator_candidate_id") or request["parent_candidate_id"])
                != campaign.incumbent_candidate_id
                or request["controller_id"] != authority["controller_id"]
                or request["control_epoch"] != authority["control_epoch"]
                or request["campaign_contract_digest"] != campaign.contract_digest
                or request["policy_version"] != configuration.version
            ):
                return _action(
                    "search.cancel_proposal",
                    {
                        "campaign_id": header.campaign["id"],
                        "request_id": view.request["id"],
                        "reason": (
                            "Portfolio proposal context is superseded; "
                            "retain its response and request cost"
                        ),
                    },
                    revision,
                )

        for trial_view in snapshot.trials:
            if trial_view is owner:
                continue
            assert trial_view.trial is not None
            data = trial_view.trial["data"]
            if data["execution_outcome"] in {"pending", "unknown"} or data["resource_status"] in {
                "active",
                "unknown",
            }:
                waits.append(
                    SearchWait(
                        state="awaiting_execution",
                        reason="Unresolved execution retains its portfolio slot",
                        evidence_ids=(trial_view.trial["id"],),
                    )
                )
                continue
            if owner is not None:
                continue
            result = plan_trial_selection(trial_view)
            if isinstance(result, SearchAction):
                return result
            waits.append(result)

        if header.context.get("stopping_status", {}).get("target_met"):
            return _idle(
                waits, SearchWait(state="target_reached", reason="Declared incumbent target is met")
            )
        for view in snapshot.requests:
            if view.response is not None:
                return plan_response_admission(header, view.request)
            waits.append(
                SearchWait(
                    state="awaiting_proposal",
                    reason="Assigned agent has not responded",
                    evidence_ids=(view.request["id"],),
                )
            )
        if len(active) + len(pending) >= configuration.max_parallel_trials:
            return _idle(
                waits,
                SearchWait(state="needs_review", reason="Portfolio slots require explicit review"),
            )
        allowance = proposal_allowance(header, pending_count=len(pending))
        if allowance is not None:
            return _idle(waits, allowance)
        ordinal = state.request_count if state else 0
        seed = portfolio.first_seed + ordinal
        if seed > 9_007_199_254_740_991 or seed in campaign.contract.promotion.confirmation_seeds:
            return _idle(
                waits,
                SearchWait(
                    state="needs_review",
                    reason=(
                        "Next exploration seed exceeds its range or is reserved for confirmation"
                    ),
                ),
            )
        theme = portfolio.themes[ordinal % len(portfolio.themes)]
        return _action(
            "search.request_proposal",
            {
                "campaign_id": header.campaign["id"],
                "proposer_id": theme.proposer_id,
                "theme_id": theme.theme_id,
                "instructions": theme.instructions,
                "seed": seed,
                "expected_incumbent_revision": campaign.incumbent_revision,
                "expected_controller_id": authority["controller_id"],
                "expected_control_epoch": authority["control_epoch"],
            },
            revision,
        )
