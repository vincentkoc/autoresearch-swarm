"""Pure planning primitives shared by research policies."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from pydantic import Field

from research_swarm.control.models import CampaignData, TrialData
from research_swarm.control.observation_models import TrialObservation
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.types import ID, Name, Revision, VersionedRecord
from research_swarm.state.database import JsonObject


class SearchAction(VersionedRecord):
    operation: Name
    payload: JsonObject
    expected_revision: Revision | None = None
    basis: JsonObject = Field(default_factory=dict)

    def command_key(self, campaign_id: str) -> str:
        return record_digest(
            "research.greedy-command-v1",
            {
                "campaign_id": campaign_id,
                "action": self.model_dump(mode="json"),
            },
        )


class SearchWait(VersionedRecord):
    state: Literal[
        "awaiting_baseline",
        "awaiting_proposal",
        "awaiting_execution",
        "awaiting_evaluation",
        "awaiting_confirmation",
        "paused",
        "terminal",
        "needs_review",
        "budget_exhausted",
        "target_reached",
    ]
    reason: str
    evidence_ids: tuple[ID, ...] = ()


@dataclass(frozen=True)
class SearchSnapshot:
    campaign: JsonObject
    context: JsonObject
    search: JsonObject | None = None
    request: JsonObject | None = None
    response: JsonObject | None = None
    trial: JsonObject | None = None
    observation: JsonObject | None = None
    pin: JsonObject | None = None
    decisions: tuple[JsonObject, ...] = ()
    confirmation: JsonObject | None = None
    confirmation_trials: tuple[JsonObject, ...] = ()
    receipt_id: str | None = None
    scan_complete: bool = True


def _action(
    operation: str, payload: JsonObject, revision: int | None = None, **basis: object
) -> SearchAction:
    return SearchAction(
        operation=operation,
        payload={"schema_version": "1.0", **payload},
        expected_revision=revision,
        basis=dict(basis),
    )


def _decision(snapshot: SearchSnapshot) -> JsonObject | None:
    if not snapshot.decisions:
        return None
    # A second principal can create an equivalent assessment. Prefer the accepted selection.
    return min(snapshot.decisions, key=lambda row: (row["state"] != "committed", row["id"]))


def plan_baseline(snapshot: SearchSnapshot) -> SearchAction | SearchWait:
    campaign_id = snapshot.campaign["id"]
    campaign = CampaignData.model_validate(snapshot.campaign["data"])
    decision = _decision(snapshot)
    if snapshot.trial is None:
        return SearchWait(state="awaiting_baseline", reason="Choose an explicit baseline trial")
    baseline = TrialData.model_validate(snapshot.trial["data"])
    if baseline.execution_outcome in {
        "failed",
        "cancelled",
        "closed_unaccepted",
    } or baseline.evaluation_status in {"invalid", "incomplete"}:
        return SearchWait(
            state="needs_review",
            reason="Baseline did not produce eligible evidence",
            evidence_ids=(snapshot.trial["id"],),
        )
    if baseline.execution_outcome in {"pending", "unknown"}:
        return SearchWait(
            state="awaiting_execution",
            reason="Baseline execution is unresolved",
            evidence_ids=(snapshot.trial["id"],),
        )
    if snapshot.receipt_id is None:
        return SearchWait(
            state="awaiting_evaluation",
            reason="Baseline requires authenticated evaluation",
            evidence_ids=(snapshot.trial["id"],),
        )
    if decision is None:
        return _action(
            "promotion.assess",
            {
                "campaign_id": campaign_id,
                "candidate_evaluation_id": snapshot.receipt_id,
                "expected_incumbent_revision": campaign.incumbent_revision,
            },
        )
    if decision["data"]["decision"]["outcome"] != "promote" or decision["state"] != "assessed":
        return SearchWait(state="needs_review", reason="Baseline assessment is not eligible")
    return _action("promotion.commit", {"decision_id": decision["id"]})


def plan_trial_selection(snapshot: SearchSnapshot) -> SearchAction | SearchWait:
    if not snapshot.scan_complete or snapshot.trial is None:
        return SearchWait(state="needs_review", reason="Bounded trial evidence is incomplete")
    campaign_id = snapshot.campaign["id"]
    campaign = CampaignData.model_validate(snapshot.campaign["data"])
    revision = snapshot.search["revision"] if snapshot.search else 0
    authority = snapshot.context["authority"]
    fence = {
        "expected_controller_id": authority["controller_id"],
        "expected_control_epoch": authority["control_epoch"],
    }
    decision = _decision(snapshot)
    trial = TrialData.model_validate(snapshot.trial["data"])
    if snapshot.pin is not None and snapshot.observation is None:
        return SearchWait(state="needs_review", reason="Pinned observation is unavailable")
    if snapshot.observation is None:
        if trial.execution_outcome == "pending" or trial.resource_status == "active":
            return SearchWait(
                state="awaiting_execution",
                reason="Trial execution is still active",
                evidence_ids=(snapshot.trial["id"],),
            )
        if trial.execution_outcome == "succeeded" and trial.evaluation_status == "pending":
            return SearchWait(
                state="awaiting_evaluation",
                reason="Trial requires independent evaluator authority",
                evidence_ids=(snapshot.trial["id"],),
            )
        return _action(
            "search.observe_trial",
            {"campaign_id": campaign_id, "trial_id": snapshot.trial["id"]},
            snapshot.trial["revision"],
            incumbent_revision=campaign.incumbent_revision,
        )
    observed = TrialObservation.model_validate(snapshot.observation["data"])
    if observed.facts.trial_revision != snapshot.trial["revision"]:
        return SearchWait(
            state="needs_review",
            reason="Chosen observation changed; preserve its original selection context",
            evidence_ids=(snapshot.observation["id"],),
        )
    if observed.facts.outcome == "execution_unknown":
        return SearchWait(
            state="awaiting_execution",
            reason="Unknown execution retains its policy slot",
            evidence_ids=(snapshot.trial["id"],),
        )
    if observed.facts.outcome in {
        "evidence_invalidated",
        "unrankable",
        "evaluation_invalid",
        "evaluation_incomplete",
    }:
        return SearchWait(
            state="needs_review",
            reason="Trial evidence needs review before research continues",
            evidence_ids=(snapshot.observation["id"],),
        )
    if snapshot.pin is None:
        return _action(
            "search.select_observation",
            {
                "campaign_id": campaign_id,
                "observation_id": snapshot.observation["id"],
                **fence,
            },
            revision,
        )
    resolution = {
        "campaign_id": campaign_id,
        "observation_id": snapshot.observation["id"],
        "decision_id": None,
        **fence,
    }
    if observed.facts.outcome != "measurement":
        return _action("search.resolve_trial", resolution, revision)
    if decision is not None:
        outcome = decision["data"]["decision"]["outcome"]
        if decision["state"] == "committed" or (
            decision["state"] == "assessed" and outcome in {"reject", "inconclusive"}
        ):
            return _action(
                "search.resolve_trial",
                {**resolution, "decision_id": decision["id"]},
                revision,
            )
        if decision["state"] == "assessed" and outcome == "promote":
            return _action("promotion.commit", {"decision_id": decision["id"]})
        return SearchWait(
            state="needs_review",
            reason="Pinned assessment is affected or unresolved",
            evidence_ids=(decision["id"],),
        )
    if (
        campaign.incumbent_revision != observed.incumbent_revision
        or campaign.incumbent_candidate_id != observed.comparator_id
    ):
        return SearchWait(
            state="needs_review",
            reason="Comparator changed without a matching committed selection",
        )
    if campaign.contract.promotion.mode == "paired_confirmation":
        if snapshot.confirmation is None:
            return SearchWait(
                state="awaiting_confirmation",
                reason="Create an explicit sealed confirmation panel",
                evidence_ids=(trial.candidate_id,),
            )
        if snapshot.confirmation["data"]["plan"]["candidate_id"] != trial.candidate_id:
            return SearchWait(
                state="needs_review",
                reason="The sole confirmation panel belongs to another candidate",
            )
        if snapshot.confirmation["state"] == "ineligible":
            return SearchWait(
                state="needs_review",
                reason="Required confirmation realization failed",
                evidence_ids=(snapshot.confirmation["id"],),
            )
        if any(
            item["data"]["execution_outcome"] in {"pending", "unknown"}
            or (
                item["data"]["execution_outcome"] == "succeeded"
                and item["data"]["evaluation_status"] == "pending"
            )
            for item in snapshot.confirmation_trials
        ):
            return SearchWait(
                state="awaiting_confirmation",
                reason="Sealed confirmation panel is not complete",
                evidence_ids=(snapshot.confirmation["id"],),
            )
        return _action(
            "confirmation.assess",
            {"plan_id": snapshot.confirmation["id"]},
            panel_revision=snapshot.confirmation["revision"],
            trial_revisions=[
                [item["id"], item["revision"]] for item in snapshot.confirmation_trials
            ],
        )
    return _action(
        "promotion.assess",
        {
            "campaign_id": campaign_id,
            "candidate_evaluation_id": observed.facts.evaluation_id,
            "expected_incumbent_revision": observed.incumbent_revision,
        },
    )


def plan_response_admission(snapshot: SearchSnapshot, request: JsonObject) -> SearchAction:
    campaign = CampaignData.model_validate(snapshot.campaign["data"])
    authority = snapshot.context["authority"]
    return _action(
        "search.admit_response",
        {
            "campaign_id": snapshot.campaign["id"],
            "request_id": request["id"],
            "expected_incumbent_revision": campaign.incumbent_revision,
            "expected_controller_id": authority["controller_id"],
            "expected_control_epoch": authority["control_epoch"],
        },
        snapshot.search["revision"] if snapshot.search else 0,
    )


def proposal_allowance(snapshot: SearchSnapshot, pending_count: int = 0) -> SearchWait | None:
    campaign = CampaignData.model_validate(snapshot.campaign["data"])
    trial_count = sum(
        item["count"] for item in snapshot.context["counts"] if item["kind"] == "trial"
    )
    if trial_count + pending_count >= campaign.contract.stopping.max_trials:
        return SearchWait(state="budget_exhausted", reason="Campaign trial limit is reached")
    budgets = {item["dimension"]: item for item in snapshot.context["budget"]}
    if "trial_admissions" in budgets and (
        Decimal(budgets["trial_admissions"]["available"]) < pending_count + 1
    ):
        return SearchWait(state="budget_exhausted", reason="No funded trial allowance remains")
    if "proposal_requests" not in budgets or Decimal(budgets["proposal_requests"]["available"]) < 1:
        return SearchWait(
            state="budget_exhausted", reason="No declared proposal-request allowance remains"
        )
    for dimension in ("attempts", "execution_ms"):
        if dimension in budgets and Decimal(budgets[dimension]["available"]) <= 0:
            return SearchWait(
                state="budget_exhausted", reason=f"No currently available {dimension} allowance"
            )
    return None
