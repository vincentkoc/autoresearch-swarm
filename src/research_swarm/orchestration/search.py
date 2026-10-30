"""Bounded, resumable search over the same API used by external agents."""

from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from research_swarm.client import ResearchClient
from research_swarm.control.models import CampaignData, JobData, TrialData
from research_swarm.control.observation_models import TrialObservation, observation_selection_id
from research_swarm.control.queries import RecordKind, RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.search.greedy import (
    GreedyConfiguration,
    GreedyPolicy,
    SearchAction,
    SearchSnapshot,
    SearchWait,
)
from research_swarm.state.database import JsonObject


class SearchProgress(VersionedRecord):
    operation: str
    command_id: ID


class SearchDriverResult(VersionedRecord):
    campaign_id: ID
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
        "contended",
        "delivery_uncertain",
        "step_limit",
        "transport_unavailable",
    ]
    reason: str
    evidence_ids: tuple[ID, ...] = ()
    actions: tuple[SearchProgress, ...] = Field(default=(), max_length=128)
    error_code: ErrorCode | None = None


class SearchDriver:
    """No native execution, provider calls, process-local custody, or implicit retries."""

    def __init__(
        self, client: ResearchClient, campaign_id: str, config: GreedyConfiguration
    ) -> None:
        self.client, self.campaign_id, self.config = client, campaign_id, config

    def _optional(self, kind: RecordKind, identifier: str) -> JsonObject | None:
        try:
            return self.client.get(kind, identifier)
        except ResearchError as error:
            if error.code != ErrorCode.NOT_FOUND:
                raise
            return None

    def _records(
        self, kind: RecordKind, *, parent_id: str | None = None
    ) -> tuple[list[JsonObject], bool]:
        records = []
        cursor = None
        for _ in range(2):
            page = self.client.list(
                RecordQuery(
                    kind=kind,
                    campaign_id=self.campaign_id,
                    parent_id=parent_id,
                    limit=50,
                    cursor=cursor,
                )
            )
            records.extend(page["items"])
            cursor = page["next_cursor"]
            if cursor is None:
                return records, True
        return records, False

    def _receipt(self, trial: JsonObject) -> str | None:
        data = TrialData.model_validate(trial["data"])
        if data.evaluation_status != "valid":
            return None
        if len(data.job_ids) > 256:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Driver stage scan exceeds its bound"
            )
        receipts = []
        for identifier in data.job_ids:
            job = JobData.model_validate(self.client.get("job", identifier)["data"])
            if job.stage.kind == "evaluate" and job.evaluation_request_id:
                request = self.client.get("evaluation_request", job.evaluation_request_id)
                if request["data"]["receipt_id"]:
                    receipts.append(request["data"]["receipt_id"])
        return receipts[0] if len(receipts) == 1 else None

    def snapshot(self) -> SearchSnapshot:
        context = self.client.context(self.campaign_id)
        campaign_row = self.client.get("campaign", self.campaign_id)
        campaign = CampaignData.model_validate(campaign_row["data"])
        state = self._optional("search_state", self.campaign_id)
        summary = context["search_state"]
        if context["campaign"]["revision"] != campaign_row["revision"] or (
            (summary["revision"] if summary else 0) != (state["revision"] if state else 0)
        ):
            raise ResearchError(
                ErrorCode.REVISION_CONFLICT,
                "Campaign changed during bounded snapshot reads",
                retryable=True,
            )
        values: JsonObject = {"campaign": campaign_row, "context": context, "search": state}
        complete = True
        if state and state["data"]["pending_request_ids"]:
            identifier = state["data"]["pending_request_ids"][0]
            values["request"] = self.client.get("proposal_request", identifier)
            values["response"] = self._optional("proposal_response", identifier)
        active = state["data"]["active_trial_ids"] if state else []
        trial_id = (
            active[0]
            if active
            else (
                self.config.baseline_trial_id if campaign.incumbent_candidate_id is None else None
            )
        )
        observation = None
        trial = None
        if trial_id:
            trial = self.client.get("trial", trial_id)
            if trial["campaign_id"] != self.campaign_id:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Selected trial belongs to another campaign"
                )
            values["trial"] = trial
            values["receipt_id"] = self._receipt(trial)
            if active:
                pin_id = observation_selection_id(
                    trial["data"]["spec"]["workspace_id"], self.campaign_id, trial_id
                )
                pin = self._optional("policy_transition", pin_id)
                values["pin"] = pin
                if pin:
                    observation = self.client.get(
                        "search_observation", pin["data"]["observation_id"]
                    )
                else:
                    observed, complete = self._records("search_observation", parent_id=trial_id)
                    matching = [
                        item
                        for item in observed
                        if item["data"]["facts"]["trial_revision"] == trial["revision"]
                        and item["data"]["incumbent_revision"] == campaign.incumbent_revision
                        and item["data"]["comparator_id"] == campaign.incumbent_candidate_id
                    ]
                    if matching:
                        observation = self.client.get("search_observation", matching[-1]["id"])
                values["observation"] = observation
        if campaign.contract.promotion.mode == "paired_confirmation":
            plans, scanned = self._records("confirmation_plan")
            complete = complete and scanned
            if len(plans) > 1:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "Campaign contains multiple fixed confirmation panels",
                )
            if plans:
                plan = self.client.get("confirmation_plan", plans[0]["id"])
                values["confirmation"] = plan
                bindings = plan["data"]["plan"]["bindings"]
                if len(bindings) > 256:
                    complete = False
                elif active:
                    values["confirmation_trials"] = tuple(
                        self.client.get("trial", item["trial_id"]) for item in bindings
                    )
        if trial is not None:
            comparisons, scanned = self._records(
                "promotion", parent_id=trial["data"]["candidate_id"]
            )
            complete = complete and scanned
            selected = TrialObservation.model_validate(observation["data"]) if observation else None
            incumbent_revision = (
                selected.incumbent_revision if selected else campaign.incumbent_revision
            )
            comparator_id = selected.comparator_id if selected else campaign.incumbent_candidate_id
            decisions = []
            for item in comparisons:
                decision = item["data"]["decision"]
                if (
                    decision["expected_incumbent_revision"] != incumbent_revision
                    or decision["comparator_id"] != comparator_id
                ):
                    continue
                full = self.client.get("promotion", item["id"])
                assessment = full["data"]
                if campaign.incumbent_candidate_id is None:
                    matches = assessment["candidate_evaluation_id"] == values["receipt_id"]
                elif selected is None:
                    matches = False
                elif campaign.contract.promotion.mode == "paired_confirmation":
                    matches = bool(
                        values.get("confirmation")
                        and assessment["confirmation_plan_id"] == values["confirmation"]["id"]
                    )
                else:
                    matches = (
                        assessment["candidate_evaluation_id"] == selected.facts.evaluation_id
                        and assessment["comparator_evaluation_id"]
                        == selected.comparator_evaluation_id
                    )
                if matches:
                    decisions.append(full)
            values["decisions"] = tuple(decisions)
        values["scan_complete"] = complete
        return SearchSnapshot(**values)

    def plan(self) -> SearchAction | SearchWait:
        return GreedyPolicy.plan(self.snapshot(), self.config)

    def advance(self, *, max_steps: int = 32) -> SearchDriverResult:
        if type(max_steps) is not int or not 1 <= max_steps <= 128:
            raise ValueError("Search advance requires between 1 and 128 actions")
        completed: list[SearchProgress] = []
        for _ in range(max_steps):
            try:
                plan = self.plan()
                if isinstance(plan, SearchWait):
                    return SearchDriverResult(
                        campaign_id=self.campaign_id,
                        actions=tuple(completed),
                        **plan.model_dump(exclude={"schema_version"}),
                    )
                reply = self.client.command(
                    plan.operation,
                    plan.payload,
                    idempotency_key=plan.command_key(self.campaign_id),
                    expected_revision=plan.expected_revision,
                )
                completed.append(
                    SearchProgress(operation=plan.operation, command_id=reply["command_id"])
                )
            except ResearchError as error:
                state = (
                    "delivery_uncertain"
                    if error.code == ErrorCode.DELIVERY_UNCERTAIN
                    else "transport_unavailable"
                    if error.code == ErrorCode.TRANSPORT_UNAVAILABLE
                    else "contended"
                    if error.code
                    in {ErrorCode.REVISION_CONFLICT, ErrorCode.INCUMBENT_REVISION_CONFLICT}
                    else "budget_exhausted"
                    if error.code == ErrorCode.BUDGET_EXHAUSTED
                    else "needs_review"
                )
                return SearchDriverResult(
                    campaign_id=self.campaign_id,
                    state=state,
                    reason=error.message,
                    actions=tuple(completed),
                    error_code=error.code,
                )
        return SearchDriverResult(
            campaign_id=self.campaign_id,
            state="step_limit",
            reason="Bounded action limit reached; read durable state before continuing",
            actions=tuple(completed),
        )

    async def advance_async(self, *, max_steps: int = 32) -> SearchDriverResult:
        return await asyncio.to_thread(self.advance, max_steps=max_steps)
