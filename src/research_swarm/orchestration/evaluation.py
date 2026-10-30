"""Advance evaluation authority and receipts without owning worker execution."""

from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from research_swarm.client import ResearchClient
from research_swarm.control.evaluations import (
    AuthorizeEvaluation,
    CompleteEvaluation,
    EvaluationAuthorization,
)
from research_swarm.control.models import JobData, TrialData
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.state.database import JsonObject


class EvaluationCursor(VersionedRecord):
    campaign_id: ID
    evaluator_id: ID
    phase: Literal["receipts", "authorization"] = "receipts"
    cursor: str | None = Field(default=None, max_length=2048)


class EvaluationProgress(VersionedRecord):
    operation: Literal["evaluation.authorize", "evaluation.complete"]
    command_id: ID
    record_id: ID


class EvaluationFinding(VersionedRecord):
    record_id: ID
    code: ErrorCode
    reason: str


EvaluationState = Literal[
    "idle", "advanced", "scan_limit", "needs_review", "delivery_uncertain", "transport_unavailable"
]


class EvaluationDriverResult(VersionedRecord):
    campaign_id: ID
    state: EvaluationState
    continuation: EvaluationCursor | None = None
    examined_count: int = Field(default=0, ge=0, le=400)
    actions: tuple[EvaluationProgress, ...] = Field(default=(), max_length=400)
    findings: tuple[EvaluationFinding, ...] = Field(default=(), max_length=400)
    error_code: ErrorCode | None = None


class EvaluationDriver:
    """Separate authenticated clients retain controller and evaluator responsibilities."""

    def __init__(
        self,
        controller: ResearchClient,
        evaluator: ResearchClient,
        campaign_id: str,
        evaluator_id: str,
    ) -> None:
        self.controller, self.evaluator = controller, evaluator
        self.initial = EvaluationCursor(campaign_id=campaign_id, evaluator_id=evaluator_id)

    def _command(
        self,
        operation: Literal["evaluation.authorize", "evaluation.complete"],
        request: AuthorizeEvaluation | CompleteEvaluation,
        revision: int | None = None,
    ) -> EvaluationProgress:
        payload = request.model_dump(mode="json")
        key = record_digest(
            "research.evaluation-driver-v1",
            {
                "campaign_id": self.initial.campaign_id,
                "operation": operation,
                "payload": payload,
                "expected_revision": revision,
            },
        )
        client = self.controller if operation == "evaluation.authorize" else self.evaluator
        result = client.command(
            operation,
            request,
            idempotency_key=key,
            expected_revision=revision,
        )
        return EvaluationProgress(
            operation=operation,
            command_id=result["command_id"],
            record_id=result["result"]["id"],
        )

    @staticmethod
    def _foreign(identifier: str) -> EvaluationFinding:
        return EvaluationFinding(
            record_id=identifier,
            code=ErrorCode.AUTHORIZATION_DENIED,
            reason="Evaluation belongs to another configured evaluator",
        )

    def _complete(self, summary: JsonObject) -> EvaluationProgress | EvaluationFinding | None:
        if summary["data"]["evaluator_id"] != self.initial.evaluator_id:
            return self._foreign(summary["id"])
        row = self.controller.get("evaluation_request", summary["id"])
        authorization = EvaluationAuthorization.model_validate(row["data"])
        if row["state"] != "authorized" or authorization.receipt_id is not None:
            return None
        trial = TrialData.model_validate(
            self.controller.get("trial", authorization.request.trial_id)["data"]
        )
        if trial.execution_outcome == "closed_unaccepted":
            return None
        job_row = self.controller.get("job", authorization.job_id)
        job = JobData.model_validate(job_row["data"])
        if job_row["state"] in {"failed", "cancelled", "not_started", "closed_unaccepted"}:
            raise ResearchError(
                ErrorCode.EVALUATION_INCOMPLETE,
                "Authorized scoring did not succeed",
            )
        if job_row["state"] != "succeeded":
            return None
        if job.current_attempt_id is None:
            raise ResearchError(ErrorCode.EVALUATION_INCOMPLETE, "Scoring attempt is unavailable")
        return self._command(
            "evaluation.complete",
            CompleteEvaluation(request_id=row["id"], attempt_id=job.current_attempt_id),
        )

    def _authorize(self, summary: JsonObject) -> EvaluationProgress | EvaluationFinding | None:
        if summary["data"]["stage_kind"] != "evaluate":
            return None
        row = self.controller.get("job", summary["id"])
        job = JobData.model_validate(row["data"])
        if row["state"] != "ready" or job.current_attempt_id is not None:
            return None
        if job.evaluation_request_id is not None:
            prior = self.controller.get("evaluation_request", job.evaluation_request_id)
            if prior["data"]["request"]["evaluator_id"] != self.initial.evaluator_id:
                return self._foreign(prior["id"])
            return None
        campaign = self.controller.get("campaign", self.initial.campaign_id)
        trial_row = self.controller.get("trial", job.trial_id)
        trial = TrialData.model_validate(trial_row["data"])
        if (
            trial.execution_outcome in {"closed_unaccepted", "unknown"}
            or (trial.resource_status == "unknown")
            or campaign["state"]
            not in {
                "running",
                "pausing",
            }
        ):
            return None
        if campaign["state"] == "pausing" and trial_row["state"] != "running":
            return None
        if trial.confirmation_plan_id is not None:
            plan = self.controller.get("confirmation_plan", trial.confirmation_plan_id)
            if plan["data"]["plan"]["evaluator_id"] != self.initial.evaluator_id:
                return self._foreign(plan["id"])
        return self._command(
            "evaluation.authorize",
            AuthorizeEvaluation(job_id=row["id"], evaluator_id=self.initial.evaluator_id),
            row["revision"],
        )

    def advance(
        self,
        *,
        continuation: EvaluationCursor | None = None,
        max_pages: int = 4,
        page_size: int = 50,
    ) -> EvaluationDriverResult:
        if type(max_pages) is not int or not 1 <= max_pages <= 8:
            raise ValueError("Evaluation advancement requires 1 to 8 pages")
        if type(page_size) is not int or not 1 <= page_size <= 50:
            raise ValueError("Evaluation pages require 1 to 50 records")
        position = continuation or self.initial
        if (position.campaign_id, position.evaluator_id) != (
            self.initial.campaign_id,
            self.initial.evaluator_id,
        ):
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Evaluation cursor owner differs"
            )
        actions: list[EvaluationProgress] = []
        findings: list[EvaluationFinding] = []
        examined = 0

        def result(
            state: EvaluationState,
            cursor: EvaluationCursor | None,
            error: ErrorCode | None = None,
        ) -> EvaluationDriverResult:
            return EvaluationDriverResult(
                campaign_id=self.initial.campaign_id,
                state=state,
                continuation=cursor,
                examined_count=examined,
                actions=tuple(actions),
                findings=tuple(findings),
                error_code=error,
            )

        for page_number in range(max_pages):
            try:
                if page_number == 0:
                    self.controller.get("campaign", self.initial.campaign_id)
                page = self.controller.list(
                    RecordQuery(
                        kind="evaluation_request" if position.phase == "receipts" else "job",
                        state="authorized" if position.phase == "receipts" else "ready",
                        campaign_id=position.campaign_id,
                        cursor=position.cursor,
                        limit=page_size,
                    )
                )
                for summary in page["items"]:
                    examined += 1
                    try:
                        progress = (
                            self._complete(summary)
                            if position.phase == "receipts"
                            else self._authorize(summary)
                        )
                        if isinstance(progress, EvaluationFinding):
                            findings.append(progress)
                        elif progress is not None:
                            actions.append(progress)
                    except ResearchError as error:
                        if error.code in {
                            ErrorCode.DELIVERY_UNCERTAIN,
                            ErrorCode.TRANSPORT_UNAVAILABLE,
                        }:
                            raise
                        findings.append(
                            EvaluationFinding(
                                record_id=summary["id"],
                                code=error.code,
                                reason=error.message,
                            )
                        )
                        if error.code == ErrorCode.AUTHORIZATION_DENIED:
                            raise
            except ResearchError as error:
                state: EvaluationState = (
                    "delivery_uncertain"
                    if error.code == ErrorCode.DELIVERY_UNCERTAIN
                    else "transport_unavailable"
                    if error.code == ErrorCode.TRANSPORT_UNAVAILABLE
                    else "needs_review"
                )
                # Re-read this page after uncertainty; never skip its unprocessed tail.
                return result(state, position, error.code)
            if page["next_cursor"] is not None:
                position = position.model_copy(update={"cursor": page["next_cursor"]})
            elif position.phase == "receipts":
                position = position.model_copy(update={"phase": "authorization", "cursor": None})
            else:
                return result(
                    "needs_review" if findings else "advanced" if actions else "idle", None
                )
        return result("needs_review" if findings else "scan_limit", position)

    async def advance_async(
        self,
        *,
        continuation: EvaluationCursor | None = None,
        max_pages: int = 4,
        page_size: int = 50,
    ) -> EvaluationDriverResult:
        return await asyncio.to_thread(
            self.advance,
            continuation=continuation,
            max_pages=max_pages,
            page_size=page_size,
        )
