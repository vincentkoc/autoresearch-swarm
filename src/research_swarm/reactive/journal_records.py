"""Local session observations; none of these records grant evaluator authority."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from research_swarm.domain.types import ID, Digest, Timestamp, VersionedRecord
from research_swarm.reactive.contracts import (
    ApplicationReceipt,
    DecisionProposal,
    DecisionRequest,
    NumericValue,
    ReactiveContract,
    RunBinding,
    Step,
)

DecisionStatus = Literal[
    "pending",
    "responded",
    "applying",
    "applied",
    "kept",
    "uncertain",
    "failed",
    "expired",
    "cancelled",
]
EventKind = Literal[
    "created",
    "owned",
    "requested",
    "responded",
    "late_response",
    "claimed",
    "receipted",
    "settled",
    "closed",
    "interrupted",
    "call_started",
    "call_fenced",
    "call_finished",
]


class SessionState(VersionedRecord):
    """Controls are the last confirmed values, not current-state proof when uncertain."""

    id: ID
    binding: RunBinding
    contract: ReactiveContract
    status: Literal["active", "closed", "uncertain"] = "active"
    revision: Step = 0
    control_revision: Step = 0
    controls: tuple[NumericValue, ...]
    decision_count: Step = 0
    last_step: Step = 0
    last_observed_step: Step | None = None
    outstanding_id: ID | None = None
    trainer_id: ID | None = None
    inflight_call_request_id: ID | None = None


class DecisionCall(VersionedRecord):
    id: ID
    policy_digest: Digest
    started_at: Timestamp
    timeout_ms: int = Field(strict=True, ge=1, le=120_000)
    outcome: Literal["started", "returned", "failed", "cancelled"] = "started"
    completed_at: Timestamp | None = None
    fence: Literal["timeout", "cancelled"] | None = None
    proposal: DecisionProposal | None = None
    disposition: Literal["accepted", "late", "conflict", "duplicate"] | None = None
    failure_code: Literal["exception", "invalid_response", "cancelled"] | None = None


class DecisionRecord(VersionedRecord):
    request: DecisionRequest
    status: DecisionStatus = "pending"
    proposal: DecisionProposal | None = None
    response_disposition: Literal["accepted", "late"] | None = None
    application_step: Step | None = None
    receipt: ApplicationReceipt | None = None
    detail: str = Field(default="", max_length=1024)
    call: DecisionCall | None = None


class JournalEvent(VersionedRecord):
    sequence: Step
    kind: EventKind
    session: SessionState
    decision: DecisionRecord | None = None


class ApplicationIntent(VersionedRecord):
    request: DecisionRequest
    proposal: DecisionProposal
    step: Step
    target: tuple[NumericValue, ...]
