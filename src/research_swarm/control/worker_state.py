"""Operational records keep presence, admission, and native closure independent."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Digest, Revision, Timestamp, VersionedRecord
from research_swarm.domain.workers import WorkerBinding
from research_swarm.state.database import JsonObject, Transaction


class WorkerAdmission(VersionedRecord):
    binding: WorkerBinding
    reason: str = Field(default="", max_length=2048)
    actor_id: ID | None = None
    changed_at: Timestamp | None = None


class WorkerPresence(VersionedRecord):
    binding: WorkerBinding
    heartbeat_sequence: Revision = 0
    driver_instance_id: ID | None = None
    received_at: Timestamp | None = None


class AttemptCancellation(VersionedRecord):
    binding: WorkerBinding
    attempt_id: ID
    dispatch_digest: Digest
    control_sequence: int = Field(default=1, strict=True, ge=1, le=1)
    action: Literal["cancel"] = "cancel"
    reason: str = Field(min_length=1, max_length=2048)
    actor_id: ID
    requested_at: Timestamp
    acknowledged_at: Timestamp | None = None
    completed_at: Timestamp | None = None
    attempt_outcome: Literal["succeeded", "failed", "cancelled", "not_started"] | None = None
    resolution: Literal["cancelled", "not_started", "completed_without_cancellation"] | None = None


OperationalKind = Literal["worker_admission", "worker_presence"]


def worker_state(
    transaction: Transaction, binding: WorkerBinding, kind: OperationalKind
) -> JsonObject:
    """Legacy enrollments have virtual revision-zero state; reads never insert it."""
    try:
        row = transaction.get(kind, binding.worker_id)
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
        data = (
            WorkerAdmission(binding=binding)
            if kind == "worker_admission"
            else WorkerPresence(binding=binding)
        )
        return {
            "id": binding.worker_id,
            "kind": kind,
            "revision": 0,
            "state": "active" if kind == "worker_admission" else "unobserved",
            "campaign_id": None,
            "parent_id": binding.worker_id,
            "data": data.model_dump(mode="json"),
        }
    if WorkerBinding.model_validate(row["data"]["binding"]) != binding:
        raise ResearchError(ErrorCode.STALE_FENCE, "Worker state belongs to another enrollment")
    return row


def write_worker_state(
    transaction: Transaction,
    previous: JsonObject,
    data: WorkerAdmission | WorkerPresence,
    state: str,
) -> JsonObject:
    if previous["revision"] == 0:
        return transaction.insert(
            previous["kind"],
            previous["id"],
            data.model_dump(mode="json"),
            state=state,
            parent_id=previous["id"],
        )
    return transaction.update(
        previous["kind"],
        previous["id"],
        expected_revision=previous["revision"],
        data=data.model_dump(mode="json"),
        state=state,
    )


def admission_open(transaction: Transaction, binding: WorkerBinding) -> bool:
    return bool(worker_state(transaction, binding, "worker_admission")["state"] == "active")


def settle_cancellation(
    transaction: Transaction, attempt_id: str, outcome: str, observed_at: str
) -> None:
    try:
        row = transaction.get("attempt_control", attempt_id)
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
        return
    data = AttemptCancellation.model_validate(row["data"])
    resolved = AttemptCancellation.model_validate(
        {
            **data.model_dump(mode="json"),
            "completed_at": observed_at,
            "attempt_outcome": outcome,
            "resolution": outcome
            if outcome in {"cancelled", "not_started"}
            else "completed_without_cancellation",
        }
    )
    updated = transaction.update(
        "attempt_control",
        attempt_id,
        expected_revision=row["revision"],
        state="completed",
        data=resolved.model_dump(mode="json"),
    )
    transaction.event("attempt.control_completed", updated, {"attempt_outcome": outcome})
