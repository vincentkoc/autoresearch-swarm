"""Transactional worker presence, drain intent, and sticky attempt cancellation."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import Field, model_validator

from research_swarm.control.acceptance import AcceptanceControl
from research_swarm.control.acceptance_models import LeaseRenewal
from research_swarm.control.worker_state import (
    AttemptCancellation,
    WorkerAdmission,
    WorkerPresence,
    worker_state,
    write_worker_state,
)
from research_swarm.control.workers import AttemptData, WorkerData, authority, worker_authority
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.controls import ControlAcknowledgment
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, VersionedRecord, utc_now
from research_swarm.domain.workers import WorkerBinding
from research_swarm.state.database import Database, JsonObject, Transaction


class WorkerHeartbeat(VersionedRecord):
    binding: WorkerBinding
    driver_instance_id: ID
    sequence: int = Field(strict=True, ge=1, le=9_007_199_254_740_991)
    acknowledgments: tuple[ControlAcknowledgment, ...] = Field(default=(), max_length=100)

    lease_renewals: tuple[LeaseRenewal, ...] = Field(
        default=(), max_length=100, exclude_if=lambda value: not value
    )

    @model_validator(mode="after")
    def unique_acknowledgments(self) -> Self:
        if len({item.attempt_id for item in self.acknowledgments}) != len(self.acknowledgments):
            raise ValueError("Control acknowledgments must identify distinct attempts")
        if len({item.attempt_id for item in self.lease_renewals}) != len(self.lease_renewals):
            raise ValueError("Acceptance renewals must identify distinct attempts")
        return self


class SetWorkerAdmission(VersionedRecord):
    binding: WorkerBinding
    admission: Literal["active", "draining"]
    reason: str = Field(min_length=1, max_length=2048)


class RequestAttemptCancellation(VersionedRecord):
    attempt_id: ID
    reason: str = Field(min_length=1, max_length=2048)


def operator_worker(transaction: Transaction, binding: WorkerBinding) -> None:
    row = transaction.get("worker", binding.worker_id)
    worker = WorkerData.model_validate(row["data"])
    controller_id, epoch = authority(transaction)
    if (
        row["state"] != "active"
        or worker.binding != binding
        or binding.controller_id != controller_id
        or binding.control_epoch != epoch
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Worker enrollment is no longer current")


class WorkerLifecycle:
    def __init__(self, database: Database, acceptance: AcceptanceControl | None = None) -> None:
        self.database = database
        self.acceptance = acceptance or AcceptanceControl(database)

    def set_admission(self, context: CommandContext, request: SetWorkerAdmission) -> JsonObject:
        def change(transaction: Transaction) -> JsonObject:
            operator_worker(transaction, request.binding)
            previous = worker_state(transaction, request.binding, "worker_admission")
            if context.expected_revision != previous["revision"]:
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT, "Worker admission revision changed"
                )
            if previous["state"] == request.admission:
                return previous
            updated = write_worker_state(
                transaction,
                previous,
                WorkerAdmission(
                    binding=request.binding,
                    reason=request.reason,
                    actor_id=context.principal.id,
                    changed_at=utc_now(),
                ),
                request.admission,
            )
            transaction.event("worker.admission_changed", updated, {"admission": request.admission})
            return updated

        return self.database.execute(
            context,
            "worker.set_admission",
            request.model_dump(mode="json"),
            "worker.manage",
            change,
        )

    def request_cancel(
        self, context: CommandContext, request: RequestAttemptCancellation
    ) -> JsonObject:
        def cancel(transaction: Transaction) -> JsonObject:
            row = transaction.get("attempt", request.attempt_id)
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Attempt revision changed")
            attempt = AttemptData.model_validate(row["data"])
            binding = WorkerBinding.model_validate(
                {key: getattr(attempt.dispatch, key) for key in WorkerBinding.model_fields}
            )
            operator_worker(transaction, binding)
            try:
                existing = transaction.get("attempt_control", request.attempt_id)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
            else:
                return {
                    "disposition": existing["state"],
                    "attempt_id": row["id"],
                    "control": existing,
                }
            if attempt.publication_status != "pending":
                return {"disposition": "already_terminal", "attempt_id": row["id"], "control": None}
            data = AttemptCancellation(
                binding=binding,
                attempt_id=request.attempt_id,
                dispatch_digest=record_digest(
                    "research.dispatch", attempt.dispatch.model_dump(mode="json")
                ),
                reason=request.reason,
                actor_id=context.principal.id,
                requested_at=utc_now(),
            )
            control = transaction.insert(
                "attempt_control",
                request.attempt_id,
                data.model_dump(mode="json"),
                state="requested",
                campaign_id=row["campaign_id"],
                parent_id=binding.worker_id,
            )
            transaction.event("attempt.cancellation_requested", control, {"scope": "attempt"})
            return {"disposition": "requested", "attempt_id": row["id"], "control": control}

        return self.database.execute(
            context,
            "attempt.request_cancel",
            request.model_dump(mode="json"),
            "campaign.manage",
            cancel,
        )

    def heartbeat(self, context: CommandContext, request: WorkerHeartbeat) -> JsonObject:
        def heartbeat(transaction: Transaction) -> JsonObject:
            worker = worker_authority(
                transaction, request.binding.worker_id, request.binding.worker_session_id
            )
            if worker.binding != request.binding:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Heartbeat binding differs from enrollment"
                )
            previous = worker_state(transaction, request.binding, "worker_presence")
            data = WorkerPresence.model_validate(previous["data"])
            if request.sequence <= data.heartbeat_sequence:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Heartbeat sequence must advance")
            acknowledgments: list[tuple[JsonObject, AttemptCancellation]] = []
            for acknowledgment in request.acknowledgments:
                try:
                    row = transaction.get("attempt_control", acknowledgment.attempt_id)
                except ResearchError as error:
                    if error.code != ErrorCode.NOT_FOUND:
                        raise
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Control is not assigned to this worker"
                    ) from None
                control = AttemptCancellation.model_validate(row["data"])
                if (
                    control.binding != request.binding
                    or control.control_sequence != acknowledgment.control_sequence
                    or control.dispatch_digest != acknowledgment.dispatch_digest
                ):
                    raise ResearchError(
                        ErrorCode.STALE_FENCE, "Control acknowledgment differs from assignment"
                    )
                acknowledgments.append((row, control))
            renewed = [
                self.acceptance.renew(transaction, worker, item, request.sequence)
                for item in request.lease_renewals
            ]
            received = utc_now()
            for row, control in acknowledgments:
                if control.acknowledged_at is None:
                    updated = transaction.update(
                        "attempt_control",
                        row["id"],
                        expected_revision=row["revision"],
                        data=control.model_copy(update={"acknowledged_at": received}).model_dump(
                            mode="json"
                        ),
                        state="completed" if row["state"] == "completed" else "acknowledged",
                    )
                    transaction.event(
                        "attempt.control_acknowledged",
                        updated,
                        {"control_sequence": control.control_sequence},
                    )
            updated = write_worker_state(
                transaction,
                previous,
                WorkerPresence(
                    binding=request.binding,
                    heartbeat_sequence=request.sequence,
                    driver_instance_id=request.driver_instance_id,
                    received_at=received,
                ),
                "observed",
            )
            transaction.event("worker.heartbeat", updated, {"sequence": request.sequence})
            return {
                **({"lease_renewals": renewed} if request.lease_renewals else {}),
                "presence": updated,
                "admission": worker_state(transaction, request.binding, "worker_admission"),
            }

        return self.database.execute(
            context,
            "worker.heartbeat",
            request.model_dump(mode="json"),
            "worker.publish",
            heartbeat,
        )
