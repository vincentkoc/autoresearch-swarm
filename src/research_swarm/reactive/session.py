"""Exclusive native ownership for training-side journal transitions."""

from __future__ import annotations

import os
import sqlite3
from types import TracebackType
from typing import Literal, Self

from filelock import FileLock

from research_swarm.domain.types import new_id
from research_swarm.reactive.contracts import (
    ApplicationReceipt,
    DecisionRequest,
    proposed_controls,
    validate_request,
)
from research_swarm.reactive.journal import DecisionJournal, JournalConflict
from research_swarm.reactive.journal_records import ApplicationIntent, DecisionRecord, SessionState


class TrainingSession:
    def __init__(self, journal: DecisionJournal, lock: FileLock) -> None:
        self.journal, self._lock = journal, lock
        self._id, self._pid = new_id(), os.getpid()
        self._closed_state: SessionState | None = None

    @classmethod
    def acquire(cls, journal: DecisionJournal) -> TrainingSession:
        lock = FileLock(str(journal.path) + ".trainer.lock", timeout=0)
        lock.acquire()
        owner = cls(journal, lock)
        try:
            with journal._connect(write=True) as connection:
                state = journal._state(connection)
                if state.status != "active":
                    raise JournalConflict("Reactive session is terminal")
                interrupted = state.trainer_id is not None
                if interrupted:
                    owner._close(connection, state, interrupted=True)
                else:
                    journal._append(connection, state, "owned", trainer_id=owner._id)
            if interrupted:
                raise JournalConflict("Interrupted training owner was fenced; create a new session")
        except BaseException:
            lock.release()
            raise
        return owner

    def _owned(self, connection: sqlite3.Connection) -> SessionState:
        state = self.journal._state(connection)
        if self._pid != os.getpid() or not self._lock.is_locked or state.trainer_id != self._id:
            raise JournalConflict("Training ownership is not held")
        return state

    @staticmethod
    def _active(state: SessionState) -> None:
        if state.status != "active":
            raise JournalConflict("Reactive session is terminal")

    def request(self, request: DecisionRequest) -> DecisionRecord:
        request = DecisionRequest.model_validate(request)
        with self.journal._connect(write=True) as connection:
            state = self._owned(connection)
            prior = self.journal._decision(connection, request.id)
            if prior is not None:
                if prior.request != request:
                    raise JournalConflict("Reactive request ID was reused with different content")
                return prior
            self._active(state)
            validate_request(state.contract, request)
            if (
                request.binding != state.binding
                or request.control_revision != state.control_revision
                or request.current_controls != state.controls
            ):
                raise JournalConflict("Reactive request differs from current run or controls")
            if state.outstanding_id is not None or request.sequence != state.decision_count + 1:
                raise JournalConflict("Only one ordered reactive request may be outstanding")
            if request.requested_step < state.last_step or (
                state.last_observed_step is not None
                and request.observation.step
                < state.last_observed_step + state.contract.interval_steps
            ):
                raise JournalConflict("Reactive request violates step or interval ordering")
            record = DecisionRecord(request=request)
            self.journal._append(
                connection,
                state,
                "requested",
                record,
                outstanding_id=request.id,
                decision_count=request.sequence,
                last_step=request.requested_step,
                last_observed_step=request.observation.step,
            )
            return record

    def claim_application(self, request_id: str, *, step: int) -> ApplicationIntent:
        with self.journal._connect(write=True) as connection:
            state = self._owned(connection)
            self._active(state)
            record = self.journal._decision(connection, request_id)
            if (
                record is None
                or record.status != "responded"
                or record.proposal is None
                or state.outstanding_id != request_id
            ):
                raise JournalConflict("Response is not available for a new application claim")
            target = proposed_controls(state.contract, record.request, record.proposal, step=step)
            if (
                record.request.control_revision != state.control_revision
                or record.request.current_controls != state.controls
                or step < state.last_step
            ):
                raise JournalConflict("Application differs from current control state or step")
            updated = DecisionRecord.model_validate(
                record.model_dump() | {"status": "applying", "application_step": step}
            )
            self.journal._append(connection, state, "claimed", updated, last_step=step)
            return ApplicationIntent(
                request=record.request, proposal=record.proposal, step=step, target=target
            )

    def record_application(self, receipt: ApplicationReceipt) -> DecisionRecord:
        receipt = ApplicationReceipt.model_validate(receipt)
        with self.journal._connect(write=True) as connection:
            state = self._owned(connection)
            record = self.journal._decision(connection, receipt.request_id)
            if record is not None and record.receipt is not None:
                if record.receipt != receipt:
                    raise JournalConflict("Application receipt differs from recorded evidence")
                return record
            self._active(state)
            if (
                record is None
                or record.status != "applying"
                or record.proposal is None
                or state.outstanding_id != receipt.request_id
            ):
                raise JournalConflict("Application was not claimed by this session")
            request = record.request
            if (
                receipt.binding != state.binding
                or receipt.request_digest != request.digest
                or receipt.decision_digest != record.proposal.digest
                or receipt.observed_step != request.observation.step
                or receipt.requested_step != request.requested_step
                or receipt.applied_step != record.application_step
                or receipt.base_revision != state.control_revision
                or receipt.before != state.controls
            ):
                raise JournalConflict("Application receipt differs from the original claim")
            target = proposed_controls(
                state.contract, request, record.proposal, step=receipt.applied_step
            )
            if receipt.outcome != "uncertain" and (
                receipt.after != target
                or receipt.outcome != ("kept" if target == state.controls else "applied")
            ):
                raise JournalConflict("Application receipt does not match the proposed controls")
            updated = DecisionRecord.model_validate(
                record.model_dump() | {"receipt": receipt, "status": receipt.outcome}
            )
            self.journal._append(
                connection,
                state,
                "receipted",
                updated,
                outstanding_id=None,
                status="uncertain" if receipt.outcome == "uncertain" else "active",
                controls=receipt.after if receipt.after is not None else state.controls,
                control_revision=receipt.new_revision
                if receipt.new_revision is not None
                else state.control_revision,
            )
            return updated

    def settle(
        self,
        request_id: str,
        status: Literal["failed", "expired", "cancelled"],
        *,
        detail: str = "",
    ) -> DecisionRecord:
        if status not in {"failed", "expired", "cancelled"}:
            raise ValueError("Invalid settlement status")
        with self.journal._connect(write=True) as connection:
            state = self._owned(connection)
            record = self.journal._decision(connection, request_id)
            if record is None:
                raise JournalConflict("Unknown reactive request")
            if record.status == status and record.detail == detail:
                return record
            self._active(state)
            if record.status not in {"pending", "responded"} or state.outstanding_id != request_id:
                raise JournalConflict("Decision can no longer be settled")
            updated = DecisionRecord.model_validate(
                record.model_dump() | {"status": status, "detail": detail}
            )
            self.journal._append(connection, state, "settled", updated, outstanding_id=None)
            return updated

    def _close(
        self, connection: sqlite3.Connection, state: SessionState, *, interrupted: bool = False
    ) -> SessionState:
        record = (
            self.journal._decision(connection, state.outstanding_id)
            if state.outstanding_id
            else None
        )
        uncertain = record is not None and record.status == "applying"
        if record is not None:
            receipt = None
            if uncertain:
                assert record.proposal is not None and record.application_step is not None
                receipt = ApplicationReceipt(
                    binding=state.binding,
                    request_id=record.request.id,
                    request_digest=record.request.digest,
                    decision_digest=record.proposal.digest,
                    observed_step=record.request.observation.step,
                    requested_step=record.request.requested_step,
                    applied_step=record.application_step,
                    base_revision=state.control_revision,
                    new_revision=None,
                    outcome="uncertain",
                    before=state.controls,
                    after=None,
                )
            record = DecisionRecord.model_validate(
                record.model_dump()
                | {
                    "status": "uncertain" if uncertain else "cancelled",
                    "receipt": receipt,
                    "detail": "Training ownership interrupted"
                    if interrupted
                    else "Training session closed",
                }
            )
        return self.journal._append(
            connection,
            state,
            "interrupted" if interrupted else "closed",
            record,
            status="uncertain" if uncertain else "closed",
            outstanding_id=None,
        )

    def close(self) -> SessionState:
        if self._closed_state is not None:
            return self._closed_state
        try:
            with self.journal._connect(write=True) as connection:
                state = self._owned(connection)
                final = self._close(connection, state) if state.status == "active" else state
            self._closed_state = final
            return final
        finally:
            self._lock.release()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
