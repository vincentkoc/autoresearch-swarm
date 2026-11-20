"""Append-only local SQLite evidence with a restricted decision-response surface.

This is a trusted-local API, not isolation from processes that can edit its files.
Opening a journal never claims training ownership or repairs interrupted work.
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, Self

from filelock import FileLock

from research_swarm.domain.canonical import MAX_SAFE_INTEGER
from research_swarm.domain.types import new_id
from research_swarm.swarm.db import JournalConflict as JournalConflict
from research_swarm.swarm.db import connection
from research_swarm.swarm.reactive.contracts import (
    ApplicationIntent,
    ApplicationReceipt,
    DecisionProposal,
    DecisionRecord,
    DecisionRequest,
    EventKind,
    JournalEvent,
    ReactiveContract,
    RunBinding,
    SessionState,
    proposed_controls,
    validate_request,
)


class DecisionJournal:
    def __init__(self, path: Path, identity: str) -> None:
        self.path, self.identity = path, identity

    @classmethod
    def create(cls, path: Path, binding: RunBinding, contract: ReactiveContract) -> DecisionJournal:
        binding, contract = (
            RunBinding.model_validate(binding),
            ReactiveContract.model_validate(contract),
        )
        path = path.expanduser().absolute()
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Never reinterpret or overwrite an existing session, including a dangling link.
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)
        path = path.resolve(strict=True)
        state = SessionState(
            id=new_id(), binding=binding, contract=contract, controls=contract.initial_controls()
        )
        with connection(path, schema="reactive", write=True) as db:
            db.execute("INSERT INTO session VALUES (1,?)", (state.model_dump_json(),))
            event = JournalEvent(sequence=0, kind="created", session=state)
            db.execute("INSERT INTO events VALUES (0,NULL,?)", (event.model_dump_json(),))
        return cls(path, state.id)

    @classmethod
    def open(cls, path: Path) -> DecisionJournal:
        path = path.expanduser().resolve(strict=True)
        journal = cls(path, "")
        with journal._connect() as connection:
            journal.identity = journal._state(connection).id
        return journal

    @contextmanager
    def _connect(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        with connection(self.path, schema="reactive", existing=True, write=write) as db:
            if self.identity and self._state(db).id != self.identity:
                raise JournalConflict("Reactive session identity changed")
            yield db

    @staticmethod
    def _state(connection: sqlite3.Connection) -> SessionState:
        row = connection.execute("SELECT payload FROM session WHERE id=1").fetchone()
        if row is None:
            raise JournalConflict("Reactive session is incomplete")
        return SessionState.model_validate_json(row[0])

    @staticmethod
    def _decision(connection: sqlite3.Connection, request_id: str) -> DecisionRecord | None:
        row = connection.execute(
            "SELECT payload FROM events WHERE request_id=? ORDER BY sequence DESC LIMIT 1",
            (request_id,),
        ).fetchone()
        return JournalEvent.model_validate_json(row[0]).decision if row else None

    @staticmethod
    def _append(
        connection: sqlite3.Connection,
        state: SessionState,
        kind: EventKind,
        decision: DecisionRecord | None = None,
        **changes: Any,
    ) -> SessionState:
        updated = SessionState.model_validate(
            state.model_dump() | changes | {"revision": state.revision + 1}
        )
        event = JournalEvent(
            sequence=updated.revision, kind=kind, session=updated, decision=decision
        )
        connection.execute(
            "INSERT INTO events VALUES (?,?,?)",
            (event.sequence, decision.request.id if decision else None, event.model_dump_json()),
        )
        result = connection.execute(
            "UPDATE session SET payload=? WHERE id=1 AND json_extract(payload,'$.revision')=?",
            (updated.model_dump_json(), state.revision),
        )
        if result.rowcount != 1:
            raise JournalConflict("Reactive session revision changed")
        return updated

    def inspect(self) -> SessionState:
        with self._connect() as connection:
            return self._state(connection)

    def get(self, request_id: str) -> DecisionRecord | None:
        with self._connect() as connection:
            return self._decision(connection, request_id)

    def pending(self) -> DecisionRecord | None:
        with self._connect() as connection:
            state = self._state(connection)
            return (
                self._decision(connection, state.outstanding_id) if state.outstanding_id else None
            )

    def events(self, *, after: int = -1, limit: int = 100) -> tuple[JournalEvent, ...]:
        if (
            type(after) is not int
            or not -1 <= after <= MAX_SAFE_INTEGER
            or type(limit) is not int
            or not 1 <= limit <= 1000
        ):
            raise ValueError("Invalid reactive event page")
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT payload FROM events WHERE sequence>? ORDER BY sequence LIMIT ?",
                (after, limit),
            ).fetchall()
        return tuple(JournalEvent.model_validate_json(row[0]) for row in rows)

    def respond(self, proposal: DecisionProposal) -> DecisionRecord:
        proposal = DecisionProposal.model_validate(proposal)
        with self._connect(write=True) as connection:
            state = self._state(connection)
            record = self._decision(connection, proposal.request_id)
            if record is None:
                raise JournalConflict("Unknown reactive request")
            if record.proposal is not None:
                if record.proposal != proposal:
                    raise JournalConflict("Reactive response differs from the recorded response")
                return record
            proposed_controls(
                state.contract, record.request, proposal, step=record.request.requested_step
            )
            accepted = state.status == "active" and record.status == "pending"
            if accepted and (
                state.outstanding_id != proposal.request_id
                or not state.matches_request(record.request)
            ):
                raise JournalConflict("Response differs from current run or control state")
            updated = DecisionRecord.model_validate(
                record.model_dump()
                | {
                    "proposal": proposal,
                    "response_disposition": "accepted" if accepted else "late",
                    "status": "responded" if accepted else record.status,
                }
            )
            self._append(connection, state, "responded" if accepted else "late_response", updated)
            return updated

    def trainer(self) -> TrainingSession:
        return TrainingSession.acquire(self)


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
            if not state.matches_request(request):
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

    def record_application(
        self, receipt: ApplicationReceipt, *, detail: str = ""
    ) -> DecisionRecord:
        receipt = ApplicationReceipt.model_validate(receipt)
        with self.journal._connect(write=True) as connection:
            state = self._owned(connection)
            record = self.journal._decision(connection, receipt.request_id)
            if record is not None and record.receipt is not None:
                if record.receipt != receipt or record.detail != detail:
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
                record.model_dump()
                | {"receipt": receipt, "status": receipt.outcome, "detail": detail}
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
