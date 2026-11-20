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
from typing import TYPE_CHECKING, Any

from research_swarm.domain.canonical import MAX_SAFE_INTEGER
from research_swarm.domain.types import new_id
from research_swarm.reactive.contracts import (
    DecisionProposal,
    ReactiveContract,
    RunBinding,
    proposed_controls,
)
from research_swarm.reactive.journal_records import (
    DecisionRecord,
    EventKind,
    JournalEvent,
    SessionState,
)
from research_swarm.swarm.db import JournalConflict as JournalConflict
from research_swarm.swarm.db import connection

if TYPE_CHECKING:
    from research_swarm.reactive.session import TrainingSession


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
                or record.request.binding != state.binding
                or record.request.control_revision != state.control_revision
                or record.request.current_controls != state.controls
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
        from research_swarm.reactive.session import TrainingSession

        return TrainingSession.acquire(self)
