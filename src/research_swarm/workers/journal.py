"""Durable worker observations. This journal never accepts scientific results."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path

from pydantic import Field

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import DispatchEnvelope, ExperimentSpec
from research_swarm.domain.types import ID, Revision, Timestamp, VersionedRecord, utc_now
from research_swarm.state.identity import initialize_identity
from research_swarm.workers.authority import VerifiedDispatch
from research_swarm.workers.contracts import NativeCommand, NativeIdentity, NativeOutcome
from research_swarm.workers.prepared import PreparedExecution


class JournalState(StrEnum):
    RECEIVED = "received"
    LAUNCHING = "launching"
    HELD = "held"
    RELEASING = "releasing"
    RUNNING = "running"
    CLOSED = "closed"
    NOT_STARTED = "not_started"
    UNKNOWN = "execution_unknown"


class JournalEntry(VersionedRecord):
    dispatch: DispatchEnvelope
    spec: ExperimentSpec
    command: NativeCommand
    prepared: PreparedExecution | None = None
    preparation_required: bool = False
    revision: Revision = 1
    state: JournalState = JournalState.RECEIVED
    supervisor_id: ID | None = None
    native: NativeIdentity | None = None
    cancel_requested: bool = False
    received_at: Timestamp = Field(default_factory=utc_now)
    release_requested_at: Timestamp | None = None
    outcome: NativeOutcome | None = None
    last_controller_sequence: Revision = 0
    finding: str = ""


class JournalEvent(VersionedRecord):
    sequence: int
    attempt_id: ID
    type: str
    entry: JournalEntry


_SCHEMA = """
CREATE TABLE IF NOT EXISTS attempts (
    id TEXT PRIMARY KEY,
    dispatch_id TEXT NOT NULL UNIQUE,
    input_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    revision INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS journal_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT NOT NULL REFERENCES attempts(id),
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    acknowledged INTEGER NOT NULL DEFAULT 0
);
PRAGMA user_version=1;
"""


class WorkerJournal:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._connect() as connection:
            if connection.execute("PRAGMA user_version").fetchone()[0] not in (0, 1):
                raise ResearchError(ErrorCode.SCHEMA_UNSUPPORTED, "Unsupported worker journal")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(_SCHEMA)
            self.identity = initialize_identity(connection)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, isolation_level=None, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=FULL")
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _get(connection: sqlite3.Connection, attempt_id: str) -> JournalEntry:
        row = connection.execute(
            "SELECT payload FROM attempts WHERE id=?", (attempt_id,)
        ).fetchone()
        if row is None:
            raise ResearchError(ErrorCode.NOT_FOUND, "Worker attempt not found")
        return JournalEntry.model_validate_json(row["payload"])

    def get(self, attempt_id: str) -> JournalEntry:
        with self._connect() as connection:
            return self._get(connection, attempt_id)

    @staticmethod
    def _event(connection: sqlite3.Connection, entry: JournalEntry, event_type: str) -> None:
        connection.execute(
            "INSERT INTO journal_events(attempt_id,type,payload) VALUES (?,?,?)",
            (entry.dispatch.attempt_id, event_type, entry.model_dump_json()),
        )

    def receive(
        self,
        verified: VerifiedDispatch,
        command: NativeCommand,
        *,
        prepared: PreparedExecution | None = None,
        require_preparation: bool = False,
    ) -> JournalEntry:
        verified.spec.verify()
        command = NativeCommand.model_validate(command)
        stage = next(
            stage for stage in verified.spec.stages if stage.id == verified.envelope.stage_id
        )
        if command.argv != stage.argv or command.timeout_ms > stage.timeout_ms:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Invocation exceeds its stage grant"
            )
        if prepared is not None:
            prepared = PreparedExecution.model_validate(prepared)
            prepared.verify(verified.envelope, verified.spec)
        entry = JournalEntry(
            dispatch=verified.envelope,
            spec=verified.spec,
            command=command,
            prepared=prepared,
            preparation_required=require_preparation,
        )
        digest = record_digest(
            "research.worker-input",
            {
                "dispatch": entry.dispatch.model_dump(mode="json"),
                "spec": entry.spec.model_dump(mode="json"),
                "command": command.model_dump(mode="json"),
                **({"prepared": prepared.model_dump(mode="json")} if prepared is not None else {}),
                **({"preparation_required": True} if require_preparation else {}),
            },
        )
        with self._transaction() as connection:
            prior = connection.execute(
                "SELECT input_digest,payload FROM attempts WHERE id=? OR dispatch_id=?",
                (entry.dispatch.attempt_id, entry.dispatch.id),
            ).fetchone()
            if prior is not None:
                if prior["input_digest"] != digest:
                    raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Worker dispatch changed")
                return JournalEntry.model_validate_json(prior["payload"])
            connection.execute(
                "INSERT INTO attempts(id,dispatch_id,input_digest,state,revision,payload) "
                "VALUES (?,?,?,?,?,?)",
                (
                    entry.dispatch.attempt_id,
                    entry.dispatch.id,
                    digest,
                    entry.state,
                    entry.revision,
                    entry.model_dump_json(),
                ),
            )
            self._event(connection, entry, "dispatch.received")
            return entry

    def _change(
        self,
        attempt_id: str,
        event_type: str,
        change: Callable[[JournalEntry], JournalEntry],
    ) -> JournalEntry:
        with self._transaction() as connection:
            before = self._get(connection, attempt_id)
            after = JournalEntry.model_validate(change(before))
            if after == before:
                return before
            after = after.model_copy(update={"revision": before.revision + 1})
            connection.execute(
                "UPDATE attempts SET payload=?,state=?,revision=? WHERE id=? AND revision=?",
                (after.model_dump_json(), after.state, after.revision, attempt_id, before.revision),
            )
            self._event(connection, after, event_type)
            return after

    def claim_launch(self, attempt_id: str, supervisor_id: str) -> JournalEntry:
        def claim(entry: JournalEntry) -> JournalEntry:
            if entry.state != JournalState.RECEIVED:
                raise ResearchError(ErrorCode.EXECUTION_UNKNOWN, "Attempt cannot be launched again")
            if (
                entry.cancel_requested
                or min(entry.dispatch.expires_at, entry.dispatch.hard_deadline_at) <= utc_now()
            ):
                return entry.model_copy(
                    update={
                        "state": JournalState.NOT_STARTED,
                        "finding": "Cancelled or expired before launch",
                    }
                )
            if entry.preparation_required and entry.prepared is None:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Required workload preparation is incomplete"
                )
            return entry.model_copy(
                update={"state": JournalState.LAUNCHING, "supervisor_id": supervisor_id}
            )

        return self._change(attempt_id, "launch.claimed", claim)

    def bind_prepared(self, attempt_id: str, prepared: PreparedExecution) -> JournalEntry:
        prepared = PreparedExecution.model_validate(prepared)

        def bind(entry: JournalEntry) -> JournalEntry:
            if entry.prepared is not None:
                if entry.prepared != prepared:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Prepared execution changed"
                    )
                return entry
            if entry.state != JournalState.RECEIVED or not entry.preparation_required:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Attempt cannot accept preparation"
                )
            prepared.verify(entry.dispatch, entry.spec)
            prepared.command(entry.command)
            return entry.model_copy(update={"prepared": prepared})

        return self._change(attempt_id, "workload.prepared", bind)

    def settle_unstarted(self, attempt_id: str) -> JournalEntry:
        """Cancellation and expiry can close an unreleased grant even if preparation is damaged."""

        def settle(entry: JournalEntry) -> JournalEntry:
            if entry.state == JournalState.RECEIVED and (
                entry.cancel_requested
                or min(entry.dispatch.expires_at, entry.dispatch.hard_deadline_at) <= utc_now()
            ):
                return entry.model_copy(
                    update={
                        "state": JournalState.NOT_STARTED,
                        "finding": "Cancelled or expired before launch",
                    }
                )
            return entry

        return self._change(attempt_id, "launch.settled_unstarted", settle)

    @staticmethod
    def _owner(entry: JournalEntry, supervisor_id: str) -> None:
        if entry.supervisor_id != supervisor_id:
            raise ResearchError(ErrorCode.STALE_FENCE, "Supervisor does not own this execution")

    def held(self, attempt_id: str, supervisor_id: str, native: NativeIdentity) -> JournalEntry:
        def record(entry: JournalEntry) -> JournalEntry:
            self._owner(entry, supervisor_id)
            if entry.state != JournalState.LAUNCHING:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Attempt is not awaiting native identity"
                )
            return entry.model_copy(update={"state": JournalState.HELD, "native": native})

        return self._change(attempt_id, "process.held", record)

    def authorize_release(self, attempt_id: str, supervisor_id: str) -> bool:
        def authorize(entry: JournalEntry) -> JournalEntry:
            self._owner(entry, supervisor_id)
            if entry.state != JournalState.HELD:
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Process is not held")
            if (
                entry.cancel_requested
                or min(entry.dispatch.expires_at, entry.dispatch.hard_deadline_at) <= utc_now()
            ):
                return entry
            return entry.model_copy(
                update={"state": JournalState.RELEASING, "release_requested_at": utc_now()}
            )

        return (
            self._change(attempt_id, "release.authorized", authorize).state
            == JournalState.RELEASING
        )

    def running(self, attempt_id: str, supervisor_id: str) -> JournalEntry:
        def record(entry: JournalEntry) -> JournalEntry:
            self._owner(entry, supervisor_id)
            if entry.state != JournalState.RELEASING:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Process release was not authorized"
                )
            return entry.model_copy(update={"state": JournalState.RUNNING})

        return self._change(attempt_id, "process.released", record)

    def request_cancel(self, attempt_id: str, controller_sequence: int) -> JournalEntry:
        """Called after transport authority validation; sequence is per worker command stream."""

        def cancel(entry: JournalEntry) -> JournalEntry:
            if controller_sequence <= entry.last_controller_sequence:
                return entry
            return entry.model_copy(
                update={"cancel_requested": True, "last_controller_sequence": controller_sequence}
            )

        return self._change(attempt_id, "cancellation.requested", cancel)

    def record_outcome(
        self, attempt_id: str, supervisor_id: str, outcome: NativeOutcome
    ) -> JournalEntry:
        outcome = NativeOutcome.model_validate(outcome)

        def record(entry: JournalEntry) -> JournalEntry:
            self._owner(entry, supervisor_id)
            if entry.outcome is not None:
                if entry.outcome != outcome:
                    raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Native outcome changed")
                return entry
            if entry.native != outcome.closure.identity:
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Outcome identifies another process"
                )
            state = JournalState.UNKNOWN
            if outcome.closure.scope_closed:
                state = (
                    JournalState.NOT_STARTED
                    if outcome.reason == "not_started"
                    else JournalState.CLOSED
                )
            return entry.model_copy(update={"state": state, "outcome": outcome})

        return self._change(attempt_id, "process.observed", record)

    def unknown(self, attempt_id: str, supervisor_id: str, finding: str) -> JournalEntry:
        def record(entry: JournalEntry) -> JournalEntry:
            self._owner(entry, supervisor_id)
            if entry.state in {JournalState.CLOSED, JournalState.NOT_STARTED}:
                return entry
            return entry.model_copy(update={"state": JournalState.UNKNOWN, "finding": finding})

        return self._change(attempt_id, "execution.unknown", record)

    def pending(self, *, limit: int = 100) -> tuple[JournalEvent, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("Invalid journal event page size")
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT sequence,attempt_id,type,payload FROM journal_events "
                "WHERE acknowledged=0 ORDER BY sequence LIMIT ?",
                (limit,),
            ).fetchall()
        return tuple(
            JournalEvent(
                sequence=row["sequence"],
                attempt_id=row["attempt_id"],
                type=row["type"],
                entry=JournalEntry.model_validate_json(row["payload"]),
            )
            for row in rows
        )

    def note_reconciliation(self, attempt_id: str, finding: str) -> JournalEntry:
        """A missing receipt does not establish that the owning supervisor has stopped."""
        return self._change(
            attempt_id,
            "reconciliation.pending",
            lambda entry: entry.model_copy(update={"finding": finding}),
        )

    def acknowledge(self, sequence: int) -> None:
        with self._transaction() as connection:
            result = connection.execute(
                "UPDATE journal_events SET acknowledged=1 WHERE sequence=?", (sequence,)
            )
            if result.rowcount != 1:
                raise ResearchError(ErrorCode.NOT_FOUND, "Worker observation not found")

    def unresolved(self) -> tuple[JournalEntry, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT payload FROM attempts WHERE state NOT IN (?,?) ORDER BY id",
                (JournalState.CLOSED, JournalState.NOT_STARTED),
            ).fetchall()
        return tuple(JournalEntry.model_validate_json(row["payload"]) for row in rows)
