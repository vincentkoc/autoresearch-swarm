"""SQLite transaction boundary, idempotency, events, and a durable outbox."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import Event
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.state.events import event_page
from research_swarm.state.sql import (
    IntegrityViolation,
    SqlSession,
    StateStore,
    StoreIdentity,
    next_counter,
)

if TYPE_CHECKING:
    from research_swarm.state.sqlite import SQLiteSession, SQLiteStore

JsonObject = dict[str, Any]

SCHEMA = """
CREATE TABLE IF NOT EXISTS workspaces (
    id TEXT PRIMARY KEY,
    controller_id TEXT NOT NULL,
    control_epoch INTEGER NOT NULL DEFAULT 1,
    next_sequence INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS records (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    kind TEXT NOT NULL,
    id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    state TEXT NOT NULL,
    campaign_id TEXT,
    parent_id TEXT,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (workspace_id, kind, id)
);
CREATE INDEX IF NOT EXISTS records_campaign
ON records(workspace_id, kind, campaign_id, state);
CREATE INDEX IF NOT EXISTS records_workspace ON records(workspace_id);
CREATE INDEX IF NOT EXISTS records_candidate_tree ON records(
    workspace_id,campaign_id,json_extract(payload,'$.source.tree_digest')
) WHERE kind='candidate';
CREATE INDEX IF NOT EXISTS records_worker_evidence ON records(
    workspace_id,parent_id,json_extract(payload,'$.role'),
    json_extract(payload,'$.output_name'),json_extract(payload,'$.artifact.id')
) WHERE kind='worker_upload' AND state='finalized';
CREATE INDEX IF NOT EXISTS records_worker_pending ON records(
    workspace_id,
    json_extract(payload,'$.dispatch.worker_id'),
    json_extract(payload,'$.dispatch.worker_session_id'),
    json_extract(payload,'$.dispatch.control_epoch'),
    json_extract(payload,'$.dispatch.controller_id'),
    json_extract(payload,'$.publication_status')
) WHERE kind='attempt';
CREATE INDEX IF NOT EXISTS records_worker_trial ON records(
    workspace_id,
    json_extract(payload,'$.dispatch.worker_id'),
    json_extract(payload,'$.dispatch.worker_session_id'),
    json_extract(payload,'$.dispatch.control_epoch'),
    json_extract(payload,'$.dispatch.controller_id'),
    json_extract(payload,'$.dispatch.trial_id')
) WHERE kind='attempt';
CREATE INDEX IF NOT EXISTS records_ready_jobs ON records(workspace_id)
WHERE kind='job' AND state='ready';
CREATE INDEX IF NOT EXISTS records_worker_controls ON records(
    workspace_id,
    json_extract(payload,'$.binding.worker_id'),
    json_extract(payload,'$.binding.worker_session_id'),
    json_extract(payload,'$.binding.control_epoch'),
    json_extract(payload,'$.binding.controller_id')
) WHERE kind='attempt_control' AND state='requested';
CREATE TABLE IF NOT EXISTS commands (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    actor_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    command_id TEXT NOT NULL,
    response TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (workspace_id, actor_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS events (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    sequence INTEGER NOT NULL,
    id TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL,
    PRIMARY KEY (workspace_id, sequence)
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY REFERENCES events(id),
    workspace_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    delivery_attempts INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS budgets (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    campaign_id TEXT NOT NULL,
    dimension TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (workspace_id, campaign_id, dimension)
);
CREATE TABLE IF NOT EXISTS budget_reservations (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    state TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (workspace_id, id),
    UNIQUE (workspace_id, owner_id)
);
CREATE TABLE IF NOT EXISTS budget_settlements (
    workspace_id TEXT NOT NULL,
    settlement_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    response TEXT NOT NULL,
    PRIMARY KEY (workspace_id, settlement_id)
);
CREATE TABLE IF NOT EXISTS budget_entries (
    workspace_id TEXT NOT NULL,
    id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    dimension TEXT NOT NULL,
    settlement_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (workspace_id, id),
    UNIQUE (workspace_id, settlement_id, campaign_id, dimension)
);
"""


def encode(value: Any) -> str:
    return canonical_bytes(value).decode("utf-8")


def _request_digest(context: CommandContext, operation: str, payload: Mapping[str, Any]) -> str:
    return record_digest(
        "research.command",
        {
            "operation": operation,
            "expected_revision": context.expected_revision,
            "payload": dict(payload),
        },
    )


def _stored_response(
    connection: SqlSession, context: CommandContext, request_digest: str
) -> JsonObject | None:
    previous = connection.execute(
        "SELECT request_digest,response FROM commands "
        "WHERE workspace_id=? AND actor_id=? AND idempotency_key=?",
        (context.principal.workspace_id, context.principal.id, context.idempotency_key),
    ).fetchone()
    if previous is None:
        return None
    if previous["request_digest"] != request_digest:
        raise ResearchError(
            ErrorCode.IDEMPOTENCY_CONFLICT, "Idempotency key was used with a different request"
        )
    return dict(strict_json_loads(previous["response"]))


def _deliver(response: JsonObject) -> JsonObject:
    if "error" in response:
        failure = response["error"]
        raise ResearchError(
            ErrorCode(failure["code"]),
            failure["message"],
            retryable=failure["retryable"],
            details=failure["details"],
            command_id=response["command_id"],
        )
    return {**response, "disposition": "accepted"}


class Transaction:
    """Private repository access used only inside a domain command."""

    def __init__(self, connection: SqlSession, context: CommandContext) -> None:
        self.connection = connection
        self.context = context
        self.workspace_id = context.principal.workspace_id

    def get(self, kind: str, identifier: str) -> JsonObject:
        row = self.connection.execute(
            "SELECT * FROM records WHERE workspace_id=? AND kind=? AND id=?",
            (self.workspace_id, kind, identifier),
        ).fetchone()
        if row is None:
            raise ResearchError(ErrorCode.NOT_FOUND, f"{kind} not found")
        return {
            "id": row["id"],
            "kind": row["kind"],
            "revision": row["revision"],
            "state": row["state"],
            "campaign_id": row["campaign_id"],
            "parent_id": row["parent_id"],
            "data": strict_json_loads(row["payload"]),
        }

    def insert(
        self,
        kind: str,
        identifier: str,
        data: Mapping[str, Any],
        *,
        state: str,
        campaign_id: str | None = None,
        parent_id: str | None = None,
    ) -> JsonObject:
        now = utc_now()
        try:
            self.connection.execute(
                """INSERT INTO records
                (workspace_id,kind,id,revision,state,campaign_id,parent_id,payload,created_at,updated_at)
                VALUES (?,?,?,1,?,?,?,?,?,?)""",
                (
                    self.workspace_id,
                    kind,
                    identifier,
                    state,
                    campaign_id,
                    parent_id,
                    encode(data),
                    now,
                    now,
                ),
            )
        except IntegrityViolation as error:
            raise ResearchError(ErrorCode.REVISION_CONFLICT, f"{kind} already exists") from error
        return self.get(kind, identifier)

    def update(
        self,
        kind: str,
        identifier: str,
        *,
        expected_revision: int,
        data: Mapping[str, Any],
        state: str,
    ) -> JsonObject:
        cursor = self.connection.execute(
            """UPDATE records SET payload=?,state=?,revision=?,updated_at=?
            WHERE workspace_id=? AND kind=? AND id=? AND revision=? RETURNING revision""",
            (
                encode(data),
                state,
                next_counter(expected_revision),
                utc_now(),
                self.workspace_id,
                kind,
                identifier,
                expected_revision,
            ),
        )
        if len(cursor.fetchall()) != 1:
            raise ResearchError(ErrorCode.REVISION_CONFLICT, f"{kind} revision changed")
        return self.get(kind, identifier)

    def event(
        self,
        event_type: str,
        aggregate: JsonObject,
        payload: Mapping[str, Any],
    ) -> Event:
        row = self.connection.execute(
            "SELECT next_sequence FROM workspaces WHERE id=?", (self.workspace_id,)
        ).fetchone()
        assert row is not None
        sequence = row["next_sequence"]
        self.connection.execute(
            "UPDATE workspaces SET next_sequence=? WHERE id=?",
            (next_counter(sequence), self.workspace_id),
        )
        now = utc_now()
        event = Event(
            workspace_id=self.workspace_id,
            sequence=sequence,
            aggregate_type=aggregate["kind"],
            aggregate_id=aggregate["id"],
            aggregate_revision=aggregate["revision"],
            type=event_type,
            occurred_at=now,
            recorded_at=now,
            actor_id=self.context.principal.id,
            command_id=self.context.command_id,
            campaign_id=aggregate.get("campaign_id"),
            payload=dict(payload),
        )
        self.connection.execute(
            "INSERT INTO events(workspace_id,sequence,id,payload) VALUES (?,?,?,?)",
            (self.workspace_id, sequence, event.id, encode(event.model_dump(mode="json"))),
        )
        self.connection.execute(
            "INSERT INTO outbox(event_id,workspace_id,sequence) VALUES (?,?,?)",
            (event.id, self.workspace_id, sequence),
        )
        from research_swarm.state.waits import consider_event

        consider_event(self, event)
        return event


class Database:
    """No global Python lock: SQLite arbitrates independent processes."""

    def __init__(self, path: Path, *, busy_timeout_ms: int = 10_000) -> None:
        from research_swarm.state.sqlite import SQLiteStore

        native = SQLiteStore(path, SCHEMA, busy_timeout_ms=busy_timeout_ms)
        self._native_store: SQLiteStore | None = native
        self.store: StateStore = native

    @classmethod
    def from_store(cls, store: StateStore) -> Database:
        """Bind the shared command kernel to an already initialized authority store."""
        instance = cls.__new__(cls)
        instance.store = store
        instance._native_store = None
        return instance

    @property
    def path(self) -> Path:
        if self._native_store is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "This state authority has no native filesystem path",
            )
        return self._native_store.path

    @property
    def authority_identity(self) -> StoreIdentity:
        return self.store.identity

    @contextmanager
    def connect(self) -> Iterator[SQLiteSession]:
        """Legacy native connection access; hosted callers must use store callbacks."""
        if self._native_store is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "This operation requires the native SQLite connection adapter",
            )
        with self._native_store.connect() as connection:
            yield connection

    def bootstrap(self, workspace_id: str, controller_id: str) -> None:
        def initialize(connection: SqlSession) -> None:
            row = connection.execute(
                "SELECT controller_id FROM workspaces WHERE id=?", (workspace_id,)
            ).fetchone()
            if row is not None and row["controller_id"] != controller_id:
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT, "Workspace already has a different controller"
                )
            connection.execute(
                "INSERT OR IGNORE INTO workspaces(id,controller_id) VALUES (?,?)",
                (workspace_id, controller_id),
            )

        self.store.write(initialize)

    def execute(
        self,
        context: CommandContext,
        operation: str,
        payload: Mapping[str, Any],
        scope: str,
        handler: Callable[[Transaction], JsonObject],
        *,
        on_rejection: Callable[[Transaction, ResearchError], None] | None = None,
    ) -> JsonObject:
        context.principal.require(scope)
        request_digest = _request_digest(context, operation, payload)
        workspace_id = context.principal.workspace_id

        def apply(connection: SqlSession) -> JsonObject:
            if (
                connection.execute(
                    "SELECT 1 FROM workspaces WHERE id=?", (workspace_id,)
                ).fetchone()
                is None
            ):
                raise ResearchError(ErrorCode.NOT_FOUND, "Workspace not found")
            previous = _stored_response(connection, context, request_digest)
            if previous is not None:
                return previous
            from research_swarm.state.drive_guard import require_reserved_command_key

            require_reserved_command_key(Transaction(connection, context), request_digest)

            def operation_effects(nested: SqlSession) -> JsonObject:
                from research_swarm.state.drive_guard import require_drive_authority

                transaction = Transaction(nested, context)
                require_drive_authority(transaction, operation, payload, request_digest)
                result = handler(transaction)
                response = {
                    "command_id": context.command_id,
                    "disposition": "accepted",
                    "result": result,
                }
                encode(response)
                return response

            try:
                response = connection.atomic(operation_effects)
            except ResearchError as error:
                if on_rejection is not None:
                    # Failed operation effects have rolled back. A conservative
                    # fence and the rejection acknowledgment share the outer commit.
                    on_rejection(Transaction(connection, context), error)
                response = {
                    "command_id": context.command_id,
                    "disposition": "rejected",
                    "error": error.as_dict()["error"],
                }
            connection.execute(
                "INSERT INTO commands "
                "(workspace_id,actor_id,idempotency_key,request_digest,"
                "command_id,response,created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    workspace_id,
                    context.principal.id,
                    context.idempotency_key,
                    request_digest,
                    context.command_id,
                    encode(response),
                    utc_now(),
                ),
            )
            return response

        response = self.store.write(apply)
        return _deliver(dict(response))

    def replay(
        self, context: CommandContext, operation: str, payload: Mapping[str, Any], scope: str
    ) -> JsonObject | None:
        """Read a committed acknowledgment before expensive or transient preflight.

        A miss grants no execution authority. The caller must still use execute(),
        which repeats the check inside its atomic write transaction.
        """
        context.principal.require(scope)
        digest = _request_digest(context, operation, payload)
        response = self.store.read(lambda connection: _stored_response(connection, context, digest))
        return _deliver(response) if response is not None else None

    def with_preflight(
        self,
        context: CommandContext,
        operation: str,
        payload: Mapping[str, Any],
        scope: str,
        handler: Callable[[], JsonObject],
    ) -> JsonObject:
        """Preserve a concurrent acknowledgment if mutable preflight subsequently fails.

        The handler must still commit through execute(). This helper retries no
        effect; it only recovers the exact authenticated, digest-matched response.
        """
        previous = self.replay(context, operation, payload, scope)
        if previous is not None:
            return previous
        try:
            return handler()
        except Exception:
            previous = self.replay(context, operation, payload, scope)
            if previous is not None:
                return previous
            raise

    def snapshot(self, context: CommandContext) -> JsonObject:
        context.principal.require("read")
        workspace_id = context.principal.workspace_id

        def read(connection: SqlSession) -> JsonObject:
            row = connection.execute(
                "SELECT next_sequence,control_epoch FROM workspaces WHERE id=?",
                (workspace_id,),
            ).fetchone()
            if row is None:
                raise ResearchError(ErrorCode.NOT_FOUND, "Workspace not found")
            rows = connection.execute(
                "SELECT kind,id,revision,state,campaign_id,parent_id,payload FROM records "
                "WHERE workspace_id=? ORDER BY kind,id",
                (workspace_id,),
            ).fetchall()
            result = {
                "workspace_id": workspace_id,
                "watermark": row["next_sequence"] - 1,
                "control_epoch": row["control_epoch"],
                "records": [
                    {**dict(item), "payload": strict_json_loads(item["payload"])} for item in rows
                ],
            }
            return result

        return self.store.read(read)

    def read_record(self, context: CommandContext, kind: str, identifier: str) -> JsonObject:
        context.principal.require("read")
        return self.store.read(
            lambda connection: Transaction(connection, context).get(kind, identifier)
        )

    def events(self, context: CommandContext, *, after: int = 0, limit: int = 100) -> list[Event]:
        return [
            Event.model_validate(item)
            for item in self.event_page(context, after=after, limit=limit)["events"]
        ]

    def event_page(
        self, context: CommandContext, *, after: int = 0, limit: int = 100
    ) -> JsonObject:
        context.principal.require("read")
        return self.store.read(
            lambda connection: event_page(
                connection, context.principal.workspace_id, after=after, limit=limit
            )
        )

    def pending_notifications(self, *, limit: int = 100) -> list[Event]:
        def read(connection: SqlSession) -> list[Event]:
            rows = connection.execute(
                """SELECT e.payload FROM outbox o JOIN events e ON e.id=o.event_id
                WHERE o.delivered=0 ORDER BY o.workspace_id,o.sequence LIMIT ?""",
                (limit,),
            ).fetchall()
            return [Event.model_validate_json(row["payload"]) for row in rows]

        return self.store.read(read)

    def acknowledge_notification(self, event_id: str) -> None:
        def acknowledge(connection: SqlSession) -> None:
            row = connection.execute(
                "SELECT delivery_attempts FROM outbox WHERE event_id=?", (event_id,)
            ).fetchone()
            if row is None:
                return
            connection.execute(
                "UPDATE outbox SET delivered=1,delivery_attempts=? WHERE event_id=?",
                (next_counter(row[0]), event_id),
            )

        self.store.write(acknowledge)

    def backup(self, destination: Path) -> None:
        if self._native_store is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "Filesystem backup requires the native SQLite adapter",
            )
        self._native_store.backup(destination)
