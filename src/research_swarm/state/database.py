"""SQLite transaction boundary, idempotency, events, and a durable outbox."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import Event
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now

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
PRAGMA user_version = 1;
"""


def encode(value: Any) -> str:
    return canonical_bytes(value).decode("utf-8")


class Transaction:
    """Private repository access used only inside a domain command."""

    def __init__(self, connection: sqlite3.Connection, context: CommandContext) -> None:
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
        except sqlite3.IntegrityError as error:
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
            """UPDATE records SET payload=?,state=?,revision=revision+1,updated_at=?
            WHERE workspace_id=? AND kind=? AND id=? AND revision=?""",
            (
                encode(data),
                state,
                utc_now(),
                self.workspace_id,
                kind,
                identifier,
                expected_revision,
            ),
        )
        if cursor.rowcount != 1:
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
            "UPDATE workspaces SET next_sequence=next_sequence+1 WHERE id=?",
            (self.workspace_id,),
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
        return event


class Database:
    """No global Python lock: SQLite arbitrates independent processes."""

    def __init__(self, path: Path, *, busy_timeout_ms: int = 10_000) -> None:
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise ResearchError(ErrorCode.SCHEMA_UNSUPPORTED, "Unsupported database schema")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(SCHEMA)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(
            self.path, isolation_level=None, timeout=self.busy_timeout_ms / 1000
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        try:
            yield connection
        finally:
            connection.close()

    def bootstrap(self, workspace_id: str, controller_id: str) -> None:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
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
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def execute(
        self,
        context: CommandContext,
        operation: str,
        payload: Mapping[str, Any],
        scope: str,
        handler: Callable[[Transaction], JsonObject],
    ) -> JsonObject:
        context.principal.require(scope)
        request_digest = record_digest(
            "research.command",
            {
                "operation": operation,
                "expected_revision": context.expected_revision,
                "payload": dict(payload),
            },
        )
        workspace_id = context.principal.workspace_id
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                if (
                    connection.execute(
                        "SELECT 1 FROM workspaces WHERE id=?", (workspace_id,)
                    ).fetchone()
                    is None
                ):
                    raise ResearchError(ErrorCode.NOT_FOUND, "Workspace not found")
                previous = connection.execute(
                    """SELECT request_digest,response FROM commands
                    WHERE workspace_id=? AND actor_id=? AND idempotency_key=?""",
                    (workspace_id, context.principal.id, context.idempotency_key),
                ).fetchone()
                if previous is not None:
                    if previous["request_digest"] != request_digest:
                        raise ResearchError(
                            ErrorCode.IDEMPOTENCY_CONFLICT,
                            "Idempotency key was used with a different request",
                        )
                    response = strict_json_loads(previous["response"])
                else:
                    connection.execute("SAVEPOINT domain_operation")
                    try:
                        result = handler(Transaction(connection, context))
                        response = {"command_id": context.command_id, "result": result}
                        encode(response)
                    except ResearchError as error:
                        connection.execute("ROLLBACK TO domain_operation")
                        response = {"command_id": context.command_id, **error.as_dict()}
                    connection.execute("RELEASE domain_operation")
                    connection.execute(
                        """INSERT INTO commands
                        (workspace_id,actor_id,idempotency_key,request_digest,
                         command_id,response,created_at) VALUES (?,?,?,?,?,?,?)""",
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
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        if "error" in response:
            failure = response["error"]
            raise ResearchError(
                ErrorCode(failure["code"]),
                failure["message"],
                retryable=failure["retryable"],
                details=failure["details"],
            )
        return dict(response)

    def snapshot(self, context: CommandContext) -> JsonObject:
        context.principal.require("read")
        workspace_id = context.principal.workspace_id
        with self.connect() as connection:
            connection.execute("BEGIN")
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
            connection.commit()
            return result

    def read_record(self, context: CommandContext, kind: str, identifier: str) -> JsonObject:
        context.principal.require("read")
        with self.connect() as connection:
            return Transaction(connection, context).get(kind, identifier)

    def events(self, context: CommandContext, *, after: int = 0, limit: int = 100) -> list[Event]:
        context.principal.require("read")
        if not after >= 0 or not 1 <= limit <= 1000:
            raise ValueError("Invalid event cursor or page size")
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT payload FROM events WHERE workspace_id=? AND sequence>? "
                "ORDER BY sequence LIMIT ?",
                (context.principal.workspace_id, after, limit),
            ).fetchall()
        return [Event.model_validate_json(row["payload"]) for row in rows]

    def pending_notifications(self, *, limit: int = 100) -> list[Event]:
        with self.connect() as connection:
            rows = connection.execute(
                """SELECT e.payload FROM outbox o JOIN events e ON e.id=o.event_id
                WHERE o.delivered=0 ORDER BY o.workspace_id,o.sequence LIMIT ?""",
                (limit,),
            ).fetchall()
        return [Event.model_validate_json(row["payload"]) for row in rows]

    def acknowledge_notification(self, event_id: str) -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE outbox SET delivered=1,delivery_attempts=delivery_attempts+1 "
                "WHERE event_id=?",
                (event_id,),
            )

    def backup(self, destination: Path) -> None:
        if destination.exists():
            raise FileExistsError(destination)
        with self.connect() as source, sqlite3.connect(destination) as target:
            source.backup(target)
