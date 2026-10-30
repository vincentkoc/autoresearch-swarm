"""Portable schema statements; adapters own version metadata and transactions."""

from typing import Any, Protocol

from research_swarm.domain.errors import ErrorCode, ResearchError

SCHEMA_VERSION = 3


class DDLConnection(Protocol):
    def execute(self, statement: str, /) -> Any: ...


RECOVERY_SCHEMA = """
CREATE TABLE IF NOT EXISTS recovery_snapshots (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    watermark INTEGER NOT NULL,
    expires_at TEXT NOT NULL,
    released_at TEXT,
    expired_at TEXT,
    payload TEXT NOT NULL,
    PRIMARY KEY(workspace_id,id)
);
CREATE INDEX IF NOT EXISTS recovery_pins ON recovery_snapshots(
    workspace_id,expires_at,watermark
) WHERE released_at IS NULL;
CREATE TABLE IF NOT EXISTS recovery_items (
    workspace_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    content_digest TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    content BLOB NOT NULL,
    PRIMARY KEY(workspace_id,snapshot_id,ordinal),
    FOREIGN KEY(workspace_id,snapshot_id) REFERENCES recovery_snapshots(workspace_id,id)
        ON DELETE CASCADE
);
"""


def apply_statements(connection: DDLConnection, schema: str) -> None:
    # These are fixed internal DDL statements, without triggers or quoted semicolons.
    for statement in schema.split(";"):
        if statement.strip():
            connection.execute(statement)


def migrate_v2(connection: DDLConnection) -> None:
    columns = {row[1] for row in connection.execute("PRAGMA table_info(workspaces)")}
    if "event_floor" not in columns:
        connection.execute(
            "ALTER TABLE workspaces ADD COLUMN event_floor INTEGER NOT NULL DEFAULT 1"
        )
    apply_statements(connection, RECOVERY_SCHEMA)


def apply_schema(connection: DDLConnection, base_schema: str, version: int) -> None:
    if version not in (0, 1, 2, SCHEMA_VERSION):
        raise ResearchError(ErrorCode.SCHEMA_UNSUPPORTED, "Unsupported database schema")
    apply_statements(connection, base_schema)
    if version < 2:
        migrate_v2(connection)
    if version < 3:
        connection.execute(
            "CREATE INDEX IF NOT EXISTS acceptance_leases ON records"
            "(workspace_id, campaign_id, state, id) WHERE kind='acceptance_lease'"
        )
