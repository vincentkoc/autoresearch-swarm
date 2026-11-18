"""Portable schema statements; adapters own version metadata and transactions."""

from typing import Any, Protocol

from research_swarm.domain.errors import ErrorCode, ResearchError

SCHEMA_VERSION = 4


class DDLConnection(Protocol):
    def execute(self, statement: str, /) -> Any: ...


def require_active_workspaces(connection: DDLConnection) -> None:
    inactive = connection.execute(
        "SELECT 1 FROM records WHERE kind='workspace_gate' AND state!='active' LIMIT 1"
    ).fetchone()
    if inactive is not None:
        raise ResearchError(
            ErrorCode.SCHEMA_UNSUPPORTED,
            "This database contains a sealed or recovering workspace. "
            "Use its original release to read it and start new work in a fresh database.",
        )


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


def apply_schema(connection: DDLConnection, base_schema: str, version: int) -> None:
    if version not in (0, 1, 2, 3, SCHEMA_VERSION):
        raise ResearchError(ErrorCode.SCHEMA_UNSUPPORTED, "Unsupported database schema")
    apply_statements(connection, base_schema)
    require_active_workspaces(connection)
    if version < 2:
        migrate_v2(connection)
    if version < 3:
        connection.execute(
            "CREATE INDEX IF NOT EXISTS acceptance_leases ON records"
            "(workspace_id, campaign_id, state, id) WHERE kind='acceptance_lease'"
        )
