"""Transactional schema changes preserve acknowledged commands and event positions."""

from __future__ import annotations

import sqlite3
from time import monotonic, sleep

from research_swarm.domain.errors import ErrorCode, ResearchError

SCHEMA_VERSION = 2


def enable_wal(connection: sqlite3.Connection, *, timeout_ms: int) -> None:
    # Changing journal mode can report BUSY without invoking SQLite's busy handler.
    deadline = monotonic() + timeout_ms / 1000
    delay = 0.01
    while True:
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as error:
            remaining = deadline - monotonic()
            if (
                getattr(error, "sqlite_errorcode", 0) & 255
                not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
                or remaining <= 0
            ):
                raise
            sleep(min(delay, remaining))
            delay = min(delay * 2, 0.1)


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


def apply_statements(connection: sqlite3.Connection, schema: str) -> None:
    # These are fixed internal DDL statements, without triggers or quoted semicolons.
    for statement in schema.split(";"):
        if statement.strip():
            connection.execute(statement)


def migrate_v2(connection: sqlite3.Connection) -> None:
    columns = {row[1] for row in connection.execute("PRAGMA table_info(workspaces)")}
    if "event_floor" not in columns:
        connection.execute(
            "ALTER TABLE workspaces ADD COLUMN event_floor INTEGER NOT NULL DEFAULT 1"
        )
    apply_statements(connection, RECOVERY_SCHEMA)


def initialize(connection: sqlite3.Connection, base_schema: str) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1, SCHEMA_VERSION):
            raise ResearchError(ErrorCode.SCHEMA_UNSUPPORTED, "Unsupported database schema")
        apply_statements(connection, base_schema)
        if version < 2:
            migrate_v2(connection)
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
