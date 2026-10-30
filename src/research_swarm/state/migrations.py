"""Transactional schema changes preserve acknowledged commands and event positions."""

from __future__ import annotations

import sqlite3
from time import monotonic, sleep

from research_swarm.state.schema import SCHEMA_VERSION, apply_schema


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


def initialize(connection: sqlite3.Connection, base_schema: str) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        apply_schema(connection, base_schema, version)
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
