"""Stable SQLite instance identities for custody that must survive process restart."""

import sqlite3
from contextlib import closing
from pathlib import Path

from pydantic import TypeAdapter

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, new_id


def initialize_identity(connection: sqlite3.Connection) -> str:
    connection.execute(
        "CREATE TABLE IF NOT EXISTS storage_identity "
        "(slot INTEGER PRIMARY KEY CHECK(slot=1), id TEXT NOT NULL)"
    )
    connection.execute("INSERT OR IGNORE INTO storage_identity VALUES (1,?)", (new_id(),))
    return str(connection.execute("SELECT id FROM storage_identity WHERE slot=1").fetchone()[0])


def read_identity(path: Path) -> str:
    """Read without creating a missing database or initializing a replacement."""
    try:
        with closing(
            sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)
        ) as connection:
            row = connection.execute("SELECT id FROM storage_identity WHERE slot=1").fetchone()
            if row is None:
                raise ValueError("Storage identity is absent")
            return str(TypeAdapter(ID).validate_python(row[0]))
    except (sqlite3.Error, ValueError) as error:
        raise ResearchError(
            ErrorCode.EXECUTION_UNKNOWN,
            "Native storage identity is unavailable; explicit recovery is required",
        ) from error


def connect_existing(path: Path, expected_identity: str) -> sqlite3.Connection:
    """Open pinned native storage without creating a replacement at a missing path."""
    connection = None
    try:
        if path.is_symlink():
            raise ValueError("Native storage must not be a symbolic link")
        connection = sqlite3.connect(
            path.absolute().as_uri() + "?mode=rw", uri=True, isolation_level=None, timeout=10
        )
        connection.row_factory = sqlite3.Row
        row = connection.execute("SELECT id FROM storage_identity WHERE slot=1").fetchone()
        if row is None or row["id"] != expected_identity:
            raise ValueError("Native storage identity differs")
        if connection.execute("PRAGMA user_version").fetchone()[0] != 1:
            raise ValueError("Native storage schema differs")
        return connection
    except (sqlite3.Error, ValueError) as error:
        if connection is not None:
            connection.close()
        raise ResearchError(
            ErrorCode.EXECUTION_UNKNOWN, "Original native storage is unavailable or changed"
        ) from error
