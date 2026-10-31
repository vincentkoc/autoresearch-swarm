"""Native SQLite implementation; filesystem, pragmas and savepoints stay here."""

from __future__ import annotations

import inspect
import sqlite3
from collections.abc import Callable, Iterator, Sequence
from contextlib import closing, contextmanager
from pathlib import Path
from typing import TypeVar, cast

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.migrations import enable_wal, initialize
from research_swarm.state.schema import SCHEMA_VERSION
from research_swarm.state.sql import (
    IntegrityViolation,
    SqlRow,
    SqlSession,
    SqlValue,
    StoreIdentity,
)

T = TypeVar("T")


def synchronous(callback: Callable[[SqlSession], T], session: SqlSession) -> T:
    result = callback(session)
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError("SQL transaction callbacks must complete synchronously")
    return result


class SQLiteResult:
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self.cursor = cursor

    def __iter__(self) -> Iterator[SqlRow]:
        return cast(Iterator[SqlRow], iter(self.cursor))

    def fetchone(self) -> SqlRow | None:
        return cast(SqlRow | None, self.cursor.fetchone())

    def fetchall(self) -> list[SqlRow]:
        return cast(list[SqlRow], self.cursor.fetchall())

    def one(self) -> SqlRow:
        rows = self.cursor.fetchmany(2)
        if len(rows) != 1:
            raise RuntimeError("SQL query did not return exactly one required row")
        return cast(SqlRow, rows[0])


class SQLiteSession:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection
        self._savepoint = 0

    def execute(self, statement: str, parameters: Sequence[SqlValue] = ()) -> SQLiteResult:
        try:
            return SQLiteResult(self.connection.execute(statement, parameters))
        except sqlite3.IntegrityError as error:
            raise IntegrityViolation(str(error)) from error

    def atomic(self, callback: Callable[[SqlSession], T]) -> T:
        if not self.connection.in_transaction:
            raise RuntimeError("Nested SQL callbacks require an outer store transaction")
        name = f"domain_operation_{self._savepoint}"
        self._savepoint += 1
        self.connection.execute("SAVEPOINT " + name)
        try:
            result = synchronous(callback, self)
        except BaseException:
            self.connection.execute("ROLLBACK TO " + name)
            self.connection.execute("RELEASE " + name)
            raise
        self.connection.execute("RELEASE " + name)
        return result

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()


class SQLiteStore:
    def __init__(self, path: Path, schema: str, *, busy_timeout_ms: int = 10_000) -> None:
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as session:
            connection = session.connection
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2, SCHEMA_VERSION):
                raise ResearchError(ErrorCode.SCHEMA_UNSUPPORTED, "Unsupported database schema")
            enable_wal(connection, timeout_ms=busy_timeout_ms)
            connection.execute("PRAGMA synchronous=FULL")
            initialize(connection, schema)

    @property
    def identity(self) -> StoreIdentity:
        return StoreIdentity("sqlite", str(self.path.resolve()))

    @contextmanager
    def connect(self) -> Iterator[SQLiteSession]:
        connection = sqlite3.connect(
            self.path, isolation_level=None, timeout=self.busy_timeout_ms / 1000
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        try:
            yield SQLiteSession(connection)
        finally:
            connection.close()

    def _run(self, callback: Callable[[SqlSession], T], *, write: bool) -> T:
        with self.connect() as session:
            session.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            try:
                result = synchronous(callback, session)
                session.commit()
            except BaseException:
                session.rollback()
                raise
        return result

    def read(self, callback: Callable[[SqlSession], T]) -> T:
        return self._run(callback, write=False)

    def write(self, callback: Callable[[SqlSession], T]) -> T:
        return self._run(callback, write=True)

    def backup(self, destination: Path) -> None:
        if destination.exists():
            raise FileExistsError(destination)
        with self.connect() as source, closing(sqlite3.connect(destination)) as target, target:
            source.connection.backup(target)
