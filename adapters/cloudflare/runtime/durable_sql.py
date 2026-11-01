"""Synchronous Durable Object SQL implementation of the shared state port."""

import math
import re

from atomic import atomic
from pyodide.ffi import JsException, jsnull, to_js

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.schema import SCHEMA_VERSION, apply_schema
from research_swarm.state.schema_contracts import (
    require_research_schema,
    require_table,
    unsupported,
)
from research_swarm.state.sql import IntegrityViolation, StoreIdentity

MAX_SAFE_INTEGER = 2**53 - 1
CONSTRAINT = re.compile(
    r": SQLITE_CONSTRAINT(?:_[A-Z]+)?(?: \(extended: SQLITE_CONSTRAINT_[A-Z]+\))?$"
)


def binding(value):
    if value is None:
        return jsnull
    if isinstance(value, bytes):
        array = to_js(value, create_pyproxies=False)
        return array.buffer.slice(array.byteOffset, array.byteOffset + array.byteLength)
    if type(value) is int:
        if abs(value) > MAX_SAFE_INTEGER:
            raise ValueError("SQL integer binding exceeds the exact JavaScript range")
        # The Workers RPC wrapper converts large Python ints to JS BigInt, which
        # SQL bindings reject. Every admitted integer is exactly representable.
        return float(value)
    elif type(value) is float:
        if not math.isfinite(value) or (value.is_integer() and abs(value) > MAX_SAFE_INTEGER):
            raise ValueError("SQL numeric binding is non-finite or inexact")
    elif not isinstance(value, str):
        raise TypeError("Unsupported SQL binding type")
    return value


def cell(value):
    if value is None or value is jsnull:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if type(value) in (int, float):
        binding(value)
        return int(value) if float(value).is_integer() else value
    return value.to_bytes()


class Row:
    def __init__(self, columns, values):
        self.columns, self.values = columns, values

    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                key = self.columns.index(key)
            except ValueError as error:
                raise IndexError("No item with that key") from error
        return self.values[key]

    def keys(self):
        return list(self.columns)


class Result:
    def __init__(self, rows):
        self.rows = iter(rows)

    def __iter__(self):
        return self.rows

    def fetchone(self):
        return next(self.rows, None)

    def fetchall(self):
        return list(self.rows)

    def one(self):
        first, second = self.fetchone(), self.fetchone()
        if first is None or second is not None:
            raise RuntimeError("Expected exactly one SQL row")
        return first


class DurableObjectSession:
    def __init__(self, store):
        self.store = store
        self.active = True

    def _require_active(self):
        if not self.active:
            raise RuntimeError("SQL session escaped its synchronous callback")

    def execute(self, statement, parameters=()):
        self._require_active()
        values = tuple(binding(value) for value in parameters)
        try:
            cursor = self.store.storage.sql.exec(statement, *values)
            columns = tuple(cursor.columnNames)
            rows = [Row(columns, tuple(cell(value) for value in row)) for row in cursor.raw()]
        except JsException as error:
            if CONSTRAINT.search(str(error.message)):
                raise IntegrityViolation(str(error.message)) from error
            raise
        return Result(rows)

    def atomic(self, callback):
        self._require_active()
        return self.store._run(callback)


class DurableObjectStore:
    def __init__(self, storage, *, namespace, object_id, schema):
        self.storage = storage
        self.identity = StoreIdentity("cloudflare-do", f"{namespace}/{object_id}")
        # Foreign-key enforcement cannot be enabled from within a transaction.
        storage.sql.exec("PRAGMA foreign_keys=ON")
        self.write(lambda sql: self._initialize(sql, schema))

    @classmethod
    def open_for_provisioning(cls, storage, *, namespace, object_id, schema):
        """Private bootstrap only: initialize empty storage; never migrate existing state."""
        rows = storage.sql.exec(
            "SELECT name FROM sqlite_master "
            "WHERE substr(name,1,7)!='sqlite_' AND substr(name,1,5)!='__cf_' LIMIT 1"
        )
        if next(iter(rows.raw()), None) is not None:
            return cls.open_existing(storage, namespace=namespace, object_id=object_id)
        return cls(storage, namespace=namespace, object_id=object_id, schema=schema)

    @classmethod
    def open_existing(cls, storage, *, namespace, object_id):
        """Open current provisioned state; ordinary requests never initialize or migrate."""
        instance = cls.__new__(cls)
        instance.storage = storage
        instance.identity = StoreIdentity("cloudflare-do", f"{namespace}/{object_id}")

        def validate(sql):
            require_table(sql, "research_schema", "singleton version", ("singleton",))
            versions = sql.execute("SELECT singleton,version FROM research_schema").fetchall()
            if len(versions) != 1 or (versions[0][0], versions[0][1]) != (1, SCHEMA_VERSION):
                raise unsupported()
            require_research_schema(sql)

        instance.read(validate)
        return instance

    def _initialize(self, sql, schema):
        if sql.execute("PRAGMA foreign_keys").one()[0] != 1:
            raise RuntimeError("Durable Object SQL foreign keys are disabled")
        exists = sql.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='research_schema'"
        ).fetchone()
        if exists is None:
            foreign = sql.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND substr(name,1,7) != 'sqlite_' AND substr(name,1,5) != '__cf_'"
            ).fetchall()
            if foreign:
                raise ResearchError(
                    ErrorCode.SCHEMA_UNSUPPORTED, "Unversioned Durable Object state exists"
                )
            sql.execute(
                "CREATE TABLE research_schema (singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                "version INTEGER NOT NULL)"
            )
            version = 0
        else:
            version = sql.execute("SELECT version FROM research_schema WHERE singleton=1").one()[0]
        apply_schema(sql, schema, version)
        sql.execute(
            "INSERT INTO research_schema VALUES (1,?) "
            "ON CONFLICT(singleton) DO UPDATE SET version=excluded.version",
            (SCHEMA_VERSION,),
        )
        if sql.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise IntegrityViolation("Existing Durable Object state violates foreign keys")

    def _run(self, callback):
        session = DurableObjectSession(self)
        try:
            return atomic(self.storage, lambda: callback(session))
        finally:
            session.active = False

    def read(self, callback):
        return self._run(callback)

    def write(self, callback):
        return self._run(callback)
