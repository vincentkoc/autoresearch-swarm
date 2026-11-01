"""Bootstrap store selection against real SQLite with a minimal synchronous JS bridge."""

import importlib.util
import sqlite3
import sys
from types import SimpleNamespace

import pytest

from adapters.cloudflare.runtime import atomic
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import SCHEMA
from research_swarm.state.schema import SCHEMA_VERSION
from tests.paths import REPO_ROOT


@pytest.fixture
def store_class(monkeypatch):
    class ForeignException(Exception):
        pass

    monkeypatch.setitem(sys.modules, "atomic", atomic)
    monkeypatch.setitem(
        sys.modules,
        "pyodide.ffi",
        SimpleNamespace(
            JsException=ForeignException,
            jsnull=object(),
            to_js=lambda value, **kwargs: value,
        ),
    )
    spec = importlib.util.spec_from_file_location(
        "durable_sql_test", REPO_ROOT / "adapters/cloudflare/runtime/durable_sql.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.DurableObjectStore


class Storage:
    def __init__(self):
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.sql = self

    def exec(self, statement, *parameters):
        cursor = self.connection.execute(statement, parameters)
        columns = [column[0] for column in cursor.description or ()]
        rows = cursor.fetchall()
        return SimpleNamespace(columnNames=columns, raw=lambda: iter(rows))

    def transactionSync(self, callback):
        self.connection.execute("SAVEPOINT bridge")
        try:
            result = callback()
        except BaseException:
            self.connection.execute("ROLLBACK TO bridge")
            raise
        finally:
            self.connection.execute("RELEASE bridge")
        return result

    def snapshot(self):
        return tuple(self.connection.iterdump())


@pytest.mark.parametrize(
    "existing",
    [
        "CREATE VIEW unrelated_view AS SELECT 1 AS retained",
        "CREATE TABLE unrelated_table(value TEXT)",
        "CREATE TABLE research_schema(singleton INTEGER PRIMARY KEY,version INTEGER);"
        "INSERT INTO research_schema VALUES(1,0)",
    ],
)
def test_private_bootstrap_refuses_foreign_or_old_state_without_schema_mutation(
    store_class, existing
):
    storage = Storage()
    storage.connection.executescript(existing)
    before = storage.snapshot()
    try:
        with pytest.raises(ResearchError) as refused:
            store_class.open_for_provisioning(
                storage, namespace="synthetic", object_id="a" * 64, schema=SCHEMA
            )
        assert refused.value.code == ErrorCode.SCHEMA_UNSUPPORTED
        assert storage.snapshot() == before
    finally:
        storage.connection.close()


def test_empty_bootstrap_initializes_once_and_existing_open_is_read_only(store_class):
    storage = Storage()
    options = {"namespace": "synthetic", "object_id": "a" * 64}
    try:
        with pytest.raises(ResearchError):
            store_class.open_existing(storage, **options)
        assert storage.snapshot() == ("BEGIN TRANSACTION;", "COMMIT;")
        created = store_class.open_for_provisioning(storage, **options, schema=SCHEMA)
        assert (
            created.read(lambda sql: sql.execute("SELECT version FROM research_schema").one()[0])
            == SCHEMA_VERSION
        )
        before = storage.snapshot()
        reopened = store_class.open_for_provisioning(
            storage, **options, schema="invalid SQL is never applied"
        )
        assert reopened.identity == created.identity
        assert storage.snapshot() == before
    finally:
        storage.connection.close()
