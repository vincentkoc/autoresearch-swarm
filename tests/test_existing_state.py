"""Opening provisioned state never initializes or repairs it."""

import pytest

from research_swarm.control.auth import CredentialStore
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Database
from research_swarm.state.schema_contracts import require_research_schema


class ReadOnlySession:
    def __init__(self, delegate):
        self.delegate = delegate

    def execute(self, statement, parameters=()):
        assert statement.lstrip().startswith(("SELECT ", "PRAGMA "))
        assert not statement.lstrip().startswith("PRAGMA ") or "=" not in statement
        return self.delegate.execute(statement, parameters)

    def atomic(self, callback):
        pytest.fail("Existing-state admission attempted a nested write")


class ReadOnlyStore:
    def __init__(self, delegate):
        self.delegate, self.identity = delegate, delegate.identity

    def read(self, callback):
        return self.delegate.read(lambda sql: callback(ReadOnlySession(sql)))

    def write(self, callback):
        pytest.fail("Existing-state admission attempted a write")


def snapshot(database):
    return database.store.read(
        lambda sql: [
            tuple(row[key] for key in ("type", "name", "tbl_name", "sql"))
            for row in sql.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name")
        ]
    )


def test_current_research_schema_admits_without_writes(database):
    before = snapshot(database)
    ReadOnlyStore(database.store).read(require_research_schema)
    assert snapshot(database) == before


@pytest.mark.parametrize(
    "damage",
    [
        "DROP TABLE recovery_items",
        "ALTER TABLE recovery_snapshots DROP COLUMN expired_at",
        "DROP INDEX acceptance_leases",
        "ALTER TABLE commands RENAME COLUMN idempotency_key TO old_key",
    ],
)
def test_current_schema_with_missing_contract_refuses_without_repair(database, damage):
    database.store.write(lambda sql: sql.execute(damage))
    before = snapshot(database)
    with pytest.raises(ResearchError) as refused:
        ReadOnlyStore(database.store).read(require_research_schema)
    assert refused.value.code == ErrorCode.SCHEMA_UNSUPPORTED
    assert snapshot(database) == before


@pytest.mark.parametrize("kind", ["missing", "view", "column", "unique", "key", "foreign_key"])
def test_incompatible_credentials_refuse_without_creation(database, kind):
    if kind == "view":
        statement = "CREATE VIEW credentials AS SELECT 1 AS id"
    else:
        fields = [
            "id TEXT" + (" PRIMARY KEY" if kind != "key" else ""),
            "token_digest TEXT NOT NULL" + (" UNIQUE" if kind != "unique" else ""),
            "workspace_id TEXT NOT NULL"
            + (" REFERENCES workspaces(id)" if kind != "foreign_key" else ""),
            "principal TEXT NOT NULL",
            "created_at TEXT NOT NULL",
            "expires_at TEXT",
        ]
        if kind != "column":
            fields.append("revoked INTEGER NOT NULL DEFAULT 0")
        statement = "CREATE TABLE credentials (" + ",".join(fields) + ")"
    if kind != "missing":
        database.store.write(lambda sql: sql.execute(statement))
    before = snapshot(database)
    portable = Database.from_store(ReadOnlyStore(database.store))
    with pytest.raises(ResearchError) as refused:
        CredentialStore.open_existing(portable)
    assert refused.value.code == ErrorCode.SCHEMA_UNSUPPORTED
    assert snapshot(database) == before


def test_existing_credentials_preserve_rows_and_resolve_current_authority(database, principal):
    credentials = CredentialStore(database)
    active = credentials.issue(principal, principal)
    revoked = credentials.issue(principal, principal)
    expired_profile = credentials.issue(principal, principal, expires_at="2999-01-01T00:00:00.000Z")
    credentials.revoke(principal, revoked.id)
    before = snapshot(database)
    rows_before = database.store.read(
        lambda sql: [dict(row) for row in sql.execute("SELECT * FROM credentials ORDER BY id")]
    )
    opened = CredentialStore.open_existing(Database.from_store(ReadOnlyStore(database.store)))
    assert opened.resolve(active.token, allow_expiring=False) == principal
    for token in (revoked.token, expired_profile.token):
        with pytest.raises(ResearchError) as denied:
            opened.resolve(token, allow_expiring=False)
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert snapshot(database) == before
    assert (
        database.store.read(
            lambda sql: [dict(row) for row in sql.execute("SELECT * FROM credentials ORDER BY id")]
        )
        == rows_before
    )
