"""Logical export admits the complete fixed schema without copying adapter authority."""

import pytest

from adapters.cloudflare.checkpoint_schema import require_checkpoint_schema
from research_swarm.checkpoints.image_schema import require_portable_image_schema
from research_swarm.control.auth import CredentialStore
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import SCHEMA
from research_swarm.state.sqlite import SQLiteStore
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.adapters.cloudflare.test_cloudflare_provisioning import run


@pytest.fixture
async def source(bootstrap):
    await run(bootstrap)

    def metadata(sql):
        sql.execute(
            "CREATE TABLE research_schema (singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "version INTEGER NOT NULL)"
        )
        sql.execute("INSERT INTO research_schema VALUES (1,3)")

    bootstrap.store.write(metadata)
    return bootstrap


def admit(source):
    return source.store.read(lambda sql: require_checkpoint_schema(sql, source.manifest))


def snapshot(source):
    return source.store.read(
        lambda sql: tuple(
            tuple(tuple(dict(row).values()) for row in sql.execute(statement))
            for statement in (
                "SELECT * FROM sqlite_master ORDER BY name",
                "SELECT * FROM records ORDER BY workspace_id,kind,id",
                "SELECT * FROM commands ORDER BY workspace_id,actor_id,idempotency_key",
                "SELECT * FROM credentials ORDER BY id",
            )
        )
    )


async def test_complete_hosted_schema_is_read_only_and_retains_revocation(source):
    identifier = source.manifest.credentials[-1].id
    source.store.write(
        lambda sql: sql.execute("UPDATE credentials SET revoked=1 WHERE id=?", (identifier,))
    )
    before = snapshot(source)
    assert admit(source) is None
    assert snapshot(source) == before


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE forgotten (value TEXT)",
        "CREATE TABLE sqlitexlegacy (value TEXT)",
        "CREATE VIEW forgotten AS SELECT * FROM records",
        "CREATE TRIGGER side_effect AFTER INSERT ON records BEGIN SELECT 1; END",
        "CREATE TRIGGER _cf_side_effect AFTER INSERT ON records BEGIN SELECT 1; END",
        "ALTER TABLE records ADD COLUMN forgotten TEXT",
        "ALTER TABLE records ADD COLUMN computed TEXT GENERATED ALWAYS AS (id) VIRTUAL",
        "ALTER TABLE hosted_provisioning ADD COLUMN forgotten TEXT",
        "DELETE FROM research_schema",
        "UPDATE research_schema SET version=4",
        "UPDATE hosted_provisioning SET state='prepared',receipt=NULL",
        "UPDATE hosted_provisioning SET digest='changed'",
        "UPDATE hosted_authority_config SET payload='{}'",
        "CREATE UNIQUE INDEX extra_unique ON records(payload)",
        "CREATE UNIQUE INDEX partial_unique ON records(id) WHERE kind='unused'",
        "CREATE UNIQUE INDEX expression_unique ON records(lower(id)) WHERE kind='unused'",
        "CREATE UNIQUE INDEX wrong_collation ON records(workspace_id,kind,id COLLATE NOCASE)",
        "CREATE UNIQUE INDEX wrong_direction ON records(workspace_id,kind,id DESC)",
    ],
)
async def test_unsupported_or_unbound_state_refuses_without_repair(source, statement):
    source.store.write(lambda sql: sql.execute(statement))
    before = snapshot(source)
    with pytest.raises(ResearchError):
        admit(source)
    assert snapshot(source) == before


async def test_reserved_provider_state_is_excluded_without_reading_it(source):
    names = ("_cf_METADATA", "__cf_kv", "_CF_fixture", "__CF_fixture")
    for name in names:
        source.store.write(
            lambda sql, name=name: sql.execute(f'CREATE TABLE "{name}" (private BLOB)')
        )

    class Guard:
        def __init__(self, sql):
            self.sql = sql

        def execute(self, statement, parameters=()):
            assert not any(name in statement or name in parameters for name in names)
            return self.sql.execute(statement, parameters)

    source.store.read(lambda sql: require_checkpoint_schema(Guard(sql), source.manifest))


async def test_duplicate_constraint_and_nonunique_acceleration_add_no_exported_state(source):
    source.store.write(
        lambda sql: sql.execute("CREATE UNIQUE INDEX same_key ON records(workspace_id,kind,id)")
    )
    source.store.write(lambda sql: sql.execute("CREATE INDEX faster_scan ON records(state)"))
    admit(source)


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ("revision INTEGER NOT NULL", "revision TEXT NOT NULL"),
        ("revision INTEGER NOT NULL", "revision INTEGER"),
        ("campaign_id TEXT,", "campaign_id TEXT NOT NULL,"),
        ("next_sequence INTEGER NOT NULL DEFAULT 1", "next_sequence INTEGER NOT NULL DEFAULT 2"),
        ("id TEXT PRIMARY KEY,", "id TEXT PRIMARY KEY COLLATE NOCASE,"),
        ("id TEXT PRIMARY KEY,", "id TEXT PRIMARY KEY DESC,"),
        ("REFERENCES workspaces(id)", "REFERENCES workspaces(id) ON DELETE CASCADE"),
        ("id TEXT NOT NULL UNIQUE,", "id TEXT NOT NULL,"),
        ("UNIQUE (workspace_id, owner_id)", "UNIQUE (workspace_id, id)"),
    ],
)
def test_portable_schema_refuses_type_nullability_default_key_and_foreign_key_drift(
    tmp_path, before, after
):
    assert before in SCHEMA
    store = SQLiteStore(tmp_path / "drift.sqlite", SCHEMA.replace(before, after))
    store.write(CredentialStore.initialize_in)
    with pytest.raises(ResearchError) as refused:
        store.read(require_portable_image_schema)
    assert refused.value.code == ErrorCode.SCHEMA_UNSUPPORTED


async def test_native_profile_without_hosted_version_metadata_is_not_hosted_export(bootstrap):
    await run(bootstrap)
    with pytest.raises(ResearchError) as refused:
        admit(bootstrap)
    assert refused.value.code == ErrorCode.SCHEMA_UNSUPPORTED


@pytest.mark.parametrize("overflow", ["columns", "indexes"])
async def test_metadata_overflow_refuses_before_unbounded_materialization(source, overflow):
    for ordinal in range(65):
        statement = (
            f"ALTER TABLE records ADD COLUMN extra_{ordinal} TEXT"
            if overflow == "columns"
            else f"CREATE INDEX extra_{ordinal} ON records(id)"
        )
        source.store.write(lambda sql, statement=statement: sql.execute(statement))

    class Bounded:
        def __init__(self, sql):
            self.sql = sql

        def execute(self, statement, parameters=()):
            assert not statement.startswith("PRAGMA ") or statement == "PRAGMA foreign_keys"
            if "pragma_" in statement:
                assert "LIMIT " in statement
            return self.sql.execute(statement, parameters)

    with pytest.raises(ResearchError) as refused:
        source.store.read(lambda sql: require_portable_image_schema(Bounded(sql)))
    assert refused.value.code == ErrorCode.SCHEMA_UNSUPPORTED
