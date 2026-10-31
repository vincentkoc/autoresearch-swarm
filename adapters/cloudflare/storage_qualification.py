"""Local-only contract probes against real workerd storage, never a public API."""

from durable_sql import MAX_SAFE_INTEGER, DurableObjectStore

from research_swarm.checkpoints.capture import CaptureLimits, capture_rows, preflight_rows
from research_swarm.checkpoints.image_format import row_values
from research_swarm.checkpoints.image_schema import require_portable_image_schema
from research_swarm.control.auth import CredentialStore
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.checkpoint_images import LogicalChunk
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state import schema as migrations
from research_swarm.state.database import SCHEMA
from research_swarm.state.sql import IntegrityViolation


def open_store(storage, object_id):
    return DurableObjectStore(
        storage, namespace="local-storage-contract", object_id=object_id, schema=SCHEMA
    )


def expect(error_type, operation):
    try:
        operation()
    except error_type as error:
        return error
    raise AssertionError(f"Expected {error_type.__name__}")


def roundtrip(store):
    escaped = []

    def probe(sql):
        escaped.append(sql)
        row = sql.execute(
            "SELECT ? AS empty, ? AS binary, ? AS absent", (b"", b"\x00\xff\x80", None)
        ).one()
        assert row[0] == row["empty"] == b"" and row["binary"] == b"\x00\xff\x80"
        assert row["absent"] is None
        duplicate = sql.execute("SELECT 1 AS same, 2 AS same").one()
        assert duplicate.keys() == ["same", "same"] and duplicate["same"] == 1
        assert duplicate[1] == 2
        cursor = sql.execute("SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3")
        assert cursor.fetchone()[0] == 1 and next(iter(cursor))[0] == 2
        assert [row[0] for row in cursor.fetchall()] == [3] and cursor.fetchone() is None
        expect(RuntimeError, lambda: sql.execute("SELECT 1 WHERE 0").one())
        expect(RuntimeError, lambda: sql.execute("SELECT 1 UNION ALL SELECT 2").one())
        for value in (MAX_SAFE_INTEGER + 1, -(MAX_SAFE_INTEGER + 1), float("inf"), float("nan")):
            expect(ValueError, lambda value=value: sql.execute("SELECT ?", (value,)))
        expect(TypeError, lambda: sql.execute("SELECT ?", (object(),)))
        expect(ValueError, lambda: sql.execute("SELECT 9007199254740992"))
        assert sql.execute("SELECT ?", (MAX_SAFE_INTEGER,)).one()[0] == MAX_SAFE_INTEGER
        for statement in (
            "INSERT INTO research_schema VALUES (2,3)",
            "INSERT INTO research_schema VALUES (1,3)",
            "UPDATE research_schema SET version=NULL",
            "INSERT INTO events VALUES ('missing',1,'id','{}')",
        ):
            expect(IntegrityViolation, lambda statement=statement: sql.execute(statement))
        try:
            sql.execute("SELECT absent FROM nonexistent_table")
        except IntegrityViolation as error:
            raise AssertionError("SQL programming error became a constraint failure") from error
        except Exception:
            pass
        else:
            raise AssertionError("Invalid SQL succeeded")

    store.write(probe)
    expect(RuntimeError, lambda: escaped[0].execute("SELECT 1"))
    expect(RuntimeError, lambda: escaped[0].atomic(lambda sql: None))

    async def pending():
        return None

    def suspend(sql):
        sql.execute("UPDATE research_schema SET version=99")
        return pending()

    expect(TypeError, lambda: store.write(suspend))
    assert store.read(lambda sql: sql.execute("SELECT version FROM research_schema").one()[0]) == 3
    store.write(CredentialStore.initialize_in)
    store.read(require_portable_image_schema)
    checkpoint_capture(store)
    return {
        "binary_null": True,
        "rows": True,
        "numeric_bounds": True,
        "constraints": True,
        "synchronous_scope": True,
        "checkpoint_schema": True,
        "checkpoint_capture": True,
    }


def checkpoint_capture(store):
    workspace, snapshot, actor, backup = (new_id() for _ in range(4))
    raw = ' {"x":1,"x":2,"text":"e\u0301"}\n'

    def seed(sql):
        sql.execute("INSERT INTO workspaces(id,controller_id) VALUES (?,?)", (workspace, new_id()))
        for key in ("\U00010000", "\ue000", "\0"):
            sql.execute(
                "INSERT INTO commands VALUES (?,?,?,?,?,?,?)",
                (workspace, actor, key, content_digest(b"request"), new_id(), raw, "created"),
            )
        sql.execute(
            "INSERT INTO recovery_snapshots VALUES (?,?,?,?,?,?,?,?)",
            (workspace, snapshot, actor, 0, "expires", None, None, "{}"),
        )
        sql.execute(
            "INSERT INTO recovery_items VALUES (?,?,?,?,?,?)",
            (workspace, snapshot, 0, content_digest(b"\0\xff"), 2, b"\0\xff"),
        )

    store.write(seed)

    def read(sql, page_rows):
        limits = CaptureLimits(page_rows=page_rows)
        return capture_rows(
            sql, backup_id=backup, inventory=preflight_rows(sql, limits), limits=limits
        )

    first = store.read(lambda sql: read(sql, 1))
    assert first == store.read(lambda sql: read(sql, 8))
    rows = {"commands": [], "recovery_items": []}
    for body in first.bodies:
        chunk = LogicalChunk.model_validate_json(body)
        if chunk.table in rows:
            rows[chunk.table].extend(
                row_values(chunk.table, row) for row in chunk.rows if row[0] == workspace
            )
    assert [row[2] for row in rows["commands"]] == ["\0", "\ue000", "\U00010000"]
    assert all(row[5] == raw for row in rows["commands"])
    assert rows["recovery_items"][0][-1] == b"\0\xff"


class ReadOnlyStorage:
    """Make attempted initialization visible even when it would roll back."""

    def __init__(self, storage):
        self.delegate = storage
        self.sql = self

    def exec(self, statement, *parameters):
        assert statement.lstrip().startswith(("SELECT ", "PRAGMA "))
        assert not statement.lstrip().startswith("PRAGMA ") or "=" not in statement
        return self.delegate.sql.exec(statement, *parameters)

    def transactionSync(self, callback):
        return self.delegate.transactionSync(callback)


def existing_state(storage, object_id, case):
    def reopen():
        return DurableObjectStore.open_existing(
            ReadOnlyStorage(storage), namespace="local-storage-contract", object_id=object_id
        )

    if case != "existing-empty":
        store = open_store(storage, object_id)
        store.write(
            lambda sql: sql.execute(
                "INSERT INTO workspaces(id,controller_id) VALUES ('workspace','controller')"
            )
        )
        store.write(
            lambda sql: sql.execute(
                "INSERT INTO commands VALUES "
                "('workspace','actor','key','digest','command',?, 'time')",
                ('{"result":{"historical":true}}',),
            )
        )
        mutations = {
            "existing-current": None,
            "existing-old": "UPDATE research_schema SET version=1",
            "existing-column": "ALTER TABLE recovery_snapshots DROP COLUMN expired_at",
            "existing-index": "DROP INDEX acceptance_leases",
            "existing-marker": "DELETE FROM research_schema",
        }
        statement = mutations[case]
        if statement:
            store.write(lambda sql: sql.execute(statement))

    def metadata():
        return [
            tuple(row)
            for row in storage.sql.exec(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name"
            ).raw()
        ]

    before = metadata()
    if case == "existing-current":
        opened = reopen()
        assert opened.identity == store.identity
    else:
        error = expect(ResearchError, reopen)
        assert error.code == ErrorCode.SCHEMA_UNSUPPORTED
    assert metadata() == before
    if case != "existing-empty":
        assert store.read(lambda sql: sql.execute("SELECT response FROM commands").one()[0]) == (
            '{"result":{"historical":true}}'
        )
    if case == "existing-old":
        assert (
            store.read(lambda sql: sql.execute("SELECT version FROM research_schema").one()[0]) == 1
        )
        open_store(storage, object_id)  # Explicit provisioning still owns migration.
        reopen()
    return {"read_only": True, "preserved": True, "contract": True}


def exercise(storage, object_id, case):
    if case.startswith("existing-"):
        return existing_state(storage, object_id, case)
    if case == "foreign-state":
        storage.sql.exec("CREATE TABLE sqlitexlegacy (value TEXT)")
        error = expect(ResearchError, lambda: open_store(storage, object_id))
        assert error.code == ErrorCode.SCHEMA_UNSUPPORTED
        assert not list(
            storage.sql.exec("SELECT name FROM sqlite_master WHERE name='research_schema'")
        )
        assert list(storage.sql.exec("SELECT name FROM sqlite_master WHERE name='sqlitexlegacy'"))
        return {"refused": True, "preserved": True}
    if case == "migration-rollback":
        # A pre-upgrade authority with one acknowledged command, not an empty database.
        for statement in SCHEMA.split(";"):
            if statement.strip():
                storage.sql.exec(statement)
        storage.sql.exec(
            "CREATE TABLE research_schema (singleton INTEGER PRIMARY KEY, version INTEGER)"
        )
        storage.sql.exec("INSERT INTO research_schema VALUES (1,1)")
        storage.sql.exec(
            "INSERT INTO workspaces(id,controller_id) VALUES ('workspace','controller')"
        )
        storage.sql.exec(
            "INSERT INTO commands VALUES ('workspace','actor','key','digest','command',?, 'time')",
            '{"result":{"historical":true}}',
        )
        original = migrations.migrate_v2

        def interrupt(sql):
            original(sql)
            raise RuntimeError("synthetic interrupted schema migration")

        migrations.migrate_v2 = interrupt
        try:
            expect(RuntimeError, lambda: open_store(storage, object_id))
        finally:
            migrations.migrate_v2 = original
        assert list(storage.sql.exec("SELECT version FROM research_schema"))[0].version == 1
        assert "event_floor" not in {
            row.name for row in storage.sql.exec("PRAGMA table_info(workspaces)")
        }
        assert not list(
            storage.sql.exec("SELECT name FROM sqlite_master WHERE name='recovery_items'")
        )
        store = open_store(storage, object_id)
        preserved = store.read(lambda sql: sql.execute("SELECT response FROM commands").one()[0])
        assert preserved == '{"result":{"historical":true}}'
        return {"rolled_back": True, "upgraded": True, "acknowledgment_preserved": True}
    store = open_store(storage, object_id)
    if case == "roundtrip":
        return roundtrip(store)
    if case == "unknown-version":
        store.write(lambda sql: sql.execute("UPDATE research_schema SET version=999"))
        error = expect(ResearchError, lambda: open_store(storage, object_id))
        assert error.code == ErrorCode.SCHEMA_UNSUPPORTED
        assert (
            store.read(lambda sql: sql.execute("SELECT version FROM research_schema").one()[0])
            == 999
        )
        return {"refused": True, "preserved": True}
    raise ValueError("Unknown local storage contract")
