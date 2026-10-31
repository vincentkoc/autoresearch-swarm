"""The command kernel uses callback storage without a native connection escape hatch."""

import builtins
import sqlite3
from contextlib import closing

import pytest

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database
from research_swarm.state.sql import MAX_SQL_COUNTER, IntegrityViolation
from tests.test_database import create_campaign


class CallbacksOnly:
    def __init__(self, store):
        self.store = store
        self.reads = self.writes = 0

    @property
    def identity(self):
        return self.store.identity

    def read(self, callback):
        self.reads += 1
        return self.store.read(callback)

    def write(self, callback):
        self.writes += 1
        return self.store.write(callback)


@pytest.mark.parametrize("fail", [False, True])
def test_backup_closes_retained_connections_before_return(
    database, context, tmp_path, monkeypatch, fail
):
    create_campaign(database, context("before-backup"))
    connect = sqlite3.connect
    retained = []

    class Connection(sqlite3.Connection):
        def backup(self, target, **kwargs):
            if fail:
                raise RuntimeError("Injected backup failure")
            return super().backup(target, **kwargs)

    def keep_connection(*args, **kwargs):
        connection = connect(*args, factory=Connection, **kwargs)
        retained.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", keep_connection)
    target = tmp_path / "snapshot.sqlite"
    if fail:
        with pytest.raises(RuntimeError, match="Injected backup failure"):
            database.backup(target)
    else:
        database.backup(target)
        with closing(connect(target)) as copy:
            assert (
                copy.execute("SELECT COUNT(*) FROM records WHERE kind='campaign'").fetchone()[0]
                == 1
            )
    assert len(retained) == 2
    for connection in retained:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")
    # Windows rejects either operation while the destination handle remains open.
    moved = target.with_suffix(".closed")
    target.rename(moved)
    moved.unlink()


@pytest.mark.parametrize("counter", ["revision", "next_sequence", "delivery_attempts"])
def test_sql_counters_fail_before_inexact_increment(database, context, counter):
    created = create_campaign(database, context("initial"))["result"]
    if counter == "revision":
        database.store.write(
            lambda sql: sql.execute("UPDATE records SET revision=?", (MAX_SQL_COUNTER,))
        )

        def operation():
            return database.execute(
                context("overflow"),
                "update",
                {},
                "write",
                lambda tx: tx.update(
                    "campaign",
                    created["id"],
                    expected_revision=MAX_SQL_COUNTER,
                    data={},
                    state="changed",
                ),
            )

        query = "SELECT revision FROM records"
    elif counter == "next_sequence":
        database.store.write(
            lambda sql: sql.execute("UPDATE workspaces SET next_sequence=?", (MAX_SQL_COUNTER,))
        )

        def operation():
            return create_campaign(database, context("overflow"))

        query = "SELECT next_sequence FROM workspaces"
    else:
        database.store.write(
            lambda sql: sql.execute("UPDATE outbox SET delivery_attempts=?", (MAX_SQL_COUNTER,))
        )
        event = database.pending_notifications()[0]

        def operation():
            return database.acknowledge_notification(event.id)

        query = "SELECT delivery_attempts FROM outbox"
    with pytest.raises(ResearchError) as exhausted:
        operation()
    assert exhausted.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert database.store.read(lambda sql: sql.execute(query).one()[0]) == MAX_SQL_COUNTER
    assert len(database.pending_notifications()) == 1
    assert len(database.snapshot(context())["records"]) == 1


def test_native_capability_refusal_does_not_import_native_sqlite(database, tmp_path, monkeypatch):
    kernel = Database.from_store(CallbacksOnly(database.store))
    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name == "sqlite3" or name == "research_swarm.state.sqlite":
            raise AssertionError("Native SQLite was imported before refusing the capability")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    for operation in (lambda: kernel.path, lambda: kernel.backup(tmp_path / "backup")):
        with pytest.raises(ResearchError) as rejected:
            operation()
        assert rejected.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    with pytest.raises(ResearchError) as rejected, kernel.connect():
        pytest.fail("Native connection exposed")
    assert rejected.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


def test_kernel_replay_snapshots_events_and_cas_use_only_store_callbacks(database, context):
    store = CallbacksOnly(database.store)
    kernel = Database.from_store(store)
    workspace = context().principal.workspace_id
    controller = database.store.read(
        lambda sql: sql.execute(
            "SELECT controller_id FROM workspaces WHERE id=?", (workspace,)
        ).one()[0]
    )
    kernel.bootstrap(workspace, controller)
    initial = create_campaign(kernel, context("portable-create"))
    assert create_campaign(kernel, context("portable-create")) == initial
    assert (
        kernel.replay(context("portable-create"), "create", {"objective": "synthetic"}, "write")
        == initial
    )
    identifier = initial["result"]["id"]
    result = kernel.execute(
        context("portable-update"),
        "update",
        {},
        "write",
        lambda transaction: transaction.update(
            "campaign",
            identifier,
            expected_revision=1,
            state="running",
            data={"objective": "updated"},
        ),
    )
    assert result["result"]["revision"] == 2
    with pytest.raises(ResearchError) as stale:
        kernel.execute(
            context("portable-stale"),
            "update",
            {},
            "write",
            lambda transaction: transaction.update(
                "campaign",
                identifier,
                expected_revision=1,
                state="running",
                data={"objective": "lost"},
            ),
        )
    assert stale.value.code == ErrorCode.REVISION_CONFLICT
    assert kernel.read_record(context(), "campaign", identifier)["data"]["objective"] == "updated"
    assert kernel.snapshot(context())["records"] == database.snapshot(context())["records"]
    events = kernel.events(context())
    assert len(events) == 1 and kernel.event_page(context())["watermark"] == 1
    assert kernel.pending_notifications() == events
    kernel.acknowledge_notification(events[0].id)
    assert kernel.pending_notifications() == []
    assert store.reads > 0 and store.writes > 0
    assert kernel.authority_identity == database.authority_identity
    with pytest.raises(ResearchError) as native, kernel.connect():
        pytest.fail("A callback-only store exposed native connection access")
    assert native.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


def test_failed_effects_rollback_before_fence_and_rejection_ack_commit(database, context):
    kernel = Database.from_store(CallbacksOnly(database.store))
    failed_id, fence_id = new_id(), new_id()
    failure = ResearchError(ErrorCode.ACCEPTANCE_EXPIRED, "Synthetic expired acceptance")
    seen = []

    def operation(transaction):
        row = transaction.insert("fixture", failed_id, {}, state="pending")
        transaction.event("fixture.wrong", row, {})
        raise failure

    def fence(transaction, error):
        seen.append(error)
        assert transaction.connection.execute("SELECT COUNT(*) FROM events").one()[0] == 0
        row = transaction.insert("fixture", fence_id, {}, state="fenced")
        transaction.event("fixture.fenced", row, {})

    command = context("rejected-with-fence")
    for _ in range(2):
        with pytest.raises(ResearchError) as rejected:
            kernel.execute(command, "fixture", {}, "write", operation, on_rejection=fence)
        assert rejected.value.code == ErrorCode.ACCEPTANCE_EXPIRED
    assert seen == [failure]
    assert kernel.read_record(context(), "fixture", fence_id)["state"] == "fenced"
    assert [event.type for event in kernel.events(context())] == ["fixture.fenced"]
    with pytest.raises(ResearchError) as absent:
        kernel.read_record(context(), "fixture", failed_id)
    assert absent.value.code == ErrorCode.NOT_FOUND


def test_hook_failure_rolls_back_fence_event_and_acknowledgment(database, context):
    kernel = Database.from_store(CallbacksOnly(database.store))
    identifier = new_id()

    def operation(transaction):
        raise ResearchError(ErrorCode.INVALID_TRANSITION, "Synthetic rejection")

    def hook(transaction, error):
        row = transaction.insert("fixture", identifier, {}, state="fenced")
        transaction.event("fixture.fenced", row, {})
        raise ValueError("Synthetic hook failure")

    command = context("hook-failed")
    with pytest.raises(ValueError, match="hook failure"):
        kernel.execute(command, "fixture", {}, "write", operation, on_rejection=hook)
    assert kernel.replay(command, "fixture", {}, "write") is None
    assert kernel.snapshot(context())["records"] == []
    assert kernel.events(context()) == []
    assert kernel.execute(command, "fixture", {}, "write", lambda transaction: {"recovered": True})[
        "result"
    ]["recovered"]


def test_sqlite_port_preserves_rows_blobs_and_integrity_failures(database):
    def check(sql):
        sql.execute("CREATE TABLE fixture_blob (id INTEGER PRIMARY KEY, content BLOB)")
        sql.execute("INSERT INTO fixture_blob VALUES (?,?)", (1, b"\x00\xff"))
        row = sql.execute("SELECT id,content FROM fixture_blob").one()
        assert row[0] == row["id"] == 1 and dict(row) == {"id": 1, "content": b"\x00\xff"}
        with pytest.raises(IntegrityViolation):
            sql.atomic(
                lambda inner: inner.execute("INSERT INTO fixture_blob VALUES (?,?)", (1, b"x"))
            )
        assert sql.execute("SELECT content FROM fixture_blob").one()[0] == b"\x00\xff"

    database.store.write(check)
    with pytest.raises(RuntimeError, match="exactly one"):
        database.store.read(lambda sql: sql.execute("SELECT id FROM fixture_blob WHERE id=2").one())

    async def unexpected(sql):
        return 1

    with pytest.raises(TypeError, match="synchronously"):
        database.store.write(unexpected)
