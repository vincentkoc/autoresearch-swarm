"""Exercise the shared SQLite boundary using independent writers and a disposable child."""

import multiprocessing
import os
import signal
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Barrier
from unittest.mock import patch

import pytest

from research_swarm.swarm.db import BUSY_TIMEOUT_MS, SCHEMA_VERSION, connection
from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec, trial


def test_eight_writers_initialize_and_finish_once(tmp_path: Path) -> None:
    queue = tmp_path / "queue.sqlite"
    start = Barrier(8)

    def worker(i: int) -> str:
        start.wait(timeout=10)
        store = SQLiteStore(queue)
        store.create_campaign(spec())
        assert store.submit(trial(i))
        attempt = store.claim("test", f"cpu-{i}")
        assert attempt is not None
        result = Result(attempt=attempt, status="ok", metrics={"loss": 1.0})
        assert store.report(result)
        assert not store.report(result)
        return attempt.trial.trial_id

    with ThreadPoolExecutor(max_workers=8) as pool:
        finished = list(pool.map(worker, range(8)))
    assert sorted(finished) == [trial(i).trial_id for i in range(8)]
    with connection(queue, read_only=True) as db:
        assert (
            db.execute("SELECT count(*) FROM swarm_events WHERE kind='finished'").fetchone()[0] == 8
        )
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def _hold_report(queue: Path, result: Result, pipe: Connection) -> None:
    store = SQLiteStore(queue)
    finish = store._finish

    def hold(*args: object) -> None:
        finish(*args)
        pipe.send("report written before commit")
        pipe.recv()

    with pipe, patch.object(store, "_finish", hold):
        store.report(result)


def test_kill_mid_report_restores_the_claim_and_accepts_one_result(tmp_path: Path) -> None:
    queue = tmp_path / "queue.sqlite"
    store = SQLiteStore(queue)
    store.create_campaign(spec())
    assert store.submit(trial(1))
    attempt = store.claim("test", "worker")
    assert attempt
    result = Result(attempt=attempt, status="ok", metrics={"loss": 1.0})
    ctx = multiprocessing.get_context("spawn")
    parent, child = ctx.Pipe()
    writer = ctx.Process(target=_hold_report, args=(queue, result, child))
    writer.start()
    child.close()
    with parent:
        try:
            assert parent.poll(15) and parent.recv() == "report written before commit"
            assert store.queue_depth("test") == (0, 1)
            assert store.results("test") == []
        finally:
            writer.kill()
            writer.join(10)
    assert writer.exitcode is not None and writer.exitcode != 0
    if os.name == "posix":
        assert writer.exitcode == -signal.SIGKILL
    reopened = SQLiteStore(queue)
    assert reopened.queue_depth("test") == (0, 1)
    assert reopened.report(result) and not reopened.report(result)
    assert reopened.results("test") == [result]
    with connection(queue, read_only=True) as db:
        assert (
            db.execute("SELECT count(*) FROM swarm_events WHERE kind='finished'").fetchone()[0] == 1
        )
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


@pytest.mark.parametrize("schema", ["queue", "panels", "notifications", "reactive"])
def test_connection_policy_and_schema_version(tmp_path: Path, schema: str) -> None:
    path = tmp_path / "database"
    with connection(path, schema=schema) as db:
        for pragma, value in (
            ("journal_mode", "wal"),
            ("synchronous", 1),
            ("foreign_keys", 1),
            ("busy_timeout", BUSY_TIMEOUT_MS),
            ("user_version", SCHEMA_VERSION),
        ):
            assert db.execute(f"PRAGMA {pragma}").fetchone()[0] == value
    with (
        connection(path, read_only=True) as db,
        pytest.raises(sqlite3.OperationalError, match="readonly"),
    ):
        db.execute("CREATE TABLE forbidden(id)")


def test_failed_schema_upgrade_rolls_back(tmp_path: Path) -> None:
    path = tmp_path / "older.sqlite"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE swarm_events(id INTEGER PRIMARY KEY)")
    with pytest.raises(sqlite3.OperationalError, match="no such column"):
        SQLiteStore(path)
    with connection(path, read_only=True) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 0
        assert [r[0] for r in db.execute("SELECT name FROM sqlite_schema")] == ["swarm_events"]


def test_future_version_is_refused_before_enabling_wal(tmp_path: Path) -> None:
    path = tmp_path / "future.sqlite"
    with sqlite3.connect(path) as db:
        db.execute(f"PRAGMA user_version={SCHEMA_VERSION + 1}")
    original = path.read_bytes()
    with pytest.raises(ValueError, match="Unsupported queue database schema version"):
        SQLiteStore(path)
    with connection(path, read_only=True) as db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert path.read_bytes() == original
