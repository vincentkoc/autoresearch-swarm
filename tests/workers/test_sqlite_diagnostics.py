"""SQLite observation failures keep evidence without leaking queries or handles."""

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from research_swarm.workers.journal import WorkerJournal
from research_swarm.workers.supervision import run_native
from tests.workers.test_supervision import command, require_closed


def test_real_sqlite_cancellation_read_failure_closes_scope_without_query_text(tmp_path):
    released = False
    reads = 0

    def on_released(_):
        nonlocal released
        released = True

    with closing(sqlite3.connect(":memory:")) as connection:

        def cancelled():
            nonlocal reads
            reads += 1
            if released:
                connection.execute("SELECT private_fixture_query_text")
            return False

        outcome = run_native(
            command(tmp_path, "import time; time.sleep(60)"),
            tmp_path / "output",
            on_released=on_released,
            cancelled=cancelled,
        )

    assert reads == 2
    assert outcome.reason == "supervisor_error"
    assert outcome.released_at is not None and outcome.closure.scope_closed
    assert (
        "SQLite failure phase=cancellation_read sqlite_errorcode=1 sqlite_errorname=SQLITE_ERROR"
        in outcome.closure.findings
    )
    assert "private_fixture_query_text" not in outcome.model_dump_json()
    require_closed(outcome.closure.identity.pid)


@pytest.mark.parametrize(
    ("code", "name", "expected"),
    [
        (sqlite3.SQLITE_BUSY, "SQLITE_BUSY", " sqlite_errorcode=5 sqlite_errorname=SQLITE_BUSY"),
        (sqlite3.SQLITE_BUSY, "SQLITE_BUSY\nprivate_fixture_value", " sqlite_errorcode=5"),
        (sqlite3.SQLITE_BUSY, "SQLITE_IOERR", " sqlite_errorcode=5"),
        (True, "SQLITE_ERROR", ""),
        (-1, "SQLITE_ERROR", ""),
        (2**40, "SQLITE_ERROR", ""),
    ],
)
def test_sqlite_metadata_is_bounded_and_names_match_known_constants(tmp_path, code, name, expected):
    error = sqlite3.OperationalError("private_fixture_value")
    error.sqlite_errorcode = code
    error.sqlite_errorname = name
    released = False
    reads = 0

    def on_released(_):
        nonlocal released
        released = True

    def cancelled():
        nonlocal reads
        reads += 1
        if released:
            raise error
        return False

    outcome = run_native(
        command(tmp_path, "import time; time.sleep(60)"),
        tmp_path / "output",
        on_released=on_released,
        cancelled=cancelled,
    )
    assert reads == 2
    assert outcome.reason == "supervisor_error" and outcome.closure.scope_closed
    assert "SQLite failure phase=cancellation_read" + expected in outcome.closure.findings
    assert "private_fixture_value" not in outcome.model_dump_json()
    require_closed(outcome.closure.identity.pid)


@pytest.mark.parametrize("statement", ["PRAGMA foreign_keys=ON", "PRAGMA synchronous=FULL"])
def test_failed_journal_connection_setup_closes_the_real_connection(
    tmp_path: Path, monkeypatch, statement
):
    original_connect = sqlite3.connect
    opened = []

    class SetupFailure(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if sql == statement:
                raise sqlite3.OperationalError("Synthetic setup failure")
            return super().execute(sql, parameters)

    def connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs, factory=SetupFailure)
        opened.append(connection)
        return connection

    monkeypatch.setattr("research_swarm.workers.journal.sqlite3.connect", connect)
    with pytest.raises(sqlite3.OperationalError, match="Synthetic setup failure"):
        WorkerJournal(tmp_path / "worker.sqlite")
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0].execute("SELECT 1")
