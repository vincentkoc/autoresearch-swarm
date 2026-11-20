"""SQLite connection policy and schema migrations for local swarm databases."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Literal

Schema = Literal["queue", "panels", "notifications", "reactive"]
BUSY_TIMEOUT_MS = 15_000
SCHEMA_VERSION = 1

QUEUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS swarm_projects (project_id TEXT PRIMARY KEY, snapshot TEXT);
CREATE TABLE IF NOT EXISTS swarm_campaigns (
    campaign_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES swarm_projects,
    spec TEXT NOT NULL,
    selected_trial_id TEXT REFERENCES swarm_trials
);
CREATE TABLE IF NOT EXISTS swarm_candidates (
    campaign_id TEXT NOT NULL REFERENCES swarm_campaigns,
    candidate_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (campaign_id, candidate_id)
);
CREATE TABLE IF NOT EXISTS swarm_trials (
    trial_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    seed INTEGER NOT NULL,
    fidelity REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (campaign_id, candidate_id) REFERENCES swarm_candidates
);
CREATE INDEX IF NOT EXISTS swarm_trial_queue ON swarm_trials(campaign_id, status);
CREATE TABLE IF NOT EXISTS swarm_attempts (
    attempt_id TEXT PRIMARY KEY,
    trial_id TEXT NOT NULL REFERENCES swarm_trials,
    number INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    started_at REAL NOT NULL,
    heartbeat_at REAL NOT NULL,
    reserve REAL NOT NULL,
    cost REAL NOT NULL DEFAULT 0,
    observation TEXT,
    UNIQUE (trial_id, number)
);
CREATE TABLE IF NOT EXISTS swarm_events (
    id INTEGER PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES swarm_campaigns,
    kind TEXT NOT NULL,
    at REAL NOT NULL,
    trial_id TEXT REFERENCES swarm_trials,
    attempt_id TEXT REFERENCES swarm_attempts,
    score REAL,
    replicates INTEGER,
    proposed_count INTEGER,
    accepted_count INTEGER
);
CREATE INDEX IF NOT EXISTS swarm_event_order ON swarm_events(campaign_id, kind, id);
CREATE UNIQUE INDEX IF NOT EXISTS swarm_workload_start ON swarm_events(attempt_id)
    WHERE kind='workload_started';
"""
_APPLICATION_ID = 0x52585331
_REACTIVE_SCHEMA = """
CREATE TABLE session (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
CREATE TABLE events (
    sequence INTEGER PRIMARY KEY, request_id TEXT, payload TEXT NOT NULL
);
CREATE INDEX events_request ON events(request_id,sequence);
CREATE TRIGGER events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'Reactive events are append-only'); END;
CREATE TRIGGER events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'Reactive events are append-only'); END;
"""


_PANEL_SCHEMA = """
CREATE TABLE IF NOT EXISTS panel_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS panel_looks (
    id INTEGER PRIMARY KEY, role TEXT NOT NULL, candidate TEXT NOT NULL, seed INTEGER NOT NULL,
    score REAL);
CREATE UNIQUE INDEX IF NOT EXISTS confirm_once ON panel_looks(candidate) WHERE role='confirmation';
"""

_NOTIFICATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications (
    campaign TEXT, created REAL, key TEXT, payload TEXT, PRIMARY KEY(campaign,created,key));
CREATE TABLE IF NOT EXISTS failure_cursor (
    campaign TEXT, created REAL, last_event INTEGER, consecutive INTEGER,
    PRIMARY KEY(campaign,created));
"""

_SCHEMAS = {
    "queue": QUEUE_SCHEMA,
    "panels": _PANEL_SCHEMA,
    "notifications": _NOTIFICATION_SCHEMA,
    "reactive": _REACTIVE_SCHEMA,
}


class LegacyDatabaseError(ValueError):
    """The selected file belongs to the retired controller."""


class JournalConflict(ValueError):
    """The submitted identity or transition does not match persisted evidence."""


def _validate(db: sqlite3.Connection, schema: Schema | None, existing: bool) -> int:
    version = db.execute("PRAGMA user_version").fetchone()[0]
    if (
        schema == "reactive"
        and existing
        and (
            db.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID
            or version != SCHEMA_VERSION
        )
    ):
        raise JournalConflict("Unsupported reactive journal")
    if (
        schema == "queue"
        and db.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table' AND name IN ('workspaces','records')"
        ).fetchone()
    ):
        raise LegacyDatabaseError(
            "Legacy control database. Use its original release to read it "
            "and a fresh database path for this campaign."
        )
    if version < 0 or version > SCHEMA_VERSION:
        raise ValueError(f"Unsupported {schema or 'swarm'} database schema version {version}")
    return int(version)


def _migrate_v1(db: sqlite3.Connection, schema: Schema) -> None:
    # executescript commits an existing transaction before running its first statement.
    # Execute complete statements so triggers, additive upgrades and the version commit together.
    statement = ""
    for line in _SCHEMAS[schema].splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            db.execute(statement)
            statement = ""
    if schema == "queue":
        columns = {
            "swarm_projects": {"snapshot": "TEXT"},
            "swarm_campaigns": {
                "ranking": "TEXT",
                "exhausted_after": "INTEGER",
                "panel_binding": "TEXT",
            },
            "swarm_events": {
                "score": "REAL",
                "replicates": "INTEGER",
                "proposed_count": "INTEGER",
                "accepted_count": "INTEGER",
                "gpu_time": "TEXT",
                "panel_evidence": "TEXT",
            },
        }
        for table, additions in columns.items():
            present = {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}
            for name, kind in additions.items():
                if name not in present:
                    db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {kind}")
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS swarm_gpu_sequence ON swarm_events("
            "campaign_id,json_extract(gpu_time,'$.allocation_id'),"
            "json_extract(gpu_time,'$.sequence')) "
            "WHERE gpu_time IS NOT NULL"
        )
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS swarm_panel_outcome ON swarm_events"
            "(campaign_id,kind,json_extract(panel_evidence,'$.key')) "
            "WHERE panel_evidence IS NOT NULL"
        )
    elif schema == "reactive":
        db.execute(f"PRAGMA application_id={_APPLICATION_ID}")


def _enable_wal(db: sqlite3.Connection) -> None:
    # SQLite can return BUSY without calling its busy handler when two first
    # openers both try to upgrade the journal mode. Retry only that upgrade.
    deadline = time.monotonic() + BUSY_TIMEOUT_MS / 1000
    while True:
        try:
            if db.execute("PRAGMA journal_mode").fetchone()[0] == "wal":
                return
            mode = db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if mode != "wal":
                raise sqlite3.OperationalError(f"Cannot enable WAL (journal_mode={mode})")
            return
        except sqlite3.OperationalError as exc:
            if (
                getattr(exc, "sqlite_errorcode", 0) & 0xFF != sqlite3.SQLITE_BUSY
                or time.monotonic() >= deadline
            ):
                raise
            time.sleep(0.01)


@contextmanager
def connection(
    path: Path,
    *,
    schema: Schema | None = None,
    write: bool = False,
    read_only: bool = False,
    existing: bool = False,
) -> Iterator[sqlite3.Connection]:
    """Open a scoped transaction. Readers of archived queues never create or migrate files."""
    mode = "ro" if read_only else "rw" if existing else "rwc"
    target = path if mode == "rwc" else path.resolve().as_uri() + "?mode=" + mode
    with (
        closing(sqlite3.connect(target, uri=mode != "rwc", timeout=BUSY_TIMEOUT_MS / 1000)) as db,
        db,
    ):
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        if not read_only:
            # Refuse incompatible files before changing journal mode.
            _validate(db, schema, existing)
            _enable_wal(db)
        db.execute("PRAGMA synchronous=NORMAL")
        migrate = schema is not None and not read_only and not (schema == "reactive" and existing)
        db.execute("BEGIN IMMEDIATE" if write or migrate else "BEGIN")
        if schema is not None and migrate:
            version = _validate(db, schema, existing)
            # Version zero covers fresh files and the original unversioned layouts.
            # Existing component tables may share a database with the requested layout.
            roots: dict[Schema, str] = {
                "queue": "swarm_campaigns",
                "panels": "panel_state",
                "notifications": "notifications",
                "reactive": "session",
            }
            tables = {
                row[0] for row in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")
            }
            if version == 0:
                for layout, root in roots.items():
                    if layout == schema or (layout != "reactive" and root in tables):
                        _migrate_v1(db, layout)
                db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            elif roots[schema] not in tables:
                _migrate_v1(db, schema)
        yield db
