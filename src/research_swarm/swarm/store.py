"""Transactional SQLite queue for swarm campaigns. Each operation owns its connection."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import asdict, replace
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Result, Trial

_SCHEMA = """
CREATE TABLE IF NOT EXISTS swarm_projects (project_id TEXT PRIMARY KEY);
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
    attempt_id TEXT REFERENCES swarm_attempts
);
CREATE INDEX IF NOT EXISTS swarm_event_order ON swarm_events(campaign_id, kind, id);
"""
_TRIAL = """SELECT t.*, c.payload FROM swarm_trials t JOIN swarm_candidates c
            USING (campaign_id, candidate_id)"""
_ATTEMPT = """SELECT t.*, c.payload, a.* FROM swarm_attempts a JOIN swarm_trials t
              USING (trial_id) JOIN swarm_candidates c USING (campaign_id, candidate_id)"""


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":"))


def _cost(value: float) -> float:
    if not math.isfinite(value) or value < 0:
        raise ValueError("Cost must be finite and non-negative")
    return value


def _trial(row: sqlite3.Row) -> Trial:
    data = json.loads(row["payload"])
    data["parents"] = tuple(data["parents"])
    return Trial(
        trial_id=row["trial_id"],
        campaign_id=row["campaign_id"],
        candidate=Candidate(**data),
        seed=row["seed"],
        fidelity=row["fidelity"],
    )


def _attempt(row: sqlite3.Row) -> Attempt:
    return Attempt(
        attempt_id=row["attempt_id"],
        trial=_trial(row),
        worker_id=row["worker_id"],
        number=row["number"],
        started_at=row["started_at"],
    )


def _event(
    db: sqlite3.Connection,
    campaign_id: str,
    kind: str,
    trial_id: str | None = None,
    attempt_id: str | None = None,
) -> None:
    db.execute(
        "INSERT INTO swarm_events(campaign_id, kind, at, trial_id, attempt_id) "
        "VALUES(?, ?, ?, ?, ?)",
        (campaign_id, kind, time.time(), trial_id, attempt_id),
    )


def _spec(db: sqlite3.Connection, campaign_id: str) -> dict[str, Any]:
    row = db.execute(
        "SELECT spec FROM swarm_campaigns WHERE campaign_id=?", (campaign_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Unknown campaign {campaign_id}")
    spec: dict[str, Any] = json.loads(row["spec"])
    return spec


class SQLiteStore:
    """Campaign-local queues with atomic claim, cost reservation and attempt completion."""

    def __init__(self, path: Path) -> None:
        self.path = path
        with self._connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(_SCHEMA)
        with self._connection(write=True) as db:
            columns = {r["name"] for r in db.execute("PRAGMA table_info(swarm_campaigns)")}
            if "ranking" not in columns:
                db.execute("ALTER TABLE swarm_campaigns ADD COLUMN ranking TEXT")

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        with closing(sqlite3.connect(self.path, timeout=15)) as db, db:
            db.execute("PRAGMA foreign_keys=ON")
            db.row_factory = sqlite3.Row
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db

    def create_campaign(self, spec: CampaignSpec) -> None:
        encoded = _json(asdict(spec))
        if min(spec.max_trials, spec.max_workers, spec.max_attempts) < 1:
            raise ValueError("Trial, worker and attempt limits must be positive")
        if spec.max_cost is not None:
            _cost(spec.max_cost)
        with self._connection(write=True) as db:
            db.execute("INSERT OR IGNORE INTO swarm_projects VALUES(?)", (spec.project_id,))
            insert = db.execute(
                "INSERT INTO swarm_campaigns(campaign_id,project_id,spec) "
                "VALUES(?,?,?) ON CONFLICT(campaign_id) DO NOTHING",
                (spec.campaign_id, spec.project_id, encoded),
            )
            if insert.rowcount:
                _event(db, spec.campaign_id, "created")
                return
            saved = _spec(db, spec.campaign_id)
            if "evaluation_mode" not in saved:
                # Earlier queues recorded the policy and panel names instead of the mode.
                panels = ("panel_policy", "search_panel", "selection_panel", "confirmation_panel")
                saved["evaluation_mode"] = (
                    "calibrated" if any(saved.get(p) for p in panels) else "smoke"
                )
            if _json(asdict(CampaignSpec(**saved))) != encoded:
                raise ValueError(f"Campaign {spec.campaign_id} already has a different spec")

    def submit(self, trial: Trial) -> bool:
        # Seed and fidelity hints belong to the trial, not the candidate's identity.
        candidate = replace(trial.candidate, seed=None, fidelity=None)
        encoded = _json(asdict(candidate))
        if not math.isfinite(trial.fidelity) or trial.fidelity <= 0:
            raise ValueError("Fidelity must be positive and finite")
        with self._connection(write=True) as db:
            existing = db.execute(_TRIAL + " WHERE trial_id=?", (trial.trial_id,)).fetchone()
            if existing is not None:
                if _trial(existing) != replace(trial, candidate=candidate):
                    raise ValueError(f"Conflicting trial {trial.trial_id}")
                return False
            spec = _spec(db, trial.campaign_id)
            count = db.execute(
                "SELECT count(*) FROM swarm_trials WHERE campaign_id=?", (trial.campaign_id,)
            ).fetchone()[0]
            if count >= spec["max_trials"]:
                raise ValueError("Trial budget exhausted")
            key = (trial.campaign_id, candidate.candidate_id)
            db.execute("INSERT OR IGNORE INTO swarm_candidates VALUES(?,?,?)", (*key, encoded))
            saved = db.execute(
                "SELECT payload FROM swarm_candidates WHERE campaign_id=? AND candidate_id=?", key
            ).fetchone()[0]
            if saved != encoded:
                raise ValueError(f"Conflicting candidate {candidate.candidate_id}")
            db.execute(
                "INSERT INTO swarm_trials(trial_id,campaign_id,candidate_id,seed,fidelity) "
                "VALUES(?,?,?,?,?)",
                (trial.trial_id, *key, trial.seed, trial.fidelity),
            )
            _event(db, trial.campaign_id, "submitted", trial.trial_id)
            return True

    def claim(
        self,
        campaign_id: str,
        worker_id: str,
        *,
        reserve_cost: float | Callable[[Trial], float] = 0.0,
    ) -> Attempt | None:
        with self._connection(write=True) as db:
            spec = _spec(db, campaign_id)
            active = db.execute(
                "SELECT count(*) FROM swarm_trials WHERE campaign_id=? AND status='running'",
                (campaign_id,),
            ).fetchone()[0]
            if active >= spec["max_workers"]:
                return None
            remaining = None
            if spec["max_cost"] is not None:
                charges = db.execute(
                    "SELECT CASE WHEN observation IS NULL THEN max(reserve,cost) ELSE cost END "
                    "FROM swarm_attempts JOIN swarm_trials USING(trial_id) WHERE campaign_id=?",
                    (campaign_id,),
                )
                spent = sum((Decimal(str(row[0])) for row in charges), Decimal(0))
                remaining = Decimal(str(spec["max_cost"])) - spent
            for row in db.execute(
                _TRIAL + " WHERE t.campaign_id=? AND status='queued' ORDER BY t.rowid",
                (campaign_id,),
            ):
                trial = _trial(row)
                reserve = _cost(reserve_cost(trial) if callable(reserve_cost) else reserve_cost)
                if remaining is None or Decimal(str(reserve)) <= remaining:
                    break
            else:
                return None
            number = db.execute(
                "UPDATE swarm_trials SET status='running', "
                "attempt_count=attempt_count+1 WHERE trial_id=? AND status='queued' "
                "RETURNING attempt_count",
                (trial.trial_id,),
            ).fetchone()[0]
            now = time.time()
            attempt = Attempt(
                attempt_id=uuid4().hex,
                trial=trial,
                worker_id=worker_id,
                number=number,
                started_at=now,
            )
            db.execute(
                "INSERT INTO swarm_attempts(attempt_id,trial_id,number,worker_id,started_at,"
                "heartbeat_at,reserve) VALUES(?,?,?,?,?,?,?)",
                (attempt.attempt_id, trial.trial_id, number, worker_id, now, now, reserve),
            )
            _event(db, campaign_id, "claimed", trial.trial_id, attempt.attempt_id)
            return attempt

    def heartbeat(self, attempt_id: str, *, cost: float = 0.0) -> bool:
        with self._connection(write=True) as db:
            updated = db.execute(
                "UPDATE swarm_attempts SET heartbeat_at=?,cost=max(cost,?) "
                "WHERE attempt_id=? AND observation IS NULL",
                (time.time(), _cost(cost), attempt_id),
            )
            return bool(updated.rowcount)

    def _finish(
        self, db: sqlite3.Connection, row: sqlite3.Row, result: Result, max_attempts: int
    ) -> None:
        observation = asdict(result)
        del observation["attempt"]
        observation["cost"] = max(_cost(result.cost), row["cost"])
        db.execute(
            "UPDATE swarm_attempts SET observation=?,cost=? WHERE attempt_id=?",
            (_json(observation), observation["cost"], row["attempt_id"]),
        )
        status = result.status
        retry = status != "ok" and row["number"] < max_attempts
        db.execute(
            "UPDATE swarm_trials SET status=? WHERE trial_id=?",
            ("queued" if retry else status, row["trial_id"]),
        )
        _event(db, row["campaign_id"], "finished", row["trial_id"], row["attempt_id"])

    def report(self, result: Result) -> bool:
        with self._connection(write=True) as db:
            row = db.execute(
                _ATTEMPT + " WHERE a.attempt_id=? AND a.observation IS NULL",
                (result.attempt.attempt_id,),
            ).fetchone()
            if row is None:
                return False
            if result.status not in ("ok", "failed", "lost") or _attempt(row) != result.attempt:
                raise ValueError("Observation does not match the claimed attempt")
            spec = _spec(db, row["campaign_id"])
            self._finish(db, row, result, spec["max_attempts"])
            return True

    def requeue_expired(self, campaign_id: str, before: float) -> int:
        with self._connection(write=True) as db:
            spec = _spec(db, campaign_id)
            rows = db.execute(
                _ATTEMPT + " WHERE t.campaign_id=? AND a.observation IS NULL "
                "AND a.heartbeat_at < ? ORDER BY a.rowid",
                (campaign_id, before),
            ).fetchall()
            for row in rows:
                result = Result(attempt=_attempt(row), status="lost", error="Heartbeat expired")
                self._finish(db, row, result, spec["max_attempts"])
            return len(rows)

    def trials(self, campaign_id: str) -> list[Trial]:
        with self._connection() as db:
            return [
                _trial(row)
                for row in db.execute(
                    _TRIAL + " WHERE t.campaign_id=? ORDER BY t.rowid", (campaign_id,)
                )
            ]

    def results(self, campaign_id: str, *, after: int = 0) -> list[Result]:
        with self._connection() as db:
            results = []
            for row in db.execute(
                _ATTEMPT + " JOIN swarm_events e ON e.attempt_id=a.attempt_id "
                "WHERE e.campaign_id=? AND e.kind='finished' "
                "ORDER BY e.id LIMIT -1 OFFSET ?",
                (campaign_id, after),
            ):
                data = json.loads(row["observation"])
                data["logs"] = tuple(data["logs"])
                results.append(Result(attempt=_attempt(row), **data))
            return results

    def queue_depth(self, campaign_id: str) -> tuple[int, int]:
        """Return queued and running trial counts from the same snapshot."""
        with self._connection() as db:
            row = db.execute(
                "SELECT count(*) FILTER (WHERE status='queued'), "
                "count(*) FILTER (WHERE status='running') "
                "FROM swarm_trials WHERE campaign_id=?",
                (campaign_id,),
            ).fetchone()
            return int(row[0]), int(row[1])

    def outstanding(self, campaign_id: str) -> int:
        return sum(self.queue_depth(campaign_id))

    def incumbent(self, campaign_id: str) -> tuple[str, float | None, int] | None:
        with self._connection() as db:
            row = db.execute(
                "SELECT selected_trial_id,ranking FROM swarm_campaigns WHERE campaign_id=?",
                (campaign_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Unknown campaign {campaign_id}")
            if row[0] is None:
                return None
            score, replicates = json.loads(row[1]) if row[1] else (None, 1)
            return row[0], score, replicates

    def select(
        self, campaign_id: str, trial_id: str, *, score: float | None = None, replicates: int = 1
    ) -> None:
        if replicates < 1:
            raise ValueError("A ranking requires at least one replicate")
        ranking = _json([score, replicates])
        with self._connection(write=True) as db:
            if not db.execute(
                "SELECT 1 FROM swarm_trials WHERE campaign_id=? AND trial_id=? AND status='ok'",
                (campaign_id, trial_id),
            ).fetchone():
                raise ValueError("Selection requires a successful trial from this campaign")
            changed = db.execute(
                "UPDATE swarm_campaigns SET selected_trial_id=?,ranking=? WHERE campaign_id=? "
                "AND (selected_trial_id IS NOT ? OR ranking IS NOT ?)",
                (trial_id, ranking, campaign_id, trial_id, ranking),
            )
            if changed.rowcount:
                _event(db, campaign_id, "selected", trial_id)
