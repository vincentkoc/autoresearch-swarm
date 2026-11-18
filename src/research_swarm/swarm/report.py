"""Read a consistent, bounded report from the campaign queue without changing its schema."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _iso(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, UTC).isoformat()


@contextmanager
def _read(database: Path) -> Iterator[sqlite3.Connection]:
    try:
        with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            yield db
    except sqlite3.Error as error:
        raise ValueError(f"Cannot read the campaign database: {error}") from error


def _usage(db: sqlite3.Connection, campaign: str, spec: dict[str, Any]) -> dict[str, Any]:
    usage = db.execute(
        "SELECT COALESCE(SUM(a.cost),0), "
        "COALESCE(SUM(CASE WHEN a.observation IS NULL THEN MAX(a.reserve-a.cost,0) END),0), "
        "COUNT(*), COUNT(a.observation) FROM swarm_attempts a "
        "JOIN swarm_trials t USING(trial_id) WHERE t.campaign_id=?",
        (campaign,),
    ).fetchone()
    trial_count = db.execute(
        "SELECT COUNT(*) FROM swarm_trials WHERE campaign_id=?", (campaign,)
    ).fetchone()[0]
    return {
        "measured_cost": usage[0],
        "pending_estimate": usage[1],
        "max_cost": spec.get("max_cost"),
        "trials": trial_count,
        "max_trials": spec["max_trials"],
        "attempts": usage[2],
        "finished_attempts": usage[3],
        "max_hours": spec.get("max_hours"),
    }


def read_meter(database: Path, campaign: str) -> dict[str, Any]:
    with _read(database) as db:
        row = db.execute(
            "SELECT spec FROM swarm_campaigns WHERE campaign_id=?", (campaign,)
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown campaign {campaign}")
        return _usage(db, campaign, json.loads(row["spec"]))


def capture(database: Path, campaign: str) -> dict[str, Any]:
    with _read(database) as db:
        row = db.execute(
            "SELECT spec,selected_trial_id FROM swarm_campaigns WHERE campaign_id=?", (campaign,)
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown campaign {campaign}")
        spec = json.loads(row["spec"])
        objective = {"name": spec["metric"], "unit": "unspecified", "direction": spec["direction"]}
        cohort = f"campaign:{campaign}"
        events = db.execute(
            "SELECT MAX(id), MIN(CASE WHEN kind='created' THEN at END), "
            "COUNT(*) FILTER (WHERE kind='finished') FROM swarm_events WHERE campaign_id=?",
            (campaign,),
        ).fetchone()
        meter = _usage(db, campaign, spec)
        finished = db.execute(
            "SELECT e.id,e.at,t.trial_id,t.candidate_id,t.seed,a.attempt_id,a.observation "
            "FROM swarm_events e JOIN swarm_trials t USING(trial_id) "
            "JOIN swarm_attempts a ON a.attempt_id=e.attempt_id "
            "WHERE e.campaign_id=? AND e.kind='finished' ORDER BY e.id DESC LIMIT 50",
            (campaign,),
        ).fetchall()
        selected = db.execute(
            "SELECT e.*,t.candidate_id FROM swarm_events e JOIN swarm_trials t USING(trial_id) "
            "WHERE e.campaign_id=? AND e.kind='selected' ORDER BY e.id DESC LIMIT 1",
            (campaign,),
        ).fetchone()
        candidates = db.execute(
            "SELECT candidate_id, json_extract(payload,'$.parents') AS parents, "
            "count(*) OVER () AS total FROM swarm_candidates WHERE campaign_id=? "
            "ORDER BY candidate_id=(SELECT candidate_id FROM swarm_trials WHERE trial_id=?) DESC, "
            "rowid DESC LIMIT 50",
            (campaign, row["selected_trial_id"]),
        ).fetchall()
        trials = db.execute(
            "SELECT trial_id AS id,status AS execution_outcome FROM swarm_trials "
            "WHERE campaign_id=? ORDER BY rowid DESC LIMIT 50",
            (campaign,),
        ).fetchall()
        attempts = db.execute(
            "SELECT a.attempt_id AS id, a.worker_id, t.trial_id FROM swarm_attempts a "
            "JOIN swarm_trials t USING(trial_id) WHERE t.campaign_id=? "
            "ORDER BY a.rowid DESC LIMIT 50",
            (campaign,),
        ).fetchall()
    selection = None
    if selected is not None and selected["trial_id"] == row["selected_trial_id"]:
        fields = dict(selected)
        selection = {
            "trial_id": fields["trial_id"],
            "candidate_id": fields["candidate_id"],
            "score": fields.get("score"),
            "replicates": fields.get("replicates"),
            "recorded_at": _iso(fields["at"]),
            "event_sequence": fields["id"],
        }
    observations = []
    for attempt in reversed(finished):
        result = json.loads(attempt["observation"])
        value = result.get("metrics", {}).get(objective["name"])
        observations.append(
            {
                "id": attempt["attempt_id"],
                "eligible": result["status"] == "ok" and value is not None,
                "comparison_digest": cohort,
                "primary_metric": {**objective, "value": value} if value is not None else None,
                "commit_event": {"sequence": attempt["id"], "recorded_at": _iso(attempt["at"])},
                "data": {
                    "trial_id": attempt["trial_id"],
                    "candidate_id": attempt["candidate_id"],
                    "observed_seed": attempt["seed"],
                    "status": result["status"],
                },
            }
        )
    family = []
    for candidate in candidates:
        parents = json.loads(candidate["parents"] or "[]")
        family.append(
            {
                "id": candidate["candidate_id"],
                "parent_ids": parents[:64],
                "parent_ids_complete": len(parents) <= 64,
            }
        )
    candidate_count = candidates[0]["total"] if candidates else 0
    return {
        "schema_version": "swarm-campaign-1",
        "campaign_id": campaign,
        "campaign_revision": events[0],
        "evidence_through": events[0],
        "captured_at": datetime.now(UTC).isoformat(),
        "campaign_created_at": _iso(events[1]) if events[1] is not None else None,
        "comparison_digest": cohort,
        "evaluation_count": events[2],
        "evaluation_window": {"complete": len(finished) == events[2]},
        "recent_evaluations": observations,
        "research": {
            "objective": {"primary_metric": objective, "name": objective["name"]},
            "candidates": {
                "items": family,
                "total": candidate_count,
                "complete": len(family) == candidate_count,
            },
            "trials": {
                "items": [dict(r) for r in trials],
                "total": meter["trials"],
                "complete": len(trials) == meter["trials"],
            },
            "attempts": {
                "items": [dict(r) for r in attempts],
                "total": meter["attempts"],
                "complete": len(attempts) == meter["attempts"],
            },
        },
        "recorded_selection": selection,
        "meter": meter,
    }
