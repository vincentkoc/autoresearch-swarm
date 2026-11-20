from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Result, Trial
from research_swarm.swarm.store import SQLiteStore


def spec(**changes: object) -> CampaignSpec:
    base = CampaignSpec(
        campaign_id="test",
        metric="loss",
        direction="minimize",
        command=("task",),
        max_workers=8,
        max_trials=16,
    )
    return replace(base, **changes)


def trial(n: int, *, campaign_id: str = "test") -> Trial:
    return Trial(
        trial_id=f"{campaign_id}-{n}",
        campaign_id=campaign_id,
        candidate=Candidate(candidate_id=f"candidate-{n}", params={"x": n}),
        seed=2,
        fidelity=1.0,
    )


def test_legacy_database_is_rejected_before_any_writes(tmp_path: Path) -> None:
    path = tmp_path / "research.sqlite"
    with closing(sqlite3.connect(path)) as legacy, legacy:
        legacy.execute("CREATE TABLE workspaces(id TEXT PRIMARY KEY)")
        legacy.execute("INSERT INTO workspaces VALUES ('archive')")
        legacy.execute("PRAGMA user_version=4")
    original = path.read_bytes()
    with pytest.raises(ValueError, match="fresh database path"):
        SQLiteStore(path)
    assert path.read_bytes() == original
    assert set(tmp_path.iterdir()) == {path}


def test_independent_connections_claim_distinct_trials_and_respect_slots(tmp_path: Path) -> None:
    path = tmp_path / "queue.db"
    queue = SQLiteStore(path)
    queue.create_campaign(spec())
    for n in range(16):
        queue.submit(trial(n))

    def claim(n: int) -> Attempt | None:
        return SQLiteStore(path).claim("test", f"worker-{n}")

    with ThreadPoolExecutor(max_workers=16) as workers:
        claims = [a for a in workers.map(claim, range(16)) if a is not None]
    assert len({a.trial.trial_id for a in claims}) == len(claims) == 8
    for attempt in claims:
        assert queue.report(Result(attempt=attempt, status="ok", metrics={"loss": 3.0}))
    assert len(queue.results("test")) == 8
    assert queue.outstanding("test") == 8
    assert claim(0) is not None


def test_search_work_admission_serializes_competing_connections(tmp_path: Path) -> None:
    path = tmp_path / "queue.db"
    queue = SQLiteStore(path)
    limited = spec(full_fidelity_work=5, max_search_work=4, max_attempts=1)
    queue.create_campaign(limited)

    def offer(i: int) -> list[tuple[str, bool]]:
        # Both deferrable screens need three units. Only one fits.
        screen = replace(trial(i), fidelity=0.5)
        return SQLiteStore(path).submit_proposals("test", 1, [screen], timed_out=False)

    with ThreadPoolExecutor(max_workers=2) as workers:
        admissions = list(workers.map(offer, (1, 2)))
    assert sorted(map(len, admissions)) == [0, 1]
    [screen] = queue.trials("test")
    assert not queue.submit(screen)
    attempt = queue.claim("test", "cpu")
    assert attempt and queue.report(Result(attempt=attempt, status="failed"))
    reopened = SQLiteStore(path)
    reopened.create_campaign(limited)
    assert reopened.claim("test", "other") is None
    with pytest.raises(ValueError, match="remaining search-work"):
        reopened.submit(replace(trial(3), fidelity=0.25))
    assert reopened.submit(replace(trial(4), fidelity=0.2))
    with pytest.raises(ValueError, match="different spec"):
        reopened.create_campaign(replace(limited, max_search_work=5))
    assert len(reopened.trials("test")) == 2


def test_wall_time_expires_inside_claim_and_survives_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1000.0
    monkeypatch.setattr("research_swarm.swarm.store.time", SimpleNamespace(time=lambda: now))
    path = tmp_path / "queue.db"
    queue = SQLiteStore(path)
    config = spec(max_hours=0.5)
    queue.create_campaign(config)
    for i in range(2):
        queue.submit(trial(i))
    first = queue.claim("test", "worker")
    assert first and queue.report(Result(attempt=first, status="failed", error="retry"))

    def estimate(trial: Trial) -> float:
        nonlocal now
        now += 1800.0
        return 0.0

    assert queue.claim("test", "late", reserve_cost=estimate) is None
    resumed = SQLiteStore(path)
    resumed.create_campaign(config)
    assert resumed.deadline("test") == now
    assert resumed.claim("test", "other") is None
    assert resumed.queue_depth("test") == (2, 0)
    assert len(resumed.results("test")) == 1


def test_retry_fences_stale_reports_and_retains_attempt_order(tmp_path: Path) -> None:
    queue = SQLiteStore(tmp_path / "queue.db")
    queue.create_campaign(spec(max_trials=1))
    assert queue.submit(trial(1))
    assert not queue.submit(trial(1))
    with pytest.raises(ValueError, match="budget"):
        queue.submit(trial(2))
    first = queue.claim("test", "one")
    assert first is not None and queue.heartbeat(first.attempt_id, cost=1.25)
    assert queue.requeue_expired("test", first.started_at) == 0
    assert queue.requeue_expired("test", first.started_at + 100.0) == 1
    assert not queue.heartbeat(first.attempt_id)
    assert not queue.report(Result(attempt=first, status="ok", metrics={"loss": 0.0}))
    second = queue.claim("test", "two")
    assert second is not None and second.trial == first.trial and second.number == 2
    result = Result(attempt=second, status="failed", error="exit 7", logs=("stderr.log",))
    assert queue.report(result)
    assert not queue.report(result)
    assert queue.claim("test", "three") is None and queue.outstanding("test") == 0
    lost, failed = queue.results("test")
    assert lost.status == "lost" and lost.cost == 1.25
    assert failed == result and queue.results("test", after=1) == [failed]
    with pytest.raises(ValueError, match="successful"):
        queue.select("test", first.trial.trial_id)


def test_atomic_cost_reservations_and_candidate_specific_estimates(tmp_path: Path) -> None:
    path = tmp_path / "queue.db"
    queue = SQLiteStore(path)
    queue.create_campaign(spec(max_cost=5.0))
    for n in (7, 3, 2, 1):
        queue.submit(trial(n))

    def claim(n: int) -> Attempt | None:
        return SQLiteStore(path).claim(
            "test", str(n), reserve_cost=lambda t: t.candidate.params["x"]
        )

    with ThreadPoolExecutor(max_workers=4) as workers:
        claims = [a for a in workers.map(claim, range(4)) if a is not None]
    assert [a.trial.candidate.params["x"] for a in sorted(claims, key=lambda a: a.started_at)] == [
        3,
        2,
    ]
    assert queue.report(Result(attempt=claims[0], status="ok", cost=1.0))
    next_attempt = claim(9)
    assert next_attempt is not None and next_attempt.trial == trial(1)


@pytest.mark.skipif(os.name != "posix", reason="uses POSIX SIGKILL")
@pytest.mark.parametrize("limit", [None, 1])
def test_worker_sigkill_retries_by_default_and_honors_explicit_one(
    tmp_path: Path, limit: int | None
) -> None:
    path = tmp_path / "queue.db"
    store = SQLiteStore(path)
    campaign = spec() if limit is None else spec(max_attempts=limit)
    store.create_campaign(campaign)
    submitted = trial(1)
    store.submit(submitted)
    program = """
import os, signal, sys
from pathlib import Path
from research_swarm.swarm.store import SQLiteStore
store = SQLiteStore(Path(sys.argv[1]))
attempt = store.claim("test", "doomed")
assert attempt is not None and store.heartbeat(attempt.attempt_id)
os.kill(os.getpid(), signal.SIGKILL)
"""
    killed = subprocess.run(
        [sys.executable, "-c", program, str(path)], capture_output=True, text=True, timeout=20
    )
    assert killed.returncode == -signal.SIGKILL, killed.stderr
    assert store.requeue_expired("test", time.time() + 1) == 1
    (lost,) = store.results("test")
    assert lost.status == "lost" and lost.attempt.trial == submitted
    retry = store.claim("test", "replacement")
    if limit is None:
        assert retry is not None and retry.number == 2 and retry.trial == submitted
        success = Result(attempt=retry, status="ok", metrics={"loss": 0.25})
        assert store.report(success) and not store.report(success)
        assert store.results("test") == [lost, success]
    else:
        assert retry is None and store.results("test") == [lost]
    assert not store.report(Result(attempt=lost.attempt, status="ok", metrics={"loss": 0}))
    assert store.outstanding("test") == 0


def test_decimal_dispatch_exactly_at_budget(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "queue.db")
    store.create_campaign(spec(max_cost=0.30))
    for n in range(3):
        store.submit(trial(n))
    first = store.claim("test", "one", reserve_cost=0.10)
    assert first is not None
    store.report(Result(attempt=first, status="ok", cost=0.10))
    assert store.claim("test", "two", reserve_cost=0.20000001) is None
    assert store.claim("test", "two", reserve_cost=0.20) is not None
    assert store.claim("test", "three", reserve_cost=0.00000001) is None


def test_campaign_isolation_resume_and_durable_selection(tmp_path: Path) -> None:
    path = tmp_path / "queue.db"
    queue = SQLiteStore(path)
    for name in ("first", "second"):
        queue.create_campaign(spec(campaign_id=name))
        queue.submit(trial(1, campaign_id=name))
    first = queue.claim("first", "worker")
    assert first is not None
    queue.report(Result(attempt=first, status="ok", metrics={"loss": 0.9}))
    with pytest.raises(ValueError, match="this campaign"):
        queue.select("second", first.trial.trial_id)
    queue.select("first", first.trial.trial_id)
    queue.select("first", first.trial.trial_id)
    reopened = SQLiteStore(path)
    reopened.create_campaign(spec(campaign_id="first"))
    assert reopened.trials("first") == [first.trial]
    assert reopened.results("second") == [] and reopened.outstanding("second") == 1
    with pytest.raises(ValueError, match="different spec"):
        reopened.create_campaign(spec(campaign_id="first", max_workers=2))
    with sqlite3.connect(path) as db:
        assert db.execute(
            "SELECT selected_trial_id FROM swarm_campaigns WHERE campaign_id='first'"
        ).fetchone() == (first.trial.trial_id,)
        assert db.execute("SELECT count(*) FROM swarm_events WHERE kind='selected'").fetchone() == (
            1,
        )


def test_selection_events_preserve_policy_scores_and_legacy_unknowns(tmp_path: Path) -> None:
    path = tmp_path / "history.db"
    store = SQLiteStore(path)
    store.create_campaign(spec())
    for n in (1, 2):
        store.submit(trial(n))
        claim = store.claim("test", "local")
        assert claim is not None
        store.report(Result(attempt=claim, status="ok", metrics={"loss": 0.9}))
    store.select("test", trial(1).trial_id, score=0.95)
    with sqlite3.connect(path) as db:
        # Earlier event rows contained only the identifiers and timestamp.
        db.execute("ALTER TABLE swarm_events DROP COLUMN score")
        db.execute("ALTER TABLE swarm_events DROP COLUMN replicates")

    reopened = SQLiteStore(path)
    reopened.select("test", trial(1).trial_id, score=0.95)
    reopened.select("test", trial(1).trial_id, score=0.85, replicates=2)
    reopened.select("test", trial(1).trial_id, score=0.75, replicates=2)
    reopened.select("test", trial(2).trial_id, score=0.65, replicates=3)
    replay = SQLiteStore(path)
    replay.select("test", trial(2).trial_id, score=0.65, replicates=3)
    assert replay.incumbent("test") == (trial(2).trial_id, 0.65, 3)
    with sqlite3.connect(path) as db:
        assert db.execute(
            "SELECT trial_id,score,replicates FROM swarm_events "
            "WHERE campaign_id=? AND kind='selected' ORDER BY id",
            ("test",),
        ).fetchall() == [
            (trial(1).trial_id, None, None),
            (trial(1).trial_id, 0.85, 2),
            (trial(1).trial_id, 0.75, 2),
            (trial(2).trial_id, 0.65, 3),
        ]


def test_trial_and_candidate_identity_survive_replicates(tmp_path: Path) -> None:
    queue = SQLiteStore(tmp_path / "queue.db")
    queue.create_campaign(spec())
    original = trial(1)
    queue.submit(original)
    replicate = replace(
        original, trial_id="replicate", seed=9, candidate=replace(original.candidate, seed=9)
    )
    assert queue.submit(replicate)
    assert queue.trials("test")[1].seed == 9
    with pytest.raises(ValueError, match="Conflicting trial"):
        queue.submit(replace(original, seed=4))
    with pytest.raises(ValueError, match="Conflicting candidate"):
        queue.submit(
            replace(
                original, trial_id="other", candidate=replace(original.candidate, text="changed")
            )
        )


def test_init_uses_six_tables_and_preserves_a_legacy_workspace(tmp_path: Path) -> None:
    from research_swarm.cli import main

    workspace = tmp_path / "fresh"
    assert main(["--state-dir", str(workspace), "init"]) == 0
    assert main(["--state-dir", str(workspace), "init"]) == 0
    with sqlite3.connect(workspace / "swarm.sqlite") as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
    assert tables - {"sqlite_sequence"} == {
        "swarm_projects",
        "swarm_campaigns",
        "swarm_candidates",
        "swarm_trials",
        "swarm_attempts",
        "swarm_events",
    }
    archive = tmp_path / "archive"
    archive.mkdir()
    legacy = archive / "research.sqlite"
    legacy.write_bytes(b"archived state")
    assert main(["--state-dir", str(archive), "init"]) == 2
    assert list(archive.iterdir()) == [legacy]
    assert legacy.read_bytes() == b"archived state"


def test_project_identity_file_does_not_make_a_legacy_workspace(tmp_path: Path) -> None:
    from research_swarm.swarm.private_files import campaign_database

    (tmp_path / "identity.json").write_text('{"name":"dataset"}')
    assert campaign_database(tmp_path, project_directory=True) == tmp_path / "swarm.sqlite"
