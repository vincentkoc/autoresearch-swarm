from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

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
        max_attempts=2,
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
