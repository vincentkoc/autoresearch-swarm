"""Reconcile returned proposal batches with real CLI admissions across fresh processes."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path

import pytest

from research_swarm.swarm.interfaces import CampaignSpec, Candidate, Result, Trial
from research_swarm.swarm.reader import CampaignReader
from research_swarm.swarm.store import _SCHEMA, SQLiteStore
from tests.swarm.test_store import spec, trial


class GeneratedProposer:
    def __init__(self, spec: CampaignSpec):
        self.offered = False

    async def ask(self, spec: CampaignSpec, archive: Sequence[Result], k: int) -> list[Candidate]:
        mode = os.environ["PROPOSAL_FIXTURE_MODE"]
        first = Candidate(candidate_id="first")
        if mode == "timeout":
            await asyncio.sleep(60)
        if mode == "malformed":
            return "bad envelope"  # type: ignore[return-value]
        if mode == "invalid":
            return [first, first, Candidate(candidate_id="bad", fidelity=-1), first]
        if mode == "oversize":
            return [first] * (k + 1)
        if mode == "replay":
            raise AssertionError("Completed campaign replay called the proposer")
        if self.offered:
            return []
        self.offered = True
        return [first, Candidate(candidate_id="second"), Candidate(candidate_id="first", seed=22)]

    def tell(self, result: Result) -> None:
        pass


def test_cli_proposal_batches_and_replay(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cli = str(Path(sys.executable).with_name("research"))
    path, database = tmp_path / "campaign.yaml", tmp_path / "queue.sqlite"
    workload = (
        "import os; from pathlib import Path; "
        "Path(os.environ['SWARM_METRICS']).write_text('{\"loss\":1}')"
    )
    campaign = spec(
        evaluation_mode="smoke",
        command=[sys.executable, "-c", workload],
        max_trials=4,
        max_workers=4,
        proposal_timeout_seconds=0.2,
        task_dir=str(tmp_path),
        proposer="tests.swarm.test_proposer_accounting:GeneratedProposer",
    )
    # The CLI runs outside pytest. Give it the same generated factory under its package name.
    path.write_text(json.dumps(asdict(campaign)))
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[2]))

    def run(mode: str, expected: int) -> subprocess.CompletedProcess[str]:
        monkeypatch.setenv("PROPOSAL_FIXTURE_MODE", mode)
        result = subprocess.run(
            [cli, "swarm", "run", str(path), "--db", str(database)],
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert result.returncode == expected, (result.stdout, result.stderr)
        return result

    def counts() -> tuple[int, int, int, float | None]:
        kpi = CampaignReader(database).snapshot("test")["kpis"]["proposer_acceptance_rate"]
        with sqlite3.connect(database) as db:
            proposed, accepted = db.execute(
                "SELECT SUM(proposed_count), SUM(accepted_count) FROM swarm_events "
                "WHERE campaign_id='test' AND kind IN ('created','proposal_batch')"
            ).fetchone()
        assert (proposed, accepted) == (kpi["proposed"], kpi["accepted"])
        assert kpi["rejected"] == proposed - accepted
        assert kpi["value"] == (pytest.approx(100 * kpi["fraction"]) if proposed else None)
        return proposed, accepted, kpi["rejected"], kpi["fraction"]

    assert json.loads(run("timeout", 3).stdout)["reason"] == "proposal_timeout"
    assert counts() == (0, 0, 0, None)
    assert "sequence of candidate entries" in run("malformed", 2).stderr
    assert counts() == (0, 0, 0, None)
    assert "exceeded requested capacity" in run("oversize", 2).stderr
    assert counts() == (5, 0, 5, 0)
    assert "Fidelity must be positive" in run("invalid", 2).stderr
    assert counts() == (9, 1, 8, 1 / 9)
    assert json.loads(run("valid", 0).stdout)["trials"] == 3
    assert counts() == (12, 3, 9, 0.25)
    reader = CampaignReader(database)
    before = reader.events(campaign_id="test")
    assert json.loads(run("replay", 0).stdout)["trials"] == 3
    assert counts() == (12, 3, 9, 0.25)
    events = subprocess.check_output(
        [cli, "events", "--db", str(database), "--campaign", "test"], text=True, timeout=15
    )
    assert json.loads(events) == before == reader.events(campaign_id="test")
    assert [
        (e["kind"], e["proposed_count"], e["accepted_count"])
        for e in before["items"]
        if e["proposed_count"] is not None
    ] == [
        ("created", 0, 0),
        ("proposal_timeout", 0, 0),
        ("proposal_batch", 5, 0),
        ("proposal_batch", 4, 1),
        ("proposal_batch", 3, 2),
    ]
    html = tmp_path / "report.html"
    subprocess.run(
        [cli, "report", "view", "--db", str(database), "--campaign", "test", "--output", str(html)],
        check=True,
        capture_output=True,
        timeout=15,
    )
    assert "25 %" in html.read_text()
    assert "3 accepted and 9 rejected out of 12 returned candidates." in html.read_text()


def test_legacy_history_and_direct_submissions_do_not_supply_a_denominator(tmp_path: Path) -> None:
    database = tmp_path / "legacy.sqlite"
    campaign = spec()
    with sqlite3.connect(database) as db:
        db.execute(
            "CREATE TABLE swarm_events(id INTEGER PRIMARY KEY, campaign_id TEXT, kind TEXT,"
            "at REAL, trial_id TEXT, attempt_id TEXT, score REAL, replicates INTEGER)"
        )
        db.executescript(_SCHEMA)
        db.execute("INSERT INTO swarm_projects(project_id) VALUES(?)", (campaign.project_id,))
        db.execute(
            "INSERT INTO swarm_campaigns(campaign_id,project_id,spec) VALUES(?,?,?)",
            ("test", campaign.project_id, json.dumps(asdict(campaign))),
        )
        db.execute("INSERT INTO swarm_events(campaign_id,kind,at) VALUES('test','created',1)")
    reader = CampaignReader(database)
    unknown = reader.snapshot("test")["kpis"]["proposer_acceptance_rate"]
    assert all(
        unknown[key] is None for key in ("value", "fraction", "proposed", "accepted", "rejected")
    )
    store = SQLiteStore(database)
    store.create_campaign(campaign)
    assert store.submit_proposals("test", 1, [trial(1)], timed_out=False) == [("test-1", True)]
    assert reader.snapshot("test")["kpis"]["proposer_acceptance_rate"] == unknown
    store.create_campaign(spec(campaign_id="other"))
    assert store.submit(trial(1, campaign_id="other"))
    kpi = reader.snapshot("other")["kpis"]["proposer_acceptance_rate"]
    assert kpi["proposed"] == kpi["accepted"] == kpi["rejected"] == 0 and kpi["fraction"] is None


def test_interrupted_and_concurrent_admissions(tmp_path: Path) -> None:
    database = tmp_path / "queue.sqlite"
    store = SQLiteStore(database)
    store.create_campaign(spec())

    def interrupted() -> Iterator[Trial]:
        yield trial(1)
        raise RuntimeError("interrupted before batch commit")

    with pytest.raises(RuntimeError, match="interrupted"):
        store.submit_proposals("test", 2, interrupted(), timed_out=False)
    assert store.trials("test") == []
    assert (
        CampaignReader(database).snapshot("test")["kpis"]["proposer_acceptance_rate"]["proposed"]
        == 0
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        asks = [
            pool.submit(store.submit_proposals, "test", 1, [trial(1)], timed_out=False)
            for _ in range(2)
        ]
        assert sorted(ask.result()[0][1] for ask in asks) == [False, True]
    kpi = CampaignReader(database).snapshot("test")["kpis"]["proposer_acceptance_rate"]
    assert (kpi["proposed"], kpi["accepted"], kpi["rejected"], kpi["fraction"]) == (2, 1, 1, 0.5)
