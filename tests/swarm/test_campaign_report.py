from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from research_swarm.cli import main
from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.reader import CampaignReader
from research_swarm.swarm.report import capture
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec, trial


def test_live_reports_distinguish_selection_measurements_and_estimates(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "queue.sqlite"
    queue = SQLiteStore(database)
    queue.create_campaign(spec(max_cost=10, max_attempts=1))
    for index in range(1, 5):
        queue.submit(trial(index))
    for value in (0.5, 2.0, None):
        attempt = queue.claim("test", "local")
        assert attempt is not None
        queue.report(
            Result(
                attempt=attempt,
                status="failed" if value is None else "ok",
                metrics={} if value is None else {"loss": value},
                cost=1,
            )
        )
    queue.select("test", "test-1", score=3, replicates=2)
    queue.select("test", "test-2", score=1.5, replicates=3)
    running = queue.claim("test", "local", reserve_cost=3)
    assert running is not None and queue.heartbeat(running.attempt_id, cost=0.5)
    snapshot = capture(database, "test")
    efficiency = snapshot["kpis"]["cost_per_0_01_improvement"]
    assert efficiency["value"] == pytest.approx(3.5 * 0.01 / 1.5)
    assert efficiency["unit"] == "campaign currency / 0.01"
    assert snapshot["kpis"]["failed_attempts_by_class"]["value"] == {
        "unclassified": pytest.approx(100 / 3)
    }
    assert snapshot["recorded_selection"]["candidate_id"] == "candidate-2"
    assert [s["score"] for s in snapshot["selection_history"]] == [3, 1.5]
    assert snapshot["recorded_selection"]["score"] == 1.5
    assert snapshot["recorded_selection"]["replicates"] == 3
    assert [item["eligible"] for item in snapshot["recent_evaluations"]] == [True, True, False]
    assert main(["budget", "test", "--db", str(database)]) == 0
    meter = json.loads(capsys.readouterr().out)
    assert (meter["measured_cost"], meter["pending_estimate"], meter["trials"]) == (3.5, 2.5, 4)
    for action, extension in (("plot", "svg"), ("view", "html")):
        output = tmp_path / f"plot.{extension}"
        assert (
            main(
                [
                    "report",
                    action,
                    "--db",
                    str(database),
                    "--campaign",
                    "test",
                    "--output",
                    str(output),
                ]
            )
            == 0
        )
        assert "recorded-selection" in output.read_text()
        assert "test-2" in output.read_text() and "1.5" in output.read_text()
    assert capture(database, "test")["evidence_through"] == snapshot["evidence_through"]
    missing = tmp_path / "missing.sqlite"
    assert main(["budget", "test", "--db", str(missing)]) == 2
    assert not missing.exists()


def test_kpis_reopen_queue_and_replay_archive(tmp_path: Path) -> None:
    database = tmp_path / "queue.sqlite"
    queue = SQLiteStore(database)
    queue.create_campaign(spec(max_attempts=2))
    queue.create_campaign(spec(campaign_id="other"))
    empty = capture(database, "test")["kpis"]
    assert len(empty) == 8
    assert empty["trials_per_hour"]["value"] == 0
    assert all(item["value"] is None for key, item in empty.items() if key != "trials_per_hour")
    queue.submit(trial(1))
    for status in ("failed", "ok"):
        attempt = queue.claim("test", "local")
        assert attempt is not None
        assert queue.report(Result(attempt=attempt, status=status, metrics={"loss": 1}))
        if status == "failed":
            assert capture(database, "test")["kpis"]["trials_per_hour"]["value"] == 0
    assert capture(database, "test")["kpis"]["time_to_first_trial"]["value"] is None
    with sqlite3.connect(database) as db:
        # Replay the additive producer fields against an existing queue.
        db.execute("UPDATE swarm_events SET at=at-10 WHERE kind='created'")
        db.execute(
            "INSERT INTO swarm_events (campaign_id,kind,at,trial_id,attempt_id) "
            "SELECT c.campaign_id,'workload_started',c.at+2.5,e.trial_id,e.attempt_id "
            "FROM swarm_events c JOIN swarm_events e USING(campaign_id) "
            "WHERE c.kind='created' AND e.kind='claimed' ORDER BY e.id LIMIT 1"
        )
        db.execute(
            "UPDATE swarm_attempts SET observation=json_set(observation,'$.failure_class',?) "
            "WHERE number=1",
            ("timeout",),
        )
    snapshot = CampaignReader(database).snapshot("test")
    kpis = snapshot["kpis"]
    assert kpis["time_to_first_trial"]["value"] == 2.5
    assert kpis["trials_per_hour"]["value"] == 1
    assert kpis["failed_attempts_by_class"]["value"] == {"timeout": 50}
    assert kpis["failed_attempts_by_class"]["finished_attempts"] == 2
    assert CampaignReader(database).snapshot("other")["kpis"] == empty
    assert capture(database, "test")["kpis"] == kpis
    # Advance the live window while the archived snapshot keeps its original evidence.
    cutoff = datetime.fromisoformat(snapshot["captured_at"]).timestamp() - 3600
    with sqlite3.connect(database) as db:
        db.execute("UPDATE swarm_events SET at=? WHERE kind='finished'", (cutoff,))
    assert CampaignReader(database).snapshot("test")["kpis"]["trials_per_hour"]["value"] == 0
    archive = tmp_path / "snapshot.json"
    for name in ("current", "legacy"):
        if name == "legacy":
            snapshot.pop("kpis")
        archive.write_text(json.dumps(snapshot))
        output = tmp_path / f"{name}.html"
        assert (
            main(
                [
                    "report",
                    "view",
                    "--file",
                    str(archive),
                    "--comparison-digest",
                    "campaign:test",
                    "--metric",
                    "loss",
                    "--unit",
                    "unspecified",
                    "--direction",
                    "minimize",
                    "--output",
                    str(output),
                ]
            )
            == 0
        )
        html = output.read_text()
        assert "Campaign KPIs" in html and "Proxy-to-target slope" in html
        if name == "legacy":
            assert html.count("This archive did not capture this KPI.") == 8
        else:
            assert "timeout 50%" in html and "1 trials/h" in html and "2.5 s" in html


def test_efficiency_uses_full_selection_history_and_requires_priced_usage(tmp_path: Path) -> None:
    database = tmp_path / "queue.sqlite"
    queue = SQLiteStore(database)
    for campaign, cost in (("priced", 2), ("free", 0)):
        queue.create_campaign(spec(campaign_id=campaign, direction="maximize"))
        queue.submit(trial(1, campaign_id=campaign))
        attempt = queue.claim(campaign, "local")
        assert attempt is not None
        queue.report(Result(attempt=attempt, status="ok", metrics={"loss": 0}, cost=cost))
        for score in range(52):
            queue.select(campaign, attempt.trial.trial_id, score=score)
        snapshot = capture(database, campaign)
        assert snapshot["selection_history"][0]["score"] == 2
        metric = snapshot["kpis"]["cost_per_0_01_improvement"]
        if cost:
            assert metric["value"] == pytest.approx(cost * 0.01 / 51)
        else:
            assert metric["value"] is None and "priced usage" in metric["detail"]
        assert snapshot["kpis"]["failed_attempts_by_class"]["value"] == {}
        queue.select(campaign, attempt.trial.trial_id, score=-1)
        assert capture(database, campaign)["kpis"]["cost_per_0_01_improvement"]["value"] is None
