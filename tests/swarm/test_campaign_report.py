from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_swarm.cli import main
from research_swarm.swarm.interfaces import Result
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
