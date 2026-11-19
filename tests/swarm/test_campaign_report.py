from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from research_swarm.api.campaign import campaign_app
from research_swarm.cli import main
from research_swarm.control.report_plot import project_hill_climb
from research_swarm.control.report_view import render_report_html
from research_swarm.domain.experiments import MetricDefinition
from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.meter import CostMeter
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
            snapshot.pop("observed_cohorts")
            for receipt in snapshot["recent_evaluations"]:
                receipt["data"].pop("fidelity")
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


@pytest.mark.parametrize(
    ("direction", "scores"),
    [("maximize", (0.1, 0.8, 0.9)), ("minimize", (0.9, 0.2, 0.1))],
)
def test_cost_per_gain_uses_comparable_selections_and_all_campaign_cost(
    tmp_path: Path, direction: str, scores: tuple[float, float, float]
) -> None:
    database = tmp_path / "queue.sqlite"
    queue = SQLiteStore(database)
    queue.create_campaign(spec(direction=direction))
    for index, (fidelity, score) in enumerate(zip((0.5, 1.0, 1.0), scores, strict=True), start=1):
        queue.submit(replace(trial(index), fidelity=fidelity))
        attempt = queue.claim("test", "worker")
        assert attempt and queue.report(
            Result(attempt=attempt, status="ok", metrics={"loss": score}, cost=index)
        )
        queue.select("test", attempt.trial.trial_id, score=score)
        metric = capture(database, "test")["kpis"]["cost_per_0_01_improvement"]
        if index < 3:
            assert metric["value"] is None
        else:
            assert metric["value"] == pytest.approx((1 + 2 + 3) * 0.01 / 0.1)
            assert "fidelity 1" in metric["detail"]


def test_reports_keep_screening_observations_and_selections_in_their_rung(tmp_path: Path) -> None:
    database = tmp_path / "queue.sqlite"
    queue = SQLiteStore(database)
    queue.create_campaign(spec())
    reader = CampaignReader(database)
    full = "campaign:test"
    screen = "fidelity:0.5:campaign:test"
    for trial_id, fidelity, loss in ((1, 0.5, 0.01), (2, 1.0, 4.0)):
        queue.submit(replace(trial(trial_id), fidelity=fidelity))
        attempt = queue.claim("test", "worker")
        assert attempt and queue.report(
            Result(attempt=attempt, status="ok", metrics={"loss": loss})
        )
        queue.select("test", attempt.trial.trial_id, score=loss, replicates=1)
        if trial_id == 1:
            assert reader.snapshot("test")["comparison_digest"] == screen

    snapshot = reader.snapshot("test")
    assert snapshot["comparison_digest"] == full
    assert snapshot["observed_cohorts"] == [
        {"comparison_digest": full, "fidelity": 1.0},
        {"comparison_digest": screen, "fidelity": 0.5},
    ]
    metric = MetricDefinition.model_validate(snapshot["research"]["objective"]["primary_metric"])
    for digest, score, trial_id in ((screen, 0.01, "test-1"), (full, 4.0, "test-2")):
        (run,) = project_hill_climb([snapshot], comparison_digest=digest, metric=metric)
        assert [p.value for p in run.points] == [score]
        assert [(value, trial) for _, value, trial in run.selections] == [(score, trial_id)]

    client = TestClient(campaign_app(reader, "reader"), headers={"Authorization": "Bearer reader"})
    path = "/v1/campaigns/test"
    assert client.get(f"{path}/snapshot").json()["observed_cohorts"] == snapshot["observed_cohorts"]
    assert client.get(f"{path}/view").status_code == 400
    for digest, selected in ((full, True), (screen, False)):
        response = client.get(f"{path}/view", params={"comparison_digest": digest})
        assert response.status_code == 200
        assert ("Selected trial <code>test-2</code>" in response.text) is selected
        assert response.text.count("data-evaluation=") == 1
        score = "4" if selected else "0.01"
        assert response.text.count("class='leader-score number'") == 1
        assert f"class='leader-score number'>{score}</span>" in response.text
    assert client.get(f"{path}/view", params={"comparison_digest": "missing"}).status_code == 404

    output = tmp_path / "screen.svg"
    assert (
        main(
            [
                "report",
                "plot",
                "--db",
                str(database),
                "--campaign",
                "test",
                "--comparison-digest",
                screen,
                "--output",
                str(output),
            ]
        )
        == 0
    )
    svg = output.read_text()
    assert svg.count("data-evaluation=") == 1 and "Selected trial test-2" not in svg


@pytest.mark.parametrize(
    ("output_rate", "tokens", "total", "kpi"),
    [
        (None, 6, None, None),  # Consumed output units with unknown price.
        (0.0, 6, 0.3, 0.003),  # Explicitly free.
        (None, 0, 0.3, 0.003),  # No output units to price.
        (25_000.0, 6, 0.6, 0.006),
    ],
)
def test_report_distinguishes_known_subtotal_from_total(
    tmp_path: Path, output_rate: float | None, tokens: int, total: float | None, kpi: float | None
) -> None:
    database = tmp_path / "queue.sqlite"
    campaign = spec(price_per_hour=36, price_per_million_output_tokens=output_rate)
    queue = SQLiteStore(database)
    queue.create_campaign(campaign)
    for index, seconds in enumerate((10, 20), start=1):
        job = trial(index)
        assert queue.submit(job)
        attempt = queue.claim("test", "fixture")
        assert attempt is not None
        cost = CostMeter().charge(campaign, attempt, seconds, 0, tokens)
        score = 5 - index
        assert queue.report(
            Result(
                attempt=attempt,
                status="ok",
                metrics={"loss": score},
                cost=cost,
                output_tokens=tokens,
            )
        )
        queue.select("test", job.trial_id, score=score)

    reopened = SQLiteStore(database)
    reopened.create_campaign(campaign)
    restored = reopened.campaign_spec("test")
    assert type(restored.price_per_hour) is float and restored.price_per_hour == 36.0
    assert restored.price_per_million_output_tokens == output_rate
    reopened.create_campaign(restored)
    opposite = 0.0 if output_rate is None else None
    with pytest.raises(ValueError, match="different spec"):
        reopened.create_campaign(replace(restored, price_per_million_output_tokens=opposite))
    reader = CampaignReader(database)
    snapshot = reader.snapshot("test")
    budget = reader.budget("test")
    assert snapshot["meter"] == budget
    assert budget["measured_cost"] == pytest.approx(total if total is not None else 0.3)
    assert budget["missing_prices"] == (
        ["price_per_million_output_tokens"] if total is None else []
    )
    efficiency = snapshot["kpis"]["cost_per_0_01_improvement"]
    if total is None:
        assert budget["total_cost"] is None and efficiency["value"] is None
        metric = MetricDefinition.model_validate(
            snapshot["research"]["objective"]["primary_metric"]
        )
        html = render_report_html(
            [snapshot], comparison_digest=snapshot["comparison_digest"], metric=metric
        )
        assert "Known cost subtotal: " in html
        assert "Total cost: Unavailable (missing price_per_million_output_tokens)" in html
        assert "<dt>Cost per 0.01 improvement</dt><dd><strong>Unavailable</strong>" in html
        # Previously exported snapshots could contain a ratio without any price coverage.
        archive = {
            **snapshot,
            "meter": {"measured_cost": 0.3, "pending_estimate": 0, "trials": 2, "max_trials": 2},
            "kpis": {
                **snapshot["kpis"],
                "cost_per_0_01_improvement": {**efficiency, "value": 0.003},
            },
        }
        historical = render_report_html(
            [archive], comparison_digest=archive["comparison_digest"], metric=metric
        )
        assert "Total cost: Unavailable (price coverage was not recorded)" in historical
        assert "<dt>Cost per 0.01 improvement</dt><dd><strong>Unavailable</strong>" in historical
    else:
        assert kpi is not None
        assert budget["total_cost"] == pytest.approx(total)
        assert efficiency["value"] == pytest.approx(kpi)


def test_historical_zero_prices_are_ambiguous_on_reopen(tmp_path: Path) -> None:
    database = tmp_path / "queue.sqlite"
    campaign = spec(price_per_million_input_tokens=1000)
    queue = SQLiteStore(database)
    queue.create_campaign(campaign)
    assert queue.submit(trial(1))
    attempt = queue.claim("test", "fixture")
    assert attempt is not None
    assert queue.report(
        Result(attempt=attempt, status="failed", cost=0.01, input_tokens=10, output_tokens=2)
    )

    with sqlite3.connect(database) as db:
        saved = json.loads(db.execute("SELECT spec FROM swarm_campaigns").fetchone()[0])
        saved.pop("price_schema")
        saved.update(price_per_hour=0.0, price_per_million_output_tokens=0.0)
        db.execute("UPDATE swarm_campaigns SET spec=?", (json.dumps(saved),))

    reopened = SQLiteStore(database)
    reopened.create_campaign(campaign)
    reopened.create_campaign(replace(campaign, price_per_hour=0, price_per_million_output_tokens=0))
    restored = reopened.campaign_spec("test")
    assert type(restored.price_per_million_input_tokens) is float
    reopened.create_campaign(restored)
    reopened.create_campaign(
        replace(restored, price_per_hour=None, price_per_million_output_tokens=None)
    )
    with pytest.raises(ValueError, match="different spec"):
        reopened.create_campaign(replace(restored, price_per_million_input_tokens=1001))
    budget = CampaignReader(database).budget("test")
    assert budget["measured_cost"] == 0.01 and budget["total_cost"] is None
    assert budget["missing_prices"] == ["price_per_hour", "price_per_million_output_tokens"]


def test_negative_declared_price_leaves_existing_queue_untouched(tmp_path: Path) -> None:
    database = tmp_path / "queue.sqlite"
    queue = SQLiteStore(database)
    queue.create_campaign(spec(price_per_hour=0))
    invalid = spec(
        campaign_id="new-campaign", project_id="new-project", price_per_million_output_tokens=-0.25
    )
    with sqlite3.connect(database) as db:
        before = tuple(db.iterdump())
        with pytest.raises(ValueError, match="price_per_million_output_tokens"):
            queue.create_campaign(invalid)
        assert tuple(db.iterdump()) == before


def test_resume_keeps_distinct_prices_and_nonprice_json_immutable(tmp_path: Path) -> None:
    database = tmp_path / "queue.sqlite"
    campaign = spec(price_per_hour=2**53, options={"setting": 1})
    SQLiteStore(database).create_campaign(campaign)
    reopened = SQLiteStore(database)
    restored = reopened.campaign_spec("test")
    with sqlite3.connect(database) as db:
        before = tuple(db.iterdump())
        reopened.create_campaign(restored)
        # The next integer shares this float representation but is a different price.
        with pytest.raises(ValueError, match="different spec"):
            reopened.create_campaign(replace(restored, price_per_hour=2**53 + 1))
        with pytest.raises(ValueError, match="different spec"):
            reopened.create_campaign(replace(restored, options={"setting": 1.0}))
        assert tuple(db.iterdump()) == before
