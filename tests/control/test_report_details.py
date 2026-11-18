from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import ReportCampaign
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from tests.control.test_evaluation_control import evaluated
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline


def report(pipeline: Pipeline, key: str, limit: int = 20) -> dict[str, Any]:
    return pipeline.client.command(
        "report.create",
        ReportCampaign(campaign_id=pipeline.campaign["id"], recent_limit=limit),
        idempotency_key=key,
    )["result"]


def test_unselected_campaign_does_not_infer_a_baseline_from_its_first_trial(
    pipeline: Pipeline,
) -> None:
    captured = report(pipeline, "unselected-report")["data"]["snapshot"]["research"]
    assert captured["baseline"] is None
    assert captured["pinned_evaluation_ids"] == []
    assert captured["trials"]["total"] == 1
    assert captured["trials"]["items"][0]["execution_outcome"] == "pending"


def test_oversized_report_does_not_publish_partial_snapshot_or_mutate_campaign(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    budget = pipeline.client.budget(campaign["id"])
    monkeypatch.setattr("research_swarm.control.reports.MAX_REPORT_BYTES", 1)
    with pytest.raises(ResearchError) as failure:
        report(pipeline, "oversized-report")
    assert failure.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert pipeline.client.list(RecordQuery(kind="report"))["items"] == []
    assert pipeline.client.get("campaign", campaign["id"]) == campaign
    assert pipeline.client.budget(campaign["id"]) == budget


def test_plot_uses_committed_event_time_and_exports_existing_report_read_only(
    pipeline: Pipeline, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from xml.etree import ElementTree

    from research_swarm.cli import main

    receipt = evaluated(pipeline)
    recorded = report(pipeline, "plot-report")
    snapshot = recorded["data"]["snapshot"]
    evaluation = snapshot["recent_evaluations"][0]
    event = next(
        item
        for item in pipeline.client.events(
            after=evaluation["commit_event"]["sequence"] - 1, limit=1
        )["events"]
        if item["type"] == "evaluation.recorded" and item["aggregate_id"] == receipt["id"]
    )
    assert evaluation["commit_event"] == {
        key: event[key] for key in ("id", "sequence", "recorded_at")
    }
    assert snapshot["campaign_created_at"] <= event["recorded_at"] <= snapshot["captured_at"]
    before = pipeline.client.get("campaign", pipeline.campaign["id"])
    output = tmp_path / "curve.svg"
    metric = evaluation["primary_metric"]
    assert (
        main(
            [
                "--state-dir",
                str(pipeline.runtime.root),
                "report",
                "plot",
                "--report-id",
                recorded["id"],
                "--comparison-digest",
                evaluation["comparison_digest"],
                "--metric",
                metric["name"],
                "--unit",
                metric["unit"],
                "--direction",
                metric["direction"],
                "--output",
                str(output),
            ]
        )
        == 0
    )
    root = ElementTree.fromstring(output.read_text(encoding="utf-8"))
    assert len(root.findall(".//{http://www.w3.org/2000/svg}circle")) == 1
    assert pipeline.client.get("campaign", pipeline.campaign["id"]) == before
    assert pipeline.client.get("report", recorded["id"]) == recorded
    assert '"format":"svg"' in capsys.readouterr().out


def test_evaluation_times_use_one_bounded_query_over_substantial_history() -> None:
    import sqlite3
    from types import SimpleNamespace
    from typing import cast

    from research_swarm.control.reports import _evaluation_commit_events
    from research_swarm.state.database import Transaction
    from research_swarm.state.sqlite import SQLiteSession

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("CREATE TABLE events(workspace_id TEXT, sequence INTEGER, payload TEXT)")
        connection.execute("CREATE UNIQUE INDEX event_sequence ON events(workspace_id, sequence)")
        for sequence in range(1, 3001):
            identifier = {11: "first", 21: "second", 31: "first", 2999: "after-watermark"}.get(
                sequence, "noise"
            )
            payload = {
                "id": f"event-{sequence}",
                "aggregate_id": identifier,
                "type": "evaluation.recorded",
                "sequence": sequence,
                "recorded_at": "2026-01-01T01:00:00.000Z",
            }
            connection.execute(
                "INSERT INTO events VALUES (?,?,?)",
                ("synthetic-workspace", sequence, canonical_bytes(payload).decode()),
            )
        queries = []
        connection.set_trace_callback(queries.append)
        transaction = cast(
            Transaction,
            SimpleNamespace(
                connection=SQLiteSession(connection), workspace_id="synthetic-workspace"
            ),
        )
        events = _evaluation_commit_events(
            transaction, ("first", "second", "after-watermark", "missing"), 100
        )
        assert len(queries) == 1
        assert {key: value["sequence"] for key, value in events.items()} == {
            "first": 11,
            "second": 21,
        }
        assert _evaluation_commit_events(transaction, (), 100) == {}
        assert len(queries) == 1
    finally:
        connection.close()
