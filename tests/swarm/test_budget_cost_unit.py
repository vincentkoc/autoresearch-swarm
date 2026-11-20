from pathlib import Path

from research_swarm.control.report_view import render_report_html
from research_swarm.domain.experiments import MetricDefinition
from research_swarm.domain.types import JsonObject
from research_swarm.swarm.reader import CampaignReader
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec


def test_budget_cost_unit_reaches_snapshots_and_archived_views(tmp_path: Path) -> None:
    database = tmp_path / "queue.sqlite"
    SQLiteStore(database).create_campaign(spec(max_cost=5))
    reader = CampaignReader(database)
    snapshot = reader.snapshot("test")
    meter = reader.budget("test")
    assert snapshot["meter"] == meter
    assert (meter["cost_unit"], meter["measured_cost"], meter["max_cost"]) == (
        "campaign currency",
        0,
        5,
    )
    metric = MetricDefinition.model_validate(snapshot["research"]["objective"]["primary_metric"])

    def view(archive: JsonObject) -> str:
        return render_report_html(
            [archive], comparison_digest=snapshot["comparison_digest"], metric=metric
        )

    label = "Budget &amp; capture details (costs in campaign currency)"
    assert label in view(snapshot)
    older_meter = {key: value for key, value in meter.items() if key != "cost_unit"}
    assert label in view({**snapshot, "meter": older_meter})
    credits = {**meter, "cost_unit": "fixture credits"}
    assert "costs in fixture credits" in view({**snapshot, "meter": credits})
    ledger = view({**snapshot, "meter": None, "budget": [{"dimension": "tokens", "consumed": 12}]})
    assert "Budget &amp; capture details</summary>" in ledger and "<td>tokens</td>" in ledger
