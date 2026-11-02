"""Generate a visibly synthetic hill-climb overlay, without opening a controller."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from research_swarm.control.report_plot import render_hill_climb_svg
from research_swarm.domain.experiments import MetricDefinition
from research_swarm.state.database import JsonObject


def synthetic_run(name: str, scores: list[float], interval: float) -> JsonObject:
    origin = datetime(2026, 1, 1, tzinfo=UTC)
    observations = [
        {
            "id": f"{name}-receipt-{index}",
            "eligible": True,
            "comparison_digest": "synthetic-shared-contract",
            "primary_metric": {
                "name": "validation_loss",
                "value": score,
                "unit": "nats",
                "direction": "minimize",
            },
            "commit_event": {
                "id": f"{name}-event-{index}",
                "sequence": index + 1,
                "recorded_at": (origin + timedelta(hours=(index + 1) * interval)).isoformat(),
            },
            "data": {
                "candidate_id": f"{name}-candidate-{index}",
                "trial_id": f"{name}-trial-{index}",
                "observed_seed": index % 3,
            },
        }
        for index, score in enumerate(scores)
    ]
    return {
        "schema_version": "1.0",
        "campaign_id": name,
        "campaign_revision": 40,
        "evidence_through": 100,
        "campaign_created_at": origin.isoformat(),
        "captured_at": (origin + timedelta(hours=12)).isoformat(),
        "evaluation_count": len(observations),
        "evaluation_window": {"complete": True},
        "recent_evaluations": observations,
        "research": {"baseline": {"evaluation_id": observations[0]["id"]}},
    }


def main() -> None:
    runs = [
        synthetic_run("synthetic-run-A", [3.3, 3.2, 3.4, 3.0, 3.1, 2.9, 2.7, 2.8, 2.6, 2.65], 0.7),
        synthetic_run(
            "synthetic-run-B", [3.3, 3.4, 3.15, 3.2, 3.05, 3.1, 2.95, 2.9, 3.0, 2.8], 1.0
        ),
    ]
    output = Path(__file__).with_name("synthetic-demo.svg")
    output.write_text(
        render_hill_climb_svg(
            runs,
            comparison_digest="synthetic-shared-contract",
            metric=MetricDefinition(name="validation_loss", unit="nats", direction="minimize"),
            title="SYNTHETIC DEMO · comparable research runs",
            labels=("Synthetic search A", "Synthetic search B"),
        ),
        encoding="utf-8",
    )
    print(output)


if __name__ == "__main__":
    main()
