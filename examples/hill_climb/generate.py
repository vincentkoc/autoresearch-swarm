"""Generate a visibly synthetic hill-climb overlay, without opening a controller."""

import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

from research_swarm.api.campaign_ui import campaign_shell
from research_swarm.control.report_plot import render_hill_climb_svg
from research_swarm.control.report_view import render_report_html
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
    snapshot: JsonObject = {
        "schema_version": "1.0",
        "campaign_id": name,
        "campaign_revision": 40,
        "evidence_through": 100,
        "campaign_created_at": origin.isoformat(),
        "captured_at": (origin + timedelta(hours=12)).isoformat(),
        "evaluation_count": len(observations),
        "evaluation_window": {"complete": True},
        "recent_evaluations": observations,
        "campaign_state": "active",
        "budget": [
            {"dimension": "trials", "consumed": str(len(scores)), "reserved": "0", "uncertain": "0"}
        ],
        "research": {
            "baseline": {"evaluation_id": observations[0]["id"]},
            "objective": {"name": "Synthetic loss optimization, not a running experiment"},
            "candidates": {
                "total": len(scores),
                "complete": True,
                "items": [
                    {
                        "id": f"{name}-candidate-{index}",
                        "state": "evaluated",
                        "parent_ids": [f"{name}-candidate-{max(0, index - 2)}"] if index else [],
                        "parent_ids_complete": True,
                    }
                    for index in range(len(scores))
                ],
            },
            "trials": {
                "total": len(scores),
                "complete": True,
                "items": [
                    {
                        "id": f"{name}-trial-{index}",
                        "candidate_id": f"{name}-candidate-{index}",
                        "execution_outcome": "succeeded",
                        "evaluation_status": "valid",
                    }
                    for index in range(len(scores))
                ],
            },
            "attempts": {
                "total": 2,
                "complete": True,
                "items": [
                    {"id": f"{name}-attempt-{index}", "worker_id": f"synthetic-worker-{index}"}
                    for index in range(2)
                ],
            },
            "proposals": {
                "total": 1,
                "complete": True,
                "items": [{"id": f"{name}-proposal", "state": "responded"}],
            },
            "confirmed_cohorts": [],
        },
    }
    for suffix, outcome, status, parent in (
        ("failed", "failed", "pending", 1),
        ("unscored", "pending", "pending", 3),
    ):
        snapshot["research"]["candidates"]["items"].append(
            {
                "id": f"{name}-candidate-{suffix}",
                "state": "rejected" if suffix == "failed" else "proposed",
                "parent_ids": [f"{name}-candidate-{parent}"],
                "parent_ids_complete": True,
            }
        )
        snapshot["research"]["trials"]["items"].append(
            {
                "id": f"{name}-trial-{suffix}",
                "candidate_id": f"{name}-candidate-{suffix}",
                "execution_outcome": outcome,
                "evaluation_status": status,
            }
        )
    for section in ("candidates", "trials"):
        snapshot["research"][section]["total"] = len(snapshot["research"][section]["items"])
    return snapshot


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
    html = output.with_suffix(".html")
    campaign = synthetic_run(
        "synthetic-run-A",
        [3.4 - 0.85 * (1 - math.exp(-i / 12)) + (0.15 if i % 4 == 2 else 0) for i in range(48)],
        0.2,
    )
    best = 0
    candidates = campaign["research"]["candidates"]["items"]
    observations = campaign["recent_evaluations"]
    for i in range(1, 48):
        candidates[i]["parent_ids"] = [candidates[best]["id"]]
        if (
            observations[i]["primary_metric"]["value"]
            < observations[best]["primary_metric"]["value"]
        ):
            best = i
    html.write_text(
        render_report_html(
            [campaign],
            comparison_digest="synthetic-shared-contract",
            metric=MetricDefinition(name="validation_loss", unit="nats", direction="minimize"),
            title="SYNTHETIC DEMO · campaign cockpit",
            labels=("Synthetic loss optimization",),
            project_repository="https://github.com/vincentkoc/autoresearch-swarm",
        ),
        encoding="utf-8",
    )
    print(html)
    html.with_name("synthetic-server-shell.html").write_text(campaign_shell(), encoding="utf-8")


if __name__ == "__main__":
    main()
