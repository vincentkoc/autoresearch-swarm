"""Synthetic report projections: no fleet data or timing inferred from training."""

from copy import deepcopy
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import pytest

from research_swarm.cli import main
from research_swarm.control.report_plot import project_hill_climb, render_hill_climb_svg
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.experiments import MetricDefinition

METRIC = MetricDefinition(name="loss", unit="ratio", direction="minimize")


def snapshot(campaign: str = "synthetic-run", *, direction: str = "minimize") -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "campaign_id": campaign,
        "campaign_revision": 12,
        "evidence_through": 100,
        "captured_at": "2026-01-01T08:00:00.000Z",
        "campaign_created_at": "2026-01-01T00:00:00.000Z",
        "evaluation_count": 3,
        "evaluation_window": {"complete": True},
        "research": {"baseline": {"evaluation_id": "receipt-0"}},
        # Deliberately reverse storage order.
        "recent_evaluations": [
            {
                "id": f"receipt-{index}",
                "eligible": True,
                "comparison_digest": "synthetic-cohort",
                "primary_metric": {
                    "name": "loss",
                    "unit": "ratio",
                    "direction": direction,
                    "value": value,
                },
                "commit_event": {
                    "id": f"event-{index}",
                    "sequence": index + 1,
                    "recorded_at": f"2026-01-01T0{index + 1}:00:00.000Z",
                },
                "data": {
                    "candidate_id": f"candidate-{index}",
                    "trial_id": f"trial-{index}",
                    "observed_seed": index,
                },
            }
            for index, value in reversed(list(enumerate([4.0, 2.0, 3.0])))
        ],
    }


def render(*snapshots: dict[str, Any], metric: MetricDefinition = METRIC, **kwargs: Any) -> str:
    return render_hill_climb_svg(
        snapshots, comparison_digest="synthetic-cohort", metric=metric, **kwargs
    )


def elements(svg: str, name: str) -> list[ET.Element]:
    return ET.fromstring(svg).findall(f".//{{http://www.w3.org/2000/svg}}{name}")


@pytest.mark.parametrize("direction", ["minimize", "maximize"])
def test_sorted_trial_points_and_directional_steps_are_not_promotions(direction: str) -> None:
    source = snapshot(direction=direction)
    original = deepcopy(source)
    metric = METRIC.model_copy(update={"direction": direction})
    run = project_hill_climb([source], comparison_digest="synthetic-cohort", metric=metric)[0]
    assert [point.hours for point in run.points] == [1, 2, 3]
    assert [point.value for point in run.points] == [4, 2, 3]
    assert run.baseline == run.points[0]
    svg = render(source, metric=metric)
    path = elements(svg, "path")[0].attrib["d"].split()
    ys = [float(path[2]), float(path[6]), float(path[10])]
    # Screen y decreases as a score increases.
    assert ys[1:] == ([ys[1], ys[1]] if direction == "minimize" else [ys[0], ys[0]])
    if direction == "minimize":
        assert ys[1] > ys[0]
    assert len(elements(svg, "circle")) == 3
    assert "not confirmed winners" in svg
    assert source == original
    assert render(source, metric=metric) == svg


def test_overlay_excludes_ineligible_wrong_digest_and_exact_metric_mismatches() -> None:
    first, second = snapshot(), snapshot("synthetic-overlay")
    for update in (
        {"eligible": False},
        {"comparison_digest": "different-hardware"},
        {
            "primary_metric": {
                "name": "loss",
                "unit": "percent",
                "direction": "minimize",
                "value": 0.1,
            }
        },
        {
            "primary_metric": {
                "name": "accuracy",
                "unit": "ratio",
                "direction": "minimize",
                "value": 0.1,
            }
        },
        {
            "primary_metric": {
                "name": "loss",
                "unit": "ratio",
                "direction": "maximize",
                "value": 0.1,
            }
        },
    ):
        extra = deepcopy(second["recent_evaluations"][0])
        extra.update(update, id=f"extra-{len(second['recent_evaluations'])}")
        second["recent_evaluations"].append(extra)
    second["evaluation_count"] = 8
    svg = render(first, second)
    assert len(elements(svg, "circle")) == 6
    assert len(elements(svg, "path")) == 2
    assert "1 ineligible, 4 other cohort/metric" in svg
    second["recent_evaluations"][-1]["id"] = "receipt-0"
    with pytest.raises(ValueError, match="Duplicate evaluation"):
        render(second)
    with pytest.raises(ValueError, match="one snapshot per campaign"):
        render(first, first)


@pytest.mark.parametrize(
    "timestamp",
    [None, "bad", "2026-01-01T01:00:00", "2025-12-31T23:00:00Z", "2026-01-02T01:00:00Z"],
)
def test_missing_or_invalid_timestamp_never_falls_back_to_training_time(
    timestamp: str | None,
) -> None:
    source = snapshot()
    for row in source["recent_evaluations"]:
        row["commit_event"]["recorded_at"] = timestamp
        row["data"]["elapsed_ms"] = 1000
    svg = render(source)
    assert not elements(svg, "circle")
    assert "No eligible timed observations" in svg
    assert "3 missing/invalid timing" in svg


def test_legacy_empty_truncated_baseline_and_capture_watermark_are_honest() -> None:
    legacy = snapshot()
    legacy.pop("campaign_created_at")
    assert "legacy report; no timestamps invented" in render(legacy)
    assert not elements(render(legacy), "circle")
    source = snapshot()
    source["recent_evaluations"] = source["recent_evaluations"][:1]
    source["evaluation_window"]["complete"] = False
    svg = render(source)
    assert "INCOMPLETE capture: 1/3" in svg
    assert "baseline unavailable" in svg
    source["recent_evaluations"][0]["commit_event"]["sequence"] = 101
    assert not elements(render(source), "circle")
    source["recent_evaluations"] = []
    assert "No eligible timed observations" in render(source)


def test_ties_zero_range_xml_escaping_and_negative_scores() -> None:
    source = snapshot('synthetic <script>&"\x01')
    for row in source["recent_evaluations"]:
        row["primary_metric"]["value"] = -2.0
        row["commit_event"]["recorded_at"] = "2026-01-01T00:00:00.000Z"
    svg = render(source, title='SYNTHETIC <script>alert("not executable")</script>')
    assert not elements(svg, "script")
    assert len(elements(svg, "circle")) == 3
    assert "nan" not in svg.lower() and "Infinity" not in svg
    assert "&lt;script&gt;" in svg
    assert "0.0" in svg


def test_limits_and_nonfinite_values_are_rejected() -> None:
    with pytest.raises(ValueError):
        render()
    with pytest.raises(ValueError):
        render(*(snapshot(str(index)) for index in range(9)))
    source = snapshot()
    source["recent_evaluations"][0]["primary_metric"]["value"] = float("nan")
    with pytest.raises(ValueError):
        render(source)


def test_offline_cli_exports_without_opening_runtime_and_never_overwrites(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Offline plot must not open runtime")

    monkeypatch.setattr("research_swarm.cli._client", forbidden)
    source, compare, output = (
        tmp_path / "report.json",
        tmp_path / "compare.json",
        tmp_path / "out.svg",
    )
    source.write_bytes(canonical_bytes({"data": {"snapshot": snapshot()}}))
    compare.write_bytes(canonical_bytes(snapshot("comparison")))
    arguments = [
        "report",
        "plot",
        "--file",
        str(source),
        "--compare-file",
        str(compare),
        "--comparison-digest",
        "synthetic-cohort",
        "--metric",
        "loss",
        "--unit",
        "ratio",
        "--direction",
        "minimize",
        "--output",
        str(output),
    ]
    assert main(arguments) == 0
    assert len(elements(output.read_text(encoding="utf-8"), "circle")) == 6
    assert '"format":"svg"' in capsys.readouterr().out
    original = output.read_bytes()
    assert main(arguments) == 2
    assert output.read_bytes() == original
    capsys.readouterr()
    assert main([*arguments, "--compare-report", "not-an-offline-source"]) == 2


def test_capture_horizon_idle_tail_event_ties_and_optional_labels() -> None:
    source = snapshot()
    for row in source["recent_evaluations"]:
        row["commit_event"]["recorded_at"] = "2026-01-01T01:00:00.000Z"
    run = project_hill_climb([source], comparison_digest="synthetic-cohort", metric=METRIC)[0]
    assert [point.event_sequence for point in run.points] == [1, 2, 3]
    assert run.capture_hours == 8
    svg = render(source, labels=["SYNTHETIC baseline search"])
    assert elements(svg, "path")[0].attrib["d"].endswith(" H 1040.000")
    assert "SYNTHETIC baseline search" in svg
    assert "held to capture time, including idle periods" in svg
    assert "#f7f6f2" in svg and "#168273" in svg
    # A later report extends the shared x range, not the earlier run's tail.
    other = snapshot("second-run")
    other["captured_at"] = "2026-01-01T16:00:00.000Z"
    paths = elements(render(source, other), "path")
    assert paths[0].attrib["d"].endswith(" H 575.000")
    assert paths[1].attrib["d"].endswith(" H 1040.000")
    with pytest.raises(ValueError, match="one nonempty label"):
        render(source, labels=[])


def test_cli_verifies_supplied_digest_before_writing(tmp_path: Path) -> None:
    from research_swarm.domain.canonical import record_digest

    content = {"snapshot": snapshot(), "markdown": "Synthetic evidence only"}
    content["report_digest"] = record_digest("research.report", content)
    source, output = tmp_path / "report.json", tmp_path / "curve.svg"
    arguments = [
        "report",
        "plot",
        "--file",
        str(source),
        "--comparison-digest",
        "synthetic-cohort",
        "--metric",
        "loss",
        "--unit",
        "ratio",
        "--direction",
        "minimize",
        "--output",
        str(output),
    ]
    source.write_bytes(canonical_bytes({"data": content}))
    assert main(arguments) == 0
    output.unlink()
    content["snapshot"]["recent_evaluations"][0]["primary_metric"]["value"] = 0.001
    source.write_bytes(canonical_bytes({"data": content}))
    assert main(arguments) == 2
    assert not output.exists()
