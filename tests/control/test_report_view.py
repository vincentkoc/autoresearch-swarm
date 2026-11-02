"""Synthetic cockpit contracts: all captured paths, no elevated scientific claims."""

import base64
import hashlib
import math
import re
from copy import deepcopy
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pytest

from research_swarm.cli import main
from research_swarm.control.report_view import render_report_html
from research_swarm.domain.canonical import canonical_bytes
from tests.control.test_report_plot import METRIC, snapshot


def evidence() -> dict[str, Any]:
    value = snapshot()
    value["research"].update(
        objective={
            "name": "Synthetic optimization",
            "primary_metric": METRIC.model_dump(mode="json"),
        },
        candidates={
            "total": 4,
            "complete": False,
            "items": [
                {"id": "candidate-0", "state": "evaluated", "parent_ids": []},
                {"id": "candidate-1", "state": "evaluated", "parent_ids": ["candidate-0"]},
                {
                    "id": "candidate-2",
                    "state": "rejected",
                    "parent_ids": ["candidate-0", "missing-parent"],
                    "parent_ids_complete": False,
                },
            ],
        },
        attempts={
            "total": 8,
            "complete": False,
            "items": [
                {"id": "attempt-a", "worker_id": "synthetic-worker"},
                {"id": "attempt-b", "worker_id": "synthetic-worker"},
            ],
        },
        trials={
            "total": 4,
            "complete": False,
            "items": [
                {
                    "id": "failed-trial",
                    "execution_outcome": "failed",
                    "evaluation_status": "pending",
                },
            ],
        },
        proposals={
            "total": 1,
            "complete": True,
            "items": [{"id": "proposal", "state": "responded"}],
        },
    )
    return value


def render(value: dict[str, Any]) -> str:
    return render_report_html([value], comparison_digest="synthetic-cohort", metric=METRIC)


class Tags(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tags: list[tuple[str, dict[str, str | None]]] = []
        self.scripts: list[str] = []
        self.in_script = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append((tag, dict(attrs)))
        if tag == "script":
            self.in_script = True
            self.scripts.append("")

    def handle_endtag(self, tag: str) -> None:
        if tag == "script":
            self.in_script = False

    def handle_data(self, data: str) -> None:
        if self.in_script:
            self.scripts[-1] += data


def test_all_captured_branches_failure_and_workers_are_honest() -> None:
    value = evidence()
    original = deepcopy(value)
    html = render(value)
    assert value == original
    assert "All 3 captured candidates shown; 2 captured parent edges" in html
    assert "1 missing-parent references; 1 truncated parent lists" in html
    assert "3/4 captured; INCOMPLETE" in html
    assert "missing-parent" in html and "rejected" in html and "failed-trial" in html
    assert "Best observed path (ties retained); no confirmed winner" in html
    assert "1 distinct historical IDs; not live concurrency" in html
    assert "--paper:#fafaf8" in html
    assert "data-tab=score" in html and "data-tab=lineage" in html
    assert "Cyclic captured lineage" not in html
    lineage = Tags()
    lineage.feed(html.split("<div data-panel=lineage>", 1)[1])
    edge_colors = [attrs["stroke"] for tag, attrs in lineage.tags if tag == "path"]
    assert edge_colors.count("#168273") == 1  # Actual root → best-observed child.
    assert edge_colors.count("#bbc1bd") == 1  # Rejected side branch remains visible.
    assert "data-tab=lineage aria-selected=true" in html
    assert "data-panel=score class=score hidden" in html
    # All observed candidates are present; order is score-driven within the leaderboard.
    table = html.split("<h2>Leaderboard</h2>", 1)[1].split("<h2>Activity", 1)[0]
    assert table.index("candidate-1") < table.index("candidate-2") < table.index("candidate-0")


@pytest.mark.parametrize("direction", ["minimize", "maximize"])
def test_tree_uses_real_scores_and_keeps_unscored_off_the_score_axis(direction: str) -> None:
    value = evidence()
    metric = METRIC.model_copy(update={"direction": direction})
    for observation in value["recent_evaluations"]:
        observation["primary_metric"]["direction"] = direction
    value["research"]["candidates"]["items"].append(
        {"id": "unscored", "state": "failed", "parent_ids": ["candidate-0"]}
    )
    html = render_report_html([value], comparison_digest="synthetic-cohort", metric=metric)
    tags = Tags()
    tags.feed(html.split("<div data-panel=lineage>", 1)[1])
    circles = [attrs for tag, attrs in tags.tags if tag == "circle"]
    assert len(circles) == 4
    # Fixture scores are 4, 2, 3 in captured candidate order.
    ys = [float(attrs["cy"]) for attrs in circles[:3]]
    assert (ys[1] < ys[2] < ys[0]) if direction == "minimize" else (ys[0] < ys[2] < ys[1])
    assert float(circles[-1]["cy"]) > max(ys)  # Separate unscored rail, not zero.
    assert "Experiment · captured candidate order" in html
    assert "Unscored / failed · no score inferred" in html


@pytest.mark.parametrize("direction", ["minimize", "maximize"])
@pytest.mark.parametrize("magnitude", [1e308, float.fromhex("0x1.fffffffffffffp+1023")])
def test_tree_extreme_finite_scores_have_finite_ordered_geometry(
    direction: str, magnitude: float
) -> None:
    value = evidence()
    metric = METRIC.model_copy(update={"direction": direction})
    scores = {"candidate-0": magnitude, "candidate-1": -magnitude, "candidate-2": 0.0}
    for observation in value["recent_evaluations"]:
        observation["primary_metric"].update(
            direction=direction, value=scores[observation["data"]["candidate_id"]]
        )
    html = render_report_html([value], comparison_digest="synthetic-cohort", metric=metric)
    tags = Tags()
    tags.feed(html.split("<div data-panel=lineage>", 1)[1])
    circles = [attrs for tag, attrs in tags.tags if tag == "circle"]
    ys = [float(attrs["cy"] or "") for attrs in circles]
    assert all(math.isfinite(y) and 42 <= y <= 520 for y in ys)
    assert (ys[1] < ys[2] < ys[0]) if direction == "minimize" else (ys[0] < ys[2] < ys[1])
    for _, attrs in tags.tags:
        for coordinate in ("cx", "cy", "x", "y", "x1", "x2", "y1", "y2"):
            if coordinate in attrs:
                assert math.isfinite(float(attrs[coordinate] or ""))
        if "d" in attrs:
            assert not re.search(r"nan|inf", attrs["d"] or "", re.IGNORECASE)
    assert ">inf<" not in html and ">nan<" not in html


def test_embedded_time_chart_keeps_overlay_legend_and_bounded_evidence_outside_svg() -> None:
    first, second = evidence(), evidence()
    second["campaign_id"] = "other-synthetic-run"
    second["evaluation_count"] = 10
    second["evaluation_window"]["complete"] = False
    second["recent_evaluations"][0]["eligible"] = False
    second["recent_evaluations"][1]["primary_metric"]["unit"] = "other"
    second["recent_evaluations"][2]["commit_event"] = None
    html = render_report_html(
        [first, second],
        comparison_digest="synthetic-cohort",
        metric=METRIC,
        labels=["Synthetic A", "Synthetic B <unavailable>"],
    )
    time = html.split("<div data-panel=score class=score hidden>", 1)[1].split(
        "<div data-panel=lineage>", 1
    )[0]
    svg = time.split("<svg", 1)[1].split("</svg>", 1)[0]
    assert 'viewBox="0 0 740 440"' in svg
    assert "RESEARCH SWARM / EVIDENCE VIEW" not in svg
    assert "Synthetic A" not in svg and "capture" not in svg
    notes = time.split("</svg>", 1)[1]
    assert "Synthetic A" in notes and "Synthetic B &lt;unavailable&gt;" in notes
    assert "Complete capture: 3/3 receipts; 3 plotted." in notes
    assert "INCOMPLETE capture: 3/10 receipts; 0 plotted." in notes
    assert "Excluded: 1 ineligible, 1 other cohort/metric, 1 missing/invalid timing." in notes
    assert "original baseline: 4 ratio" in notes
    assert "original baseline: unavailable for this cohort/time window" in notes
    assert "not new evaluations or confirmed winners" in notes
    assert "Best observed" in notes and "Original baseline" in notes
    assert "loss [ratio] · minimize" in time


@pytest.mark.parametrize("labels", [[], ["a", "b"], [" "]])
def test_time_embedding_retains_label_validation(labels: list[str]) -> None:
    with pytest.raises(ValueError, match="one nonempty label"):
        render_report_html(
            [evidence()], comparison_digest="synthetic-cohort", metric=METRIC, labels=labels
        )


def test_confirmed_branch_requires_current_qualified_matching_cohort() -> None:
    value = evidence()
    value["incumbent"] = {
        "candidate_id": "candidate-2",
        "evidence_level": "confirmed",
        "eligibility": "eligible",
    }
    cohort = {
        "comparison_digest": "synthetic-cohort",
        "history_complete": True,
        "status": "selected",
        "best_confirmed": {"candidate_id": "candidate-2", "is_current_incumbent": True},
    }
    value["research"]["confirmed_cohorts"] = [cohort]
    assert "Confirmed current selection" in render(value)
    for key, invalid in (
        ("comparison_digest", "other-hardware"),
        ("history_complete", False),
        ("status", "ambiguous"),
    ):
        changed = deepcopy(value)
        changed["research"]["confirmed_cohorts"][0][key] = invalid
        assert "Confirmed current selection" not in render(changed)
    for field, mismatch in (("name", "other"), ("unit", "other"), ("direction", "maximize")):
        different_metric = deepcopy(value)
        different_metric["research"]["objective"]["primary_metric"][field] = mismatch
        assert "Confirmed current selection" not in render(different_metric)
    missing_metric = deepcopy(value)
    del missing_metric["research"]["objective"]["primary_metric"]
    assert "Confirmed current selection" not in render(missing_metric)
    value["incumbent"]["eligibility"] = "suspended"
    assert "Confirmed current selection" not in render(value)


def test_html_escapes_source_fields_without_executable_json_or_external_assets() -> None:
    value = evidence()
    attack = '</script><img src=x onerror="alert(1)"><script>alert(2)</script>'
    value["research"]["objective"]["name"] = attack
    value["research"]["candidates"]["items"][0]["state"] = attack
    value["research"]["proposals"]["items"][0]["state"] = attack
    value["research"]["trials"]["items"][0]["execution_outcome"] = attack
    html = render_report_html(
        [value], comparison_digest="synthetic-cohort", metric=METRIC, title=attack, labels=[attack]
    )
    tags = Tags()
    tags.feed(html)
    assert sum(tag == "script" for tag, _ in tags.tags) == 1
    assert not any(tag in {"img", "iframe", "link", "object"} for tag, _ in tags.tags)
    assert not any(key.startswith("on") for _, attrs in tags.tags for key in attrs)
    assert "script-src 'sha256-" in html
    expected = base64.b64encode(hashlib.sha256(tags.scripts[0].encode()).digest()).decode()
    csp = next(
        attrs["content"]
        for tag, attrs in tags.tags
        if tag == "meta" and attrs.get("http-equiv") == "Content-Security-Policy"
    )
    assert csp is not None and f"script-src 'sha256-{expected}'" in csp
    assert "default-src 'none'" in csp
    assert "'unsafe-inline'" not in csp.split("script-src", 1)[1]
    assert "&lt;/script&gt;" in html
    assert "fetch(" not in html and "innerHTML" not in html


def test_empty_missing_details_and_cycles_do_not_claim_winners() -> None:
    value = snapshot()
    value["recent_evaluations"] = []
    value["evaluation_window"]["complete"] = False
    html = render(value)
    assert "No eligible timed observations; no winner claim" in html
    assert "No captured candidate lineage." in html
    assert "Captured workers: unavailable" in html
    value = evidence()
    value["research"]["candidates"]["items"][0]["parent_ids"] = ["candidate-1"]
    assert "Cyclic captured lineage; selection path unavailable" in render(value)


def test_offline_html_cli_shares_selection_and_output_integrity(tmp_path: Path) -> None:
    source, output = tmp_path / "snapshot.json", tmp_path / "view.html"
    source.write_bytes(canonical_bytes(evidence()))
    assert (
        main(
            [
                "report",
                "view",
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
                "--label",
                "Synthetic local run",
                "--project-repository",
                "https://github.com/example/research-project",
            ]
        )
        == 0
    )
    html = output.read_text(encoding="utf-8")
    assert html.startswith("<!doctype html>")
    assert "Synthetic local run" in html
    assert "https://github.com/example/research-project" in html


@pytest.mark.parametrize(
    "malformation",
    [
        "duplicate",
        "missing-id",
        "numeric-id",
        "empty-id",
        "many-parents",
        "string-parents",
        "numeric-parent",
        "complete-count",
        "included-count",
    ],
)
def test_malformed_lineage_is_rejected_not_collapsed(malformation: str) -> None:
    value = evidence()
    page = value["research"]["candidates"]
    row = page["items"][0]
    if malformation == "duplicate":
        page["items"][1]["id"] = row["id"]
    elif malformation == "missing-id":
        del row["id"]
    elif malformation == "numeric-id":
        row["id"] = 12
    elif malformation == "empty-id":
        row["id"] = " "
    elif malformation == "many-parents":
        row["parent_ids"] = [f"parent-{index}" for index in range(65)]
    elif malformation == "string-parents":
        row["parent_ids"] = "not-a-list"
    elif malformation == "numeric-parent":
        row["parent_ids"] = [12]
    elif malformation == "complete-count":
        page["complete"] = True
    elif malformation == "included-count":
        page["included"] = 52
    with pytest.raises(ValueError):
        render(value)


def test_project_link_is_optional_and_normalized_without_mutating_evidence() -> None:
    value = evidence()
    original = deepcopy(value)
    html = render_report_html(
        [value],
        comparison_digest="synthetic-cohort",
        metric=METRIC,
        project_repository="https://github.com/example/research-project.git",
    )
    assert value == original
    tags = Tags()
    tags.feed(html)
    links = [attrs for tag, attrs in tags.tags if tag == "a"]
    assert {attrs["href"] for attrs in links} == {
        "https://github.com/vincentkoc/autoresearch-swarm",
        "https://github.com/example/research-project",
    }
    assert all(attrs.get("rel") == "noopener noreferrer" for attrs in links)
    assert "Project repository on GitHub" not in render(value)


@pytest.mark.parametrize(
    "url",
    [
        "javascript:alert(1)",
        "https://github.com.evil.test/example/repo",
        "https://user:secret@github.com/example/repo",
        "https://github.com/example/repo?token=x",
        "https://github.com/example/..",
        "https://github.com/example/.git",
        "https://github.com/example/repo/pull/1",
    ],
)
def test_project_link_rejects_non_repository_urls(url: str) -> None:
    with pytest.raises(ValueError, match="repository"):
        render_report_html(
            [evidence()],
            comparison_digest="synthetic-cohort",
            metric=METRIC,
            project_repository=url,
        )
