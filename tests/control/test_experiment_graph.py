"""Synthetic inventory contracts, independent of report measurement limits."""

import json
from copy import deepcopy
from html.parser import HTMLParser
from typing import Any

import pytest

from research_swarm.control.experiment_graph import (
    MAX_GRAPH_NODES,
    ExperimentGraph,
    render_experiment_graph_html,
)
from research_swarm.control.report_plot import MAX_OBSERVATIONS


def dataset(count: int = 240) -> dict[str, Any]:
    return {
        "captured_at": "2026-01-01T00:00:00Z",
        "source_revision": "synthetic-revision",
        "nodes": [
            {
                "id": f"experiment-{i}",
                "label": f"Experiment {i}",
                "cluster": f"Category {i % 6}",
                "status": "failed" if i % 7 == 0 else "recorded",
                "source_url": f"https://github.com/example/research/blob/main/run-{i}.md",
                "summary": f"Synthetic experiment {i}; not scientific evidence.",
                "score": None if i % 3 == 0 else i / 10,
            }
            for i in range(count)
        ],
        "edges": [
            {
                "source": f"experiment-{i - 1}",
                "target": f"experiment-{i}",
                "kind": "source-reference" if i % 4 == 0 else "causal",
            }
            for i in range(1, count)
        ],
    }


class Scripts(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.scripts: list[dict[str, str | None]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "script":
            self.scripts.append(dict(attrs))


def test_full_inventory_does_not_change_native_measurement_window() -> None:
    source = dataset(MAX_GRAPH_NODES)
    original = deepcopy(source)
    html = render_experiment_graph_html(source)
    data = html.split('type="application/json">')[1].split("</script>")[0]
    assert len(json.loads(data)["nodes"]) == 5000
    assert MAX_OBSERVATIONS == 52
    assert source == original
    assert html == render_experiment_graph_html(source)
    assert "Category proximity is layout only" in html
    assert "Source reference · not ancestry" in html


def test_script_breakout_and_title_are_escaped_without_external_assets() -> None:
    source = dataset(1)
    source["nodes"][0]["summary"] = '</script><script>alert("bad")</script>'
    source["nodes"][0]["label"] = "<img src=x onerror=alert(1)>"
    html = render_experiment_graph_html(source, title="<unsafe>")
    parser = Scripts()
    parser.feed(html)
    assert len(parser.scripts) == 2
    assert all("src" not in script for script in parser.scripts)
    assert "<title>&lt;unsafe&gt;</title>" in html
    assert '</script><script>alert("bad")' not in html
    assert "default-src 'none'" in html
    assert "sha256-" in html


@pytest.mark.parametrize(
    "url",
    [
        "javascript:alert(1)",
        "https://github.com.evil.test/x",
        "https://github.com@evil.test/x",
        "https://user@github.com/x",
        "http://github.com/x",
        "https://github.com:443/x",
        "https://github.com\\@evil.test/x",
        "https://github.com/\nx",
    ],
)
def test_only_exact_https_github_sources_become_links(url: str) -> None:
    source = dataset(1)
    source["nodes"][0]["source_url"] = url
    assert ExperimentGraph.model_validate(source).nodes[0].source_url is None


def test_invalid_inventory_is_rejected_not_truncated_or_reinterpreted() -> None:
    source = dataset(2)
    source["edges"][0]["target"] = "missing"
    with pytest.raises(ValueError, match="endpoint"):
        render_experiment_graph_html(source)
    source = dataset(2)
    source["nodes"][1]["id"] = source["nodes"][0]["id"]
    with pytest.raises(ValueError, match="unique"):
        render_experiment_graph_html(source)
    source = dataset(2)
    source["edges"][0]["kind"] = "category"
    with pytest.raises(ValueError, match="kind"):
        render_experiment_graph_html(source)
    with pytest.raises(ValueError, match="at most 5000"):
        render_experiment_graph_html(dataset(5001))
    source = dataset(1)
    source["nodes"][0]["score"] = float("nan")
    with pytest.raises(ValueError, match="finite"):
        render_experiment_graph_html(source)


def test_empty_capture_is_a_valid_inspectable_document() -> None:
    assert "No experiments match" in render_experiment_graph_html(dataset(0))
