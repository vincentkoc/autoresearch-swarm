"""Offline force-layout inventory, independent of bounded scientific report observations.

Edges are directed source -> target. Only kind="causal" asserts ancestry;
"source-reference" is a non-causal association. Clusters affect layout only.
"""

from __future__ import annotations

import base64
import hashlib
import json
from html import escape
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from research_swarm.control.experiment_graph_assets import SCRIPT, STYLE

MAX_GRAPH_NODES = 5000
MAX_GRAPH_EDGES = 25000


class GraphNode(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    id: str = Field(min_length=1, max_length=1000)
    label: str = Field(max_length=2000)
    cluster: str = Field(max_length=1000)
    status: str = Field(max_length=1000)
    source_url: str | None = None
    summary: str = Field(max_length=20000)
    score: float | None = Field(default=None, allow_inf_nan=False)
    node_type: str | None = Field(default=None, max_length=1000)

    @field_validator("source_url")
    @classmethod
    def github_only(cls, value: str | None) -> str | None:
        if not value:
            return None
        try:
            parsed = urlsplit(value)
            valid = (
                parsed.scheme == "https"
                and parsed.netloc == "github.com"
                and parsed.path.startswith("/")
                and not any(ord(char) < 33 for char in value)
                and "\\" not in value
            )
        except ValueError:
            valid = False
        # Keep the node even when its source cannot safely become a link.
        return value if valid else None


class GraphEdge(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    source: str
    target: str
    kind: Literal["causal", "source-reference"]
    label: str | None = Field(default=None, max_length=2000)


class ExperimentGraph(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    nodes: list[GraphNode] = Field(max_length=MAX_GRAPH_NODES)
    edges: list[GraphEdge] = Field(max_length=MAX_GRAPH_EDGES)
    captured_at: str
    source_revision: str
    note: str = Field(default="", max_length=20000)

    @model_validator(mode="after")
    def validate_endpoints(self) -> ExperimentGraph:
        ids = {node.id for node in self.nodes}
        if len(ids) != len(self.nodes):
            raise ValueError("Node IDs must be unique")
        if any(edge.source not in ids or edge.target not in ids for edge in self.edges):
            raise ValueError("Every edge endpoint must identify a captured node")
        return self


def render_experiment_graph_html(
    dataset: ExperimentGraph | dict[str, object], title: str = "Swarm · experiment graph"
) -> str:
    """Render up to 5,000 captured inventory nodes without network dependencies.

    Reject oversized/invalid inventories rather than silently dropping evidence.
    Scores are displayed verbatim, never used as coordinates or compared/ranked.
    Missing or non-GitHub source URLs render as unavailable links.
    """
    graph = ExperimentGraph.model_validate(dataset)
    data = json.dumps(graph.model_dump(), ensure_ascii=True, allow_nan=False)
    data = data.replace("<", "\\u003c")
    script_hash = base64.b64encode(hashlib.sha256(SCRIPT.encode()).digest()).decode()
    style_hash = base64.b64encode(hashlib.sha256(STYLE.encode()).digest()).decode()
    policy = (
        "default-src 'none'; "
        f"script-src 'sha256-{script_hash}'; style-src 'sha256-{style_hash}'; "
        "worker-src blob:; base-uri 'none'; form-action 'none'"
    )
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f'<meta http-equiv="Content-Security-Policy" content="{policy}">'
        f"<title>{escape(title)}</title><style>{STYLE}</style></head><body>"
        f'<header><h1>{escape(title)}</h1><a href="https://github.com/vincentkoc/'
        'autoresearch-swarm" target="_blank" rel="noopener noreferrer">Swarm ↗</a></header>'
        '<main><aside class="inventory"><section><h2>Experiment inventory</h2>'
        '<p id="count" role="status" aria-live="polite"></p>'
        '<label for="search">Search captured experiments</label>'
        '<input id="search" type="search" placeholder="Name, ID, status, summary…">'
        '<label for="cluster">Category</label><select id="cluster">'
        '<option value="">All categories</option></select>'
        '<label class="check"><input id="group" type="checkbox">'
        'Group by category</label><p class="muted">Category proximity is layout only, '
        "not ancestry or scientific similarity. Disconnected components are packed "
        "separately.</p></section>"
        '<div id="node-list" aria-label="Matching experiments"></div>'
        '<button id="more" hidden>Show 100 more</button></aside>'
        '<section class="graph"><nav aria-label="Graph controls">'
        '<button id="fit" title="Fit all matching nodes">Fit</button>'
        '<button id="zoom-in" aria-label="Zoom in">+</button>'
        '<button id="zoom-out" aria-label="Zoom out">−</button>'
        '<button id="pause" aria-pressed="false">Pause</button>'
        '<button id="reset" title="Reset positions and resume layout">Reset layout</button>'
        '<span id="motion" role="status">Arranging…</span></nav>'
        '<div class="viewport"><canvas id="graph" tabindex="0" '
        'aria-label="Experiment graph. Drag to pan, scroll to zoom. '
        'Use inventory buttons to select nodes with the keyboard.">'
        "Select an experiment from the inventory to read its connections.</canvas>"
        '<div id="tooltip" role="tooltip" hidden></div>'
        '<p id="empty" hidden>No experiments match these filters.</p></div>'
        '<footer><span class="causal">━━▶</span> Explicit causal link '
        '<span class="reference">──</span> Source reference · not ancestry'
        "<p>Drag a node to pin it; double-click to release. Drag space to pan. "
        "Scroll / + / − to zoom; F to fit; Escape clears selection.</p></footer></section>"
        '<aside class="details"><section><h2>Inspect experiment</h2>'
        '<div id="detail" aria-live="polite"><p>Select a node or inventory item.</p>'
        '<p class="muted">The graph includes failed and unscored work. '
        "Position and color are not performance measurements.</p></div></section>"
        "<section><h2>Capture provenance</h2><dl><dt>Captured at</dt>"
        f"<dd>{escape(graph.captured_at)}</dd><dt>Source revision</dt>"
        f"<dd>{escape(graph.source_revision)}</dd></dl><p>{escape(graph.note)}</p>"
        '<p class="muted">Read-only inventory. Scores retain their source meaning; '
        "no cross-experiment ranking is inferred. This graph does not expand the "
        "native report measurement window.</p></section></aside></main>"
        f'<script id="graph-data" type="application/json">{data}</script>'
        f"<script>{SCRIPT}</script></body></html>"
    )
