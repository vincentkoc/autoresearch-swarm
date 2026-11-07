"""Offline campaign cockpit using only captured report evidence."""

from __future__ import annotations

import base64
import hashlib
import math
import re
from collections.abc import Sequence
from html import escape
from xml.etree.ElementTree import Element, SubElement, fromstring, tostring

from research_swarm.control.native_history import NativeHistory, project_native_history
from research_swarm.control.panel_layout import PANEL_SCRIPT, PANEL_STYLE
from research_swarm.control.report_plot import (
    MAX_OBSERVATIONS,
    PlotRun,
    _embedded_time_chart,
    _time,
    project_hill_climb,
)
from research_swarm.domain.experiments import MetricDefinition
from research_swarm.state.database import JsonObject

_STYLE = """
:root{color-scheme:light;--paper:#fafaf8;--ink:#21272a;--muted:#7e8588;--line:#e9eae8;
--teal:#348a92}*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);
font:12px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}header{height:52px;
padding:0 20px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px
solid var(--line);background:#fdfdfc;gap:20px}h1{font-size:16px;font-weight:600;margin:0}
h2{font-size:10px;letter-spacing:.08em;text-transform:uppercase;font-weight:650;margin:0}
h3{font-size:11px;font-weight:600;margin:16px 0 7px}p{margin:8px 0}small,.muted{color:var(--muted)}
main{--left-width:18%;--right-width:28.5%;display:grid;
grid-template-columns:var(--left-width) 8px minmax(0,1fr) 8px var(--right-width);
height:calc(100vh - 52px);
min-height:600px;margin:0}.sidebar{min-width:0;border-right:1px solid var(--line);overflow:auto}
.sidebar{grid-column:1}.splitter[data-resize=left]{grid-column:2}
.graph-panel{grid-column:3}.splitter[data-resize=right]{grid-column:4}.right{grid-column:5}
.sidebar-section{padding:18px 20px;border-bottom:1px solid var(--line)}
.sidebar h2{margin-bottom:14px}.graph-panel{display:flex;flex-direction:column;min-width:0;
overflow:hidden}.panel-heading{display:flex;align-items:center;justify-content:space-between;
padding:14px 16px;gap:12px;min-height:52px}.graph-body{flex:1;min-height:0;position:relative}
.graph-body>[data-panel]{height:100%;display:flex;flex-direction:column}
.graph-body [data-run]{height:100%;display:flex;flex-direction:column}
.graph-body .lineage{flex:1;min-height:0}.graph-body .graph-note{order:2}
.lineage svg{width:100%;height:100%;display:block}
.graph-note{padding:10px 20px;font-size:10px;color:var(--muted);border-top:1px solid var(--line)}
.graph-note p{margin:2px 0}.right{border-left:1px solid var(--line);display:grid;
grid-template-rows:44% 56%;min-width:0;min-height:0}.right-section{display:flex;
flex-direction:column;min-height:0;overflow:hidden}.right-section+.right-section{border-top:1px
solid var(--line)}.scroll{overflow:auto;min-height:0}.number,code,svg text{font-family:
ui-monospace,SFMono-Regular,Menlo,monospace}code{font-size:10px;overflow-wrap:anywhere}
table{width:100%;border-collapse:collapse;font-size:10px}td,th{text-align:left;border-bottom:1px
solid var(--line);padding:7px 3px;overflow-wrap:anywhere}th{font-weight:400;color:var(--muted)}
.tabs{display:flex;padding:2px;background:#f1f2ef;border-radius:5px;gap:2px}
button,select{font:inherit;font-size:10px;color:inherit;border:0;background:transparent;
padding:4px 10px;border-radius:3px;cursor:pointer}
button[aria-selected=true]{background:#fff;box-shadow:0 1px 3px #00000012}
select{border:1px solid var(--line);max-width:180px}.accent{color:var(--teal)}
.badge{font-size:10px;color:var(--muted)}details{margin:10px 0}summary{cursor:pointer;color:
var(--muted);font-size:10px}[hidden]{display:none!important}.run+.run{border-top:1px solid
var(--line);margin-top:18px;padding-top:8px}.leader{display:grid;grid-template-columns:22px
28px minmax(0,1fr) auto;align-items:center;gap:9px;padding:11px 15px;border-bottom:1px solid
#eff0ed}.leader.selected{background:#eff5f8}.rank{font-size:10px;color:#6f858f}
.avatar{display:flex;align-items:center;justify-content:center;width:27px;height:27px;
border-radius:50%;background:#e6edef;color:#42828a;font:9px ui-monospace,monospace}
.leader-name{font-size:11px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.leader-detail{font-size:10px;color:var(--muted);white-space:nowrap;overflow:hidden;
text-overflow:ellipsis}.leader-score{font-size:11px;align-self:start;padding-top:2px}
.activity-item{display:grid;grid-template-columns:24px minmax(0,1fr) auto;gap:10px;padding:
13px 15px;border-bottom:1px solid #eff0ed}.activity-icon{border:1px solid var(--line);
border-radius:50%;width:23px;height:23px;text-align:center;line-height:21px;color:#8b9597}
.activity-title{font-size:11px;overflow-wrap:anywhere}.activity-meta{font-size:10px;
color:var(--muted);margin-top:3px}.scope{padding:0 16px 10px;font-size:10px;color:var(--muted)}
.graph-selection{min-height:32px;padding:7px 20px;border-top:1px solid var(--line);
font:10px ui-monospace,monospace;color:var(--muted)}.chart-node{cursor:pointer}
.chart-node:focus-visible circle,.chart-node:hover circle{stroke:#243c44;stroke-width:2}
.score svg{width:100%;height:100%}
.time-plot{flex:1;min-height:120px}.time-evidence{flex:none;max-height:45%;overflow:auto;
border-top:1px solid var(--line);padding:10px 16px;font-size:11px;overflow-wrap:anywhere}
.time-evidence p{margin:3px 0}.time-run{border-left:3px solid;padding-left:9px;margin-top:10px}
.time-metric{padding:8px 16px;font-size:11px;overflow-wrap:anywhere}
.header-meta{font-size:11px;color:var(--muted);text-align:right}
a{color:var(--teal);text-decoration:none}a:hover{text-decoration:underline}
.repo-link{display:block;overflow-wrap:anywhere;margin:8px 0;font:10px/1.5
ui-monospace,SFMono-Regular,Menlo,monospace}.header-title{display:flex;gap:16px;
align-items:center;min-width:0}.header-actions{display:flex;gap:8px;align-items:center}
.rail-toggle{border:1px solid var(--line);white-space:nowrap}
.rail-toggle[aria-expanded=false]{background:#e6edef}.splitter{cursor:col-resize;
touch-action:none;position:relative;background:var(--paper)}
.splitter:after{content:"";position:absolute;top:45%;bottom:45%;left:3px;
border-left:2px solid #c7ccca}.splitter:hover,.splitter:focus-visible{background:#e6edef;
outline:1px solid var(--teal);outline-offset:-1px}
main[data-left-collapsed=true]{--left-width:0px!important}
main[data-right-collapsed=true]{--right-width:0px!important}
body.resizing{cursor:col-resize;user-select:none}
@media(min-width:1800px){main{--left-width:300px;--right-width:480px}}
@media(max-width:1000px){main{--left-width:185px;--right-width:270px}
.sidebar-section{padding:14px}.leader{padding:10px;gap:6px}.header-meta{font-size:10px}}
@media(max-width:720px){header{height:auto;min-height:52px;padding:12px;align-items:start}
.header-title{display:block;flex:1}.header-actions{flex:1;min-width:0;flex-wrap:wrap;justify-content:flex-end}
.header-title .repo-link{font-size:9px;margin:3px 0}.splitter{display:none}
h1{font-size:13px}main{display:flex;flex-direction:column;height:auto;min-height:0}
.sidebar{order:2;border:0}.graph-panel{order:0;height:600px}.right{order:1;border:0;
grid-template-rows:300px 350px}.graph-note{font-size:9px}.header-meta{max-width:140px}
.sidebar-section{padding:16px;border-top:1px solid var(--line)}}
"""
_STYLE += PANEL_STYLE

_SCRIPT = """
const tabs = [...document.querySelectorAll('[data-tab]')];
function showPanel(tab) {
  tabs.forEach(button => {
    const active = button === tab;
    button.setAttribute('aria-selected', String(active));
    button.tabIndex = active ? 0 : -1;
  });
  document.querySelectorAll('[data-panel]').forEach(panel => {
    panel.hidden = panel.dataset.panel !== tab.dataset.tab;
  });
  document.querySelector('.graph-selection').textContent =
    'Select a point to inspect its candidate and observed score.';
}
tabs.forEach((tab, index) => {
  tab.addEventListener('click', () => showPanel(tab));
  tab.addEventListener('keydown', event => {
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 :
      event.key === 'ArrowRight' ? (index + 1) % tabs.length :
      event.key === 'ArrowLeft' ? (index + tabs.length - 1) % tabs.length : null;
    if (next !== null) {
      event.preventDefault();
      showPanel(tabs[next]);
      tabs[next].focus();
    }
  });
});
function highlightLineage(node) {
  const graph = node.closest('.lineage');
  if (!graph || graph.querySelector('[data-cyclic=true]')) return;
  const ancestors = new Set([node.dataset.candidate]);
  const edges = [...graph.querySelectorAll('[data-parent]')];
  for (let i = 0; i < graph.querySelectorAll('[data-candidate]').length; i++) {
    edges.forEach(edge => {
      if (ancestors.has(edge.dataset.child)) ancestors.add(edge.dataset.parent);
    });
  }
  edges.forEach(edge => {
    const active = ancestors.has(edge.dataset.parent) && ancestors.has(edge.dataset.child);
    edge.setAttribute('stroke', active ? '#168273' : '#bbc1bd');
    edge.setAttribute('stroke-width', active ? '1.8' : '1');
    edge.setAttribute('stroke-opacity', active ? '.95' : edge.dataset.opacity);
    edge.dataset.highlighted = String(active);
  });
  graph.querySelectorAll('[data-candidate]').forEach(point => {
    const active = ancestors.has(point.dataset.candidate);
    const circle = point.querySelector('circle');
    circle.setAttribute('fill', active ? '#348a92' : '#b9c4c3');
    circle.setAttribute('fill-opacity', active ? '1' : '.4');
    circle.setAttribute('r', active ? '5' : '3.5');
    point.setAttribute('aria-pressed', String(point === node));
  });
  graph.parentElement.querySelector('[data-lineage-claim]').textContent =
    'Inspected candidate ancestry · not a promotion or winner claim';
}
const runSelector = document.querySelector('[data-run-selector]');
if (runSelector) runSelector.addEventListener('change', () => {
  document.querySelectorAll('[data-run]').forEach(run => {
    run.hidden = run.dataset.run !== runSelector.value;
  });
  document.querySelector('.graph-selection').textContent =
    'Select a point to inspect its candidate and observed score.';
});
document.querySelectorAll('[data-candidate], [data-observation]').forEach(node => {
  const select = () => {
    document.querySelector('.graph-selection').textContent = node.getAttribute('aria-label');
    highlightLineage(node);
  };
  node.addEventListener('click', select);
  node.addEventListener('keydown', event => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      select();
    }
  });
});
"""
_SCRIPT = PANEL_SCRIPT + _SCRIPT


def _e(value: object) -> str:
    return escape(str(value), quote=True)


def _page(research: JsonObject, name: str) -> tuple[list[JsonObject], str]:
    page = research.get(name) or {}
    if not isinstance(page, dict):
        raise ValueError("Captured detail page must be an object")
    rows = page.get("items", [])
    if (
        not isinstance(rows, list)
        or len(rows) > MAX_OBSERVATIONS
        or any(not isinstance(row, dict) for row in rows)
    ):
        raise ValueError("Report view requires bounded captured detail pages")
    total = page.get("total", "unknown")
    if total != "unknown" and (type(total) is not int or total < len(rows)):
        raise ValueError("Captured page total is inconsistent")
    if page.get("complete") is True and total != len(rows):
        raise ValueError("Complete detail page must include its total")
    if "included" in page and (type(page["included"]) is not int or page["included"] != len(rows)):
        raise ValueError("Captured page included count is inconsistent")
    status = "complete" if page.get("complete") is True else "INCOMPLETE / unknown"
    return rows, f"{len(rows)}/{total} captured; {status}"


def _leaders(snapshot: JsonObject, run: PlotRun, metric: MetricDefinition) -> tuple[set[str], str]:
    incumbent = snapshot.get("incumbent") or {}
    captured_metric = (snapshot["research"].get("objective") or {}).get("primary_metric") or {}
    if (
        incumbent.get("evidence_level") == "confirmed"
        and incumbent.get("eligibility") == "eligible"
        and tuple(captured_metric.get(key) for key in ("name", "unit", "direction"))
        == (metric.name, metric.unit, metric.direction)
    ):
        for cohort in snapshot["research"].get("confirmed_cohorts", []):
            selected = cohort.get("best_confirmed")
            if (
                selected
                and selected.get("is_current_incumbent") is True
                and selected.get("candidate_id") == incumbent.get("candidate_id")
                and cohort.get("history_complete") is True
                and cohort.get("status") == "selected"
                and cohort.get("comparison_digest") == snapshot.get("_view_comparison")
            ):
                return {selected["candidate_id"]}, "Confirmed current selection"
    if not run.points:
        return set(), "No eligible timed observations; no winner claim"
    best = (min if metric.direction == "minimize" else max)(p.value for p in run.points)
    return (
        {p.candidate_id for p in run.points if p.value == best},
        "Best observed path (ties retained); no confirmed winner",
    )


def _lineage(snapshot: JsonObject, run: PlotRun, metric: MetricDefinition, run_index: int) -> str:
    rows, scope = _page(snapshot["research"], "candidates")
    selected, claim = _leaders(snapshot, run, metric)
    by_id: dict[str, JsonObject] = {}
    for row in rows:
        identifier = row.get("id")
        parents = row.get("parent_ids", [])
        if not isinstance(identifier, str) or not identifier.strip() or identifier in by_id:
            raise ValueError("Captured candidates require unique nonempty string IDs")
        if (
            not isinstance(parents, list)
            or len(parents) > 64
            or any(not isinstance(parent, str) or not parent.strip() for parent in parents)
        ):
            raise ValueError("Candidate parents require at most 64 nonempty string IDs")
        by_id[identifier] = row
    edges = list(
        dict.fromkeys((parent, row["id"]) for row in rows for parent in row.get("parent_ids", []))
    )
    missing = [(parent, child) for parent, child in edges if parent not in by_id]
    known = [(parent, child) for parent, child in edges if parent in by_id]
    highlighted = set(selected)
    for _ in rows:
        highlighted.update(parent for parent, child in known if child in highlighted)
    depth: dict[str, int] = {}
    pending = set(by_id)
    for _ in rows:
        ready = sorted(
            identifier
            for identifier in pending
            if all(parent not in pending for parent, child in known if child == identifier)
        )
        if not ready:
            break
        for identifier in ready:
            depth[identifier] = max(
                (depth[parent] + 1 for parent, child in known if child == identifier), default=0
            )
            pending.remove(identifier)
    # Malformed cyclic source data is disclosed, never presented as a winning lineage.
    if pending:
        highlighted.clear()
        claim = "Cyclic captured lineage; selection path unavailable"
        depth.update(dict.fromkeys(pending, 0))
    # The horizontal axis is captured candidate order, not invented execution time.
    # A separate rail keeps unscored/failed branches visible without assigning a score.
    values: dict[str, float] = {}
    extremum = min if metric.direction == "minimize" else max
    for point in run.points:
        values[point.candidate_id] = extremum(
            values.get(point.candidate_id, point.value), point.value
        )
    # Normalize before subtraction: opposite finite extremes must not yield NaN positions.
    scale = max((abs(value) for value in values.values()), default=1.0) or 1.0
    normalized = [value / scale for value in values.values()]
    low, high = min(normalized, default=0.0), max(normalized, default=1.0)
    span = high - low or 0.1
    low, high = low - span * 0.12, high + span * 0.12
    positions: dict[str, tuple[float, float]] = {}
    for index, row in enumerate(rows):
        identifier = row["id"]
        x = 64 + index * 626 / max(1, len(rows) - 1)
        value = values.get(identifier)
        if value is None:
            y = 586.0
        else:
            fraction = (value / scale - low) / (high - low)
            y = 42 + (fraction if metric.direction == "minimize" else 1 - fraction) * 478
        positions[identifier] = (x, y)
    svg = Element(
        "svg",
        {
            "xmlns": "http://www.w3.org/2000/svg",
            "viewBox": "0 0 740 640",
            "role": "img",
            "aria-label": "Captured candidate parent relationships positioned by observed score",
            "data-cyclic": str(bool(pending)).lower(),
        },
    )
    marker = SubElement(
        SubElement(svg, "defs"),
        "marker",
        {
            "id": f"parent-arrow-{run_index}",
            "viewBox": "0 0 10 10",
            "refX": "14",
            "refY": "5",
            "markerWidth": "5",
            "markerHeight": "5",
            "orient": "auto",
        },
    )
    SubElement(marker, "path", {"d": "M 0 0 L 10 5 L 0 10 z", "fill": "context-stroke"})
    for tick in range(7):
        fraction = tick / 6
        y = 42 + fraction * 478
        value = low + (fraction if metric.direction == "minimize" else 1 - fraction) * (high - low)
        SubElement(
            svg,
            "line",
            {
                "x1": "60",
                "x2": "702",
                "y1": str(y),
                "y2": str(y),
                "stroke": "#f0f1ee",
                "stroke-dasharray": "2 5",
            },
        )
        SubElement(
            svg,
            "text",
            {
                "x": "49",
                "y": str(y + 4),
                "text-anchor": "end",
                "font-size": "10",
                "fill": "#909799",
            },
        ).text = f"{value * scale:.4g}" if math.isfinite(value * scale) else ""
    for tick in range(min(len(rows), 8)):
        index = round(tick * max(0, len(rows) - 1) / max(1, min(len(rows), 8) - 1))
        x = 64 + index * 626 / max(1, len(rows) - 1)
        SubElement(
            svg,
            "text",
            {
                "x": str(x),
                "y": "550",
                "text-anchor": "middle",
                "font-size": "10",
                "fill": "#909799",
            },
        ).text = str(index + 1)
    SubElement(
        svg,
        "text",
        {"x": "375", "y": "572", "text-anchor": "middle", "font-size": "10", "fill": "#909799"},
    ).text = "Experiment · captured candidate order"
    SubElement(
        svg,
        "text",
        {
            "x": "14",
            "y": "285",
            "transform": "rotate(-90 14 285)",
            "text-anchor": "middle",
            "font-size": "10",
            "fill": "#909799",
        },
    ).text = f"{metric.name} [{metric.unit}]"
    SubElement(
        svg, "text", {"x": "64", "y": "613", "font-size": "10", "fill": "#909799"}
    ).text = "Unscored / failed · no score inferred"
    latest_generation = max(depth.values(), default=0)
    for parent, child in known:
        age = latest_generation - depth[child] if not pending else 0
        opacity = max(0.08, 0.5 * 0.8**age)
        x1, y1 = positions[parent]
        x2, y2 = positions[child]
        active = parent in highlighted and child in highlighted
        edge = SubElement(
            svg,
            "path",
            {
                "d": f"M {x1:.2f} {y1:.2f} L {x2:.2f} {y2:.2f}",
                "fill": "none",
                "stroke": "#168273" if active else "#bbc1bd",
                "stroke-width": "1.8" if active else "1",
                "stroke-opacity": "0.95" if active else f"{opacity:.4f}",
                "data-parent": parent,
                "data-child": child,
                "data-generation-age": str(age),
                "data-opacity": f"{opacity:.4f}",
                "data-highlighted": str(active).lower(),
                "marker-end": f"url(#parent-arrow-{run_index})",
                "stroke-dasharray": "3 4"
                if parent not in values or child not in values
                else "none",
            },
        )
        SubElement(edge, "title").text = (
            f"Parent {parent} → child {child}; captured generation age {age}; "
            f"muted opacity {opacity:.4f}; highlighted opacity 0.95"
            if not pending
            else f"Parent {parent} → child {child}; cyclic lineage; age unavailable"
        )
    for identifier, row in by_id.items():
        x, y = positions[identifier]
        value = values.get(identifier)
        description = f"{identifier} · {row.get('state', 'unknown')} · " + (
            f"{metric.name}={value:.6g} {metric.unit}" if value is not None else "unscored"
        )
        node = SubElement(
            svg,
            "g",
            {
                "class": "chart-node",
                "data-candidate": identifier,
                "aria-label": description,
                "role": "button",
                "tabindex": "0",
            },
        )
        SubElement(node, "title").text = description
        SubElement(
            node,
            "circle",
            {
                "cx": str(x),
                "cy": str(y),
                "r": "5" if identifier in highlighted else "3.5",
                "fill": "#348a92" if identifier in highlighted else "#b9c4c3",
                "fill-opacity": "1" if identifier in highlighted else "0.4",
                "stroke": "#fafaf8",
                "stroke-width": "1.2",
            },
        )
    omitted = sum(row.get("parent_ids_complete") is False for row in rows)
    missing_list = "".join(
        f"<li><code>{_e(parent)}</code> → <code>{_e(child)}</code></li>"
        for parent, child in missing
    )
    return (
        f"<div class=graph-note><p class=accent data-lineage-claim>{_e(claim)}</p>"
        f"<p class=muted>{_e(scope)}. All {len(rows)} captured candidates shown; "
        f"{len(known)} captured parent edges, {len(missing)} missing-parent references; "
        f"{omitted} truncated parent lists.</p>"
        "<p>Arrows: explicit parent → child only. Older generations fade; "
        "highlighted ancestry stays visible.</p></div><div class=lineage>"
        + tostring(svg, encoding="unicode")
        + "</div>"
        + (
            "<details><summary>Missing parents (not invented)</summary>"
            f"<ul>{missing_list}</ul></details>"
            if missing
            else ""
        )
        + ("<p>No captured candidate lineage.</p>" if not rows else "")
    )


def _hill_climb(run: PlotRun, metric: MetricDefinition) -> str:
    """Reuse the bounded chronological projection, keeping each run's best independent."""
    svg = fromstring(_embedded_time_chart([run], metric))
    namespace = "{http://www.w3.org/2000/svg}"
    for dot in svg.iter(namespace + "circle"):
        title = dot.find(namespace + "title")
        dot.set("data-observation", dot.attrib["data-evaluation"])
        dot.set("tabindex", "0")
        dot.set("role", "button")
        dot.set("aria-label", title.text if title is not None and title.text else "Observation")
    for element in svg.iter():
        element.tag = element.tag.removeprefix(namespace)
    svg.set("xmlns", "http://www.w3.org/2000/svg")
    status = "complete" if run.history_complete else "INCOMPLETE"
    baseline = (
        f"Original baseline: {run.baseline.value:.6g} {metric.unit} (dashed)."
        if run.baseline
        else "Original baseline unavailable for this cohort/time window."
    )
    return (
        "<div class='score time-plot'>" + tostring(svg, encoding="unicode") + "</div>"
        "<div class=time-evidence>"
        "<p>Dots: chronological evaluation receipts. Steps: running best observed for this "
        "campaign only, not a confirmed winner or parent relationship.</p>"
        "<p>Last best held to capture time; idle tail is not a new observation.</p>"
        f"<p>{_e(baseline)}</p><p>{status}: {run.included}/{run.total} receipts captured; "
        f"{len(run.points)} plotted. Excluded: {run.ineligible} ineligible, "
        f"{run.nonmatching} other cohort/metric, {run.untimed} missing/invalid timing.</p>"
        f"<p>Origin: {_e(run.origin or 'unavailable')}; captured {_e(run.captured_at)}; "
        f"revision {run.revision}; event {run.evidence_through}.</p></div>"
    )


def render_report_html(
    snapshots: Sequence[JsonObject],
    *,
    comparison_digest: str,
    metric: MetricDefinition,
    title: str = "Swarm · campaign evidence",
    labels: Sequence[str] | None = None,
    project_repository: str | None = None,
) -> str:
    """Render captured evidence with local layout controls and optional project attribution."""
    project_link = ""
    if project_repository is not None:
        match = re.fullmatch(
            r"https://github[.]com/([A-Za-z0-9][A-Za-z0-9-]*)/([A-Za-z0-9_.-]+)/?",
            project_repository,
        )
        if not match or match[2] in {".", ".."}:
            raise ValueError("Project repository must be an HTTPS GitHub repository URL")
        owner, repo = match[1], match[2].removesuffix(".git")
        if not repo or repo in {".", ".."}:
            raise ValueError("Project repository must name a repository")
        project_link = (
            f"<a class=repo-link href='https://github.com/{owner}/{repo}' "
            f"target=_blank rel='noopener noreferrer' aria-label='Project repository on GitHub'>"
            f"Project ↗ {_e(owner)}/{_e(repo)}</a>"
        )
    runs = project_hill_climb(snapshots, comparison_digest=comparison_digest, metric=metric)
    if labels is not None and (
        len(labels) != len(runs) or any(not label.strip() for label in labels)
    ):
        raise ValueError("Supply one nonempty label per report, or omit labels")
    about, lineage, hill_climb, leaderboard, activity = [], [], [], [], []
    for index, (snapshot, run) in enumerate(zip(snapshots, runs, strict=True)):
        label = labels[index] if labels is not None else str(snapshot["campaign_id"])
        research = snapshot["research"]
        # A local copy supplies the selector, without mutating report evidence.
        view = {**snapshot, "_view_comparison": comparison_digest}
        selected, claim = _leaders(view, run, metric)
        objective = research.get("objective") or {}
        attempts, attempt_scope = _page(research, "attempts")
        workers = {row["worker_id"] for row in attempts if row.get("worker_id")}
        budget = "".join(
            f"<tr><td>{_e(item.get('dimension', 'unknown'))}</td>"
            f"<td class=number>{_e(item.get('consumed', 'unknown'))}</td>"
            f"<td class=number>{_e(item.get('reserved', 'unknown'))}</td>"
            f"<td class=number>{_e(item.get('uncertain', 'unknown'))}</td></tr>"
            for item in snapshot.get("budget", [])
        )
        about.append(
            f"<div class=run data-run='{index}'"
            + (" hidden>" if index else ">")
            + f"<h3>{_e(label)}</h3>"
            f"<p>{_e(objective.get('name', 'Objective unavailable'))}</p>"
            f"<code>{_e(run.campaign_id)}</code>"
            f"<p>State: {_e(snapshot.get('campaign_state', 'unknown'))}</p>"
            f"<p class=number>{_e(metric.name)} [{_e(metric.unit)}] · {_e(metric.direction)}</p>"
            f"<p class=badge>{_e(claim)}</p><p>Captured workers: "
            f"{len(workers) if workers else 'unavailable'} distinct historical IDs; "
            "not live concurrency.</p>"
            f"<details><summary>Budget &amp; capture details</summary>"
            f"<small>Attempts: {_e(attempt_scope)}</small><h3>Budget ledger</h3>"
            "<table><thead><tr><th>Unit</th><th>Used</th><th>Held</th><th>Uncertain</th></tr></thead>"
            f"<tbody>{budget}</tbody></table>"
            + ("<p class=muted>No captured budget ledger.</p>" if not budget else "")
            + "<details><summary>Capture provenance</summary>"
            f"<p class=muted>Captured {_e(run.captured_at)} · revision {run.revision} · "
            f"event {run.evidence_through}</p></details></details></div>"
        )
        lineage.append(
            f"<div data-run='{index}'"
            + (" hidden>" if index else ">")
            + _lineage(view, run, metric, index)
            + "</div>"
        )
        hill_climb.append(
            f"<div data-run='{index}'"
            + (" hidden>" if index else ">")
            + _hill_climb(run, metric)
            + "</div>"
        )
        candidates: dict[str, float] = {}
        extremum = min if metric.direction == "minimize" else max
        for point in run.points:
            candidates[point.candidate_id] = extremum(
                candidates.get(point.candidate_id, point.value), point.value
            )
        ordered = sorted(
            candidates.items(),
            key=lambda item: (item[1] if metric.direction == "minimize" else -item[1], item[0]),
        )
        rows = "".join(
            f"<div class='leader{' selected' if identifier in selected else ''}'>"
            f"<span class='rank number'>{rank:02d}</span><span class=avatar>{index + 1:02d}</span>"
            f"<div><div class=leader-name title='{_e(identifier)}'>"
            f"Experiment {_e(identifier.split('-candidate-')[-1])}"
            "</div>"
            f"<div class=leader-detail>{_e(label)} · best observed receipt"
            f"{' · highlighted path' if identifier in selected else ''}</div></div>"
            f"<span class='leader-score number'>{value:.6g}</span></div>"
            for rank, (identifier, value) in enumerate(ordered, 1)
        )
        baseline = run.baseline.value if run.baseline else None
        change = ""
        if ordered and baseline is not None:
            delta = (
                baseline - ordered[0][1]
                if metric.direction == "minimize"
                else ordered[0][1] - baseline
            )
            difference = f"{delta:.6g}" if math.isfinite(delta) else "outside finite numeric range"
            change = (
                "<p>Observed improvement vs original baseline: "
                f"<span class=number>{difference} {_e(metric.unit)}</span> "
                "(absolute, not a statistical win).</p>"
            )
        leaderboard.append(
            f"<div data-run='{index}'"
            + (" hidden>" if index else ">")
            + f"<div class=scope>{_e(label)} · {run.included}/{run.total} receipts captured. "
            "Observed scores, not confirmed rankings.</div>"
            + rows
            + ("<p class=scope>No eligible timed scores.</p>" if not rows else "")
            + (
                f"<details class=scope><summary>Baseline comparison</summary>{change}</details>"
                if change
                else ""
            )
            + "</div>"
        )
        trials, trial_scope = _page(research, "trials")
        trial_rows = "".join(
            f"<tr><td><code>{_e(row.get('id', 'unknown'))}</code></td>"
            f"<td>{_e(row.get('execution_outcome', 'unknown'))}</td>"
            f"<td>{_e(row.get('evaluation_status', 'unknown'))}</td></tr>"
            for row in trials
        )
        observations = "".join(
            f"<div class=activity-item><span class=activity-icon>↗</span><div>"
            f"<div class=activity-title>Experiment "
            f"{_e(point.candidate_id.split('-candidate-')[-1])} evaluated</div>"
            f"<div class=activity-meta>{_e(label)} · {point.hours:.3g}h · "
            f"event {point.event_sequence}"
            "</div></div>"
            f"<span class=number>{point.value:.6g}</span></div>"
            for point in reversed(run.points)
        )
        proposals, proposal_scope = _page(research, "proposals")
        proposal_rows = "".join(
            f"<li><code>{_e(row.get('id', 'unknown'))}</code> · "
            f"{_e(row.get('state', 'unknown'))}</li>"
            for row in proposals
        )
        activity.append(
            f"<div data-run='{index}'"
            + (" hidden>" if index else ">")
            + f"{observations}<details class=scope>"
            "<summary>All captured trials &amp; proposal actions</summary>"
            f"<p class=muted>Chronological commit times, not causal agent attribution.</p>"
            f"<h3>All captured trials</h3><small>{_e(trial_scope)}</small>"
            "<table><thead><tr><th>Trial</th><th>Execution</th><th>Evaluation</th></tr></thead>"
            f"<tbody>{trial_rows}</tbody></table><h3>Proposal actions</h3>"
            f"<small>{_e(proposal_scope)}; states only, timestamps unavailable.</small>"
            f"<ul>{proposal_rows}</ul></details></div>"
        )
    script_hash = base64.b64encode(hashlib.sha256(_SCRIPT.encode()).digest()).decode()
    selectors = "".join(
        f"<option value='{i}'>{_e(labels[i] if labels else runs[i].campaign_id)}</option>"
        for i in range(len(runs))
    )

    workers = {
        row["worker_id"]
        for snapshot in snapshots
        for row in _page(snapshot["research"], "attempts")[0]
        if row.get("worker_id")
    }
    trial_count = sum(len(_page(snapshot["research"], "trials")[0]) for snapshot in snapshots)
    return (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        "<meta name=viewport content='width=device-width,initial-scale=1'>"
        f"<meta http-equiv=Content-Security-Policy content=\"default-src 'none'; "
        f"style-src 'unsafe-inline'; script-src 'sha256-{script_hash}'; "
        "base-uri 'none'; form-action 'none'\">"
        f"<title>{_e(title)}</title><style>{_STYLE}</style></head><body>"
        f"<header><div class=header-title><h1>{_e(title)}</h1>"
        "<a class=repo-link href='https://github.com/vincentkoc/autoresearch-swarm' "
        "target=_blank rel='noopener noreferrer'>vincentkoc/autoresearch-swarm ↗</a></div>"
        "<div class=header-actions><button class=rail-toggle data-toggle=left "
        "aria-expanded=true aria-controls=about-rail>About</button>"
        "<button class=rail-toggle data-toggle=right aria-expanded=true "
        "aria-controls=activity-rail>Leaderboard / activity</button><span class=header-meta>"
        f"{len(workers)} captured worker IDs · {trial_count} captured trials"
        " · read-only capture</span></div></header>"
        "<main><aside class=sidebar id=about-rail><div class=sidebar-section><h2>About</h2>"
        + project_link
        + "".join(about)
        + "</div><div class=sidebar-section><h2>Evidence</h2>"
        "<p class=muted>Captured report only. No live concurrency inferred.</p>"
        "<details><summary>Comparison identity</summary>"
        f"<code>{_e(comparison_digest)}</code></details>"
        "<p class=muted>Observed extrema are not confirmed or official wins.</p></div></aside>"
        "<div class=splitter role=separator tabindex=0 data-resize=left "
        "aria-orientation=vertical aria-label='Resize About panel' "
        "aria-controls=about-rail aria-valuemin=0></div>"
        "<section class=graph-panel><div class=panel-heading>"
        "<div class=tabs role=tablist aria-label='Campaign visualization'>"
        "<button id=hill-tab role=tab data-tab=score aria-selected=true "
        "aria-controls=hill-panel>Hill climb</button>"
        "<button id=graph-tab role=tab data-tab=lineage aria-selected=false "
        "aria-controls=graph-panel tabindex=-1>Graph</button></div>"
        + (
            f"<select data-run-selector aria-label='Campaign'>{selectors}</select>"
            if len(runs) > 1
            else ""
        )
        + "</div><div class=graph-body>"
        "<div data-panel=score id=hill-panel role=tabpanel aria-labelledby=hill-tab>"
        + "".join(hill_climb)
        + "</div><div data-panel=lineage id=graph-panel role=tabpanel "
        "aria-labelledby=graph-tab hidden>" + "".join(lineage) + "</div></div>"
        "<div class=graph-selection>Select a point to inspect "
        "its candidate and observed score.</div>"
        "</section><div class=splitter role=separator tabindex=0 data-resize=right "
        "aria-orientation=vertical aria-label='Resize leaderboard and activity panel' "
        "aria-controls=activity-rail aria-valuemin=0></div>"
        "<aside class=right id=activity-rail><section class=right-section>"
        "<div class=panel-heading><h2>Leaderboard</h2><span class=badge>Observed score</span></div>"
        "<div class=scroll>" + "".join(leaderboard) + "</div></section>"
        "<section class=right-section><div class=panel-heading><h2>Activity</h2>"
        "<span class=badge>Evaluation receipts</span></div><div class=scroll>"
        + "".join(activity)
        + "</div></section></aside></main>"
        f"<script>{_SCRIPT}</script></body></html>\n"
    )


def render_native_history_html(history: NativeHistory, status: JsonObject) -> str:
    """The original timeline and workspace, with explicitly separate native evidence."""
    from datetime import UTC, datetime

    script = """
const picker = document.querySelector('#native-metric');
picker.addEventListener('change', () => {
  document.querySelectorAll('[data-native-metric]').forEach(section => {
    section.hidden = section.dataset.nativeMetric !== picker.value;
  });
});
picker.dispatchEvent(new Event('change'));
document.querySelectorAll('circle[data-evaluation]').forEach(point => {
  point.setAttribute('tabindex', '0');
  point.setAttribute('role', 'button');
  const text = point.querySelector('title').textContent;
  point.setAttribute('aria-label', text);
  point.addEventListener('click', () => {
    document.querySelector('#native-selection').textContent = text;
  });
});
setTimeout(() => location.reload(), 60000);
"""
    digest = base64.b64encode(hashlib.sha256(script.encode()).digest()).decode()
    captured = datetime.fromisoformat(history.captured_at.replace("Z", "+00:00"))
    age = max(0, (datetime.now(UTC) - captured).total_seconds())
    last_native_event = max(
        (
            row.observed_at or row.finished_at or ""
            for row in history.observations
            if row.observed_at or row.finished_at
        ),
        default="unavailable",
    )
    status_time = _time(status.get("updated_at_utc"))
    status_age = (datetime.now(UTC) - status_time).total_seconds() if status_time else None
    status_label = (
        "STALE coordinator status"
        if status_age is None or status_age > 300
        else "Recent coordinator status"
    )
    phase_label = str(status.get("phase", "unavailable")).replace("_", " ")
    metrics = sorted({metric for row in history.observations for metric in row.metrics})
    timed_cohorts = {
        row.cohort
        for row in history.observations
        if row.status == "completed" and row.metrics and (row.observed_at or row.finished_at)
    }
    cohorts = sorted(
        {row.cohort for row in history.observations},
        key=lambda cohort: (cohort not in timed_cohorts, cohort),
    )
    panels = []
    for metric in metrics:
        for cohort in cohorts:
            run, rows = project_native_history(history, cohort, metric)
            if not any(metric in row.metrics for row in rows):
                continue
            definition = MetricDefinition(name=metric, unit="degrees", direction="minimize")
            chart = _embedded_time_chart(
                [run],
                definition,
                time_label=f"Execution wall hours since {run.origin} (UTC)",
            )
            panels.append(
                f"<section data-native-metric='{_e(metric)}' class=native-cohort>"
                f"<h3>{_e(cohort)}</h3><div class='score native-chart'>{chart}</div>"
                f"<p>{len(run.points)} scored observations; {run.untimed} missing execution time. "
                "Native measurements do not create evaluator or promotion authority. "
                "Steps show best observed within this exact cohort and metric.</p></section>"
            )
    counters = status.get("live_research_counters", {})
    if not isinstance(counters, dict):
        counters = {}
    fitting = counters.get("current_fitting_workers", "unknown")
    source_tests = counters.get("source_only_synthetic_tests_reported_pass", "unknown")
    inventory = "".join(
        f"<tr><td>{_e(key.replace('_', ' '))}</td><td>{value}</td></tr>"
        for key, value in sorted(history.inventory.items())
    )
    activity = "".join(
        "<div class=activity-item><span class=activity-icon>·</span><div>"
        f"<div class=activity-title>{_e(row.study)} · {_e(row.arm)} · seed {_e(row.seed)}</div>"
        f"<div class=activity-meta>{_e(row.kind)} · {_e(row.status)}<br>"
        f"start {_e(row.started_at or 'unavailable')}<br>"
        f"finish {_e(row.finished_at or 'unavailable')}<br>"
        f"measurement event {_e(row.observed_at or row.finished_at or 'unavailable')}<br>"
        f"time basis {_e(row.timestamp_basis)}<br>"
        f"trial <code>{_e(row.trial_id)}</code><br>"
        f"attempt <code>{_e(row.attempt_id)}</code><br>"
        f"metrics {_e(row.metrics)}</div></div></div>"
        for row in sorted(
            history.observations, key=lambda item: item.finished_at or "", reverse=True
        )
    )
    options = "".join(f"<option value='{_e(metric)}'>{_e(metric)}</option>" for metric in metrics)
    limitations = "".join(f"<li>{_e(item)}</li>" for item in history.limitations)
    return (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        "<meta name=viewport content='width=device-width,initial-scale=1'>"
        "<meta http-equiv=Content-Security-Policy content=\"default-src 'none'; "
        f"script-src 'sha256-{digest}'; style-src 'unsafe-inline'; base-uri 'none'; "
        "form-action 'none'\"><title>Swarm · native experiment timeline</title>"
        f"<style>{_STYLE}.graph-panel{{overflow:auto;padding:0 16px}}"
        ".native-chart{height:420px}.native-cohort{border-bottom:1px solid var(--line)}"
        ".right{display:block;overflow:auto}"
        ".native-status{white-space:normal;overflow-wrap:anywhere}"
        ".native-selection{white-space:pre-wrap;overflow-wrap:anywhere}"
        "@media(max-width:700px){main{display:block;height:auto}.graph-panel{min-height:500px}"
        ".sidebar,.right{max-height:400px}.native-chart{height:300px}}</style></head><body>"
        "<header><h1>Swarm · native experiment timeline</h1>"
        "<a href=https://github.com/vincentkoc/autoresearch-swarm>Original Swarm renderer ↗</a>"
        "</header><main><aside class=sidebar><section class=sidebar-section>"
        "<h2>Coordinator</h2>"
        f"<p class=native-status>{_e(phase_label)}</p>"
        f"<p>Status updated: {_e(status.get('updated_at_utc', 'unavailable'))}</p>"
        f"<p>{status_label}</p>"
        f"<p>Last verified fitting workers: {_e(fitting)}</p>"
        f"<p>Source-only synthetic tests: {_e(source_tests)}"
        " (not scored experiments)</p>"
        "</section><section class=sidebar-section><h2>Captured evidence</h2>"
        f"<p>Snapshot {_e(history.captured_at)}<br>Age {age:.0f}s · "
        f"{'STALE snapshot' if age > 300 else 'recent snapshot'}</p>"
        f"<p>Last actual native event: {_e(last_native_event)}</p>"
        "<p>Page reloads each minute. A reload is not new optimizer progress.</p>"
        f"<table>{inventory}</table><details><summary>Scope and limitations</summary>"
        f"<ul>{limitations}</ul></details></section></aside>"
        "<div></div><section class=graph-panel><div class=panel-heading>"
        "<h2>Hill climb · real trials</h2>"
        f"<select id=native-metric aria-label='Metric'>{options}</select></div>"
        + "".join(panels)
        + "<h3>Selected observation</h3><pre id=native-selection class=native-selection>"
        "Select a dot to inspect its exact provenance.</pre></section><div></div>"
        "<aside class=right><div class=panel-heading><h2>All captured activity</h2></div>"
        f"{activity}</aside></main><script>{script}</script></body></html>"
    )
