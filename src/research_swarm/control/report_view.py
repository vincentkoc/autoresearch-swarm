"""Offline campaign cockpit using only captured report evidence."""

from __future__ import annotations

import base64
import hashlib
import math
import re
from collections.abc import Sequence
from html import escape
from xml.etree.ElementTree import Element, SubElement, tostring

from research_swarm.control.report_plot import (
    MAX_OBSERVATIONS,
    RUN_COLORS,
    PlotRun,
    _embedded_time_chart,
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
.chart-node:hover circle{stroke:#243c44;stroke-width:2}.score svg{width:100%;height:100%}
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
_SCRIPT = """
const workspace = document.querySelector('main');
const narrow = () => matchMedia('(max-width:720px)').matches;
const rail = side => document.querySelector(side === 'left' ? '.sidebar' : '.right');
function collapse(side, collapsed) {
  workspace.dataset[side + 'Collapsed'] = String(collapsed);
  rail(side).hidden = collapsed;
  document.querySelector('[data-toggle="' + side + '"]').setAttribute(
    'aria-expanded', String(!collapsed));
  syncDividers();
}
function syncDividers() {
  document.querySelectorAll('[data-resize]').forEach(handle => {
    const side = handle.dataset.resize;
    const width = Math.round(rail(side).getBoundingClientRect().width);
    handle.setAttribute('aria-valuenow', String(width));
    handle.setAttribute('aria-valuemax', String(Math.round(workspace.clientWidth * .45)));
    handle.setAttribute('aria-valuetext', rail(side).hidden ? 'Collapsed' : width + ' pixels');
  });
}
function resizeRail(side, width) {
  if (narrow()) return;
  const other = rail(side === 'left' ? 'right' : 'left').getBoundingClientRect().width;
  const maximum = Math.max(0, Math.min(workspace.clientWidth * .45,
    workspace.clientWidth - other - 296));
  const minimum = Math.min(160, maximum);
  collapse(side, false);
  workspace.style.setProperty('--' + side + '-width',
    Math.round(Math.max(minimum, Math.min(maximum, width))) + 'px');
  syncDividers();
}
document.querySelectorAll('[data-toggle]').forEach(button => {
  button.addEventListener('click', () => {
    const side = button.dataset.toggle;
    collapse(side, !rail(side).hidden);
  });
});
document.querySelectorAll('[data-resize]').forEach(handle => {
  const side = handle.dataset.resize;
  let drag = null;
  handle.addEventListener('pointerdown', event => {
    if (narrow() || event.button !== 0) return;
    event.preventDefault();
    drag = {x: event.clientX, width: rail(side).getBoundingClientRect().width};
    handle.setPointerCapture(event.pointerId);
    document.body.classList.add('resizing');
  });
  handle.addEventListener('pointermove', event => {
    if (drag) resizeRail(side, drag.width + (side === 'left' ? 1 : -1) *
      (event.clientX - drag.x));
  });
  const stop = () => { drag = null; document.body.classList.remove('resizing'); };
  handle.addEventListener('pointerup', stop);
  handle.addEventListener('pointercancel', stop);
  handle.addEventListener('lostpointercapture', stop);
  handle.addEventListener('keydown', event => {
    const width = rail(side).getBoundingClientRect().width;
    if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
      event.preventDefault();
      const delta = (event.key === 'ArrowRight' ? 20 : -20) * (side === 'left' ? 1 : -1);
      resizeRail(side, width + delta);
    } else if (event.key === 'Enter') {
      event.preventDefault();
      collapse(side, !rail(side).hidden);
    }
  });
});
addEventListener('resize', () => {
  // A desktop drag must not impose stale pixel widths after shrinking the viewport.
  workspace.style.removeProperty('--left-width');
  workspace.style.removeProperty('--right-width');
  syncDividers();
});
syncDividers();
document.querySelectorAll('[data-tab]').forEach(button => {
  button.addEventListener('click', () => {
    const target = button.dataset.tab;
    document.querySelectorAll('[data-panel]').forEach(panel => {
      panel.hidden = panel.dataset.panel !== target;
    });
    document.querySelectorAll('[data-tab]').forEach(tab => {
      tab.setAttribute('aria-selected', String(tab === button));
    });
  });
});
const runSelector = document.querySelector('[data-run-selector]');
if (runSelector) runSelector.addEventListener('change', () => {
  document.querySelectorAll('[data-run]').forEach(run => {
    run.hidden = run.dataset.run !== runSelector.value;
  });
});
document.querySelectorAll('[data-candidate]').forEach(node => {
  const select = () => {
    document.querySelector('.graph-selection').textContent = node.getAttribute('aria-label');
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


def _lineage(snapshot: JsonObject, run: PlotRun, metric: MetricDefinition) -> str:
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
    edges = [(parent, row["id"]) for row in rows for parent in row.get("parent_ids", [])]
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
        },
    )
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
    for parent, child in known:
        x1, y1 = positions[parent]
        x2, y2 = positions[child]
        active = parent in highlighted and child in highlighted
        SubElement(
            svg,
            "path",
            {
                "d": f"M {x1:.2f} {y1:.2f} L {x2:.2f} {y2:.2f}",
                "fill": "none",
                "stroke": "#168273" if active else "#bbc1bd",
                "stroke-width": "1.8" if active else "1",
                "stroke-opacity": "0.9" if active else "0.3",
                "stroke-dasharray": "3 4"
                if parent not in values or child not in values
                else "none",
            },
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
        f"<div class=graph-note><p class=accent>{_e(claim)}</p>"
        f"<p class=muted>{_e(scope)}. All {len(rows)} captured candidates shown; "
        f"{len(known)} captured parent edges, {len(missing)} missing-parent references; "
        f"{omitted} truncated parent lists.</p></div><div class=lineage>"
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


def _time_panel(
    runs: Sequence[PlotRun], metric: MetricDefinition, labels: Sequence[str] | None
) -> str:
    evidence = []
    for index, run in enumerate(runs):
        label = labels[index] if labels is not None else f"Run {index + 1}"
        scope = "Complete capture" if run.history_complete else "INCOMPLETE capture"
        baseline = (
            f"{run.baseline.value:.6g} {metric.unit}"
            if run.baseline
            else "unavailable for this cohort/time window"
        )
        evidence.append(
            f"<div class=time-run style='border-color:{RUN_COLORS[index]}'>"
            f"<strong>{_e(label)}</strong> · <code>{_e(run.campaign_id)}</code>"
            f"<p>{scope}: {run.included}/{run.total} receipts; {len(run.points)} plotted.</p>"
            f"<p>Excluded: {run.ineligible} ineligible, {run.nonmatching} other cohort/metric, "
            f"{run.untimed} missing/invalid timing.</p>"
            f"<p>Dashed reference · original baseline: {_e(baseline)}.</p>"
            "<details><summary>Time provenance</summary>"
            f"<p>Origin: {_e(run.origin or 'unavailable; no timestamps invented')}; "
            f"captured: {_e(run.captured_at)}; revision {run.revision}; "
            f"evidence through event {run.evidence_through}.</p></details></div>"
        )
    return (
        f"<div class=time-metric>{_e(metric.name)} [{_e(metric.unit)}] · "
        f"{_e(metric.direction)} · exact comparison cohort</div>"
        "<div class=time-plot>"
        + _embedded_time_chart(runs, metric)
        + "</div><section class=time-evidence aria-label='Time chart legend and evidence'>"
        "<p>● Trial receipts · ━ Best observed · ╌ Original baseline</p>"
        "<p class=muted>Best observed is held to capture time, including idle periods; "
        "not new evaluations or confirmed winners. Evaluation commit time, not training time.</p>"
        + "".join(evidence)
        + "</section>"
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
    chart = _time_panel(runs, metric, labels)
    about, lineage, leaderboard, activity = [], [], [], []
    for index, (snapshot, run) in enumerate(zip(snapshots, runs, strict=True)):
        label = labels[index] if labels is not None else f"Run {index + 1}"
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
            f"<div class=run><h3>{_e(label)}</h3>"
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
            + _lineage(view, run, metric)
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
            f"<div class=scope>{_e(label)} · {run.included}/{run.total} receipts captured. "
            "Observed scores, not confirmed rankings.</div>"
            + rows
            + ("<p class=scope>No eligible timed scores.</p>" if not rows else "")
            + (
                f"<details class=scope><summary>Baseline comparison</summary>{change}</details>"
                if change
                else ""
            )
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
            f"{observations}<details class=scope>"
            "<summary>All captured trials &amp; proposal actions</summary>"
            f"<p class=muted>Chronological commit times, not causal agent attribution.</p>"
            f"<h3>All captured trials</h3><small>{_e(trial_scope)}</small>"
            "<table><thead><tr><th>Trial</th><th>Execution</th><th>Evaluation</th></tr></thead>"
            f"<tbody>{trial_rows}</tbody></table><h3>Proposal actions</h3>"
            f"<small>{_e(proposal_scope)}; states only, timestamps unavailable.</small>"
            f"<ul>{proposal_rows}</ul></details>"
        )
    script_hash = base64.b64encode(hashlib.sha256(_SCRIPT.encode()).digest()).decode()
    selectors = "".join(
        f"<option value='{i}'>{_e(labels[i] if labels else f'Run {i + 1}')}</option>"
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
        " · offline snapshot</span></div></header>"
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
        "<section class=graph-panel><div class=panel-heading><h2>Graph</h2>"
        f"<select data-run-selector aria-label='Campaign'>{selectors}</select>"
        "<div class=tabs role=tablist aria-label='Evidence views'>"
        "<button role=tab data-tab=score aria-selected=false>Time</button>"
        "<button role=tab data-tab=lineage aria-selected=true>Tree</button></div></div>"
        "<div class=graph-body><div data-panel=score class=score hidden>"
        + chart
        + "</div><div data-panel=lineage>"
        + "".join(lineage)
        + "</div></div>"
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
