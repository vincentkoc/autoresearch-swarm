"""Offline campaign cockpit using only captured report evidence."""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Sequence
from html import escape
from xml.etree.ElementTree import Element, SubElement, tostring

from research_swarm.control.report_plot import (
    MAX_OBSERVATIONS,
    PlotRun,
    project_hill_climb,
    render_hill_climb_svg,
)
from research_swarm.domain.experiments import MetricDefinition
from research_swarm.state.database import JsonObject

_STYLE = """
:root{color-scheme:light;--paper:#f7f6f2;--ink:#23312e;--muted:#6c7773;--line:#dedfd7;
--teal:#168273}*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);
font:14px/1.5 system-ui,"DejaVu Sans",sans-serif}header{padding:26px 32px;border-bottom:1px
solid var(--line)}h1{font-size:26px;font-weight:550;margin:5px 0}h2{font-size:16px;
font-weight:600}h3{font-size:13px;margin:18px 0 8px}p{margin:8px 0}.kicker,small,.muted{
color:var(--muted)}.kicker{letter-spacing:.16em;font-size:11px}main{display:grid;
grid-template-columns:minmax(220px,1fr) minmax(500px,3fr) minmax(260px,1.2fr);gap:20px;
padding:24px;max-width:1800px;margin:auto}.card{background:#fffefa;border:1px solid
var(--line);border-radius:8px;padding:18px;margin-bottom:18px;min-width:0}aside,section{
min-width:0}code,.number,th,svg text{font-family:ui-monospace,"DejaVu Sans Mono",monospace}
code{font-size:11px;overflow-wrap:anywhere}table{width:100%;border-collapse:collapse;
font-size:12px}td,th{text-align:left;border-bottom:1px solid var(--line);padding:8px 4px;
overflow-wrap:anywhere}th{font-size:10px;color:var(--muted);font-weight:400}.scroll{
max-height:600px;overflow:auto}.score svg{width:100%;height:auto}.lineage svg{
min-width:100%;max-width:none;height:auto}.tabs{display:flex;gap:8px;margin-bottom:12px}
button{font:inherit;padding:7px 15px;background:transparent;border:1px solid var(--line);
border-radius:5px;cursor:pointer}button[aria-selected=true]{color:var(--teal);
border-color:var(--teal);background:#eaf3ee}.accent{color:var(--teal)}.badge{font-size:11px;
padding:3px 6px;border:1px solid var(--line);border-radius:4px}.run{border-top:1px solid
var(--line);padding-top:12px;margin-top:12px}details{margin:10px 0}summary{cursor:pointer}
[hidden]{display:none!important}footer{padding:20px 32px;color:var(--muted);font-size:12px}
@media(max-width:1100px){main{grid-template-columns:220px 1fr}.right{grid-column:1/-1;
display:grid;grid-template-columns:1fr 1fr;gap:20px}}@media(max-width:720px){main,.right{
display:block;padding:12px}header{padding:18px}.tabs{flex-wrap:wrap}}
"""
_SCRIPT = """
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
    positions: dict[str, tuple[int, int]] = {}
    levels: dict[int, int] = {}
    for identifier in sorted(by_id, key=lambda item: (depth[item], item)):
        level = depth[identifier]
        positions[identifier] = (24 + level * 220, 24 + levels.get(level, 0) * 100)
        levels[level] = levels.get(level, 0) + 1
    width = max((x for x, _ in positions.values()), default=24) + 200
    height = max((y for _, y in positions.values()), default=24) + 90
    svg = Element(
        "svg",
        {
            "xmlns": "http://www.w3.org/2000/svg",
            "viewBox": f"0 0 {width} {height}",
            "width": str(width),
            "role": "img",
            "aria-label": "Captured candidate parent relationships",
        },
    )
    for parent, child in known:
        x1, y1 = positions[parent]
        x2, y2 = positions[child]
        active = parent in highlighted and child in highlighted
        SubElement(
            svg,
            "path",
            {
                "d": (
                    f"M {x1 + 170} {y1 + 30} C {x1 + 195} {y1 + 30}, "
                    f"{x2 - 25} {y2 + 30}, {x2} {y2 + 30}"
                ),
                "fill": "none",
                "stroke": "#168273" if active else "#bbc1bd",
                "stroke-width": "2.5" if active else "1",
            },
        )
    for identifier, row in by_id.items():
        x, y = positions[identifier]
        node = SubElement(svg, "g")
        SubElement(
            node,
            "rect",
            {
                "x": str(x),
                "y": str(y),
                "width": "170",
                "height": "62",
                "rx": "5",
                "fill": "#edf5ef" if identifier in highlighted else "#f3f3ef",
                "stroke": "#168273" if identifier in highlighted else "#cdd2cd",
            },
        )
        SubElement(node, "title").text = identifier
        SubElement(node, "text", {"x": str(x + 10), "y": str(y + 23), "font-size": "11"}).text = (
            identifier if len(identifier) <= 24 else identifier[:8] + "…" + identifier[-12:]
        )
        SubElement(
            node, "text", {"x": str(x + 10), "y": str(y + 45), "font-size": "10", "fill": "#6c7773"}
        ).text = str(row.get("state", "unknown"))[:24]
    omitted = sum(row.get("parent_ids_complete") is False for row in rows)
    missing_list = "".join(
        f"<li><code>{_e(parent)}</code> → <code>{_e(child)}</code></li>"
        for parent, child in missing
    )
    return (
        f"<h3>{_e(run.campaign_id)}</h3><p class=accent>{_e(claim)}</p>"
        f"<p class=muted>{_e(scope)}. All {len(rows)} captured candidates shown; "
        f"{len(known)} captured parent edges, {len(missing)} missing-parent references; "
        f"{omitted} truncated parent lists.</p><div class='scroll lineage'>"
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


def render_report_html(
    snapshots: Sequence[JsonObject],
    *,
    comparison_digest: str,
    metric: MetricDefinition,
    title: str = "Swarm · campaign evidence",
    labels: Sequence[str] | None = None,
) -> str:
    """Render a self-contained read-only cockpit; scripts only switch local panels."""
    runs = project_hill_climb(snapshots, comparison_digest=comparison_digest, metric=metric)
    chart = render_hill_climb_svg(
        snapshots, comparison_digest=comparison_digest, metric=metric, title=title, labels=labels
    )
    chart = chart.split("\n", 1)[1]  # Inline SVG has no XML document declaration.
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
            f"<div class=run><h2>{_e(label)}</h2>"
            f"<p>{_e(objective.get('name', 'Objective unavailable'))}</p>"
            f"<code>{_e(run.campaign_id)}</code>"
            f"<p>State: {_e(snapshot.get('campaign_state', 'unknown'))}</p>"
            f"<p class=number>{_e(metric.name)} [{_e(metric.unit)}] · {_e(metric.direction)}</p>"
            f"<p class=badge>{_e(claim)}</p><p>Captured workers: "
            f"{len(workers) if workers else 'unavailable'} distinct historical IDs; "
            "not live concurrency.</p>"
            f"<small>Attempts: {_e(attempt_scope)}</small><h3>Budget ledger</h3>"
            "<table><thead><tr><th>Unit</th><th>Used</th><th>Held</th><th>Uncertain</th></tr></thead>"
            f"<tbody>{budget}</tbody></table>"
            + ("<p class=muted>No captured budget ledger.</p>" if not budget else "")
            + "<details><summary>Capture provenance</summary>"
            f"<p class=muted>Captured {_e(run.captured_at)} · revision {run.revision} · "
            f"event {run.evidence_through}</p></details></div>"
        )
        lineage.append(_lineage(view, run, metric))
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
            f"<tr><td><code>{_e(identifier)}</code>"
            f"{' <span class=badge>highlighted path</span>' if identifier in selected else ''}</td>"
            f"<td class=number>{value:.6g}</td></tr>"
            for identifier, value in ordered
        )
        baseline = run.baseline.value if run.baseline else None
        change = ""
        if ordered and baseline is not None:
            delta = (
                baseline - ordered[0][1]
                if metric.direction == "minimize"
                else ordered[0][1] - baseline
            )
            change = (
                "<p>Observed improvement vs original baseline: "
                f"<span class=number>{delta:.6g} {_e(metric.unit)}</span> "
                "(absolute, not a statistical win).</p>"
            )
        leaderboard.append(
            f"<h3>{_e(label)}</h3><p class=muted>One best captured receipt per candidate; "
            "not seed averages or confirmed rankings. "
            f"{run.included}/{run.total} receipts captured.</p>"
            + change
            + "<table><thead><tr><th>Candidate</th><th>Observed score</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
            + ("<p>No eligible timed scores.</p>" if not rows else "")
        )
        trials, trial_scope = _page(research, "trials")
        trial_rows = "".join(
            f"<tr><td><code>{_e(row.get('id', 'unknown'))}</code></td>"
            f"<td>{_e(row.get('execution_outcome', 'unknown'))}</td>"
            f"<td>{_e(row.get('evaluation_status', 'unknown'))}</td></tr>"
            for row in trials
        )
        observations = "".join(
            f"<li><span class=number>{point.hours:.3g}h · {point.value:.6g}</span> "
            f"<code>{_e(point.candidate_id)}</code> · event {point.event_sequence}</li>"
            for point in run.points
        )
        proposals, proposal_scope = _page(research, "proposals")
        proposal_rows = "".join(
            f"<li><code>{_e(row.get('id', 'unknown'))}</code> · "
            f"{_e(row.get('state', 'unknown'))}</li>"
            for row in proposals
        )
        activity.append(
            f"<h3>{_e(label)} · evaluated outcomes</h3><ol>{observations}</ol>"
            f"<p class=muted>Chronological commit times, not causal agent attribution.</p>"
            f"<h3>All captured trials</h3><small>{_e(trial_scope)}</small>"
            "<table><thead><tr><th>Trial</th><th>Execution</th><th>Evaluation</th></tr></thead>"
            f"<tbody>{trial_rows}</tbody></table><h3>Proposal actions</h3>"
            f"<small>{_e(proposal_scope)}; states only, timestamps unavailable.</small>"
            f"<ul>{proposal_rows}</ul>"
        )
    script_hash = base64.b64encode(hashlib.sha256(_SCRIPT.encode()).digest()).decode()
    return (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        "<meta name=viewport content='width=device-width,initial-scale=1'>"
        f"<meta http-equiv=Content-Security-Policy content=\"default-src 'none'; "
        f"style-src 'unsafe-inline'; script-src 'sha256-{script_hash}'; "
        "base-uri 'none'; form-action 'none'\">"
        f"<title>{_e(title)}</title><style>{_STYLE}</style></head><body>"
        "<header><div class=kicker>RESEARCH SWARM / IMMUTABLE CAMPAIGN REPORT</div>"
        f"<h1>{_e(title)}</h1>"
        "<p class=muted>Read-only captured evidence · all captured experiment paths</p>"
        "<details><summary>Comparison identity</summary>"
        f"<code>{_e(comparison_digest)}</code></details></header>"
        "<main><aside><div class=card><h2>About this experiment</h2>"
        + "".join(about)
        + "</div></aside><section><div class=card>"
        "<div class=tabs role=tablist aria-label='Evidence views'>"
        "<button role=tab data-tab=score aria-selected=false>Score</button>"
        "<button role=tab data-tab=lineage aria-selected=true>Lineage</button></div>"
        "<div data-panel=score class=score hidden>" + chart + "</div>"
        "<div data-panel=lineage><p class=muted>"
        "All captured branches remain visible in grey. "
        "Teal highlights a qualified current selection, or explicitly labelled "
        "best-observed ancestry. "
        "Missing parents and omitted history are not reconstructed.</p>"
        + "".join(lineage)
        + "</div>"
        "</div></section><aside class=right><div class=card><h2>Best observed candidates</h2>"
        "<div class=scroll>" + "".join(leaderboard) + "</div></div>"
        "<div class=card><h2>Captured activity</h2><div class=scroll>"
        + "".join(activity)
        + "</div></div></aside></main>"
        "<footer>Offline snapshot, not live telemetry. "
        "Failed or unscored experiments remain in trial "
        "outcomes. Observed extrema do not establish confirmed, "
        "protected or official wins.</footer>"
        f"<script>{_SCRIPT}</script></body></html>\n"
    )
