"""Dependency-free SVG views of bounded immutable report snapshots, never live state."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring

from pydantic import BaseModel, ConfigDict, Field

from research_swarm.domain.experiments import Metric, MetricDefinition
from research_swarm.state.database import JsonObject

MAX_RUNS = 8
MAX_OBSERVATIONS = 52  # At most 50 recent evaluations plus two pinned receipts.


class _Observation(BaseModel):
    id: str
    eligible: bool = Field(strict=True)
    comparison_digest: str | None = None
    primary_metric: Metric | None = None
    commit_event: JsonObject | None = None
    data: JsonObject


class _Snapshot(BaseModel):
    model_config = ConfigDict(extra="ignore")

    campaign_id: str
    campaign_revision: int
    schema_version: str
    evidence_through: int
    captured_at: str
    campaign_created_at: str | None = None
    evaluation_count: int = Field(ge=0)
    recent_evaluations: list[_Observation] = Field(max_length=MAX_OBSERVATIONS)
    evaluation_window: JsonObject
    research: JsonObject


@dataclass(frozen=True)
class PlotPoint:
    evaluation_id: str
    trial_id: str
    candidate_id: str
    seed: str
    recorded_at: str
    event_sequence: int
    hours: float
    value: float


@dataclass(frozen=True)
class PlotRun:
    campaign_id: str
    revision: int
    schema: str
    evidence_through: int
    captured_at: str
    origin: str | None
    capture_hours: float | None
    points: tuple[PlotPoint, ...]
    baseline: PlotPoint | None
    history_complete: bool
    included: int
    total: int
    ineligible: int
    nonmatching: int
    untimed: int


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.utcoffset() is not None else None
    except ValueError:
        return None


def project_hill_climb(
    snapshots: Sequence[JsonObject], *, comparison_digest: str, metric: MetricDefinition
) -> tuple[PlotRun, ...]:
    """Select exact metric/cohort evidence; omit untimed points, never invent chronology.

    Each input is the report's data.snapshot, not a live query or report summary.
    Elapsed wall hours start at each campaign's recorded creation. The receipt's
    committed evaluation.recorded event supplies the observation time.
    """
    if not 1 <= len(snapshots) <= MAX_RUNS or not comparison_digest:
        raise ValueError("Select one to eight reports and a comparison digest")
    runs = []
    identities: set[str] = set()
    for raw in snapshots:
        snapshot = _Snapshot.model_validate(raw)
        if snapshot.campaign_id in identities:
            raise ValueError("Use one snapshot per campaign, not duplicate historical windows")
        identities.add(snapshot.campaign_id)
        origin, captured = _time(snapshot.campaign_created_at), _time(snapshot.captured_at)
        points = []
        ineligible = nonmatching = untimed = 0
        seen: set[str] = set()
        for observation in snapshot.recent_evaluations:
            if observation.id in seen:
                raise ValueError("Duplicate evaluation in report")
            seen.add(observation.id)
            if not observation.eligible:
                ineligible += 1
                continue
            value = observation.primary_metric
            if (
                observation.comparison_digest != comparison_digest
                or value is None
                or (value.name, value.unit, value.direction)
                != (metric.name, metric.unit, metric.direction)
            ):
                nonmatching += 1
                continue
            if not math.isfinite(value.value):
                raise ValueError("Metric value must be finite")
            event = observation.commit_event or {}
            recorded = _time(event.get("recorded_at"))
            if (
                origin is None
                or captured is None
                or recorded is None
                or not origin <= recorded <= captured
                or type(event.get("sequence")) is not int
                or not 0 < event["sequence"] <= snapshot.evidence_through
            ):
                untimed += 1
                continue
            points.append(
                PlotPoint(
                    evaluation_id=observation.id,
                    trial_id=str(observation.data.get("trial_id", "unknown")),
                    candidate_id=str(observation.data.get("candidate_id", "unknown")),
                    seed=str(observation.data.get("observed_seed", "unknown")),
                    recorded_at=str(event["recorded_at"]),
                    event_sequence=event["sequence"],
                    hours=(recorded - origin).total_seconds() / 3600,
                    value=value.value,
                )
            )
        points.sort(key=lambda item: (item.hours, item.event_sequence, item.evaluation_id))
        baseline = snapshot.research.get("baseline") or {}
        if not isinstance(baseline, dict):
            raise ValueError("Baseline must be an object or null")
        if len(snapshot.recent_evaluations) > snapshot.evaluation_count:
            raise ValueError("Captured receipt count exceeds total")
        runs.append(
            PlotRun(
                campaign_id=snapshot.campaign_id,
                revision=snapshot.campaign_revision,
                schema=snapshot.schema_version,
                evidence_through=snapshot.evidence_through,
                captured_at=snapshot.captured_at,
                origin=snapshot.campaign_created_at,
                capture_hours=(
                    (captured - origin).total_seconds() / 3600
                    if captured is not None and origin is not None and captured >= origin
                    else None
                ),
                points=tuple(points),
                baseline=next(
                    (
                        point
                        for point in points
                        if point.evaluation_id == baseline.get("evaluation_id")
                    ),
                    None,
                ),
                history_complete=(
                    snapshot.evaluation_window.get("complete") is True
                    and len(snapshot.recent_evaluations) == snapshot.evaluation_count
                ),
                included=len(snapshot.recent_evaluations),
                total=snapshot.evaluation_count,
                ineligible=ineligible,
                nonmatching=nonmatching,
                untimed=untimed,
            )
        )
    return tuple(runs)


def _clean(value: object) -> str:
    # XML 1.0 forbids controls and lone surrogates, even when escaped.
    return "".join(
        c
        for c in str(value)
        if c in "\n\r\t"
        or 32 <= ord(c) <= 0xD7FF
        or 0xE000 <= ord(c) <= 0xFFFD
        or 0x10000 <= ord(c) <= 0x10FFFF
    )


def _element(parent: Element, tag: str, **attributes: object) -> Element:
    return SubElement(
        parent, tag, {key.replace("_", "-"): _clean(value) for key, value in attributes.items()}
    )


def _text(parent: Element, x: float, y: float, text: str, **attributes: object) -> Element:
    item = _element(parent, "text", x=x, y=y, **attributes)
    item.text = _clean(text)
    return item


RUN_COLORS = (
    "#168273",
    "#507aac",
    "#b76a3b",
    "#8661ad",
    "#9a801a",
    "#aa5282",
    "#427a94",
    "#677b3b",
)


def _draw_time_chart(
    group: Element,
    runs: Sequence[PlotRun],
    metric: MetricDefinition,
    *,
    bounds: tuple[float, float, float, float],
) -> None:
    """Draw only axes and captured series; the caller owns legend and evidence text."""
    left, right, top, bottom = bounds
    points = [point for run in runs for point in run.points]
    xmax = (
        max([point.hours for point in points] + [run.capture_hours or 0.0 for run in runs]) or 1.0
    )
    # Normalize before subtraction to avoid overflow for finite extreme floats.
    scale = max((abs(point.value) for point in points), default=1.0) or 1.0
    values = [point.value / scale for point in points]
    low, high = min(values, default=0.0), max(values, default=1.0)
    padding = (high - low) * 0.08 or 0.08
    low, high = low - padding, high + padding

    def x(hours: float) -> float:
        return left + hours / xmax * (right - left)

    def y(value: float) -> float:
        return bottom - (value / scale - low) / (high - low) * (bottom - top)

    for index in range(6):
        fraction = index / 5
        px, py = left + fraction * (right - left), bottom - fraction * (bottom - top)
        _element(group, "line", x1=left, x2=right, y1=py, y2=py, stroke="#e4e9ee")
        tick = (low + fraction * (high - low)) * scale
        _text(
            group,
            left - 12,
            py + 4,
            f"{tick:.4g}" if math.isfinite(tick) else "",
            font_size=12,
            font_family="ui-monospace, DejaVu Sans Mono, monospace",
            text_anchor="end",
            fill="#627280",
        )
        _text(
            group,
            px,
            bottom + 23,
            f"{fraction * xmax:.3g}",
            font_size=12,
            font_family="ui-monospace, DejaVu Sans Mono, monospace",
            text_anchor="middle",
            fill="#627280",
        )
    _text(
        group,
        (left + right) / 2,
        bottom + 50,
        "Elapsed wall hours since each campaign's creation (evaluation commit time)",
        font_size=13,
        text_anchor="middle",
    )
    for index, run in enumerate(runs):
        color = RUN_COLORS[index]
        line = _element(group, "g", data_campaign=run.campaign_id)
        if run.baseline:
            _element(
                line,
                "line",
                x1=left,
                x2=right,
                y1=y(run.baseline.value),
                y2=y(run.baseline.value),
                stroke=color,
                stroke_dasharray="6 5",
                opacity=0.65,
            )
        if run.points:
            best = run.points[0].value
            path = f"M {x(run.points[0].hours):.3f} {y(best):.3f}"
            for point in run.points[1:]:
                new_best = (
                    min(best, point.value)
                    if metric.direction == "minimize"
                    else max(best, point.value)
                )
                path += f" H {x(point.hours):.3f} V {y(new_best):.3f}"
                best = new_best
            if run.capture_hours is not None:
                path += f" H {x(run.capture_hours):.3f}"
            _element(
                line,
                "path",
                d=path,
                fill="none",
                stroke=color,
                stroke_width=2.5,
                data_series="best-observed",
            )
        for point in run.points:
            dot = _element(
                line,
                "circle",
                cx=f"{x(point.hours):.3f}",
                cy=f"{y(point.value):.3f}",
                r=4.5,
                fill="#8b969e",
                fill_opacity=0.55,
                data_evaluation=point.evaluation_id,
            )
            _element(dot, "title").text = _clean(
                f"Trial {point.trial_id}; candidate {point.candidate_id}; seed {point.seed}\n"
                f"Evaluation {point.evaluation_id}; committed {point.recorded_at}\n"
                f"{point.hours:.6g} h; {metric.name}={point.value:.8g} {metric.unit}"
            )
    if not points:
        _text(
            group,
            (left + right) / 2,
            (top + bottom) / 2,
            "No eligible timed observations in the selected cohort",
            font_size=16,
            text_anchor="middle",
        )


def _embedded_time_chart(runs: Sequence[PlotRun], metric: MetricDefinition) -> str:
    """Chart-only viewport; HTML supplies readable, wrapping evidence outside the SVG."""
    svg = Element(
        "svg",
        {
            "xmlns": "http://www.w3.org/2000/svg",
            "viewBox": "0 0 740 440",
            "role": "img",
            "aria-label": "Trial scores and best observed over elapsed wall hours",
        },
    )
    group = _element(svg, "g", fill="#627280")
    _draw_time_chart(group, runs, metric, bounds=(90.0, 715.0, 20.0, 370.0))
    return tostring(svg, encoding="unicode")


def render_hill_climb_svg(
    snapshots: Sequence[JsonObject],
    *,
    comparison_digest: str,
    metric: MetricDefinition,
    title: str = "Swarm · hill-climb observations",
    labels: Sequence[str] | None = None,
) -> str:
    """Return standalone UTF-8 SVG, without scripts, external assets or plotting dependencies."""
    runs = project_hill_climb(snapshots, comparison_digest=comparison_digest, metric=metric)
    if labels is not None and (
        len(labels) != len(runs) or any(not label.strip() for label in labels)
    ):
        raise ValueError("Supply one nonempty label per report, or omit labels")
    height = 660 + 138 * len(runs)
    svg = Element(
        "svg",
        {
            "xmlns": "http://www.w3.org/2000/svg",
            "viewBox": f"0 0 1120 {height}",
            "width": "1120",
            "height": str(height),
            "role": "img",
            "aria-labelledby": "plot-title plot-description",
        },
    )
    _element(svg, "title", id="plot-title").text = _clean(title)
    _element(svg, "desc", id="plot-description").text = (
        "Eligible trial receipt scores versus elapsed wall-clock hours since campaign creation. "
        "Steps show best observed among captured points, not confirmed winners. "
        "Dashed lines show each run's eligible original baseline when available. "
        "Best observed is carried to report capture time, not extrapolated improvement. "
        "No worker count, training-time or speedup inference is made."
    )
    _element(svg, "rect", width=1120, height=height, fill="#f7f6f2")
    group = _element(svg, "g", font_family="system-ui, DejaVu Sans, sans-serif", fill="#192632")
    _text(group, 48, 42, "RESEARCH SWARM / EVIDENCE VIEW", font_size=12, letter_spacing=2)
    _text(
        group, 48, 80, title[:65] + ("…" if len(title) > 65 else ""), font_size=28, font_weight=700
    )
    _text(
        group,
        48,
        110,
        f"{metric.name} [{metric.unit}] · {metric.direction} · exact comparison cohort",
        font_size=15,
    )
    _text(group, 48, 135, comparison_digest, font_size=12, fill="#627280")
    _element(group, "rect", x=48, y=156, width=1024, height=405, rx=12, fill="#fffefa")
    _draw_time_chart(group, runs, metric, bounds=(110.0, 1040.0, 190.0, 495.0))
    for index, run in enumerate(runs):
        color = RUN_COLORS[index]
        note_y = 660 + index * 138
        _element(group, "rect", x=48, y=note_y - 20, width=4, height=112, fill=color)
        label = labels[index] if labels is not None else f"Run {index + 1}"
        heading = _text(
            group, 64, note_y, f"{label[:64]} · {run.campaign_id}", font_size=14, font_weight=650
        )
        _element(heading, "title").text = _clean(f"{label} · {run.campaign_id}")
        scope = "complete capture" if run.history_complete else "INCOMPLETE capture"
        _text(
            group,
            64,
            note_y + 22,
            f"{scope}: {run.included}/{run.total} receipts; {len(run.points)} plotted. "
            f"Excluded: {run.ineligible} ineligible, {run.nonmatching} other cohort/metric, "
            f"{run.untimed} missing/invalid timing.",
            font_size=12,
        )
        baseline = (
            f"original baseline = {run.baseline.value:.6g} [{metric.unit}]"
            if run.baseline
            else "original baseline unavailable for this cohort/time window"
        )
        _text(group, 64, note_y + 44, f"Dashed reference: {baseline}.", font_size=12)
        _text(
            group,
            64,
            note_y + 66,
            f"Schema {run.schema}; campaign revision {run.revision}; "
            f"evidence through event {run.evidence_through}; captured {run.captured_at}.",
            font_size=12,
            fill="#627280",
        )
        _text(
            group,
            64,
            note_y + 88,
            f"Time origin: {run.origin or 'unavailable (legacy report; no timestamps invented)'}.",
            font_size=12,
            fill="#627280",
        )
    _text(
        group,
        48,
        590,
        "● Trial receipts   ━ Best observed in captured evidence   ╌ Original baseline",
        font_size=14,
    )
    _text(
        group,
        48,
        614,
        "Captured best is held to capture time, including idle periods; not new evaluations or "
        "confirmed winners.",
        font_size=13,
        fill="#627280",
    )
    return '<?xml version="1.0" encoding="utf-8"?>\n' + tostring(svg, encoding="unicode") + "\n"
