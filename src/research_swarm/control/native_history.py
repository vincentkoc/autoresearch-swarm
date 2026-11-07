"""Explicit native measurements for the original timeline, without evaluator authority."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from research_swarm.control.report_plot import PlotPoint, PlotRun, _time


class NativeObservation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    kind: Literal["native_fit", "evaluation", "historical_import", "attempt"]
    study: str
    cohort: str
    campaign_id: str | None = None
    trial_id: str | None = None
    attempt_id: str | None = None
    arm: str
    seed: int | None = None
    status: str
    started_at: str | None = None
    finished_at: str | None = None
    observed_at: str | None = None
    fit_completed_at: str | None = None
    timestamp_basis: str
    source_revision: str
    source_sha256: str
    metrics: dict[str, float] = Field(default_factory=dict)
    completed_updates: int | None = None

    @model_validator(mode="after")
    def validate_measurement(self) -> NativeObservation:
        import math

        for value in self.metrics.values():
            if not math.isfinite(value):
                raise ValueError("Nonfinite native measurement")
        for timestamp in (
            self.started_at,
            self.finished_at,
            self.observed_at,
            self.fit_completed_at,
        ):
            if timestamp is not None and _time(timestamp) is None:
                raise ValueError("Native timestamps must include a timezone")
        started, finished = _time(self.started_at), _time(self.finished_at)
        if started is not None and finished is not None and started > finished:
            raise ValueError("Native completion precedes start")
        if self.kind != "historical_import" and not self.trial_id:
            raise ValueError("Native records need their original trial identity")
        return self


class NativeHistory(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["native-history-v1"] = "native-history-v1"
    captured_at: str
    observations: list[NativeObservation] = Field(max_length=10000)
    inventory: dict[str, int]
    sources: list[dict[str, str | int]]
    limitations: list[str]

    @model_validator(mode="after")
    def validate_inventory(self) -> NativeHistory:
        captured = _time(self.captured_at)
        if captured is None:
            raise ValueError("Capture timestamp must include a timezone")
        ids: set[str] = set()
        identities: set[tuple[str, str | None, str, int | None]] = set()
        for row in self.observations:
            identity = (row.kind, row.attempt_id or row.id, row.arm, row.seed)
            if row.id in ids or identity in identities:
                raise ValueError("Duplicate native observation identity")
            ids.add(row.id)
            identities.add(identity)
            for value in (row.started_at, row.finished_at, row.observed_at, row.fit_completed_at):
                observed = _time(value)
                if observed is not None and observed > captured:
                    raise ValueError("Native timestamp is later than capture")
        return self


def project_native_history(
    history: NativeHistory, cohort: str, metric: str
) -> tuple[PlotRun, list[NativeObservation]]:
    """Reuse original plot geometry with genuine native execution time and identities."""
    rows = [row for row in history.observations if row.cohort == cohort]
    dates = [
        date
        for row in history.observations
        for date in (_time(row.started_at), _time(row.finished_at), _time(row.observed_at))
        if date is not None
    ]
    captured = datetime.fromisoformat(history.captured_at.replace("Z", "+00:00"))
    origin = min(dates, default=captured)
    points = []
    for row in rows:
        event_at = row.observed_at or row.finished_at
        finished = _time(event_at)
        if metric not in row.metrics or finished is None or row.status != "completed":
            continue
        points.append(
            PlotPoint(
                evaluation_id=row.id,
                trial_id=row.trial_id or "historical import",
                candidate_id=row.arm,
                seed=str(row.seed),
                recorded_at=event_at or "",
                event_sequence=0,
                hours=(finished - origin).total_seconds() / 3600,
                value=row.metrics[metric],
                evidence_kind=row.kind,
                details=(
                    f"Study {row.study}; campaign {row.campaign_id}; attempt {row.attempt_id}; "
                    f"start {row.started_at or 'unavailable'}; "
                    f"finish {row.finished_at or 'unavailable'}; "
                    f"fit barrier {row.fit_completed_at or 'unavailable'}; "
                    f"time basis {row.timestamp_basis}; source {row.source_revision}; "
                    f"artifact {row.source_sha256}"
                ),
            )
        )
    points.sort(key=lambda point: (point.hours, point.evaluation_id))
    return (
        PlotRun(
            campaign_id="native-history:" + cohort,
            revision=1,
            schema=history.schema_version,
            evidence_through=0,
            captured_at=history.captured_at,
            origin=origin.isoformat(),
            capture_hours=(captured - origin).total_seconds() / 3600,
            points=tuple(points),
            baseline=None,
            history_complete=True,
            included=len(points),
            total=len(rows),
            ineligible=0,
            nonmatching=0,
            untimed=sum(
                metric in row.metrics and not (row.observed_at or row.finished_at) for row in rows
            ),
        ),
        rows,
    )
