"""Reconcile dedicated device allocations in the campaign snapshot transaction."""

from __future__ import annotations

import sqlite3
from typing import Any

# The event order is transactional. A claim fits only inside its worker's
# allocation, and two owners cannot both attribute an overlapping physical GPU.
_SPANS = """
WITH spans AS (
    SELECT s.id AS first, f.id AS last, s.campaign_id, json_extract(s.gpu_time,'$.device') device,
           json_extract(s.gpu_time,'$.worker_id') worker
    FROM swarm_events s LEFT JOIN swarm_events f
      ON f.campaign_id=s.campaign_id AND f.kind='gpu_allocation_finished'
      AND json_extract(f.gpu_time,'$.allocation_id')=json_extract(s.gpu_time,'$.allocation_id')
    WHERE s.kind='gpu_allocation_started'
)
"""


def gpu_utilization(db: sqlite3.Connection, campaign: str, spec: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "value": None,
        "unit": "%",
        "coverage": "unavailable",
        "busy_device_seconds": None,
        "allocated_device_seconds": None,
        "covered_device_seconds": None,
        "open_allocations": 0,
        "missing_samples": 0,
        "uncovered_claims": 0,
        "detail": "No dedicated GPU allocation was measured.",
    }
    if spec.get("resource") != "gpu":
        result["detail"] = "This campaign has no measured dedicated GPU allocation."
        return result
    if not any(column[1] == "gpu_time" for column in db.execute("PRAGMA table_info(swarm_events)")):
        return result
    spans = db.execute(
        _SPANS
        + """
        SELECT device, COUNT(*)-COUNT(last) FROM spans WHERE campaign_id=? GROUP BY device
        """,
        (campaign,),
    ).fetchall()
    if not spans:
        return result
    result["open_allocations"] = sum(row[1] for row in spans)
    result["coverage"] = "partial"
    if db.execute(
        _SPANS
        + """
        SELECT 1 FROM spans a JOIN spans b ON a.device=b.device AND a.first<>b.first
        AND (b.last IS NULL OR a.first<b.last) AND (a.last IS NULL OR b.first<a.last)
        WHERE a.campaign_id=? LIMIT 1
        """,
        (campaign,),
    ).fetchone():
        result["detail"] = "Overlapping allocations prevent attribution of this physical GPU."
        return result
    allocated, covered, busy, missing = db.execute(
        """
        SELECT SUM(allocated), SUM(CASE WHEN busy IS NOT NULL THEN allocated ELSE 0 END),
               SUM(busy), COUNT(*)-COUNT(busy)
        FROM (SELECT json_extract(gpu_time,'$.allocated_device_seconds') allocated,
                     json_extract(gpu_time,'$.busy_device_seconds') busy
              FROM swarm_events WHERE campaign_id=? AND kind='gpu_device_interval')
        """,
        (campaign,),
    ).fetchone()
    result.update(
        allocated_device_seconds=allocated, covered_device_seconds=covered, busy_device_seconds=busy
    )
    options = spec.get("options", {})
    expected = set(options.get("resources", {}).get("devices_mib", {}))
    if not expected:
        expected = {options.get("backend", {}).get("device")}
    observed = {row[0] for row in spans}
    uncovered = db.execute(
        _SPANS
        + """
        SELECT COUNT(*) FROM swarm_events c JOIN swarm_attempts a USING(attempt_id)
        LEFT JOIN swarm_events f ON f.kind='finished' AND f.attempt_id=c.attempt_id
        WHERE c.campaign_id=? AND c.kind='claimed' AND (
            SELECT COUNT(DISTINCT s.device) FROM spans s WHERE s.campaign_id=c.campaign_id
            AND s.worker=a.worker_id AND s.first<c.id AND (s.last IS NULL OR s.last>f.id)
        )<>?
        """,
        (campaign, len(expected)),
    ).fetchone()[0]
    result.update(missing_samples=missing, uncovered_claims=uncovered)
    if result["open_allocations"] or missing or not allocated or observed != expected or uncovered:
        result["detail"] = (
            f"{covered or 0:g} of {allocated or 0:g} recorded device-seconds have a busy sample. "
            f"{result['open_allocations']} open allocations, {missing} missing samples, "
            f"{len(expected - observed)} missing devices and {uncovered} uncovered claims."
        )
        return result
    result.update(
        value=100 * (busy / allocated),
        coverage="complete",
        detail=f"{busy:g} sampled busy / {allocated:g} allocated device-seconds. "
        "Time-weighted driver samples across dedicated local allocations from first "
        "dispatch through drain, including idle time, staging and retries.",
    )
    return result
