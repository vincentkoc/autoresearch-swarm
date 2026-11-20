"""Campaign KPIs derived from durable evidence in the report's read transaction."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

KPI_LABELS = {
    "time_to_first_trial": "Time to first trial",
    "trials_per_hour": "Trials per hour",
    "gpu_utilization": "GPU utilization",
    "cost_per_0_01_improvement": "Cost per 0.01 improvement",
    "failed_attempts_by_class": "Failed attempts by class",
    "proposer_acceptance_rate": "Proposer acceptance rate",
    "confirmation_pass_rate": "Confirmation pass rate",
    "proxy_to_target_slope": "Proxy-to-target slope",
}


def _metric(value: float | dict[str, float] | None, unit: str, detail: str) -> dict[str, Any]:
    return {"value": value, "unit": unit, "detail": detail}


def _proposer_acceptance(db: sqlite3.Connection, campaign: str) -> dict[str, Any]:
    # A pre-accounting campaign can have trials but no evidence of its rejected proposals.
    created = db.execute(
        "SELECT * FROM swarm_events WHERE campaign_id=? AND kind='created' ORDER BY id LIMIT 1",
        (campaign,),
    ).fetchone()
    if created is None or dict(created).get("proposed_count") is None:
        return {
            **_metric(None, "%", "Proposal accounting was unavailable at campaign creation."),
            "proposed": None,
            "accepted": None,
            "rejected": None,
            "fraction": None,
        }
    proposed, accepted = db.execute(
        "SELECT COALESCE(SUM(proposed_count),0), COALESCE(SUM(accepted_count),0) "
        "FROM swarm_events WHERE campaign_id=? AND kind='proposal_batch'",
        (campaign,),
    ).fetchone()
    fraction = accepted / proposed if proposed else None
    rejected = proposed - accepted
    return {
        **_metric(
            fraction * 100 if fraction is not None else None,
            "%",
            f"{accepted} accepted and {rejected} rejected out of {proposed} returned candidates."
            if proposed
            else "No candidates were returned for admission.",
        ),
        "proposed": proposed,
        "accepted": accepted,
        "rejected": rejected,
        "fraction": fraction,
    }


def _cost_efficiency(
    db: sqlite3.Connection,
    campaign: str,
    spec: dict[str, Any],
    meter: dict[str, Any],
    current_selection: dict[str, Any] | None,
) -> dict[str, Any]:
    unit = "campaign currency / 0.01"
    if current_selection is None or current_selection.get("score") is None:
        return _metric(None, unit, "The current recorded selection needs a score.")
    current_score = current_selection["score"]
    fidelity = current_selection["fidelity"]
    first = db.execute(
        "SELECT e.* FROM swarm_events e JOIN swarm_trials t USING(trial_id) "
        "WHERE e.campaign_id=? AND e.kind='selected' AND t.fidelity=? ORDER BY e.id LIMIT 1",
        (campaign, fidelity),
    ).fetchone()
    baseline = dict(first).get("score") if first else None
    if baseline is None:
        return _metric(
            None, unit, f"The first recorded selection at fidelity {fidelity:g} needs a score."
        )
    gain = current_score - baseline if spec["direction"] == "maximize" else baseline - current_score
    if gain <= 0:
        return _metric(
            None,
            unit,
            f"No net improvement over the first recorded selection at fidelity {fidelity:g}.",
        )
    measured = meter["total_cost"]
    if measured is None:
        missing = ", ".join(meter["missing_prices"])
        detail = (
            "No priced usage was recorded for this campaign."
            if meter["measured_cost"] == 0
            else f"Known recorded cost {meter['measured_cost']:g} is a subtotal."
        )
        return _metric(None, unit, f"{detail} Missing prices: {missing}.")
    return _metric(
        measured * 0.01 / gain,
        unit,
        f"Recorded campaign cost {measured:g} over a {gain:g} gain from first to current "
        f"selection at fidelity {fidelity:g}. Pending estimates are excluded. "
        "Invoiced USD is unavailable.",
    )


def _panel_kpis(db: sqlite3.Connection, campaign: str) -> dict[str, Any]:
    row = db.execute("SELECT * FROM swarm_campaigns WHERE campaign_id=?", (campaign,)).fetchone()
    binding = dict(row).get("panel_binding")
    calibration = db.execute(
        "SELECT * FROM swarm_events WHERE campaign_id=? AND kind='panel_calibration'",
        (campaign,),
    ).fetchone()
    evidence = json.loads(calibration["panel_evidence"]) if calibration else {}
    missing = (
        "No panel study identity was recorded for this campaign."
        if not binding
        else "No calibration outcome was captured for this campaign's bound study."
    )
    rate = {
        **_metric(None, "%", missing),
        "passes": None,
        "decisions": None,
        "recorded_passes": None,
        "recorded_decisions": None,
        "last_confirmation": None,
    }
    slope = {**_metric(None, "target / proxy", missing), "seed_noise": None}
    metrics = {"confirmation_pass_rate": rate, "proxy_to_target_slope": slope}
    if not binding or evidence.get("binding") != binding:
        return metrics

    noise = evidence["seed_noise"]
    if noise is not None:
        slope.update(
            value=evidence["slope"],
            seed_noise=noise,
            detail=f"Bound calibration recorded target seed noise {noise:g}.",
        )
        if slope["value"] is None:
            slope["detail"] += " The calibration proxy was constant, so the slope is undefined."

    # An upgraded queue may have selected or rejected challengers before capture began.
    earlier_results = db.execute(
        "SELECT 1 FROM swarm_events e JOIN swarm_attempts a USING(attempt_id) "
        "WHERE e.campaign_id=? AND e.id<? AND e.kind='finished' "
        "AND json_extract(a.observation,'$.status')='ok' LIMIT 1",
        (campaign, calibration["id"]),
    ).fetchone()
    passes, decisions, unknown, latest = db.execute(
        """WITH outcomes AS (
            SELECT id, panel_evidence, json_extract(panel_evidence,'$.reason') AS reason,
                   json_type(panel_evidence,'$.confirmation_gain') AS gain_type,
                   json_extract(panel_evidence,'$.promoted') AS promoted
            FROM swarm_events WHERE campaign_id=? AND kind='panel_decision'
                AND json_extract(panel_evidence,'$.binding')=?),
           classified AS (
                SELECT *, CASE
                    WHEN reason='confirmation' AND gain_type IN ('real','integer')
                        AND promoted IN (0,1) THEN 1
                    WHEN reason IN ('selection','reference','evaluation_failed') THEN 0
                END AS eligible FROM outcomes)
           SELECT COUNT(*) FILTER (WHERE eligible AND promoted),
                  COUNT(*) FILTER (WHERE eligible), COUNT(*) FILTER (WHERE eligible IS NULL),
                  (SELECT panel_evidence FROM classified WHERE eligible ORDER BY id DESC LIMIT 1)
           FROM classified""",
        (campaign, binding),
    ).fetchone()
    rate.update(recorded_passes=passes, recorded_decisions=decisions)
    if earlier_results:
        rate["detail"] = (
            "Successful trials predate decision capture. The campaign total is unknown."
        )
    elif unknown:
        rate["detail"] = "A bound panel decision lacks the evidence needed to classify it."
    else:
        rate.update(
            value=100 * passes / decisions if decisions else None,
            passes=passes,
            decisions=decisions,
            detail=f"{passes} of {decisions} distinct measured confirmation comparisons passed."
            if decisions
            else "No measured confirmation comparisons were recorded for this campaign.",
        )
    if latest:
        decision = json.loads(latest)
        gain = decision["confirmation_gain"]
        rate["last_confirmation"] = {"promoted": decision["promoted"], "confirmation_gain": gain}
        verdict = "passed" if decision["promoted"] else "failed"
        rate["detail"] += f" Last recorded confirmation {verdict} with gain {gain:g}."
    return metrics


def read_kpis(
    db: sqlite3.Connection,
    campaign: str,
    spec: dict[str, Any],
    meter: dict[str, Any],
    *,
    created_at: float | None,
    captured_at: float,
    current_selection: dict[str, Any] | None,
) -> dict[str, Any]:
    started = db.execute(
        "SELECT MIN(at) FROM swarm_events WHERE campaign_id=? AND kind='workload_started'",
        (campaign,),
    ).fetchone()[0]
    latency = started - created_at if created_at is not None and started is not None else None
    terminal_trials = db.execute(
        "SELECT COUNT(DISTINCT e.trial_id) FROM swarm_events e "
        "JOIN swarm_attempts a USING(attempt_id) "
        "WHERE e.campaign_id=? AND e.kind='finished' AND e.at>? AND e.at<=? "
        "AND (json_extract(a.observation,'$.status')='ok' OR a.number>=?)",
        (campaign, captured_at - 3600, captured_at, spec["max_attempts"]),
    ).fetchone()[0]
    finished = meter["finished_attempts"]
    failures = dict(
        db.execute(
            "SELECT COALESCE(NULLIF(json_extract(a.observation,'$.failure_class'),''),"
            "'unclassified'), COUNT(*) FROM swarm_attempts a JOIN swarm_trials t USING(trial_id) "
            "WHERE t.campaign_id=? AND json_extract(a.observation,'$.status') IN ('failed','lost') "
            "GROUP BY 1 ORDER BY 1",
            (campaign,),
        ).fetchall()
    )
    return {
        "time_to_first_trial": _metric(
            latency if latency is not None and latency >= 0 else None,
            "s",
            "Campaign creation to first acknowledged workload launch."
            if latency is not None and latency >= 0
            else "No ordered campaign creation and acknowledged workload launch were recorded.",
        ),
        "trials_per_hour": _metric(
            terminal_trials,
            "trials/h",
            "Distinct terminal trials in the 60 minutes ending at report capture.",
        ),
        "gpu_utilization": gpu_utilization(db, campaign, spec),
        "cost_per_0_01_improvement": _cost_efficiency(db, campaign, spec, meter, current_selection),
        "failed_attempts_by_class": {
            **_metric(
                {key: count * 100 / finished for key, count in failures.items()}
                if finished
                else None,
                "%",
                f"{sum(failures.values())} of {finished} finished attempts failed or were lost. "
                "Each retry counts separately. Missing causes are unclassified."
                if finished
                else "No finished attempts were recorded.",
            ),
            "counts": failures,
            "finished_attempts": finished,
        },
        "proposer_acceptance_rate": _proposer_acceptance(db, campaign),
        **_panel_kpis(db, campaign),
    }


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
