from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.commands import ProposeCandidate
from research_swarm.control.promotions import AssessPromotion, InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import ReportCampaign
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from tests.control.test_promotions import assess, commit, evaluated, second_evaluation
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline


def report(pipeline: Pipeline, key: str, limit: int = 20) -> dict[str, Any]:
    return pipeline.client.command(
        "report.create",
        ReportCampaign(campaign_id=pipeline.campaign["id"], recent_limit=limit),
        idempotency_key=key,
    )["result"]


def test_report_keeps_baseline_and_source_lineage_outside_a_truncated_window(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline_receipt = evaluated(pipeline)
    promotion = commit(pipeline, assess(pipeline, baseline_receipt))
    second_evaluation(pipeline, tmp_path)
    candidates = pipeline.client.list(RecordQuery(kind="candidate"))["items"]
    changed = pipeline.client.get("candidate", candidates[-1]["id"])
    baseline_id = promotion["data"]["decision"]["candidate_id"]
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    child = pipeline.client.command(
        "candidate.propose",
        ProposeCandidate(
            campaign_id=campaign["id"],
            source_artifact=ArtifactRef.model_validate(changed["data"]["source_artifact"]),
            parent_ids=(baseline_id,),
            hypothesis="Retain explicit source ancestry without claiming an evaluated win",
        ),
        idempotency_key="lineage-child",
        expected_revision=campaign["revision"],
    )["result"]
    before = pipeline.client.get("campaign", campaign["id"])
    recorded = report(pipeline, "bounded-report", limit=1)
    snapshot = recorded["data"]["snapshot"]
    details = snapshot["research"]
    assert details["baseline"]["promotion"]["id"] == promotion["id"]
    assert details["baseline"]["evaluation_id"] == baseline_receipt["id"]
    assert details["candidates"]["total"] == 3
    assert details["candidates"]["included"] == 2
    assert details["candidates"]["complete"] is False
    captured = {item["id"]: item for item in details["candidates"]["items"]}
    assert set(captured) == {baseline_id, child["id"]}
    lineage = captured[child["id"]]
    assert lineage["parent_ids"] == [baseline_id]
    change = lineage["changes"][0]
    assert change["parent"]["id"] == baseline_id
    assert change["total"] == 1 and change["complete"]
    assert change["paths"][0]["path"] == "train.py"
    assert change["paths"][0]["before"]["digest"] != change["paths"][0]["after"]["digest"]
    assert {item["id"] for item in snapshot["recent_evaluations"]} >= {baseline_receipt["id"]}
    assert details["attempts"]["included"] == 1 and details["attempts"]["total"] == 4
    attempt = details["attempts"]["items"][0]
    assert attempt["result"]["exit_code"] == 0
    assert attempt["result"]["execution_fingerprint"] == attempt["profile"]["execution_fingerprint"]
    assert attempt["result"]["process_closure_ref"]["digest"].startswith("sha256:")
    assert details["objective"]["primary_metric"]["direction"] == "minimize"
    assert details["scope"]["timing"]["scope"] == "active_training"
    assert details["resource_disposition"]["open_reservations"]["total"] == 0
    assert details["workspace_id"] == pipeline.runtime.identity.workspace_id
    assert "Included 2 of 3 candidates. Complete: False" in recorded["data"]["markdown"]
    assert baseline_id in recorded["data"]["markdown"]
    assert "train.py" in recorded["data"]["markdown"]
    assert pipeline.client.get("campaign", campaign["id"]) == before
    assert report(pipeline, "bounded-report", limit=1) == recorded
    pinned_trial = next(
        item for item in details["trials"]["items"] if item["id"] == pipeline.trial["id"]
    )
    detail_budget = (
        len(canonical_bytes(captured[baseline_id]))
        + sum(
            len(canonical_bytes(item))
            for item in details["promotions"]["items"]
            if item["id"] in details["promotions"]["pinned_ids"]
        )
        + max(len(canonical_bytes(lineage)), len(canonical_bytes(pinned_trial)))
        + 1
    )
    monkeypatch.setattr("research_swarm.control.report_details.DETAIL_BYTES", detail_budget)
    compact = report(pipeline, "reserve-pins-before-history", limit=1)["data"]["snapshot"][
        "research"
    ]
    assert [item["id"] for item in compact["candidates"]["items"]] == [baseline_id]
    assert any(item["id"] == pipeline.trial["id"] for item in compact["trials"]["items"])
    assert compact["candidates"]["complete"] is False


@pytest.mark.parametrize("committed", [False, True])
def test_baseline_requires_historical_commit_and_survives_correction_and_reinitialization(
    pipeline: Pipeline, tmp_path: Path, committed: bool
) -> None:
    receipt = evaluated(pipeline)
    assessment = assess(pipeline, receipt)
    if committed:
        commit(pipeline, assessment)
    pipeline.client.command(
        "evidence.invalidate",
        InvalidateEvidence(
            target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
            reason="Correct original evidence without rewriting baseline history",
        ),
        idempotency_key="correct-baseline",
    )
    assert pipeline.client.get("promotion", assessment["id"])["state"] == "affected"
    corrected = report(pipeline, "corrected-baseline")
    baseline = corrected["data"]["snapshot"]["research"]["baseline"]
    if not committed:
        assert baseline is None
        return
    assert baseline["promotion"]["id"] == assessment["id"]
    assert baseline["eligible_at_capture"] is False
    assert baseline["current_promotion_state"] == "affected"
    event = pipeline.client.events(after=baseline["commit_event"]["sequence"] - 1, limit=1)[
        "events"
    ][0]
    assert event["id"] == baseline["commit_event"]["id"]
    assert event["type"] == "promotion.committed"
    fresh = second_evaluation(pipeline, tmp_path)
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    reinitialized = pipeline.client.command(
        "promotion.reinitialize",
        AssessPromotion(
            campaign_id=campaign["id"],
            candidate_evaluation_id=fresh["id"],
            expected_incumbent_revision=campaign["data"]["incumbent_revision"],
        ),
        idempotency_key="reinitialize-baseline",
    )["result"]
    commit(pipeline, reinitialized, "commit-new-baseline")
    updated = report(pipeline, "reinitialized-baseline")["data"]["snapshot"]
    assert updated["research"]["baseline"] == baseline
    assert updated["incumbent"]["candidate_id"] != baseline["candidate_id"]
    assert updated["incumbent"]["eligibility"] == "eligible"
    assert pipeline.client.get("report", corrected["id"]) == corrected


def test_unselected_campaign_does_not_infer_a_baseline_from_its_first_trial(
    pipeline: Pipeline,
) -> None:
    captured = report(pipeline, "unselected-report")["data"]["snapshot"]["research"]
    assert captured["baseline"] is None
    assert captured["pinned_evaluation_ids"] == []
    assert captured["trials"]["total"] == 1
    assert captured["trials"]["items"][0]["execution_outcome"] == "pending"


def test_oversized_report_does_not_publish_partial_snapshot_or_mutate_campaign(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    budget = pipeline.client.budget(campaign["id"])
    monkeypatch.setattr("research_swarm.control.reports.MAX_REPORT_BYTES", 1)
    with pytest.raises(ResearchError) as failure:
        report(pipeline, "oversized-report")
    assert failure.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert pipeline.client.list(RecordQuery(kind="report"))["items"] == []
    assert pipeline.client.get("campaign", campaign["id"]) == campaign
    assert pipeline.client.budget(campaign["id"]) == budget
