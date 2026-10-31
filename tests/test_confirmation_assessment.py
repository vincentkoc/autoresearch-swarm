from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.confirmation import AssessConfirmation
from research_swarm.control.confirmation_models import ConfirmationData
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import ReportCampaign
from research_swarm.control.validity import EvidenceReference
from tests.test_confirmation import create_plan, options, setup_plan
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_promotions import commit


@pytest.mark.parametrize(
    "pipeline", [{**options(attempts=30), "source_learning_rate": 0.0}], indirect=True
)
@pytest.mark.parametrize("failed_realization", [False, True])
def test_complete_fixed_panel_selects_confirmed_candidate_or_rejects_failed_realization(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
    failed_realization: bool,
) -> None:
    intent, campaign = setup_plan(
        pipeline, candidate_source=Path(__file__).resolve().parents[1] / "examples" / "cpu_bigram"
    )
    planned = create_plan(pipeline, intent, campaign)
    plan = ConfirmationData.model_validate(planned["data"]).plan
    pending = pipeline.client.command(
        "confirmation.assess",
        AssessConfirmation(plan_id=plan.id),
        idempotency_key="before-confirmation",
    )["result"]
    assert pending["outcome"] == "replicate" and len(pending["pending_trial_ids"]) == 10
    receipts = []
    for index, binding in enumerate(plan.bindings):
        trial = pipeline.client.get("trial", binding.trial_id)
        training = pipeline.client.get("job", trial["data"]["job_ids"][0])
        attempt = pipeline.worker.claim(training)
        if index == 0 and failed_realization:

            def broken(*args: Any, **kwargs: Any) -> Any:
                raise ValueError("A required realization failed")

            with monkeypatch.context() as patch:
                patch.setattr("research_swarm.workers.local.read_report", broken)
                pipeline.worker.execute(attempt["id"])
            continue
        pipeline.worker.execute(attempt["id"])
        job = pipeline.client.get("job", binding.scoring_job_id)
        authorization = pipeline.client.command(
            "evaluation.authorize",
            AuthorizeEvaluation(job_id=job["id"], evaluator_id=intent.evaluator_id),
            idempotency_key=f"authority-{index}",
            expected_revision=job["revision"],
        )["result"]
        attempt = pipeline.worker.claim(pipeline.client.get("job", job["id"]))
        pipeline.worker.execute(attempt["id"])
        receipts.append(
            pipeline.client.command(
                "evaluation.complete",
                CompleteEvaluation(request_id=authorization["id"], attempt_id=attempt["id"]),
                idempotency_key=f"receipt-{index}",
            )["result"]
        )
    assessment = pipeline.client.command(
        "confirmation.assess",
        AssessConfirmation(plan_id=plan.id),
        idempotency_key="after-confirmation",
    )["result"]
    assert (
        pipeline.client.command(
            "confirmation.assess",
            AssessConfirmation(plan_id=plan.id),
            idempotency_key="redelivered-confirmation",
        )["result"]
        == assessment
    )
    if failed_realization:
        assert assessment["state"] == "ineligible"
        assert assessment["data"]["ineligible_trial_ids"] == [plan.bindings[0].trial_id]
        assert len(pipeline.client.list(RecordQuery(kind="promotion"))["items"]) == 1
        return
    assert assessment["data"]["gate"]["outcome"] == "promote"
    assert assessment["data"]["gate"]["p_value"]["numerator"] == "1"
    assert assessment["data"]["gate"]["p_value"]["denominator"] == "32"
    assert assessment["data"]["evidence_level"] == "confirmed"
    assert len(assessment["data"]["decision"]["evaluation_ids"]) == 10
    assert commit(pipeline, assessment, "commit-confirmed")["state"] == "committed"
    selected = pipeline.client.get("campaign", campaign["id"])["data"]
    assert selected["incumbent_candidate_id"] == intent.candidate_id
    assert (
        selected["incumbent_revision"] == 2 and selected["incumbent_evidence_level"] == "confirmed"
    )
    report = pipeline.client.command(
        "report.create",
        ReportCampaign(campaign_id=campaign["id"]),
        idempotency_key="report-confirmed-selection",
    )["result"]["data"]
    snapshot = report["snapshot"]
    assert snapshot["incumbent"]["evidence_level"] == "confirmed"
    assert snapshot["evaluation_claim_support"]["protected"] is False
    assert snapshot["evaluation_claim_support"]["official"] is False
    exposures = {
        item["data"]["exposure_authorization_id"]
        for item in snapshot["recent_evaluations"]
        if item["data"]["exposure_class"] == "confirmation"
    }
    assert exposures == {binding.exposure_id for binding in plan.bindings}
    assert all(
        item["data"]["measurement_trust"] == "authenticated"
        for item in snapshot["recent_evaluations"]
    )
    assert "| authenticated | confirmation | yes |" in report["markdown"]
    assert (
        "Protected evaluation: unavailable. Official evaluation: unavailable." in report["markdown"]
    )
    pipeline.client.command(
        "evidence.invalidate",
        InvalidateEvidence(
            target=EvidenceReference(kind="evaluation_receipt", id=receipts[-1]["id"]),
            reason="A paired comparator receipt was corrected",
        ),
        idempotency_key="correct-one-pair",
    )
    assert (
        pipeline.client.get("campaign", campaign["id"])["data"]["incumbent_eligibility"]
        == "suspended"
    )
