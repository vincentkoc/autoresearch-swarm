from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.commands import ProposeCandidate, SubmitTrial, ValidateCandidate
from research_swarm.control.confirmation import CreateConfirmationPlan
from research_swarm.control.confirmation_models import ConfirmationData
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.models import PromotionConfiguration, StoppingRule, TrialData
from research_swarm.control.queries import RecordQuery
from research_swarm.control.workers import AttemptData, ClaimJob
from research_swarm.domain.budgets import BudgetKind, BudgetLimit
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from tests.control.test_promotions import assess, commit, evaluated
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.support import LIMITS


def options(exposures: int = 10, *, attempts: int = 20) -> dict[str, Any]:
    return {
        "promotion": PromotionConfiguration(
            mode="paired_confirmation",
            version="paired-sign-v1",
            confirmation_seeds=(101, 102, 103, 104, 105),
        ),
        "stopping": StoppingRule(max_trials=20),
        "limits": (
            BudgetLimit(dimension="attempts", unit="count", limit=attempts),
            LIMITS[1],
            BudgetLimit(
                dimension="evaluation_exposures",
                unit="count",
                kind=BudgetKind.EXPOSURE,
                limit=exposures,
            ),
        ),
    }


def setup_plan(
    pipeline: Pipeline, *, wrong_profile: bool = False, candidate_source: Path | None = None
) -> tuple[CreateConfirmationPlan, dict[str, Any]]:
    commit(pipeline, assess(pipeline, evaluated(pipeline)))
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    previous = pipeline.client.get("candidate", pipeline.trial["data"]["candidate_id"])
    artifact = (
        previous["data"]["source_artifact"]
        if candidate_source is None
        else pipeline.client.capture_project(
            candidate_source, idempotency_key="confirmation-source"
        )
    )
    candidate = pipeline.client.command(
        "candidate.propose",
        ProposeCandidate(
            campaign_id=campaign["id"],
            source_artifact=artifact,
            hypothesis="A preselected no-op replication control",
        ),
        idempotency_key="confirmation-candidate",
        expected_revision=campaign["revision"],
    )["result"]
    validation = pipeline.client.command(
        "candidate.validate",
        ValidateCandidate(candidate_id=candidate["id"]),
        idempotency_key="confirmation-validation",
        expected_revision=candidate["revision"],
    )["result"]
    fingerprint = pipeline.worker.profile.execution_fingerprint
    return CreateConfirmationPlan(
        campaign_id=campaign["id"],
        candidate_id=candidate["id"],
        validation_id=validation["id"],
        expected_incumbent_revision=1,
        evaluator_id=pipeline.runtime.principal().id,
        execution_fingerprints={
            "train": content_digest(b"unavailable-profile") if wrong_profile else fingerprint,
            "evaluate": fingerprint,
        },
    ), campaign


def create_plan(
    pipeline: Pipeline, intent: CreateConfirmationPlan, campaign: dict[str, Any]
) -> dict[str, Any]:
    return pipeline.client.command(
        "confirmation.create",
        intent,
        idempotency_key="sealed-plan",
        expected_revision=campaign["revision"],
    )["result"]


def exposure_budget(pipeline: Pipeline) -> dict[str, Any]:
    return next(
        item
        for item in pipeline.client.budget(pipeline.campaign["id"])["accounts"]
        if item["dimension"] == "evaluation_exposures"
    )


@pytest.mark.parametrize("pipeline", [options()], indirect=True)
def test_plan_atomically_binds_all_trials_and_reserves_exposure(pipeline: Pipeline) -> None:
    intent, campaign = setup_plan(pipeline)
    record = create_plan(pipeline, intent, campaign)
    plan = ConfirmationData.model_validate(record["data"]).plan
    plan.verify()
    assert create_plan(pipeline, intent, campaign) == record
    assert len(plan.bindings) == 10
    assert len({item.trial_id for item in plan.bindings}) == 10
    assert {(item.seed, item.side) for item in plan.bindings} == {
        (seed, side) for seed in (101, 102, 103, 104, 105) for side in ("candidate", "comparator")
    }
    assert exposure_budget(pipeline)["reserved"] == "10"
    for binding in plan.bindings:
        trial = pipeline.client.get("trial", binding.trial_id)
        assert trial["state"] == "queued"
        assert trial["data"]["spec"]["spec_digest"] == binding.spec_digest
    with pytest.raises(ResearchError) as second:
        pipeline.client.command(
            "confirmation.create",
            intent,
            idempotency_key="second-plan",
            expected_revision=campaign["revision"],
        )
    assert second.value.code == ErrorCode.EXPOSURE_LIMIT_REACHED
    with pytest.raises(ResearchError) as reuse:
        pipeline.client.command(
            "trial.submit",
            SubmitTrial(
                campaign_id=campaign["id"],
                candidate_id=intent.candidate_id,
                validation_id=intent.validation_id,
                seed=101,
            ),
            idempotency_key="unplanned-seed",
            expected_revision=campaign["revision"],
        )
    assert reuse.value.code == ErrorCode.COMPARISON_INELIGIBLE


@pytest.mark.parametrize("pipeline", [options(9)], indirect=True)
def test_insufficient_exposure_rolls_back_the_entire_panel(pipeline: Pipeline) -> None:
    intent, campaign = setup_plan(pipeline)
    with pytest.raises(ResearchError) as failure:
        create_plan(pipeline, intent, campaign)
    assert failure.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert (
        len(pipeline.client.list(RecordQuery(kind="trial", campaign_id=campaign["id"]))["items"])
        == 1
    )
    assert pipeline.client.list(RecordQuery(kind="evaluation_exposure"))["items"] == []
    assert exposure_budget(pipeline)["reserved"] == "0"


@pytest.mark.parametrize("pipeline", [options()], indirect=True)
def test_plan_rejects_wrong_training_profile_before_dispatch(pipeline: Pipeline) -> None:
    intent, campaign = setup_plan(pipeline, wrong_profile=True)
    create_plan(pipeline, intent, campaign)
    with pytest.raises(ResearchError) as failure:
        pipeline.worker.claim(pipeline.ready())
    assert failure.value.code == ErrorCode.COMPARISON_INELIGIBLE
    assert exposure_budget(pipeline)["uncertain"] == "0"


@pytest.mark.parametrize("pipeline", [options()], indirect=True)
def test_scoring_requires_prior_authority_and_consumes_separate_exposure(
    pipeline: Pipeline,
) -> None:
    intent, campaign = setup_plan(pipeline)
    record = create_plan(pipeline, intent, campaign)
    binding = ConfirmationData.model_validate(record["data"]).plan.bindings[0]
    trial = pipeline.client.get("trial", binding.trial_id)
    train_job = pipeline.client.get("job", trial["data"]["job_ids"][0])
    attempt = pipeline.worker.claim(train_job)
    pipeline.worker.execute(attempt["id"])
    scoring = pipeline.client.get("job", binding.scoring_job_id)
    with pytest.raises(ResearchError) as failure:
        pipeline.worker.client.command(
            "worker.claim_job",
            ClaimJob(
                worker_id=pipeline.worker.worker.binding.worker_id,
                session_id=pipeline.worker.worker.binding.worker_session_id,
                job_id=scoring["id"],
            ),
            idempotency_key="score-before-authority",
            expected_revision=scoring["revision"],
        )
    assert failure.value.code == ErrorCode.AUTHORIZATION_DENIED
    authority = pipeline.client.command(
        "evaluation.authorize",
        AuthorizeEvaluation(job_id=scoring["id"], evaluator_id=intent.evaluator_id),
        idempotency_key="confirm-authority",
        expected_revision=scoring["revision"],
    )["result"]
    assert authority["data"]["request"]["exposure_class"] == "confirmation"
    scoring = pipeline.client.get("job", scoring["id"])
    attempt = pipeline.worker.claim(scoring)
    assert exposure_budget(pipeline)["uncertain"] == "1"
    assert exposure_budget(pipeline)["reserved"] == "9"
    assert len(attempt["data"]["dispatch"]["reservation_ids"]) == 1
    pipeline.worker.execute(attempt["id"])
    assert exposure_budget(pipeline)["uncertain"] == "0"
    assert exposure_budget(pipeline)["consumed"] == "1"
    receipt = pipeline.client.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authority["id"], attempt_id=attempt["id"]),
        idempotency_key="confirm-complete",
    )["result"]
    assert receipt["state"] == "valid"
    assert pipeline.client.get("evaluation_exposure", binding.exposure_id)["state"] == "consumed"


@pytest.mark.parametrize("pipeline", [options()], indirect=True)
@pytest.mark.parametrize("cancel_scoring", [False, True])
def test_positive_no_access_proof_releases_exposure(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, cancel_scoring: bool
) -> None:
    intent, campaign = setup_plan(pipeline)
    binding = ConfirmationData.model_validate(
        create_plan(pipeline, intent, campaign)["data"]
    ).plan.bindings[0]
    trial_row = pipeline.client.get("trial", binding.trial_id)
    trial = TrialData.model_validate(trial_row["data"])
    attempt = pipeline.worker.claim(pipeline.client.get("job", trial.job_ids[0]))
    if not cancel_scoring:

        def broken(*args: Any, **kwargs: Any) -> Any:
            raise ValueError("Fixture training report failure")

        monkeypatch.setattr("research_swarm.workers.local.read_report", broken)
        assert pipeline.worker.execute(attempt["id"])["state"] == "failed"
    else:
        pipeline.worker.execute(attempt["id"])
        job = pipeline.client.get("job", binding.scoring_job_id)
        pipeline.client.command(
            "evaluation.authorize",
            AuthorizeEvaluation(job_id=job["id"], evaluator_id=intent.evaluator_id),
            idempotency_key="unstarted-authority",
            expected_revision=job["revision"],
        )
        attempt = pipeline.worker.claim(pipeline.client.get("job", job["id"]))
        assert exposure_budget(pipeline)["uncertain"] == "1"
        pipeline.worker._prepare(AttemptData.model_validate(attempt["data"]), trial)
        pipeline.worker.journal.request_cancel(attempt["id"], 1)
        assert pipeline.worker.execute(attempt["id"])["state"] == "not_started"
    budget = exposure_budget(pipeline)
    assert budget["reserved"] == "9" and budget["consumed"] == "0" and budget["uncertain"] == "0"
    assert budget["released_total"] == "1"
    assert pipeline.client.get("evaluation_exposure", binding.exposure_id)["state"] == "released"
