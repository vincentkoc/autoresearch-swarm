from __future__ import annotations

import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.commands import ProposeCandidate, SubmitTrial, ValidateCandidate
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.promotions import AssessPromotion, CommitPromotion, InvalidateEvidence
from research_swarm.control.reports import ReportCampaign
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import RunResult
from research_swarm.domain.identity import CommandContext
from research_swarm.state.database import Transaction
from tests.test_evaluation_control import prepare_evaluation
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline


def evaluated(pipeline: Pipeline) -> dict[str, Any]:
    authority, evaluator, _ = prepare_evaluation(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker.execute(attempt["id"])
    return evaluator.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authority["id"], attempt_id=attempt["id"]),
        idempotency_key="baseline-evaluation",
    )["result"]


def assess(
    pipeline: Pipeline, receipt: dict[str, Any], key: str = "assess", revision: int = 0
) -> dict[str, Any]:
    return pipeline.client.command(
        "promotion.assess",
        AssessPromotion(
            campaign_id=pipeline.campaign["id"],
            candidate_evaluation_id=receipt["id"],
            expected_incumbent_revision=revision,
        ),
        idempotency_key=key,
    )["result"]


def commit(pipeline: Pipeline, assessment: dict[str, Any], key: str = "commit") -> dict[str, Any]:
    return pipeline.client.command(
        "promotion.commit", CommitPromotion(decision_id=assessment["id"]), idempotency_key=key
    )["result"]


def second_evaluation(pipeline: Pipeline, tmp_path: Path) -> dict[str, Any]:
    source = tmp_path / "zero-learning"
    shutil.copytree(Path(__file__).resolve().parents[1] / "examples" / "cpu_bigram", source)
    train = source / "train.py"
    train.write_text(
        train.read_text(encoding="utf-8").replace("LEARNING_RATE = 0.35", "LEARNING_RATE = 0.0"),
        encoding="utf-8",
    )
    artifact = pipeline.client.capture_project(source, idempotency_key="candidate-source")
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    candidate = pipeline.client.command(
        "candidate.propose",
        ProposeCandidate(
            campaign_id=campaign["id"],
            source_artifact=artifact,
            hypothesis="A zero-learning control",
        ),
        idempotency_key="second-candidate",
        expected_revision=campaign["revision"],
    )["result"]
    validation = pipeline.client.command(
        "candidate.validate",
        ValidateCandidate(candidate_id=candidate["id"]),
        idempotency_key="second-validation",
        expected_revision=candidate["revision"],
    )["result"]
    pipeline.client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=candidate["id"],
            validation_id=validation["id"],
            seed=7,
        ),
        idempotency_key="second-trial",
        expected_revision=campaign["revision"],
    )
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker.execute(attempt["id"])
    job = pipeline.ready()
    authority = pipeline.client.command(
        "evaluation.authorize",
        AuthorizeEvaluation(job_id=job["id"], evaluator_id=pipeline.runtime.principal().id),
        idempotency_key="second-authority",
        expected_revision=job["revision"],
    )["result"]
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker.execute(attempt["id"])
    return pipeline.client.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authority["id"], attempt_id=attempt["id"]),
        idempotency_key="second-evaluation",
    )["result"]


def test_baseline_is_observed_and_concurrent_commits_preserve_losing_assessment(
    pipeline: Pipeline,
) -> None:
    receipt = evaluated(pipeline)
    assessments = [assess(pipeline, receipt, f"assessment-{index}") for index in range(2)]
    assert assessments[0]["data"]["decision"]["reason_codes"] == ["baseline_initialized"]
    assert assessments[0]["data"]["decision"]["improvement"] is None

    def attempt(index: int) -> dict[str, Any] | ErrorCode:
        try:
            return commit(pipeline, assessments[index], f"commit-{index}")
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, range(2)))
    assert results.count(ErrorCode.INCUMBENT_REVISION_CONFLICT) == 1
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])["data"]
    assert campaign["incumbent_revision"] == 1
    assert campaign["incumbent_evidence_level"] == "observed"
    assert {pipeline.client.get("promotion", item["id"])["state"] for item in assessments} == {
        "assessed",
        "committed",
    }
    winner = next(index for index, result in enumerate(results) if isinstance(result, dict))
    assert commit(pipeline, assessments[winner], f"commit-{winner}") == results[winner]


def test_observed_worse_candidate_is_recorded_and_cannot_commit(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    baseline = assess(pipeline, evaluated(pipeline))
    commit(pipeline, baseline)
    receipt = second_evaluation(pipeline, tmp_path)
    rejected = assess(pipeline, receipt, "worse", 1)
    assert rejected["data"]["decision"]["outcome"] == "reject"
    assert rejected["data"]["decision"]["improvement"] < 0
    with pytest.raises(ResearchError) as failure:
        commit(pipeline, rejected, "reject-cannot-commit")
    assert failure.value.code == ErrorCode.INVALID_TRANSITION
    assert (
        pipeline.client.get("campaign", pipeline.campaign["id"])["data"]["incumbent_revision"] == 1
    )


@pytest.mark.parametrize("after_commit", [False, True])
def test_correction_preserves_evidence_and_suspends_dependent_selection(
    pipeline: Pipeline, after_commit: bool
) -> None:
    receipt = evaluated(pipeline)
    assessment = assess(pipeline, receipt)
    if after_commit:
        commit(pipeline, assessment)
    intent = InvalidateEvidence(
        target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
        reason="Fixture discovered an invalid panel",
    )
    corrected = pipeline.client.command("evidence.invalidate", intent, idempotency_key="correction")
    assert (
        pipeline.client.command("evidence.invalidate", intent, idempotency_key="correction")
        == corrected
    )
    assert pipeline.client.get("evaluation_receipt", receipt["id"]) == receipt
    affected = pipeline.client.get("promotion", assessment["id"])
    assert affected["state"] == "affected" and affected["data"] == assessment["data"]
    with pytest.raises(ResearchError) as failure:
        commit(pipeline, assessment, "commit-after-correction")
    assert failure.value.code == ErrorCode.EVIDENCE_INVALIDATED
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])["data"]
    assert campaign["incumbent_eligibility"] == ("suspended" if after_commit else "eligible")
    assert campaign["incumbent_revision"] == (2 if after_commit else 0)
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == "invalid"
    )


def test_comparison_uses_training_hardware_not_only_evaluator(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    commit(pipeline, assess(pipeline, evaluated(pipeline)))
    receipt = second_evaluation(pipeline, tmp_path)
    authority = pipeline.client.get("evaluation_request", receipt["data"]["receipt"]["request_id"])
    trial = pipeline.client.get("trial", authority["data"]["request"]["trial_id"])
    job = pipeline.client.get("job", trial["data"]["job_ids"][0])

    # Model a different, independently accepted training profile. Evaluator is unchanged.
    def change(transaction: Transaction) -> dict[str, Any]:
        row = transaction.get("run_result", job["data"]["current_attempt_id"])
        result = (
            RunResult.model_validate(row["data"])
            .model_copy(
                update={
                    "execution_fingerprint": content_digest(b"other-physical-training-engine"),
                    "result_digest": None,
                }
            )
            .sealed()
        )
        return transaction.update(
            "run_result",
            row["id"],
            expected_revision=row["revision"],
            data=result.model_dump(mode="json"),
            state=row["state"],
        )

    pipeline.runtime.database.execute(
        CommandContext(
            principal=pipeline.runtime.principal(), idempotency_key="fixture-different-training"
        ),
        "fixture.training",
        {},
        "*",
        change,
    )
    with pytest.raises(ResearchError) as failure:
        assess(pipeline, receipt, "hardware-mismatch", 1)
    assert failure.value.code == ErrorCode.COMPARISON_INELIGIBLE
    assert "contracts differ" in failure.value.message
    report = pipeline.client.command(
        "report.create",
        ReportCampaign(campaign_id=pipeline.campaign["id"]),
        idempotency_key="separate-hardware-report",
    )["result"]["data"]["snapshot"]
    assert report["evaluation_window"]["complete"]
    assert len(report["observed_cohorts"]) == 2
    assert all(item["observation_count"] == 1 for item in report["observed_cohorts"])


def test_explicit_reinitialization_uses_fresh_evidence_without_claiming_improvement(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    first = evaluated(pipeline)
    baseline = assess(pipeline, first)
    commit(pipeline, baseline)
    pipeline.client.command(
        "evidence.invalidate",
        InvalidateEvidence(
            target=EvidenceReference(kind="evaluation_receipt", id=first["id"]),
            reason="Baseline evidence corrected",
        ),
        idempotency_key="invalidate-baseline",
    )
    fresh = second_evaluation(pipeline, tmp_path)
    with pytest.raises(ResearchError) as ordinary:
        assess(pipeline, fresh, "ordinary-cannot-clear-suspension", 2)
    assert ordinary.value.code == ErrorCode.EVIDENCE_INVALIDATED
    recovery = pipeline.client.command(
        "promotion.reinitialize",
        AssessPromotion(
            campaign_id=pipeline.campaign["id"],
            candidate_evaluation_id=fresh["id"],
            expected_incumbent_revision=2,
        ),
        idempotency_key="explicit-recovery",
    )["result"]
    assert recovery["data"]["decision"]["reason_codes"] == ["baseline_reinitialized"]
    assert recovery["data"]["decision"]["improvement"] is None
    assert recovery["data"]["evidence_level"] == "observed"
    commit(pipeline, recovery, "commit-recovery")
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])["data"]
    assert campaign["incumbent_revision"] == 3
    assert campaign["incumbent_eligibility"] == "eligible"
    assert pipeline.client.get("promotion", baseline["id"])["state"] == "affected"
