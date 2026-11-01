from __future__ import annotations

import pytest

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef, EvaluationReceipt, EvaluationRequest
from research_swarm.domain.experiments import Direction, Metric, MetricDefinition
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.evaluation.eligibility import validate_receipt


def fixtures() -> tuple[Principal, EvaluationRequest, EvaluationReceipt]:
    digest = content_digest(b"synthetic-evaluation")
    actor = Principal(id=new_id(), workspace_id=new_id(), scopes=frozenset({"evaluation.publish"}))
    request = EvaluationRequest(
        workspace_id=actor.workspace_id,
        campaign_id=new_id(),
        candidate_id=new_id(),
        trial_id=new_id(),
        input_artifact=ArtifactRef(id=new_id(), digest=digest, size_bytes=20),
        evaluator_id=actor.id,
        evaluator_contract_digest=digest,
        panel_digest=digest,
        comparison_class="synthetic-v1",
        metric_schema=(MetricDefinition(name="error", unit="mse", direction=Direction.MINIMIZE),),
        expected_seed=42,
        expected_members=frozenset({"a", "b"}),
        exposure_class="confirmation",
    )
    receipt = EvaluationReceipt(
        request_id=request.id,
        evaluator_id=actor.id,
        input_digest=digest,
        evaluator_contract_digest=digest,
        panel_digest=digest,
        observed_seed=42,
        observed_members=frozenset({"a", "b"}),
        metrics=(Metric(name="error", value=0.3, unit="mse", direction=Direction.MINIMIZE),),
        validity="valid",
        execution_fingerprint=digest,
    )
    return actor, request, receipt


def test_authenticated_complete_receipt() -> None:
    validate_receipt(*fixtures())


def test_training_worker_cannot_publish_measurements_as_evaluator() -> None:
    actor, request, receipt = fixtures()
    worker = actor.model_copy(update={"id": new_id(), "scopes": frozenset({"worker.publish"})})
    with pytest.raises(ResearchError) as caught:
        validate_receipt(worker, request, receipt)
    assert caught.value.code == ErrorCode.AUTHORIZATION_DENIED


@pytest.mark.parametrize("value", [False, True, "0.0"])
def test_non_numeric_metric_is_not_a_score(value: object) -> None:
    actor, request, receipt = fixtures()
    invalid_metric = receipt.metrics[0].model_copy(update={"value": value})
    with pytest.raises(ResearchError) as caught:
        validate_receipt(actor, request, receipt.model_copy(update={"metrics": (invalid_metric,)}))
    assert caught.value.code == ErrorCode.PROJECT_CONTRACT_INVALID


@pytest.mark.parametrize("empty", [True, False])
def test_empty_or_duplicate_expected_metrics_are_rejected(empty: bool) -> None:
    actor, request, receipt = fixtures()
    metrics = () if empty else (request.metric_schema[0], request.metric_schema[0])
    with pytest.raises(ResearchError) as caught:
        validate_receipt(actor, request.model_copy(update={"metric_schema": metrics}), receipt)
    assert caught.value.code == ErrorCode.PROJECT_CONTRACT_INVALID


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("observed_seed", 43, ErrorCode.COMPARISON_INELIGIBLE),
        ("observed_members", frozenset({"a"}), ErrorCode.EVALUATION_INCOMPLETE),
        ("input_digest", content_digest(b"other"), ErrorCode.COMPARISON_INELIGIBLE),
    ],
)
def test_mismatched_evaluation_is_not_rankable(field: str, value: object, code: ErrorCode) -> None:
    actor, request, receipt = fixtures()
    with pytest.raises(ResearchError) as caught:
        validate_receipt(actor, request, receipt.model_copy(update={field: value}))
    assert caught.value.code == code
