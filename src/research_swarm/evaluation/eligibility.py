"""Independent validation of an authenticated evaluator's receipt."""

from __future__ import annotations

from pydantic import ValidationError

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import EvaluationReceipt, EvaluationRequest
from research_swarm.domain.identity import Principal


def validate_receipt(
    principal: Principal, request: EvaluationRequest, receipt: EvaluationReceipt
) -> None:
    principal.require("evaluation.publish")
    try:
        request = EvaluationRequest.model_validate(request)
        receipt = EvaluationReceipt.model_validate(receipt)
    except ValidationError as error:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid evaluation request or receipt"
        ) from error
    if (
        principal.workspace_id != request.workspace_id
        or principal.id != request.evaluator_id
        or receipt.evaluator_id != principal.id
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Evaluator identity does not match")
    if (
        receipt.request_id != request.id
        or receipt.input_digest != request.input_artifact.digest
        or receipt.evaluator_contract_digest != request.evaluator_contract_digest
        or receipt.panel_digest != request.panel_digest
    ):
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Evaluation provenance does not match")
    if receipt.observed_seed != request.expected_seed:
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Observed seed does not match")
    if receipt.validity != "valid" or receipt.observed_members != request.expected_members:
        raise ResearchError(ErrorCode.EVALUATION_INCOMPLETE, "Evaluation panel is incomplete")
    expected = {definition.name: definition for definition in request.metric_schema}
    observed = {metric.name: metric for metric in receipt.metrics}
    if len(observed) != len(receipt.metrics) or set(expected) != set(observed):
        raise ResearchError(ErrorCode.EVALUATION_INCOMPLETE, "Metric panel does not match")
    for name, definition in expected.items():
        metric = observed[name]
        if metric.unit != definition.unit or metric.direction != definition.direction:
            raise ResearchError(
                ErrorCode.COMPARISON_INELIGIBLE, f"Metric semantics changed: {name}"
            )
