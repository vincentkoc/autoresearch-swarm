"""Shared admission and settlement rules for preselected confirmation trials."""

from decimal import Decimal

from research_swarm.control.confirmation_models import (
    ConfirmationData,
    ConfirmationPlan,
    ExposureData,
    PlanBinding,
)
from research_swarm.control.models import JobData, TrialData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import EvaluationRequest
from research_swarm.state.database import Transaction
from research_swarm.state.ledger import BudgetLedger


def bound_confirmation(
    transaction: Transaction, trial: TrialData
) -> tuple[ConfirmationPlan, PlanBinding] | None:
    if trial.confirmation_plan_id is None:
        return None
    plan = ConfirmationData.model_validate(
        transaction.get("confirmation_plan", trial.confirmation_plan_id)["data"]
    ).plan
    plan.verify()
    binding = next((item for item in plan.bindings if item.trial_id == trial.spec.trial_id), None)
    if binding is None or (
        binding.spec_digest != trial.spec.spec_digest
        or binding.seed != trial.spec.seed
        or binding.candidate_id != trial.candidate_id
        or binding.side != trial.confirmation_side
        or trial.spec.extensions.get("confirmation_plan_id") != plan.id
    ):
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Trial differs from confirmation plan")
    return plan, binding


def claim_confirmation(
    transaction: Transaction, trial: TrialData, job: JobData, fingerprint: str, attempt_id: str
) -> None:
    bound = bound_confirmation(transaction, trial)
    if bound is None:
        return
    plan, binding = bound
    if plan.execution_fingerprints.get(job.stage.id) != fingerprint:
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Execution profile differs from plan")
    if job.stage.kind != "evaluate":
        return
    if job.evaluation_request_id is None or job.evaluation_exposure_id != binding.exposure_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Confirmation scoring lacks authority")
    authorization = transaction.get("evaluation_request", job.evaluation_request_id)
    request = EvaluationRequest.model_validate(authorization["data"]["request"])
    if (
        request.trial_id != binding.trial_id
        or request.evaluator_id != plan.evaluator_id
        or request.exposure_class != "confirmation"
        or request.exposure_authorization_id != binding.exposure_id
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Scoring authority differs from plan")
    row = transaction.get("evaluation_exposure", binding.exposure_id)
    exposure = ExposureData.model_validate(row["data"])
    if row["state"] != "reserved" or exposure.attempt_id is not None:
        raise ResearchError(ErrorCode.EXPOSURE_LIMIT_REACHED, "Exposure was already admitted")
    # Dispatch permits access. Until native closure proves otherwise, retain uncertainty.
    BudgetLedger.mark_uncertain(transaction, exposure.reservation_id)
    changed = transaction.update(
        "evaluation_exposure",
        row["id"],
        expected_revision=row["revision"],
        state="uncertain",
        data=exposure.model_copy(update={"attempt_id": attempt_id}).model_dump(mode="json"),
    )
    transaction.event("evaluation.exposure_admitted", changed, {"attempt_id": attempt_id})


def settle_exposure(
    transaction: Transaction, job: JobData, attempt_id: str, *, not_started: bool
) -> None:
    if job.evaluation_exposure_id is None:
        return
    row = transaction.get("evaluation_exposure", job.evaluation_exposure_id)
    exposure = ExposureData.model_validate(row["data"])
    if exposure.attempt_id != attempt_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Exposure belongs to another attempt")
    BudgetLedger.settle(
        transaction,
        exposure.reservation_id,
        row["id"],
        {"evaluation_exposures": Decimal(0 if not_started else 1)},
    )
    changed = transaction.update(
        "evaluation_exposure",
        row["id"],
        expected_revision=row["revision"],
        state="released" if not_started else "consumed",
        data=row["data"],
    )
    transaction.event("evaluation.exposure_settled", changed, {"not_started": not_started})


def release_unused_exposure(transaction: Transaction, job: JobData) -> None:
    if job.evaluation_exposure_id is None or job.current_attempt_id is not None:
        return
    row = transaction.get("evaluation_exposure", job.evaluation_exposure_id)
    exposure = ExposureData.model_validate(row["data"])
    if row["state"] != "reserved" or exposure.attempt_id is not None:
        raise ResearchError(ErrorCode.EXECUTION_UNKNOWN, "Exposure may have been dispatched")
    BudgetLedger.settle(
        transaction, exposure.reservation_id, row["id"], {"evaluation_exposures": Decimal(0)}
    )
    changed = transaction.update(
        "evaluation_exposure",
        row["id"],
        expected_revision=row["revision"],
        state="released",
        data=row["data"],
    )
    transaction.event(
        "evaluation.exposure_released", changed, {"reason": "dependency_failed_before_admission"}
    )
