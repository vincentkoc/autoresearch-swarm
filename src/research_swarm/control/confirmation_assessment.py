"""Assess only the complete evidence panel selected before confirmation execution."""

from research_swarm.control.comparisons import EligibleEvaluation, eligible_evaluation
from research_swarm.control.confirmation_models import ConfirmationData, ConfirmationPlan
from research_swarm.control.confirmation_runtime import bound_confirmation
from research_swarm.control.models import CampaignData, JobData, TrialData
from research_swarm.control.promotions import PromotionAssessment
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import RunResult
from research_swarm.state.database import Transaction


def collect_panel(
    transaction: Transaction, plan: ConfirmationPlan, campaign: CampaignData
) -> tuple[EligibleEvaluation, ...]:
    evaluations: list[EligibleEvaluation] = []
    for binding in plan.bindings:
        trial = TrialData.model_validate(transaction.get("trial", binding.trial_id)["data"])
        bound = bound_confirmation(transaction, trial)
        if bound is None or bound[0] != plan or bound[1] != binding:
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Confirmation binding changed")
        job = JobData.model_validate(transaction.get("job", binding.scoring_job_id)["data"])
        if job.evaluation_request_id is None:
            raise ResearchError(ErrorCode.EVALUATION_INCOMPLETE, "Planned scoring lacks authority")
        authorization = transaction.get("evaluation_request", job.evaluation_request_id)["data"]
        if authorization["receipt_id"] is None:
            raise ResearchError(ErrorCode.EVALUATION_INCOMPLETE, "Planned scoring lacks a receipt")
        evaluation = eligible_evaluation(
            transaction, authorization["receipt_id"], plan.campaign_id, campaign
        )
        request = evaluation.authorization.request
        exposure = transaction.get("evaluation_exposure", binding.exposure_id)
        if (
            request.trial_id != binding.trial_id
            or request.evaluator_id != plan.evaluator_id
            or request.exposure_class != "confirmation"
            or request.exposure_authorization_id != binding.exposure_id
            or exposure["state"] != "consumed"
            or exposure["data"]["attempt_id"] != evaluation.evidence.attempt_id
        ):
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Planned exposure differs")
        for reference in evaluation.dependencies:
            if reference.kind == "run_result":
                result = RunResult.model_validate(
                    transaction.get(reference.kind, reference.id)["data"]
                )
                stage = JobData.model_validate(transaction.get("job", result.job_id)["data"]).stage
                if result.execution_fingerprint != plan.execution_fingerprints.get(stage.id):
                    raise ResearchError(
                        ErrorCode.COMPARISON_INELIGIBLE, "Actual profile differs from plan"
                    )
        if evaluations and evaluation.comparison_digest != evaluations[0].comparison_digest:
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Planned comparisons differ")
        evaluations.append(evaluation)
    return tuple(evaluations)


def verify_planned_assessment(transaction: Transaction, assessment: PromotionAssessment) -> None:
    if assessment.confirmation_plan_id is None:
        return
    row = transaction.get("confirmation_plan", assessment.confirmation_plan_id)
    data = ConfirmationData.model_validate(row["data"])
    plan = data.plan
    plan.verify()
    decision = assessment.decision
    if (
        row["state"] != "assessed"
        or data.decision_id != decision.id
        or decision.candidate_id != plan.candidate_id
        or decision.comparator_id != plan.comparator_id
        or decision.expected_incumbent_revision != plan.expected_incumbent_revision
        or decision.policy_version != plan.policy.version
        or assessment.gate is None
        or assessment.gate.outcome != "promote"
    ):
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Assessment differs from sealed plan")
