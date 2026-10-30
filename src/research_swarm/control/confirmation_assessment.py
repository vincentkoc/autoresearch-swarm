"""Assess only the complete evidence panel selected before confirmation execution."""

from research_swarm.control.comparisons import EligibleEvaluation, eligible_evaluation
from research_swarm.control.confirmation_models import ConfirmationData, ConfirmationPlan
from research_swarm.control.confirmation_runtime import bound_confirmation
from research_swarm.control.models import CampaignData, JobData, TrialData
from research_swarm.control.promotions import PromotionAssessment
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import PromotionDecision, RunResult
from research_swarm.evaluation.statistics import PairedObservation, paired_sign_gate
from research_swarm.state.database import JsonObject, Transaction


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


def assess_plan(transaction: Transaction, plan_id: str) -> JsonObject:
    row = transaction.get("confirmation_plan", plan_id)
    data = ConfirmationData.model_validate(row["data"])
    plan = data.plan
    plan.verify()
    if data.decision_id is not None:
        return transaction.get("promotion", data.decision_id)
    if row["state"] == "ineligible":
        return row
    campaign = CampaignData.model_validate(transaction.get("campaign", plan.campaign_id)["data"])
    if campaign.contract_digest != plan.campaign_contract_digest:
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Campaign contract differs from plan")
    pending, failed = [], []
    for binding in plan.bindings:
        trial = TrialData.model_validate(transaction.get("trial", binding.trial_id)["data"])
        if trial.execution_outcome in {"pending", "unknown"} or (
            trial.execution_outcome == "succeeded" and trial.evaluation_status == "pending"
        ):
            pending.append(binding.trial_id)
        elif trial.execution_outcome != "succeeded" or trial.evaluation_status != "valid":
            failed.append(binding.trial_id)
    if pending:
        return {
            "plan_id": plan_id,
            "outcome": "replicate",
            "pending_trial_ids": pending,
            "ineligible_trial_ids": failed,
            "reason": "fixed_panel_incomplete",
        }
    if failed:
        updated = transaction.update(
            "confirmation_plan",
            plan_id,
            expected_revision=row["revision"],
            state="ineligible",
            data=data.model_copy(update={"ineligible_trial_ids": tuple(failed)}).model_dump(
                mode="json"
            ),
        )
        transaction.event(
            "confirmation.ineligible", updated, {"reason": "required_realizations_failed"}
        )
        return updated
    evaluations = collect_panel(transaction, plan, campaign)
    indexed = {
        (binding.seed, binding.side): evaluation
        for binding, evaluation in zip(plan.bindings, evaluations, strict=True)
    }
    pairs = tuple(
        PairedObservation(
            seed=seed,
            candidate=indexed[seed, "candidate"].metric.value,
            comparator=indexed[seed, "comparator"].metric.value,
        )
        for seed in plan.policy.seeds
    )
    gate = paired_sign_gate(plan.policy, pairs, evaluations[0].metric.direction)
    first_candidate = indexed[plan.policy.seeds[0], "candidate"]
    first_comparator = indexed[plan.policy.seeds[0], "comparator"]
    decision = PromotionDecision(
        campaign_id=plan.campaign_id,
        candidate_id=plan.candidate_id,
        comparator_id=plan.comparator_id,
        expected_incumbent_revision=plan.expected_incumbent_revision,
        evaluation_ids=tuple(item.evidence.receipt.id for item in evaluations),
        comparison_class=campaign.contract.comparison_class,
        policy_version=plan.policy.version,
        outcome=gate.outcome,
        improvement=gate.finite_mean(),
        reason_codes=(gate.reason,),
        explanation="Fixed paired sign gate under the declared contract. No generalization claim.",
    )
    assessment = PromotionAssessment(
        decision=decision,
        candidate_evaluation_id=first_candidate.evidence.receipt.id,
        comparator_evaluation_id=first_comparator.evidence.receipt.id,
        comparison_digest=first_candidate.comparison_digest,
        dependencies=tuple(reference for item in evaluations for reference in item.dependencies),
        evidence_level="confirmed" if gate.outcome == "promote" else "observed",
        exact_improvement=gate.mean_improvement,
        confirmation_plan_id=plan.id,
        gate=gate,
    )
    record = transaction.insert(
        "promotion",
        decision.id,
        assessment.model_dump(mode="json"),
        state="assessed",
        campaign_id=plan.campaign_id,
        parent_id=plan.candidate_id,
    )
    updated = transaction.update(
        "confirmation_plan",
        plan_id,
        expected_revision=row["revision"],
        state="assessed",
        data=data.model_copy(update={"decision_id": decision.id}).model_dump(mode="json"),
    )
    transaction.event("promotion.assessed", record, {"outcome": gate.outcome, "plan_id": plan_id})
    transaction.event("confirmation.assessed", updated, {"decision_id": decision.id})
    return record


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
