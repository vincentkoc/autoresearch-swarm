"""Current execution and input bindings for a completed ordinary training trial."""

from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.evaluations import EvaluationAuthorization, evaluation_inputs
from research_swarm.control.learning_costs import attempt_cost
from research_swarm.control.learning_inputs import require_prepared_inputs
from research_swarm.control.learning_models import LearningPlan, QualifiedTrajectoryDataset
from research_swarm.control.learning_profiles import require_training_profile
from research_swarm.control.learning_training_models import (
    PolicyTrainingAdmission,
    PolicyTrainingConfiguration,
)
from research_swarm.control.models import CampaignData, JobData
from research_swarm.control.worker_records import AttemptData
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import RunResult
from research_swarm.domain.experiments import Direction
from research_swarm.domain.learning import (
    BudgetSettlementEvidence,
    CategoricalPolicy,
    FiniteNumber,
    LearningEvidence,
    TrainingJob,
)
from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.state.database import Transaction


class TrainingEvidence(VersionedRecord):
    training: TrainingJob
    admission_digest: Digest
    profile_digest: Digest
    inputs_digest: Digest
    checker_job_id: ID
    checker_attempt_id: ID
    reward_spec_digest: Digest
    sample_count: int
    behavior: CategoricalPolicy
    configuration: PolicyTrainingConfiguration
    evaluation_digest: Digest
    executions: tuple[LearningEvidence, ...]
    settlements: tuple[BudgetSettlementEvidence, ...]
    update_error: FiniteNumber

    @property
    def digest(self) -> str:
        return record_digest("research.training-evidence.v1", self.model_dump(mode="json"))


def invalid(message: str) -> None:
    raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, message)


def training_evidence(transaction: Transaction, admission_id: str) -> TrainingEvidence:
    row = transaction.get("learning_admission", admission_id)
    admission = PolicyTrainingAdmission.model_validate(row["data"])
    intent = admission.request
    profile = require_training_profile(transaction, intent.profile_id)
    inputs = require_prepared_inputs(transaction, intent.inputs_id)
    inputs_digest = record_digest(
        "research.prepared-policy-inputs.v1", inputs.model_dump(mode="json")
    )
    campaign = CampaignData.model_validate(transaction.get("campaign", intent.campaign_id)["data"])
    constants = {"behavior": inputs.behavior, "trajectories": inputs.trajectories}
    if (
        row["state"] != "immutable"
        or row["id"] != intent.admission_id
        or row["parent_id"] != admission.trial_id
        or row["campaign_id"] != intent.campaign_id
        or admission.profile_digest != profile.digest
        or admission.inputs_digest != inputs_digest
        or admission.dataset_digest != inputs.dataset_digest
        or admission.behavior_policy_digest != inputs.behavior_policy_digest
        or campaign.contract.project_id != profile.project_id
        or campaign.contract.project_revision_id != profile.request.project_revision_id
        or campaign.contract.primary_metric != "update_error"
        or campaign.contract.input_artifacts != constants
    ):
        invalid("Training admission no longer matches its profile and inputs")
    trainer_row = transaction.get("job", admission.trainer_job_id)
    checker_row = transaction.get("job", admission.checker_job_id)
    trainer = JobData.model_validate(trainer_row["data"])
    checker = JobData.model_validate(checker_row["data"])
    if (
        trainer.current_attempt_id is None
        or checker.current_attempt_id is None
        or checker.evaluation_request_id is None
    ):
        raise ResearchError(ErrorCode.EVALUATION_INCOMPLETE, "Training lacks completed evaluation")
    authorization = EvaluationAuthorization.model_validate(
        transaction.get("evaluation_request", checker.evaluation_request_id)["data"]
    )
    if authorization.receipt_id is None:
        raise ResearchError(ErrorCode.EVALUATION_INCOMPLETE, "Training checker lacks its receipt")
    eligible = eligible_evaluation(
        transaction, authorization.receipt_id, intent.campaign_id, campaign
    )
    trial = eligible.trial
    if (
        trial.spec.trial_id != admission.trial_id
        or trial.candidate_id != intent.candidate_id
        or trial.validation_id != intent.validation_id
        or trial.spec.seed != intent.seed
        or set(trial.job_ids) != {admission.trainer_job_id, admission.checker_job_id}
        or trainer.stage.id != profile.request.trainer_stage_id
        or checker.stage.id != profile.request.checker_stage_id
        or trainer.inputs != constants
        or trainer.input_sources
        or trainer.predecessor_ids
        or set(trainer.outputs) != {"policy", "update"}
        or checker.predecessor_ids != (admission.trainer_job_id,)
        or checker.input_sources
        != {"policy": admission.trainer_job_id, "update": admission.trainer_job_id}
        or authorization.job_id != admission.checker_job_id
        or eligible.evidence.job_id != admission.checker_job_id
        or eligible.evidence.attempt_id != checker.current_attempt_id
    ):
        invalid("Training jobs or checker authority differ from admission")
    expected_checker_inputs = {**constants, **trainer.outputs}
    if (
        checker.inputs != expected_checker_inputs
        or evaluation_inputs(transaction, checker) != expected_checker_inputs
        or authorization.inputs != expected_checker_inputs
    ):
        invalid("Checker did not evaluate the admitted trainer outputs")
    metric = eligible.metric
    if (
        metric.name != "update_error"
        or metric.direction != Direction.MINIMIZE
        or metric.unit != "absolute_parameter_error"
        or not 0 <= metric.value <= profile.configuration.maximum_update_error
    ):
        invalid("Training checker exceeds the approved numerical tolerance")
    evidence: list[LearningEvidence] = []
    settlements = []
    for job_row, job, expected_inputs in (
        (trainer_row, trainer, constants),
        (checker_row, checker, expected_checker_inputs),
    ):
        assert job.current_attempt_id is not None
        attempt_row = transaction.get("attempt", job.current_attempt_id)
        attempt = AttemptData.model_validate(attempt_row["data"])
        result = RunResult.model_validate(
            transaction.get("run_result", job.current_attempt_id)["data"]
        )
        if (
            job_row["parent_id"] != admission.trial_id
            or job_row["campaign_id"] != intent.campaign_id
            or job.trial_id != admission.trial_id
            or attempt_row["state"] != "succeeded"
            or attempt.publication_status != "accepted"
            or attempt.inputs != expected_inputs
            or attempt.dispatch.job_id != job_row["id"]
            or attempt.dispatch.stage_id != job.stage.id
            or (job is checker and attempt.evaluation_request_id != checker.evaluation_request_id)
            or any(reference not in result.artifacts for reference in job.outputs.values())
        ):
            invalid("Training execution inputs or named outputs differ")
        proof = attempt_cost(
            transaction, job.current_attempt_id, admission.trial_id, trial, evidence
        )
        settlements.append(
            BudgetSettlementEvidence(
                id=proof.settlement_id,
                request_digest=proof.request_digest,
                response_digest=proof.response_digest,
            )
        )
    qualified = QualifiedTrajectoryDataset.model_validate(
        transaction.get("learning_dataset", inputs.request.dataset_id)["data"]
    )
    plan = LearningPlan.model_validate(transaction.get("learning_plan", qualified.plan_id)["data"])
    return TrainingEvidence(
        training=TrainingJob(
            campaign_id=intent.campaign_id,
            trial_id=admission.trial_id,
            job_id=admission.trainer_job_id,
            attempt_id=trainer.current_attempt_id,
            dataset_digest=inputs.dataset_digest,
            behavior_policy_digest=inputs.behavior_policy_digest,
            policy_artifact=trainer.outputs["policy"],
            update_report=trainer.outputs["update"],
            evaluator_receipt_id=authorization.receipt_id,
        ),
        admission_digest=record_digest(
            "research.policy-training-admission.v1", admission.model_dump(mode="json")
        ),
        profile_digest=profile.digest,
        inputs_digest=inputs_digest,
        checker_job_id=admission.checker_job_id,
        checker_attempt_id=checker.current_attempt_id,
        reward_spec_digest=qualified.dataset.reward_spec.digest,
        sample_count=len(qualified.dataset.trajectories),
        behavior=plan.request.policy,
        configuration=profile.configuration,
        evaluation_digest=record_digest(
            "research.training-evaluation.v1", eligible.evidence.model_dump(mode="json")
        ),
        executions=tuple(evidence),
        settlements=tuple(settlements),
        update_error=metric.value,
    )
