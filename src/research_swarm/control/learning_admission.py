"""Admit qualified policy training through the ordinary campaign trial transaction."""

from collections.abc import Callable

from research_swarm.control.benchmark_guard import require_benchmark_training
from research_swarm.control.commands import SubmitTrial
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_inputs import require_prepared_inputs
from research_swarm.control.learning_profiles import require_training_profile
from research_swarm.control.learning_training_models import (
    AdmitPolicyTraining,
    PolicyTrainingAdmission,
)
from research_swarm.control.models import CampaignData, JobData, TrialData
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, JsonObject, Transaction


class LearningAdmissionControl:
    def __init__(
        self,
        database: Database,
        submit_trial: Callable[[Transaction, SubmitTrial, str], JsonObject],
    ) -> None:
        self.database = database
        self.submit_trial = submit_trial

    def admit(self, context: CommandContext, intent: AdmitPolicyTraining) -> JsonObject:
        context.principal.require("trial.submit")
        context.principal.require("read")
        payload = intent.model_dump(mode="json")

        def commit(transaction: Transaction) -> JsonObject:
            previous = optional_record(transaction, "learning_admission", intent.admission_id)
            if previous is not None:
                if previous["data"]["request"] != payload:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Training admission is immutable"
                    )
                return previous
            campaign_row = transaction.get("campaign", intent.campaign_id)
            if context.expected_revision != campaign_row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Training campaign changed")
            campaign = CampaignData.model_validate(campaign_row["data"])
            profile = require_training_profile(transaction, intent.profile_id)
            inputs = require_prepared_inputs(transaction, intent.inputs_id)
            require_benchmark_training(transaction, intent, inputs, profile)
            if (
                campaign.contract.project_id != profile.project_id
                or campaign.contract.project_revision_id != profile.request.project_revision_id
                or campaign.contract.primary_metric != "update_error"
                or campaign.contract.input_artifacts
                != {"behavior": inputs.behavior, "trajectories": inputs.trajectories}
            ):
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "Training campaign must use its approved profile and exact prepared inputs",
                )
            trial_row = self.submit_trial(
                transaction,
                SubmitTrial(
                    campaign_id=intent.campaign_id,
                    candidate_id=intent.candidate_id,
                    validation_id=intent.validation_id,
                    seed=intent.seed,
                ),
                intent.admission_id,
            )
            trial = TrialData.model_validate(trial_row["data"])
            jobs = {
                JobData.model_validate(transaction.get("job", job_id)["data"]).stage.id: job_id
                for job_id in trial.job_ids
            }
            data = PolicyTrainingAdmission(
                request=intent,
                profile_digest=profile.digest,
                inputs_digest=record_digest(
                    "research.prepared-policy-inputs.v1", inputs.model_dump(mode="json")
                ),
                dataset_digest=inputs.dataset_digest,
                behavior_policy_digest=inputs.behavior_policy_digest,
                trial_id=trial_row["id"],
                trainer_job_id=jobs[profile.request.trainer_stage_id],
                checker_job_id=jobs[profile.request.checker_stage_id],
                actor_id=context.principal.id,
                recorded_at=utc_now(),
            )
            row = transaction.insert(
                "learning_admission",
                intent.admission_id,
                data.model_dump(mode="json"),
                state="immutable",
                campaign_id=intent.campaign_id,
                parent_id=trial_row["id"],
            )
            transaction.event("learning.training_admitted", row, {"trial_id": trial_row["id"]})
            return row

        return self.database.execute(
            context, "learning.admit_training", payload, "learning.manage", commit
        )
