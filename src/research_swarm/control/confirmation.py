"""Create a whole confirmation panel before any of its trials can execute."""

from collections.abc import Callable
from decimal import Decimal
from typing import Literal

from pydantic import Field

from research_swarm.control.commands import SubmitTrial
from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.confirmation_models import (
    ConfirmationData,
    ConfirmationPlan,
    ExposureData,
    PlanBinding,
)
from research_swarm.control.models import CampaignData, JobData, TrialData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, Digest, Name, Revision, VersionedRecord, new_id
from research_swarm.evaluation.statistics import FixedPairedPolicy
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.ledger import BudgetLedger


class CreateConfirmationPlan(VersionedRecord):
    campaign_id: ID
    candidate_id: ID
    validation_id: ID
    expected_incumbent_revision: Revision
    evaluator_id: ID
    execution_fingerprints: dict[Name, Digest] = Field(min_length=1, max_length=256)


class ConfirmationControl:
    def __init__(
        self,
        database: Database,
        submit_trial: Callable[
            [Transaction, SubmitTrial, str | None, Literal["candidate", "comparator"] | None],
            JsonObject,
        ],
    ) -> None:
        self.database, self.submit_trial = database, submit_trial

    def create(self, context: CommandContext, intent: CreateConfirmationPlan) -> JsonObject:
        def create(transaction: Transaction) -> JsonObject:
            row = transaction.get("campaign", intent.campaign_id)
            campaign = CampaignData.model_validate(row["data"])
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Campaign revision changed")
            if campaign.incumbent_revision != intent.expected_incumbent_revision:
                raise ResearchError(ErrorCode.INCUMBENT_REVISION_CONFLICT, "Incumbent changed")
            promotion = campaign.contract.promotion
            if promotion.mode != "paired_confirmation" or promotion.version != "paired-sign-v1":
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Campaign lacks a paired sign policy"
                )
            if (
                campaign.incumbent_candidate_id is None
                or campaign.incumbent_evaluation_id is None
                or campaign.incumbent_eligibility != "eligible"
                or campaign.incumbent_candidate_id == intent.candidate_id
            ):
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE, "Plan requires a distinct eligible incumbent"
                )
            previous = transaction.connection.execute(
                "SELECT 1 FROM records WHERE workspace_id=? "
                "AND kind='confirmation_plan' AND campaign_id=?",
                (transaction.workspace_id, row["id"]),
            ).fetchone()
            if previous:
                raise ResearchError(
                    ErrorCode.EXPOSURE_LIMIT_REACHED,
                    "This policy permits one confirmation panel per campaign",
                )
            incumbent = eligible_evaluation(
                transaction, campaign.incumbent_evaluation_id, row["id"], campaign
            )
            policy = FixedPairedPolicy(
                seeds=promotion.confirmation_seeds,
                minimum_effect=promotion.minimum_effect,
                confidence=promotion.confidence,
            )
            if set(intent.execution_fingerprints) != {
                stage.id for stage in incumbent.trial.spec.stages
            }:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Plan must bind every stage profile"
                )
            if sum(stage.kind == "evaluate" for stage in incumbent.trial.spec.stages) != 1:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Plan requires one scoring stage"
                )
            # Protect against campaigns created by an older implementation that exposed seeds.
            prior_trials = transaction.connection.execute(
                "SELECT payload FROM records WHERE workspace_id=? "
                "AND kind='trial' AND campaign_id=?",
                (transaction.workspace_id, row["id"]),
            ).fetchall()
            if any(
                TrialData.model_validate_json(item["payload"]).spec.seed in policy.seeds
                for item in prior_trials
            ):
                raise ResearchError(
                    ErrorCode.EXPOSURE_LIMIT_REACHED, "Confirmation seeds were already used"
                )
            plan_id = new_id()
            bindings = []
            sides: tuple[tuple[Literal["candidate", "comparator"], str, str], ...] = (
                ("candidate", intent.candidate_id, intent.validation_id),
                ("comparator", campaign.incumbent_candidate_id, incumbent.trial.validation_id),
            )
            for seed in policy.seeds:
                for side, candidate_id, validation_id in sides:
                    submitted = self.submit_trial(
                        transaction,
                        SubmitTrial(
                            campaign_id=row["id"],
                            candidate_id=candidate_id,
                            validation_id=validation_id,
                            seed=seed,
                        ),
                        plan_id,
                        side,
                    )
                    trial = TrialData.model_validate(submitted["data"])
                    jobs = [transaction.get("job", identifier) for identifier in trial.job_ids]
                    scoring = next(
                        item for item in jobs if item["data"]["stage"]["kind"] == "evaluate"
                    )
                    exposure_id = new_id()
                    reservation = BudgetLedger.reserve(
                        transaction, row["id"], exposure_id, {"evaluation_exposures": Decimal(1)}
                    )
                    exposure = transaction.insert(
                        "evaluation_exposure",
                        exposure_id,
                        ExposureData(
                            plan_id=plan_id, job_id=scoring["id"], reservation_id=reservation.id
                        ).model_dump(mode="json"),
                        state="reserved",
                        campaign_id=row["id"],
                        parent_id=plan_id,
                    )
                    transaction.update(
                        "job",
                        scoring["id"],
                        expected_revision=scoring["revision"],
                        state=scoring["state"],
                        data=JobData.model_validate(scoring["data"])
                        .model_copy(update={"evaluation_exposure_id": exposure_id})
                        .model_dump(mode="json"),
                    )
                    transaction.event(
                        "evaluation.exposure_reserved", exposure, {"plan_id": plan_id}
                    )
                    assert trial.spec.spec_digest is not None
                    bindings.append(
                        PlanBinding(
                            trial_id=submitted["id"],
                            spec_digest=trial.spec.spec_digest,
                            candidate_id=candidate_id,
                            source_tree_digest=trial.spec.source.tree_digest,
                            seed=seed,
                            side=side,
                            scoring_job_id=scoring["id"],
                            exposure_id=exposure_id,
                        )
                    )
            assert campaign.contract_digest is not None
            plan = ConfirmationPlan(
                id=plan_id,
                campaign_id=row["id"],
                campaign_contract_digest=campaign.contract_digest,
                candidate_id=intent.candidate_id,
                comparator_id=campaign.incumbent_candidate_id,
                expected_incumbent_revision=intent.expected_incumbent_revision,
                evaluator_id=intent.evaluator_id,
                policy=policy,
                execution_fingerprints=intent.execution_fingerprints,
                bindings=tuple(bindings),
            ).sealed()
            record = transaction.insert(
                "confirmation_plan",
                plan_id,
                ConfirmationData(plan=plan).model_dump(mode="json"),
                state="sealed",
                campaign_id=row["id"],
                parent_id=intent.candidate_id,
            )
            transaction.event("confirmation.planned", record, {"plan_digest": plan.plan_digest})
            return record

        return self.database.execute(
            context,
            "confirmation.create",
            intent.model_dump(mode="json"),
            "confirmation.manage",
            create,
        )
