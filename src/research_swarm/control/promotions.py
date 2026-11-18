"""Persist assessment before atomic incumbent selection and evidence correction."""

from __future__ import annotations

from fractions import Fraction
from typing import Literal

from pydantic import Field

from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.models import CampaignData, TrialData
from research_swarm.control.validity import EvidenceReference, require_current
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import PromotionDecision
from research_swarm.domain.experiments import Direction
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.lifecycle import TERMINAL_CAMPAIGNS
from research_swarm.domain.types import ID, Digest, Revision, VersionedRecord, new_id
from research_swarm.evaluation.statistics import GateResult, Rational
from research_swarm.state.database import Database, JsonObject, Transaction


class AssessPromotion(VersionedRecord):
    campaign_id: ID
    candidate_evaluation_id: ID
    expected_incumbent_revision: Revision


class CommitPromotion(VersionedRecord):
    decision_id: ID


class InvalidateEvidence(VersionedRecord):
    target: EvidenceReference
    reason: str = Field(min_length=1, max_length=4096)


class PromotionAssessment(VersionedRecord):
    decision: PromotionDecision
    candidate_evaluation_id: ID
    comparator_evaluation_id: ID | None
    comparison_digest: Digest
    dependencies: tuple[EvidenceReference, ...]
    evidence_level: Literal["observed", "confirmed"] = "observed"
    exact_improvement: Rational | None = None
    replaces_suspension_id: ID | None = None
    confirmation_plan_id: ID | None = None
    gate: GateResult | None = None


class PromotionControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def assess(self, context: CommandContext, intent: AssessPromotion) -> JsonObject:
        return self._assess(context, intent, reinitialize=False)

    def reinitialize(self, context: CommandContext, intent: AssessPromotion) -> JsonObject:
        return self._assess(context, intent, reinitialize=True)

    def _assess(
        self, context: CommandContext, intent: AssessPromotion, *, reinitialize: bool
    ) -> JsonObject:
        def assess(transaction: Transaction) -> JsonObject:
            row = transaction.get("campaign", intent.campaign_id)
            campaign = CampaignData.model_validate(row["data"])
            if campaign.incumbent_revision != intent.expected_incumbent_revision:
                raise ResearchError(ErrorCode.INCUMBENT_REVISION_CONFLICT, "Incumbent changed")
            if reinitialize and (
                campaign.incumbent_eligibility != "suspended"
                or campaign.incumbent_suspension_id is None
            ):
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Incumbent is not suspended")
            if not reinitialize and campaign.incumbent_eligibility == "suspended":
                raise ResearchError(
                    ErrorCode.EVIDENCE_INVALIDATED, "Incumbent evidence is suspended"
                )
            candidate = eligible_evaluation(
                transaction, intent.candidate_evaluation_id, row["id"], campaign
            )
            comparator_id = campaign.incumbent_candidate_id
            comparator_evaluation_id = None if reinitialize else campaign.incumbent_evaluation_id
            dependencies = candidate.dependencies
            improvement = None
            decision_improvement = None
            outcome: Literal["promote", "reject"] = "promote"
            reason = "baseline_initialized"
            if reinitialize:
                positions = transaction.connection.execute(
                    "SELECT kind,rowid AS position FROM records WHERE workspace_id=? "
                    "AND ((kind='trial' AND id=?) OR (kind='evidence_correction' AND id=?))",
                    (
                        transaction.workspace_id,
                        candidate.trial.spec.trial_id,
                        campaign.incumbent_suspension_id,
                    ),
                ).fetchall()
                order = {item["kind"]: item["position"] for item in positions}
                if order["trial"] <= order["evidence_correction"]:
                    raise ResearchError(
                        ErrorCode.COMPARISON_INELIGIBLE,
                        "Replacement baseline requires a trial submitted after correction",
                    )
                reason = "baseline_reinitialized"
            if candidate.authorization.request.exposure_class != "exploratory":
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE,
                    "Observed assessment requires exploratory evidence",
                )
            if comparator_id is not None and not reinitialize:
                if campaign.contract.promotion.mode != "observed":
                    raise ResearchError(
                        ErrorCode.COMPARISON_INELIGIBLE, "Campaign requires paired confirmation"
                    )
                if candidate.trial.candidate_id == comparator_id:
                    raise ResearchError(
                        ErrorCode.COMPARISON_INELIGIBLE, "Candidate is already incumbent"
                    )
                if comparator_evaluation_id is None:
                    raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Incumbent lacks evidence")
                comparator = eligible_evaluation(
                    transaction, comparator_evaluation_id, row["id"], campaign
                )
                if candidate.comparison_digest != comparator.comparison_digest:
                    raise ResearchError(
                        ErrorCode.COMPARISON_INELIGIBLE, "Comparison contracts differ"
                    )
                improvement = (
                    Fraction(str(comparator.metric.value)) - Fraction(str(candidate.metric.value))
                ) * (1 if candidate.metric.direction == Direction.MINIMIZE else -1)
                try:
                    decision_improvement = float(improvement)
                except OverflowError:
                    decision_improvement = None
                outcome = (
                    "promote"
                    if improvement > Fraction(str(campaign.contract.promotion.minimum_effect))
                    else "reject"
                )
                reason = (
                    "observed_improvement" if outcome == "promote" else "observed_effect_not_met"
                )
                dependencies += comparator.dependencies
            decision = PromotionDecision(
                campaign_id=row["id"],
                candidate_id=candidate.trial.candidate_id,
                comparator_id=comparator_id,
                expected_incumbent_revision=intent.expected_incumbent_revision,
                evaluation_ids=(intent.candidate_evaluation_id,)
                + (
                    (comparator_evaluation_id,)
                    if comparator_evaluation_id and not reinitialize
                    else ()
                ),
                comparison_class=campaign.contract.comparison_class,
                policy_version=campaign.contract.promotion.version,
                outcome=outcome,
                improvement=decision_improvement,
                reason_codes=(reason,),
                explanation="Observed evidence only. No confirmed improvement is claimed.",
            )
            assessment = PromotionAssessment(
                decision=decision,
                candidate_evaluation_id=intent.candidate_evaluation_id,
                comparator_evaluation_id=comparator_evaluation_id,
                comparison_digest=candidate.comparison_digest,
                dependencies=dependencies,
                exact_improvement=Rational.of(improvement) if improvement is not None else None,
                replaces_suspension_id=campaign.incumbent_suspension_id if reinitialize else None,
            )
            record = transaction.insert(
                "promotion",
                decision.id,
                assessment.model_dump(mode="json"),
                state="assessed",
                campaign_id=row["id"],
                parent_id=decision.candidate_id,
            )
            transaction.event("promotion.assessed", record, {"outcome": outcome})
            return record

        operation = "promotion.reinitialize" if reinitialize else "promotion.assess"
        return self.database.execute(
            context, operation, intent.model_dump(mode="json"), operation, assess
        )

    def commit(self, context: CommandContext, intent: CommitPromotion) -> JsonObject:
        def commit(transaction: Transaction) -> JsonObject:
            row = transaction.get("promotion", intent.decision_id)
            assessment = PromotionAssessment.model_validate(row["data"])
            decision = assessment.decision
            require_current(transaction, assessment.dependencies)
            if row["state"] == "committed":
                return row
            if row["state"] != "assessed" or decision.outcome != "promote":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Decision cannot promote")
            campaign_row = transaction.get("campaign", decision.campaign_id)
            campaign = CampaignData.model_validate(campaign_row["data"])
            if campaign_row["state"] in TERMINAL_CAMPAIGNS:
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Completed selection is frozen")
            if (
                campaign.incumbent_revision != decision.expected_incumbent_revision
                or campaign.incumbent_candidate_id != decision.comparator_id
            ):
                raise ResearchError(
                    ErrorCode.INCUMBENT_REVISION_CONFLICT, "Incumbent changed after assessment"
                )
            if assessment.replaces_suspension_id is not None:
                if (
                    campaign.incumbent_eligibility != "suspended"
                    or campaign.incumbent_suspension_id != assessment.replaces_suspension_id
                ):
                    raise ResearchError(ErrorCode.INCUMBENT_REVISION_CONFLICT, "Suspension changed")
            elif campaign.incumbent_eligibility == "suspended":
                raise ResearchError(
                    ErrorCode.EVIDENCE_INVALIDATED, "Incumbent evidence is suspended"
                )
            if assessment.confirmation_plan_id is not None:
                from research_swarm.control.confirmation_assessment import verify_planned_assessment

                verify_planned_assessment(transaction, assessment)
            for identifier in decision.evaluation_ids:
                eligible = eligible_evaluation(
                    transaction, identifier, campaign_row["id"], campaign
                )
                if eligible.comparison_digest != assessment.comparison_digest:
                    raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Comparison changed")
            updated = transaction.update(
                "campaign",
                campaign_row["id"],
                expected_revision=campaign_row["revision"],
                state=campaign_row["state"],
                data=campaign.model_copy(
                    update={
                        "incumbent_candidate_id": decision.candidate_id,
                        "incumbent_revision": campaign.incumbent_revision + 1,
                        "incumbent_promotion_id": decision.id,
                        "incumbent_evaluation_id": assessment.candidate_evaluation_id,
                        "incumbent_evidence_level": assessment.evidence_level,
                        "incumbent_eligibility": "eligible",
                        "incumbent_suspension_id": None,
                    }
                ).model_dump(mode="json"),
            )
            record = transaction.update(
                "promotion",
                row["id"],
                expected_revision=row["revision"],
                data=row["data"],
                state="committed",
            )
            transaction.event(
                "promotion.committed",
                record,
                {"incumbent_revision": campaign.incumbent_revision + 1},
            )
            transaction.event("campaign.incumbent_changed", updated, {"promotion_id": row["id"]})
            return record

        return self.database.execute(
            context, "promotion.commit", intent.model_dump(mode="json"), "promotion.commit", commit
        )

    def invalidate(self, context: CommandContext, intent: InvalidateEvidence) -> JsonObject:
        def invalidate(transaction: Transaction) -> JsonObject:
            target = transaction.get(intent.target.kind, intent.target.id)
            correction = transaction.insert(
                "evidence_correction",
                new_id(),
                intent.model_dump(mode="json"),
                state="effective",
                campaign_id=target["campaign_id"],
                parent_id=intent.target.id,
            )
            affected = transaction.connection.execute(
                "SELECT id FROM records WHERE workspace_id=? "
                "AND kind='promotion' AND campaign_id=?",
                (transaction.workspace_id, target["campaign_id"]),
            ).fetchall()
            affected_ids = set()
            for item in affected:
                row = transaction.get("promotion", item["id"])
                assessment = PromotionAssessment.model_validate(row["data"])
                if intent.target in assessment.dependencies:
                    affected_ids.add(row["id"])
                    if row["state"] != "affected":
                        changed = transaction.update(
                            "promotion",
                            row["id"],
                            expected_revision=row["revision"],
                            data=row["data"],
                            state="affected",
                        )
                        transaction.event(
                            "promotion.evidence_affected",
                            changed,
                            {"correction_id": correction["id"]},
                        )
            campaign_row = transaction.get("campaign", target["campaign_id"])
            campaign = CampaignData.model_validate(campaign_row["data"])
            if campaign.incumbent_promotion_id in affected_ids:
                changed = transaction.update(
                    "campaign",
                    campaign_row["id"],
                    expected_revision=campaign_row["revision"],
                    state=campaign_row["state"],
                    data=campaign.model_copy(
                        update={
                            "incumbent_eligibility": "suspended",
                            "incumbent_suspension_id": correction["id"],
                            "incumbent_revision": campaign.incumbent_revision + 1,
                        }
                    ).model_dump(mode="json"),
                )
                transaction.event(
                    "campaign.incumbent_suspended", changed, {"correction_id": correction["id"]}
                )
            trial_id = (
                target["data"]["trial_id"]
                if intent.target.kind == "run_result"
                else transaction.get("evaluation_request", target["data"]["receipt"]["request_id"])[
                    "data"
                ]["request"]["trial_id"]
            )
            trial_row = transaction.get("trial", trial_id)
            trial = TrialData.model_validate(trial_row["data"])
            changed = transaction.update(
                "trial",
                trial_id,
                expected_revision=trial_row["revision"],
                state=trial_row["state"],
                data=trial.model_copy(update={"evaluation_status": "invalid"}).model_dump(
                    mode="json"
                ),
            )
            transaction.event(
                "trial.evidence_invalidated", changed, {"correction_id": correction["id"]}
            )
            transaction.event(
                "evidence.invalidated", correction, {"affected_promotions": sorted(affected_ids)}
            )
            return correction

        return self.database.execute(
            context,
            "evidence.invalidate",
            intent.model_dump(mode="json"),
            "evidence.invalidate",
            invalidate,
        )
