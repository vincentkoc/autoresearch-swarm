"""Capture evidence once and consume it only after an independently authorized decision."""

from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.models import CampaignData, JobData, TrialData
from research_swarm.control.observation_models import (
    ObservationConsumption,
    ObserveTrial,
    ResolveTrialObservation,
    TrialFacts,
    TrialObservation,
    TrialResolution,
)
from research_swarm.control.promotions import PromotionAssessment
from research_swarm.control.proposals import search_state, write_search_state
from research_swarm.control.search_models import SearchState
from research_swarm.control.validity import EvidenceReference, invalidated, require_current
from research_swarm.control.workers import authority
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.database import Database, JsonObject, Transaction


def trial_facts(transaction: Transaction, row: JsonObject, campaign: CampaignData) -> TrialFacts:
    trial = TrialData.model_validate(row["data"])
    if row["campaign_id"] is None or trial.spec.campaign_id != row["campaign_id"]:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Trial campaign identity differs")
    trial.spec.verify()
    evidence: list[EvidenceReference] = []
    receipts = []
    attempt_outcomes = {}
    for identifier in trial.job_ids:
        job = JobData.model_validate(transaction.get("job", identifier)["data"])
        if job.current_attempt_id:
            attempt_outcomes[job.current_attempt_id] = transaction.get(
                "attempt", job.current_attempt_id
            )["state"]
            try:
                transaction.get("run_result", job.current_attempt_id)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
            else:
                evidence.append(EvidenceReference(kind="run_result", id=job.current_attempt_id))
        if job.evaluation_request_id:
            request = transaction.get("evaluation_request", job.evaluation_request_id)["data"]
            if request["receipt_id"]:
                receipts.append(request["receipt_id"])
                evidence.append(
                    EvidenceReference(kind="evaluation_receipt", id=request["receipt_id"])
                )
    if len(evidence) > 1024 or len(attempt_outcomes) > 1024:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Trial observation exceeds its evidence bound"
        )
    values = {
        "campaign_id": row["campaign_id"],
        "trial_id": row["id"],
        "trial_revision": row["revision"],
        "candidate_id": trial.candidate_id,
        "spec_digest": trial.spec.spec_digest,
        "trial_state": row["state"],
        "execution_outcome": trial.execution_outcome,
        "evaluation_status": trial.evaluation_status,
        "resource_status": trial.resource_status,
        "evidence": tuple(evidence),
        "attempt_outcomes": attempt_outcomes,
        "evaluation_id": receipts[0] if len(receipts) == 1 else None,
    }
    if trial.execution_outcome == "unknown" or trial.resource_status == "unknown":
        return TrialFacts(
            **values, outcome="execution_unknown", reason_code=ErrorCode.EXECUTION_UNKNOWN
        )
    if trial.resource_status == "active" or trial.execution_outcome == "pending":
        raise ResearchError(ErrorCode.INVALID_TRANSITION, "Trial does not yet have an observation")
    if trial.execution_outcome == "failed":
        actual = set(attempt_outcomes.values())
        outcome = (
            "execution_failed"
            if "failed" in actual
            else "cancelled"
            if "cancelled" in actual
            else "not_started"
            if "not_started" in actual
            else "execution_failed"
        )
        return TrialFacts(**values, outcome=outcome)
    if trial.execution_outcome in {"cancelled", "closed_unaccepted"}:
        return TrialFacts(**values, outcome=trial.execution_outcome)
    if any(invalidated(transaction, item) for item in evidence):
        return TrialFacts(
            **values, outcome="evidence_invalidated", reason_code=ErrorCode.EVIDENCE_INVALIDATED
        )
    if trial.evaluation_status in {"invalid", "incomplete"}:
        return TrialFacts(**values, outcome="evaluation_" + trial.evaluation_status)
    if trial.evaluation_status == "pending":
        raise ResearchError(
            ErrorCode.EVALUATION_INCOMPLETE, "Trial lacks completed evaluation authority"
        )
    if len(receipts) != 1:
        return TrialFacts(
            **values, outcome="unrankable", reason_code=ErrorCode.CAPABILITY_UNAVAILABLE
        )
    try:
        evaluation = eligible_evaluation(transaction, receipts[0], row["campaign_id"], campaign)
    except ResearchError as error:
        if error.code not in {
            ErrorCode.COMPARISON_INELIGIBLE,
            ErrorCode.EVALUATION_INCOMPLETE,
            ErrorCode.ARTIFACT_UNVERIFIED,
            ErrorCode.EVIDENCE_INVALIDATED,
            ErrorCode.CAPABILITY_UNAVAILABLE,
            ErrorCode.NOT_FOUND,
        }:
            raise
        return TrialFacts(**values, outcome="unrankable", reason_code=error.code)
    return TrialFacts(
        **values,
        outcome="measurement",
        metric=evaluation.metric,
        comparison_digest=evaluation.comparison_digest,
    )


class ObservationControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def observe(self, context: CommandContext, intent: ObserveTrial) -> JsonObject:
        def record(transaction: Transaction) -> JsonObject:
            campaign = CampaignData.model_validate(
                transaction.get("campaign", intent.campaign_id)["data"]
            )
            row = transaction.get("trial", intent.trial_id)
            if row["campaign_id"] != intent.campaign_id:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Trial belongs to another campaign"
                )
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Trial evidence revision changed")
            facts = trial_facts(transaction, row, campaign)
            digest = record_digest("research.trial-facts", facts.model_dump(mode="json"))
            identity = record_digest(
                "research.trial-observation",
                {
                    "workspace_id": transaction.workspace_id,
                    "facts_digest": digest,
                    "incumbent_revision": campaign.incumbent_revision,
                    "comparator_id": campaign.incumbent_candidate_id,
                    "comparator_evaluation_id": campaign.incumbent_evaluation_id,
                },
            )
            identifier = str(uuid5(NAMESPACE_URL, "research-swarm:trial-observation:" + identity))
            try:
                return transaction.get("search_observation", identifier)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
            sequence = transaction.connection.execute(
                "SELECT next_sequence-1 FROM workspaces WHERE id=?", (transaction.workspace_id,)
            ).fetchone()[0]
            observation = TrialObservation(
                facts=facts,
                facts_digest=digest,
                incumbent_revision=campaign.incumbent_revision,
                comparator_id=campaign.incumbent_candidate_id,
                comparator_evaluation_id=campaign.incumbent_evaluation_id,
                observed_by=context.principal.id,
                observed_at=utc_now(),
                watermark=sequence,
            )
            recorded = transaction.insert(
                "search_observation",
                identifier,
                observation.model_dump(mode="json"),
                state="immutable",
                campaign_id=intent.campaign_id,
                parent_id=intent.trial_id,
            )
            transaction.event(
                "search.trial_observed",
                recorded,
                {"outcome": facts.outcome, "trial_revision": facts.trial_revision},
            )
            return recorded

        return self.database.execute(
            context,
            "search.observe_trial",
            intent.model_dump(mode="json"),
            "search.observe",
            record,
        )

    def resolve(self, context: CommandContext, intent: ResolveTrialObservation) -> JsonObject:
        def consume(transaction: Transaction) -> JsonObject:
            if authority(transaction) != (
                intent.expected_controller_id,
                intent.expected_control_epoch,
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Search resolution authority changed")
            campaign = CampaignData.model_validate(
                transaction.get("campaign", intent.campaign_id)["data"]
            )
            previous = search_state(transaction, intent.campaign_id, campaign)
            if context.expected_revision != previous["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Search state revision changed")
            state = SearchState.model_validate(previous["data"])
            observed = transaction.get("search_observation", intent.observation_id)
            observation = TrialObservation.model_validate(observed["data"])
            facts = observation.facts
            if (
                facts.campaign_id != intent.campaign_id
                or facts.trial_id not in state.active_trial_ids
            ):
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Observation does not own an active policy trial"
                )
            current = trial_facts(transaction, transaction.get("trial", facts.trial_id), campaign)
            if (
                record_digest("research.trial-facts", current.model_dump(mode="json"))
                != observation.facts_digest
            ):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Trial evidence changed after observation"
                )
            if facts.outcome == "execution_unknown" or facts.resource_status not in {
                "closed",
                "unallocated",
            }:
                raise ResearchError(
                    ErrorCode.EXECUTION_UNKNOWN, "Unresolved execution retains its policy slot"
                )
            outcome = "non_scientific"
            if facts.outcome == "measurement":
                outcome = self._selection(transaction, campaign, observation, intent.decision_id)
            elif intent.decision_id is not None:
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE,
                    "Non-scientific outcomes cannot carry a selection decision",
                )
            updated = write_search_state(
                transaction,
                previous,
                state.model_copy(
                    update={
                        "active_trial_ids": tuple(
                            item for item in state.active_trial_ids if item != facts.trial_id
                        ),
                        "generation": state.generation + 1,
                    }
                ),
            )
            transition_id = new_id()
            resolution = TrialResolution(
                campaign_id=intent.campaign_id,
                trial_id=facts.trial_id,
                ordered_observation_ids=(intent.observation_id,),
                decision_id=intent.decision_id,
                outcome=outcome,
                before_revision=previous["revision"],
                after_revision=updated["revision"],
                generation=state.generation + 1,
                committed_by=context.principal.id,
                committed_at=utc_now(),
            )
            transition = transaction.insert(
                "policy_transition",
                transition_id,
                resolution.model_dump(mode="json"),
                state="immutable",
                campaign_id=intent.campaign_id,
                parent_id=intent.observation_id,
            )
            consumed = transaction.insert(
                "observation_consumption",
                intent.observation_id,
                ObservationConsumption(
                    observation_id=intent.observation_id,
                    transition_id=transition_id,
                    policy_revision=updated["revision"],
                    ordinal=0,
                ).model_dump(mode="json"),
                state="immutable",
                campaign_id=intent.campaign_id,
                parent_id=transition_id,
            )
            transaction.event(
                "search.observation_consumed",
                transition,
                {"outcome": outcome, "generation": state.generation + 1},
            )
            return {"transition": transition, "consumption": consumed, "search": updated}

        return self.database.execute(
            context,
            "search.resolve_trial",
            intent.model_dump(mode="json"),
            "search.manage",
            consume,
        )

    @staticmethod
    def _selection(
        transaction: Transaction,
        campaign: CampaignData,
        observation: TrialObservation,
        decision_id: str | None,
    ) -> str:
        if decision_id is None:
            raise ResearchError(
                ErrorCode.EVALUATION_INCOMPLETE,
                "Measurement requires an independently authorized selection",
            )
        row = transaction.get("promotion", decision_id)
        assessment = PromotionAssessment.model_validate(row["data"])
        decision = assessment.decision
        facts = observation.facts
        if (
            decision.campaign_id != facts.campaign_id
            or decision.candidate_id != facts.candidate_id
            or decision.comparator_id != observation.comparator_id
            or decision.expected_incumbent_revision != observation.incumbent_revision
            or assessment.comparison_digest != facts.comparison_digest
            or assessment.replaces_suspension_id is not None
        ):
            raise ResearchError(
                ErrorCode.COMPARISON_INELIGIBLE, "Selection differs from observation context"
            )
        if campaign.contract.promotion.mode == "paired_confirmation":
            from research_swarm.control.confirmation_assessment import collect_panel
            from research_swarm.control.confirmation_models import ConfirmationData

            if assessment.confirmation_plan_id is None or assessment.gate is None:
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE,
                    "Paired campaign requires a sealed panel assessment",
                )
            planned = transaction.get("confirmation_plan", assessment.confirmation_plan_id)
            data = ConfirmationData.model_validate(planned["data"])
            plan = data.plan
            plan.verify()
            if (
                planned["state"] != "assessed"
                or data.decision_id != decision_id
                or plan.campaign_contract_digest != campaign.contract_digest
                or plan.campaign_id != facts.campaign_id
                or plan.candidate_id != facts.candidate_id
                or plan.comparator_id != observation.comparator_id
                or plan.expected_incumbent_revision != observation.incumbent_revision
                or decision.policy_version != plan.policy.version
                or assessment.gate.outcome != decision.outcome
                or (decision.outcome == "promote" and assessment.evidence_level != "confirmed")
            ):
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE, "Selection differs from the sealed panel"
                )
            panel = collect_panel(transaction, plan, campaign)
            if tuple(item.evidence.receipt.id for item in panel) != decision.evaluation_ids:
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE, "Selection omits planned panel evidence"
                )
        elif (
            assessment.evidence_level != "observed"
            or assessment.candidate_evaluation_id != facts.evaluation_id
            or assessment.comparator_evaluation_id != observation.comparator_evaluation_id
        ):
            raise ResearchError(
                ErrorCode.COMPARISON_INELIGIBLE, "Observed selection uses another evaluation"
            )
        require_current(transaction, assessment.dependencies)
        for identifier in decision.evaluation_ids:
            eligible = eligible_evaluation(transaction, identifier, facts.campaign_id, campaign)
            if eligible.comparison_digest != facts.comparison_digest:
                raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Selection evidence changed")
        if decision.outcome == "promote" and row["state"] == "committed":
            return "promoted"
        if decision.outcome == "reject" and row["state"] == "assessed":
            return "rejected"
        if decision.outcome == "inconclusive" and row["state"] == "assessed":
            return "inconclusive"
        raise ResearchError(
            ErrorCode.INVALID_TRANSITION, "Selection is not resolved or its evidence is affected"
        )
