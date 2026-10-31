"""Revisioned archive membership is separate from incumbent selection and budgets."""

from research_swarm.control.candidates import retain_source
from research_swarm.control.comparisons import EligibleEvaluation, eligible_evaluation
from research_swarm.control.models import CampaignData, CandidateData, CandidateValidation
from research_swarm.control.population_models import (
    PopulationChange,
    PopulationMember,
    RetainPopulationMember,
    RetirePopulationMember,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Database, JsonObject, Transaction

MAX_ACTIVE_MEMBERS = 64


def member_evidence(
    transaction: Transaction, request: RetainPopulationMember, campaign: CampaignData
) -> tuple[CandidateData, EligibleEvaluation]:
    candidate = CandidateData.model_validate(
        transaction.get("candidate", request.candidate_id)["data"]
    )
    validation_row = transaction.get("candidate_validation", request.validation_id)
    validation = CandidateValidation.model_validate(validation_row["data"])
    if (
        candidate.campaign_id != request.campaign_id
        or candidate.project_id != campaign.contract.project_id
        or validation_row["state"] != "valid"
        or validation.candidate_id != request.candidate_id
        or validation.source_tree_digest != candidate.source.tree_digest
        or validation.campaign_contract_digest != campaign.contract_digest
        or not validation.valid
    ):
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Archive candidate lacks matching mutation proof"
        )
    ArtifactCatalog.available(transaction, candidate.source_artifact)
    evaluation = eligible_evaluation(
        transaction, request.evaluation_id, request.campaign_id, campaign
    )
    if evaluation.trial.candidate_id != request.candidate_id:
        raise ResearchError(
            ErrorCode.COMPARISON_INELIGIBLE, "Archive evaluation belongs to another candidate"
        )
    return candidate, evaluation


def writable_campaign(transaction: Transaction, campaign_id: str) -> CampaignData:
    row = transaction.get("campaign", campaign_id)
    campaign = CampaignData.model_validate(row["data"])
    if campaign.contract_digest is None or row["state"] not in {
        "ready",
        "running",
        "paused",
        "awaiting_input",
    }:
        raise ResearchError(ErrorCode.INVALID_TRANSITION, "Campaign archive is closed")
    return campaign


def record_change(transaction: Transaction, row: JsonObject, before: int, reason: str) -> None:
    member = PopulationMember.model_validate(row["data"])
    change = PopulationChange(
        member_id=row["id"],
        before_revision=before,
        after_revision=row["revision"],
        state=row["state"],
        member=member,
        actor_id=transaction.context.principal.id,
        reason=reason,
        recorded_at=utc_now(),
    )
    history = transaction.insert(
        "population_change",
        new_id(),
        change.model_dump(mode="json"),
        state="immutable",
        campaign_id=row["campaign_id"],
        parent_id=row["id"],
    )
    transaction.event("population.changed", history, {"member_id": row["id"]})


class PopulationControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def retain(self, context: CommandContext, request: RetainPopulationMember) -> JsonObject:
        context.principal.require("read")

        def commit(transaction: Transaction) -> JsonObject:
            campaign = writable_campaign(transaction, request.campaign_id)
            try:
                previous = transaction.get("population_member", request.candidate_id)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
                previous = None
            revision: int = previous["revision"] if previous else 0
            if context.expected_revision != revision:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Archive membership changed")
            if previous is not None and previous["state"] != "retired":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Candidate is already retained")
            count = transaction.connection.execute(
                "SELECT COUNT(*) FROM records WHERE workspace_id=? AND campaign_id=? "
                "AND kind='population_member' AND state='active'",
                (transaction.workspace_id, request.campaign_id),
            ).one()[0]
            if count >= MAX_ACTIVE_MEMBERS:
                raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Campaign archive is full")
            candidate, evaluation = member_evidence(transaction, request, campaign)
            assert campaign.contract_digest is not None
            member = PopulationMember(
                request=request,
                source_artifact=candidate.source_artifact,
                source_tree_digest=candidate.source.tree_digest,
                campaign_contract_digest=campaign.contract_digest,
                comparison_digest=evaluation.comparison_digest,
                actor_id=context.principal.id,
                recorded_at=utc_now(),
            )
            if previous is None:
                row = transaction.insert(
                    "population_member",
                    request.candidate_id,
                    member.model_dump(mode="json"),
                    state="active",
                    campaign_id=request.campaign_id,
                    parent_id=request.candidate_id,
                )
            else:
                row = transaction.update(
                    "population_member",
                    request.candidate_id,
                    expected_revision=revision,
                    state="active",
                    data=member.model_dump(mode="json"),
                )
            retain_source(transaction, member.source_artifact, "population_member", row["id"])
            record_change(transaction, row, revision, request.reason)
            return row

        return self.database.execute(
            context, "population.retain", request.model_dump(mode="json"), "search.manage", commit
        )

    def retire(self, context: CommandContext, request: RetirePopulationMember) -> JsonObject:
        context.principal.require("read")

        def commit(transaction: Transaction) -> JsonObject:
            previous = transaction.get("population_member", request.member_id)
            writable_campaign(transaction, previous["campaign_id"])
            if context.expected_revision != previous["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Archive membership changed")
            if previous["state"] != "active":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Candidate is already retired")
            row = transaction.update(
                "population_member",
                request.member_id,
                expected_revision=previous["revision"],
                state="retired",
                data=previous["data"],
            )
            record_change(transaction, row, previous["revision"], request.reason)
            return row

        return self.database.execute(
            context, "population.retire", request.model_dump(mode="json"), "search.manage", commit
        )
