"""Publish one preserved external response as an atomic research-policy transition."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import nullcontext

from research_swarm.control.candidates import propose_candidate, validate_candidate
from research_swarm.control.commands import ProposeCandidate, SubmitTrial
from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.models import CampaignData
from research_swarm.control.preflight import verified_source
from research_swarm.control.proposals import search_state, write_search_state
from research_swarm.control.search_models import (
    AdmitProposalResponse,
    ProposalAdmission,
    ProposalRequest,
    ProposalResponse,
    SearchState,
)
from research_swarm.control.workers import authority
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.projects.source import SourceManifest
from research_swarm.state.artifacts import ArtifactService
from research_swarm.state.database import Database, JsonObject, Transaction

ADMISSION_SCOPES = (
    "search.manage",
    "candidate.propose",
    "candidate.validate",
    "trial.submit",
    "read",
)


class SearchAdmission:
    def __init__(
        self,
        database: Database,
        artifacts: ArtifactService,
        submit_trial: Callable[[Transaction, SubmitTrial, str], JsonObject],
    ) -> None:
        self.database = database
        self.artifacts = artifacts
        self.submit_trial = submit_trial

    @staticmethod
    def _guard(
        transaction: Transaction, intent: AdmitProposalResponse
    ) -> tuple[JsonObject, SearchState, JsonObject, ProposalRequest, ProposalResponse]:
        campaign_row = transaction.get("campaign", intent.campaign_id)
        campaign = CampaignData.model_validate(campaign_row["data"])
        if authority(transaction) != (intent.expected_controller_id, intent.expected_control_epoch):
            raise ResearchError(ErrorCode.STALE_FENCE, "Search controller authority changed")
        previous = search_state(transaction, intent.campaign_id, campaign)
        if transaction.context.expected_revision != previous["revision"]:
            raise ResearchError(ErrorCode.REVISION_CONFLICT, "Search state revision changed")
        state = SearchState.model_validate(previous["data"])
        if campaign_row["state"] != "running" or campaign.incumbent_eligibility != "eligible":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Campaign cannot admit research")
        if intent.expected_incumbent_revision != campaign.incumbent_revision:
            raise ResearchError(ErrorCode.INCUMBENT_REVISION_CONFLICT, "Search incumbent changed")
        row = transaction.get("proposal_request", intent.request_id)
        request = ProposalRequest.model_validate(row["data"])
        response = ProposalResponse.model_validate(
            transaction.get("proposal_response", intent.request_id)["data"]
        )
        if (
            request.campaign_id != intent.campaign_id
            or response.campaign_id != intent.campaign_id
            or response.responder_id != request.proposer_id
            or response.request.request_id != intent.request_id
        ):
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Proposal response identity differs"
            )
        if row["state"] != "responded" or intent.request_id not in state.pending_request_ids:
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Proposal response is not pending")
        if (request.controller_id, request.control_epoch) != (
            intent.expected_controller_id,
            intent.expected_control_epoch,
        ):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Proposal belongs to a prior controller epoch"
            )
        if (
            request.incumbent_revision != campaign.incumbent_revision
            or request.parent_candidate_id != campaign.incumbent_candidate_id
            or request.campaign_contract_digest != campaign.contract_digest
            or request.policy_version != state.policy_version
        ):
            raise ResearchError(
                ErrorCode.INCUMBENT_REVISION_CONFLICT, "Proposal context requires explicit rebasing"
            )
        if campaign.incumbent_evaluation_id is None:
            raise ResearchError(
                ErrorCode.COMPARISON_INELIGIBLE, "Incumbent lacks evaluated evidence"
            )
        eligible_evaluation(
            transaction, campaign.incumbent_evaluation_id, intent.campaign_id, campaign
        )
        slots = 1 if state.policy == "greedy" else campaign.contract.search.max_parallel_trials
        if len(state.pending_request_ids) + len(state.active_trial_ids) > slots:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Search exceeds its sealed capacity"
            )
        return previous, state, row, request, response

    def admit(self, context: CommandContext, intent: AdmitProposalResponse) -> JsonObject:
        for scope in ADMISSION_SCOPES:
            context.principal.require(scope)
        operation, payload = "search.admit_response", intent.model_dump(mode="json")

        def preflight() -> JsonObject:
            response = self.database.store.read(
                lambda connection: self._guard(Transaction(connection, context), intent)[4]
            )
            source_context = (
                verified_source(self.artifacts, context, response.request.source_artifact)
                if response.request.source_artifact is not None
                else nullcontext(None)
            )
            with source_context as verified:
                source = verified[1] if verified is not None else None
                return self.database.execute(
                    context,
                    operation,
                    payload,
                    "search.manage",
                    lambda transaction: self._commit(transaction, intent, source),
                )

        return self.database.with_preflight(context, operation, payload, "search.manage", preflight)

    def _commit(
        self, transaction: Transaction, intent: AdmitProposalResponse, source: SourceManifest | None
    ) -> JsonObject:
        previous, state, row, request, response = self._guard(transaction, intent)
        admission = ProposalAdmission(
            campaign_id=intent.campaign_id,
            request_id=intent.request_id,
            response_digest=response.response_digest,
            policy_version=state.policy_version,
            before_revision=previous["revision"],
            after_revision=previous["revision"] + 1,
            outcome="agent_failed",
            finding=response.request.finding,
            committed_by=transaction.context.principal.id,
            committed_at=utc_now(),
        )
        candidate = validation = trial = None
        if response.request.outcome == "candidate":
            assert source is not None and response.request.source_artifact is not None
            existing = transaction.connection.execute(
                "SELECT id FROM records WHERE workspace_id=? AND campaign_id=? "
                "AND kind='candidate' "
                "AND json_extract(payload,'$.source.tree_digest')=? ORDER BY rowid LIMIT 1",
                (transaction.workspace_id, intent.campaign_id, source.tree_digest),
            ).fetchone()
            if existing is not None:
                admission = admission.model_copy(
                    update={"outcome": "duplicate", "candidate_id": existing["id"]}
                )
            else:
                candidate = propose_candidate(
                    transaction,
                    ProposeCandidate(
                        campaign_id=intent.campaign_id,
                        source_artifact=response.request.source_artifact,
                        parent_ids=(request.parent_candidate_id,),
                        hypothesis=response.request.hypothesis,
                    ),
                    source,
                    proposer_id=response.responder_id,
                )
                validation = validate_candidate(transaction, candidate["id"])
                if validation["data"]["valid"]:
                    trial = self.submit_trial(
                        transaction,
                        SubmitTrial(
                            campaign_id=intent.campaign_id,
                            candidate_id=candidate["id"],
                            validation_id=validation["id"],
                            seed=request.seed,
                        ),
                        intent.request_id,
                    )
                admission = admission.model_copy(
                    update={
                        "outcome": "admitted" if trial else "invalid",
                        "candidate_id": candidate["id"],
                        "validation_id": validation["id"],
                        "trial_id": trial["id"] if trial else None,
                    }
                )
        updated = write_search_state(
            transaction,
            previous,
            state.model_copy(
                update={
                    "pending_request_ids": tuple(
                        item for item in state.pending_request_ids if item != intent.request_id
                    ),
                    "active_trial_ids": (*state.active_trial_ids, trial["id"])
                    if trial
                    else state.active_trial_ids,
                }
            ),
        )
        transaction.update(
            "proposal_request",
            row["id"],
            expected_revision=row["revision"],
            data=row["data"],
            state="consumed",
        )
        transition = transaction.insert(
            "policy_transition",
            intent.request_id,
            admission.model_dump(mode="json"),
            state="immutable",
            campaign_id=intent.campaign_id,
            parent_id=intent.request_id,
        )
        transaction.event(
            "search.proposal_consumed",
            transition,
            {"outcome": admission.outcome, "policy_revision": updated["revision"]},
        )
        return {
            "transition": transition,
            "search": updated,
            "candidate": candidate,
            "validation": validation,
            "trial": trial,
        }
