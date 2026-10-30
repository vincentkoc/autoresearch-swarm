"""Assign external research work and preserve its output before policy publication."""

from __future__ import annotations

from decimal import Decimal

from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.models import CampaignData, CandidateData
from research_swarm.control.portfolio import portfolio_contract, validate_portfolio_request
from research_swarm.control.queries import summarize
from research_swarm.control.search_models import (
    CancelProposal,
    ProposalCancellation,
    ProposalRead,
    ProposalRequest,
    ProposalResponse,
    RequestProposal,
    RespondToProposal,
    SearchState,
)
from research_swarm.control.trial_allowance import require_funded_trials
from research_swarm.control.workers import authority
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.budgets import BudgetKind
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.ledger import BudgetLedger

MAX_CONTEXT_BYTES = 64 * 1024


def search_state(transaction: Transaction, campaign_id: str, campaign: CampaignData) -> JsonObject:
    try:
        row = transaction.get("search_state", campaign_id)
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
        config = campaign.contract.search
        portfolio_contract(campaign)
        if (config.policy, config.version) not in {
            ("greedy", "greedy-v1"),
            ("external", "external-v1"),
            ("portfolio", "portfolio-v1"),
        }:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Search policy version is not implemented"
            ) from None
        if campaign.contract_digest is None:
            raise ResearchError(
                ErrorCode.INVALID_TRANSITION, "Search requires a sealed campaign"
            ) from None
        state = SearchState(
            campaign_id=campaign_id,
            campaign_contract_digest=campaign.contract_digest,
            policy=config.policy,
            policy_version=config.version,
        )
        return {
            "kind": "search_state",
            "id": campaign_id,
            "revision": 0,
            "state": "ready",
            "campaign_id": campaign_id,
            "parent_id": campaign_id,
            "data": state.model_dump(mode="json"),
        }
    state = SearchState.model_validate(row["data"])
    if state.campaign_contract_digest != campaign.contract_digest or (
        state.policy,
        state.policy_version,
    ) != (campaign.contract.search.policy, campaign.contract.search.version):
        raise ResearchError(ErrorCode.STALE_FENCE, "Search contract changed")
    return row


def write_search_state(
    transaction: Transaction, previous: JsonObject, state: SearchState
) -> JsonObject:
    phase = "waiting" if state.pending_request_ids or state.active_trial_ids else "ready"
    if previous["revision"] == 0:
        row = transaction.insert(
            "search_state",
            state.campaign_id,
            state.model_dump(mode="json"),
            state=phase,
            campaign_id=state.campaign_id,
            parent_id=state.campaign_id,
        )
    else:
        row = transaction.update(
            "search_state",
            state.campaign_id,
            expected_revision=previous["revision"],
            data=state.model_dump(mode="json"),
            state=phase,
        )
    transaction.event(
        "search.state_changed",
        row,
        {
            "pending_count": len(state.pending_request_ids),
            "active_count": len(state.active_trial_ids),
        },
    )
    return row


class ProposalControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def request(self, context: CommandContext, request: RequestProposal) -> JsonObject:
        def create(transaction: Transaction) -> JsonObject:
            campaign_row = transaction.get("campaign", request.campaign_id)
            campaign = CampaignData.model_validate(campaign_row["data"])
            controller_id, epoch = authority(transaction)
            if (request.expected_controller_id, request.expected_control_epoch) != (
                controller_id,
                epoch,
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Proposal controller authority changed")
            if campaign_row["state"] != "running" or campaign.incumbent_eligibility != "eligible":
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Campaign cannot request research"
                )
            if request.expected_incumbent_revision != campaign.incumbent_revision:
                raise ResearchError(
                    ErrorCode.INCUMBENT_REVISION_CONFLICT, "Proposal incumbent changed"
                )
            if campaign.incumbent_candidate_id is None or campaign.incumbent_evaluation_id is None:
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE, "Research requires an evaluated incumbent"
                )
            evaluation = eligible_evaluation(
                transaction, campaign.incumbent_evaluation_id, request.campaign_id, campaign
            )
            previous = search_state(transaction, request.campaign_id, campaign)
            if context.expected_revision != previous["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Search state revision changed")
            state = SearchState.model_validate(previous["data"])
            validate_portfolio_request(transaction, campaign, state, request)
            require_funded_trials(transaction, request.campaign_id, campaign, Decimal(1))
            slots = 1 if state.policy == "greedy" else campaign.contract.search.max_parallel_trials
            if len(state.pending_request_ids) + len(state.active_trial_ids) >= slots:
                raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Search slots are occupied")
            parent = CandidateData.model_validate(
                transaction.get("candidate", campaign.incumbent_candidate_id)["data"]
            )
            identifier = new_id()
            account = BudgetLedger.account(transaction, request.campaign_id, "proposal_requests")
            if account.unit != "count" or account.kind != BudgetKind.CUMULATIVE:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "Proposal request budget must count cumulative requests",
                )
            reservation = BudgetLedger.reserve(
                transaction, request.campaign_id, identifier, {"proposal_requests": Decimal(1)}
            )
            # This counts issued research requests, never inferred LLM calls or token billing.
            BudgetLedger.settle(
                transaction, reservation.id, new_id(), {"proposal_requests": Decimal(1)}
            )
            sequence = transaction.connection.execute(
                "SELECT next_sequence-1 FROM workspaces WHERE id=?", (transaction.workspace_id,)
            ).fetchone()[0]
            recent = transaction.connection.execute(
                "SELECT kind,id FROM records WHERE workspace_id=? AND campaign_id=? "
                "AND kind IN ('candidate','trial','evaluation_receipt','promotion') "
                "ORDER BY rowid DESC LIMIT 8",
                (transaction.workspace_id, request.campaign_id),
            ).fetchall()
            snapshot = {
                "campaign": summarize(campaign_row),
                "policy_revision": previous["revision"],
                "pending_request_ids": list(state.pending_request_ids[:32]),
                "pending_request_count": len(state.pending_request_ids),
                "active_trial_ids": list(state.active_trial_ids[:32]),
                "active_trial_count": len(state.active_trial_ids),
                "incumbent_evaluation_id": campaign.incumbent_evaluation_id,
                "comparison_digest": evaluation.comparison_digest,
                "primary_metric": evaluation.metric.model_dump(mode="json"),
                "recent_evidence": [
                    summarize(transaction.get(item["kind"], item["id"])) for item in recent
                ],
                "proposal_requests_available": str(
                    BudgetLedger.account(
                        transaction, request.campaign_id, "proposal_requests"
                    ).available
                ),
                "usage_authority": "External model usage is agent-reported, not verified billing.",
            }
            if len(canonical_bytes(snapshot)) > MAX_CONTEXT_BYTES:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Proposal context exceeds its bound"
                )
            data = ProposalRequest(
                theme_id=request.theme_id,
                campaign_id=request.campaign_id,
                proposer_id=request.proposer_id,
                controller_id=controller_id,
                control_epoch=epoch,
                policy_revision=previous["revision"] + 1,
                policy_version=state.policy_version,
                campaign_contract_digest=state.campaign_contract_digest,
                incumbent_revision=campaign.incumbent_revision,
                parent_candidate_id=campaign.incumbent_candidate_id,
                parent_source=parent.source_artifact,
                seed=request.seed,
                instructions=request.instructions,
                context=snapshot,
                context_digest=record_digest("research.proposal-context", snapshot),
                context_watermark=sequence,
                requested_at=utc_now(),
                budget_reservation_id=reservation.id,
            )
            ArtifactCatalog.retain(
                transaction,
                ArtifactRetention(
                    artifact_id=parent.source_artifact.id,
                    owner_kind="proposal_request",
                    owner_id=identifier,
                    purpose="parent_source",
                ),
            )
            row = transaction.insert(
                "proposal_request",
                identifier,
                data.model_dump(mode="json"),
                state="pending",
                campaign_id=request.campaign_id,
                parent_id=request.campaign_id,
            )
            updated = write_search_state(
                transaction,
                previous,
                state.model_copy(
                    update={
                        "pending_request_ids": (*state.pending_request_ids, identifier),
                        "request_count": state.request_count + 1,
                    }
                ),
            )
            transaction.event("proposal.requested", row, {"policy_revision": updated["revision"]})
            return {"request": row, "search": updated}

        return self.database.execute(
            context,
            "search.request_proposal",
            request.model_dump(mode="json"),
            "search.manage",
            create,
        )

    @staticmethod
    def _owned(transaction: Transaction, identifier: str) -> tuple[JsonObject, ProposalRequest]:
        row = transaction.get("proposal_request", identifier)
        request = ProposalRequest.model_validate(row["data"])
        if request.proposer_id != transaction.context.principal.id:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Proposal request belongs to another agent"
            )
        return row, request

    def respond(self, context: CommandContext, response: RespondToProposal) -> JsonObject:
        def record(transaction: Transaction) -> JsonObject:
            row, request = self._owned(transaction, response.request_id)
            digest = record_digest("research.proposal-response", response.model_dump(mode="json"))
            try:
                existing = transaction.get("proposal_response", response.request_id)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
            else:
                if existing["data"]["response_digest"] != digest:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Original proposal response differs"
                    )
                return existing
            for reference, purpose in (
                (response.source_artifact, "response_source"),
                (response.response_artifact, "response_trace"),
            ):
                if reference is not None:
                    ArtifactCatalog.available(transaction, reference)
                    ArtifactCatalog.retain(
                        transaction,
                        ArtifactRetention(
                            artifact_id=reference.id,
                            owner_kind="proposal_response",
                            owner_id=response.request_id,
                            purpose=purpose,
                        ),
                    )
            campaign_row = transaction.get("campaign", request.campaign_id)
            campaign = CampaignData.model_validate(campaign_row["data"])
            late = (
                row["state"] != "pending"
                or authority(transaction) != (request.controller_id, request.control_epoch)
                or campaign_row["state"] != "running"
                or campaign.incumbent_revision != request.incumbent_revision
                or campaign.incumbent_eligibility != "eligible"
            )
            data = ProposalResponse(
                request=response,
                response_digest=digest,
                campaign_id=request.campaign_id,
                responder_id=context.principal.id,
                recorded_at=utc_now(),
                late_at_recording=late,
            )
            recorded = transaction.insert(
                "proposal_response",
                response.request_id,
                data.model_dump(mode="json"),
                state="immutable",
                campaign_id=request.campaign_id,
                parent_id=response.request_id,
            )
            if row["state"] == "pending":
                transaction.update(
                    "proposal_request",
                    row["id"],
                    expected_revision=row["revision"],
                    data=row["data"],
                    state="responded",
                )
            transaction.event(
                "proposal.response_recorded",
                recorded,
                {"late_at_recording": late, "outcome": response.outcome},
            )
            return recorded

        return self.database.execute(
            context, "search.respond", response.model_dump(mode="json"), "proposal.respond", record
        )

    def cancel(self, context: CommandContext, intent: CancelProposal) -> JsonObject:
        def change(transaction: Transaction) -> JsonObject:
            campaign = CampaignData.model_validate(
                transaction.get("campaign", intent.campaign_id)["data"]
            )
            previous = search_state(transaction, intent.campaign_id, campaign)
            if context.expected_revision != previous["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Search state revision changed")
            state = SearchState.model_validate(previous["data"])
            row = transaction.get("proposal_request", intent.request_id)
            request = ProposalRequest.model_validate(row["data"])
            if (
                request.campaign_id != intent.campaign_id
                or intent.request_id not in state.pending_request_ids
            ):
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Proposal request is not pending in this search"
                )
            if row["state"] not in {"pending", "responded"}:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Proposal request cannot be cancelled"
                )
            receipt = transaction.insert(
                "proposal_cancellation",
                row["id"],
                ProposalCancellation(
                    request_id=row["id"],
                    actor_id=context.principal.id,
                    reason=intent.reason,
                    recorded_at=utc_now(),
                ).model_dump(mode="json"),
                state="immutable",
                campaign_id=intent.campaign_id,
                parent_id=row["id"],
            )
            cancelled = transaction.update(
                "proposal_request",
                row["id"],
                expected_revision=row["revision"],
                data=row["data"],
                state="cancelled",
            )
            updated = write_search_state(
                transaction,
                previous,
                state.model_copy(
                    update={
                        "pending_request_ids": tuple(
                            item for item in state.pending_request_ids if item != row["id"]
                        )
                    }
                ),
            )
            transaction.event("proposal.cancelled", receipt, {"native_termination_claimed": False})
            return {"request": cancelled, "cancellation": receipt, "search": updated}

        return self.database.execute(
            context,
            "search.cancel_proposal",
            intent.model_dump(mode="json"),
            "search.manage",
            change,
        )

    def get(self, context: CommandContext, request: ProposalRead) -> JsonObject:
        context.principal.require("proposal.respond")
        with self.database.connect() as connection:
            connection.execute("BEGIN")
            transaction = Transaction(connection, context)
            row, _ = self._owned(transaction, request.request_id)
            try:
                response = transaction.get("proposal_response", request.request_id)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
                response = None
            return {"request": row, "response": response}
