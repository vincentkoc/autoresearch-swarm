"""Freeze scientific conclusions while original resource obligations remain enforceable."""

from research_swarm.control.campaigns import target_met
from research_swarm.control.completion_models import (
    CampaignResourceClosure,
    CaptureCampaignObligations,
    CloseCampaignResources,
    CompleteScientificCampaign,
    ObligationReview,
    ScientificCompletion,
)
from research_swarm.control.models import CampaignData, TrialData
from research_swarm.control.obligations import inspect_obligations, require_review
from research_swarm.control.reports import create_report
from research_swarm.control.results import ResultControl
from research_swarm.domain.budgets import BudgetAccount, BudgetKind
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.lifecycle import TERMINAL_CAMPAIGNS
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.funding import FundingLedger


def previous(
    transaction: Transaction, kind: str, identifier: str, digest: str
) -> JsonObject | None:
    try:
        row = transaction.get(kind, identifier)
    except ResearchError as error:
        if error.code == ErrorCode.NOT_FOUND:
            return None
        raise
    if row["data"]["request_digest"] != digest:
        raise ResearchError(
            ErrorCode.IDEMPOTENCY_CONFLICT, "Completion identity has another intent"
        )
    return row


def command_digest(context: CommandContext, payload: JsonObject) -> str:
    return record_digest(
        "research.campaign-completion-command-v1",
        {"payload": payload, "expected_revision": context.expected_revision},
    )


def chunks(
    transaction: Transaction, review_id: str, campaign_id: str, entries: tuple[JsonObject, ...]
) -> tuple[str, ...]:
    identifiers: list[str] = []
    pending: list[JsonObject] = []

    def data(values: list[JsonObject]) -> JsonObject:
        return {
            "schema_version": "1.0",
            "review_id": review_id,
            "ordinal": len(identifiers) + 1,
            "entries": values,
        }

    def persist() -> None:
        if len(identifiers) >= 128:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Obligation chunk count exceeds its bound"
            )
        identifier = new_id()
        transaction.insert(
            "campaign_obligation_chunk",
            identifier,
            data(pending),
            state="immutable",
            campaign_id=campaign_id,
            parent_id=review_id,
        )
        identifiers.append(identifier)

    for entry in entries:
        if pending and (
            len(pending) >= 128 or len(canonical_bytes(data([*pending, entry]))) > 256 * 1024
        ):
            persist()
            pending = []
        if len(canonical_bytes(data([entry]))) > 256 * 1024:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Obligation entry exceeds its chunk bound"
            )
        pending.append(entry)
    if pending:
        persist()
    return tuple(identifiers)


class ScientificCompletionControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def capture(self, context: CommandContext, intent: CaptureCampaignObligations) -> JsonObject:
        payload = intent.model_dump(mode="json")
        digest = command_digest(context, payload)

        def capture(transaction: Transaction) -> JsonObject:
            prior = previous(transaction, "campaign_obligations", intent.review_id, digest)
            if prior:
                return prior
            campaign = transaction.get("campaign", intent.campaign_id)
            if context.expected_revision != campaign["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Campaign revision changed")
            view = inspect_obligations(transaction, campaign["id"])
            review = ObligationReview(
                campaign_id=campaign["id"],
                campaign_revision=campaign["revision"],
                obligations_digest=view.digest,
                chunk_ids=chunks(transaction, intent.review_id, campaign["id"], view.entries),
                entry_count=len(view.entries),
                counts=view.counts,
                request_digest=digest,
                captured_by=context.principal.id,
                captured_at=utc_now(),
            )
            row = transaction.insert(
                "campaign_obligations",
                intent.review_id,
                review.model_dump(mode="json"),
                state="immutable",
                campaign_id=campaign["id"],
            )
            transaction.event("campaign.obligations_captured", row, {"campaign_id": campaign["id"]})
            return row

        return self.database.execute(
            context, "campaign.capture_obligations", payload, "campaign.manage", capture
        )

    def complete(self, context: CommandContext, intent: CompleteScientificCampaign) -> JsonObject:
        payload = intent.model_dump(mode="json")
        digest = command_digest(context, payload)

        def complete(transaction: Transaction) -> JsonObject:
            prior = previous(transaction, "scientific_completion", intent.campaign_id, digest)
            if prior:
                return prior
            row = transaction.get("campaign", intent.campaign_id)
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Campaign revision changed")
            if row["state"] not in {"running", "pausing", "paused", "awaiting_input"}:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Campaign cannot complete scientifically"
                )
            campaign = CampaignData.model_validate(row["data"])
            review, view = require_review(transaction, row, intent.obligations_id)
            for entry in view.entries:
                if entry["category"] == "attempt" and entry["state"] != "execution_unknown":
                    raise ResearchError(
                        ErrorCode.RESOURCE_UNAVAILABLE,
                        "Drain known active attempts before scientific completion",
                    )
                if entry["category"] == "child" and (
                    entry["state"] not in TERMINAL_CAMPAIGNS or entry["final_report_id"] is None
                ):
                    raise ResearchError(
                        ErrorCode.RESOURCE_UNAVAILABLE, "Child research has not finished"
                    )
            trial_count = transaction.connection.execute(
                "SELECT COUNT(*) FROM records WHERE workspace_id=? "
                "AND campaign_id=? AND kind='trial'",
                (transaction.workspace_id, row["id"]),
            ).one()[0]
            accounts = [
                BudgetAccount.model_validate(entry["value"])
                for entry in view.entries
                if entry["category"] == "budget"
            ]
            exhausted = any(
                account.dimension == "trial_admissions" and account.available < 1
                for account in accounts
            )
            if trial_count >= campaign.contract.stopping.max_trials:
                basis = "max_trials"
            elif exhausted:
                basis = "trial_allowance"
            elif target_met(transaction, row["id"], campaign):
                basis = "target_metric"
            elif campaign.contract.stopping.permit_manual_completion:
                basis = "manual"
            else:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Declared stopping condition is not met"
                )
            report_id = new_id()
            watermark = transaction.connection.execute(
                "SELECT next_sequence-1 FROM workspaces WHERE id=?", (transaction.workspace_id,)
            ).one()[0]
            updated = transaction.update(
                "campaign",
                row["id"],
                expected_revision=row["revision"],
                state="completed",
                data=campaign.model_copy(
                    update={"stopping_reason": intent.reason, "final_report_id": report_id}
                ).model_dump(mode="json"),
            )
            completion = ScientificCompletion(
                campaign_id=row["id"],
                campaign_revision=updated["revision"],
                obligations_id=intent.obligations_id,
                obligations_digest=review.obligations_digest,
                request_digest=digest,
                final_report_id=report_id,
                incumbent_candidate_id=campaign.incumbent_candidate_id,
                incumbent_revision=campaign.incumbent_revision,
                incumbent_promotion_id=campaign.incumbent_promotion_id,
                incumbent_eligibility=campaign.incumbent_eligibility,
                stopping_basis=basis,
                reason=intent.reason,
                evidence_through=watermark,
                completed_by=context.principal.id,
                completed_at=utc_now(),
            )
            recorded = transaction.insert(
                "scientific_completion",
                row["id"],
                completion.model_dump(mode="json"),
                state="immutable",
                campaign_id=row["id"],
            )
            for entry in view.entries:
                if entry["category"] == "trial":
                    trial_row = transaction.get("trial", entry["id"])
                    ResultControl._advance_trial(
                        transaction, trial_row, TrialData.model_validate(trial_row["data"])
                    )
            transaction.event(
                "campaign.scientifically_completed", recorded, {"final_report_id": report_id}
            )
            create_report(transaction, updated, report_id=report_id)
            return recorded

        return self.database.execute(
            context, "campaign.complete_scientific", payload, "campaign.manage", complete
        )

    def close_resources(
        self, context: CommandContext, intent: CloseCampaignResources
    ) -> JsonObject:
        payload = intent.model_dump(mode="json")
        digest = command_digest(context, payload)

        def close(transaction: Transaction) -> JsonObject:
            prior = previous(transaction, "campaign_resource_closure", intent.campaign_id, digest)
            if prior:
                return prior
            campaign = transaction.get("campaign", intent.campaign_id)
            if context.expected_revision != campaign["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Campaign revision changed")
            if campaign["state"] not in TERMINAL_CAMPAIGNS:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Research is not scientifically terminal"
                )
            completion = ScientificCompletion.model_validate(
                transaction.get("scientific_completion", campaign["id"])["data"]
            )
            review, view = require_review(transaction, campaign, intent.obligations_id)
            if any(entry["category"] != "budget" for entry in view.entries):
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    "Campaign resource obligations remain unresolved",
                )
            for entry in view.entries:
                account = BudgetAccount.model_validate(entry["value"])
                if (
                    account.reserved
                    or account.uncertain
                    or (account.kind == BudgetKind.CAPACITY and account.consumed)
                ):
                    raise ResearchError(
                        ErrorCode.RESOURCE_UNAVAILABLE, "Campaign accounting remains unresolved"
                    )
            data = CampaignData.model_validate(campaign["data"])
            settlement = (
                FundingLedger.close(transaction, campaign["id"])
                if data.parent_campaign_id
                else None
            )
            closure = CampaignResourceClosure(
                campaign_id=campaign["id"],
                scientific_completion_id=campaign["id"],
                obligations_id=intent.obligations_id,
                obligations_digest=review.obligations_digest,
                request_digest=digest,
                final_report_id=completion.final_report_id,
                funding_settlement_id=settlement["id"] if settlement else None,
                reason=intent.reason,
                closed_by=context.principal.id,
                closed_at=utc_now(),
            )
            row = transaction.insert(
                "campaign_resource_closure",
                campaign["id"],
                closure.model_dump(mode="json"),
                state="immutable",
                campaign_id=campaign["id"],
            )
            transaction.event(
                "campaign.resources_closed", row, {"final_report_id": completion.final_report_id}
            )
            return row

        return self.database.execute(
            context, "campaign.close_resources", payload, "campaign.manage", close
        )
