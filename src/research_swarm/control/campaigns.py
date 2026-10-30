"""Drain admission and complete campaigns only after their recorded work settles."""

from fractions import Fraction

from research_swarm.control.commands import CompleteCampaign, PauseCampaign
from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.models import CampaignData, TrialData
from research_swarm.control.promotions import PromotionAssessment
from research_swarm.control.reports import ReportCampaign, create_report
from research_swarm.domain.budgets import BudgetAccount, BudgetKind
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import Direction
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.lifecycle import CampaignState, campaign_transition
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.funding import FundingLedger


def settle_pause(transaction: Transaction, campaign_id: str) -> JsonObject:
    row = transaction.get("campaign", campaign_id)
    if row["state"] != "pausing":
        return row
    active = transaction.connection.execute(
        "SELECT 1 FROM records WHERE workspace_id=? AND kind='trial' AND campaign_id=? "
        "AND state IN ('running','execution_unknown') LIMIT 1",
        (transaction.workspace_id, campaign_id),
    ).fetchone()
    if active:
        return row
    updated = transaction.update(
        "campaign", campaign_id, expected_revision=row["revision"], data=row["data"], state="paused"
    )
    transaction.event("campaign.paused", updated, {"mode": "drain"})
    return updated


def target_met(transaction: Transaction, campaign_id: str, campaign: CampaignData) -> bool:
    target = campaign.contract.stopping.target_metric
    if (
        target is None
        or campaign.incumbent_promotion_id is None
        or campaign.incumbent_eligibility != "eligible"
    ):
        return False
    selection = PromotionAssessment.model_validate(
        transaction.get("promotion", campaign.incumbent_promotion_id)["data"]
    )
    values = []
    direction = None
    for receipt_id in selection.decision.evaluation_ids:
        evaluation = eligible_evaluation(transaction, receipt_id, campaign_id, campaign)
        if evaluation.trial.candidate_id == campaign.incumbent_candidate_id:
            values.append(Fraction(str(evaluation.metric.value)))
            direction = evaluation.metric.direction
    if not values:
        return False
    mean = sum(values, Fraction()) / len(values)
    threshold = Fraction(str(target))
    return mean <= threshold if direction == Direction.MINIMIZE else mean >= threshold


class CampaignControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def pause(self, context: CommandContext, intent: PauseCampaign) -> JsonObject:
        def pause(transaction: Transaction) -> JsonObject:
            row = transaction.get("campaign", intent.campaign_id)
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Campaign revision changed")
            if intent.mode != "drain":
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "This pause mode is not qualified"
                )
            campaign_transition(CampaignState(row["state"]), CampaignState.PAUSING)
            changed = transaction.update(
                "campaign",
                row["id"],
                expected_revision=row["revision"],
                state="pausing",
                data=row["data"],
            )
            transaction.event("campaign.pause_requested", changed, {"mode": "drain"})
            return settle_pause(transaction, row["id"])

        return self.database.execute(
            context, "campaign.pause", intent.model_dump(mode="json"), "campaign.manage", pause
        )

    def report(self, context: CommandContext, intent: ReportCampaign) -> JsonObject:
        return self.database.execute(
            context,
            "report.create",
            intent.model_dump(mode="json"),
            "report.create",
            lambda transaction: create_report(
                transaction,
                transaction.get("campaign", intent.campaign_id),
                recent_limit=intent.recent_limit,
            ),
        )

    def complete(self, context: CommandContext, intent: CompleteCampaign) -> JsonObject:
        def complete(transaction: Transaction) -> JsonObject:
            row = transaction.get("campaign", intent.campaign_id)
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Campaign revision changed")
            campaign_transition(CampaignState(row["state"]), CampaignState.COMPLETED)
            campaign = CampaignData.model_validate(row["data"])
            trials = transaction.connection.execute(
                "SELECT state,payload FROM records WHERE workspace_id=? "
                "AND campaign_id=? AND kind='trial'",
                (transaction.workspace_id, row["id"]),
            )
            trial_count = 0
            for item in trials:
                trial_count += 1
                trial = TrialData.model_validate_json(item["payload"])
                if trial.execution_outcome == "unknown" or trial.resource_status == "unknown":
                    raise ResearchError(
                        ErrorCode.EXECUTION_UNKNOWN, "Campaign has uncertain execution"
                    )
                if (
                    item["state"] not in {"succeeded", "failed", "cancelled", "closed_unaccepted"}
                    or trial.resource_status != "closed"
                ):
                    raise ResearchError(
                        ErrorCode.INVALID_TRANSITION, "Campaign has unfinished work"
                    )
                if trial.execution_outcome == "succeeded" and trial.evaluation_status == "pending":
                    raise ResearchError(
                        ErrorCode.EVALUATION_INCOMPLETE, "Successful work still awaits evaluation"
                    )
            FundingLedger.require_children_closed(transaction, row["id"])
            accounts = transaction.connection.execute(
                "SELECT payload FROM budgets WHERE workspace_id=? AND campaign_id=?",
                (transaction.workspace_id, row["id"]),
            ).fetchall()
            trial_allowance_exhausted = False
            for item in accounts:
                account = BudgetAccount.model_validate_json(item["payload"])
                if (
                    account.reserved
                    or account.uncertain
                    or (account.kind == BudgetKind.CAPACITY and account.consumed)
                ):
                    raise ResearchError(
                        ErrorCode.RESOURCE_UNAVAILABLE,
                        "Campaign has unresolved budget reservations",
                    )
                if account.dimension == "trial_admissions" and account.available < 1:
                    trial_allowance_exhausted = True
            stopped = (
                trial_count >= campaign.contract.stopping.max_trials or trial_allowance_exhausted
            )
            if (
                not campaign.contract.stopping.permit_manual_completion
                and not stopped
                and not target_met(transaction, row["id"], campaign)
            ):
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Declared stopping condition is not met"
                )
            report_id = new_id()
            updated = transaction.update(
                "campaign",
                row["id"],
                expected_revision=row["revision"],
                state="completed",
                data=campaign.model_copy(
                    update={"stopping_reason": intent.reason, "final_report_id": report_id}
                ).model_dump(mode="json"),
            )
            transaction.event(
                "campaign.completed", updated, {"report_id": report_id, "reason": intent.reason}
            )
            settlement = (
                FundingLedger.close(transaction, row["id"]) if campaign.parent_campaign_id else None
            )
            report = create_report(transaction, updated, report_id=report_id)
            return {
                "campaign": updated,
                "report": report,
                **({"funding_settlement": settlement} if settlement else {}),
            }

        return self.database.execute(
            context,
            "campaign.complete",
            intent.model_dump(mode="json"),
            "campaign.manage",
            complete,
        )
