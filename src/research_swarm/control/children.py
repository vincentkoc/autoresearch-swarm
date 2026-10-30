"""Parent-funded child lifecycle over the same transactional research service."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from research_swarm.control.campaigns import target_met
from research_swarm.control.child_models import (
    CancelChildCampaign,
    CreateChildCampaign,
    SettleChildCampaign,
)
from research_swarm.control.commands import CreateCampaign
from research_swarm.control.models import CampaignData, DelegationConfiguration
from research_swarm.control.reports import create_report
from research_swarm.control.trial_allowance import require_funded_trials
from research_swarm.domain.budgets import BudgetKind
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.funding import CampaignFunding
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.lifecycle import CampaignState, campaign_transition
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.funding import FundingLedger
from research_swarm.state.ledger import BudgetLedger


def validate_delegation(campaign: CampaignData) -> None:
    limits = {item.dimension: item for item in campaign.contract.limits}
    required = set()
    if campaign.parent_campaign_id is not None or campaign.contract.delegation is not None:
        required.add("trial_admissions")
    if campaign.contract.delegation is not None:
        required.add("child_campaigns")
    for dimension in {"trial_admissions", "child_campaigns"}:
        limit = limits.get(dimension)
        if limit is None and dimension not in required:
            continue
        if limit is None or limit.kind != BudgetKind.CUMULATIVE or limit.unit != "count":
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID,
                f"Delegation requires cumulative {dimension}/count funding",
            )
    if required and limits["trial_admissions"].limit > campaign.contract.stopping.max_trials:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID,
            "Funded trial allowance exceeds the inherited stopping limit",
        )


def charge_trial(
    transaction: Transaction,
    campaign: CampaignData,
    campaign_id: str,
    trial_id: str,
    *,
    consuming_proposal_id: str | None = None,
) -> None:
    if not any(item.dimension == "trial_admissions" for item in campaign.contract.limits):
        return
    amounts = {"trial_admissions": Decimal(1)}
    require_funded_trials(
        transaction, campaign_id, campaign, Decimal(1), consuming_proposal_id=consuming_proposal_id
    )
    reservation = BudgetLedger.reserve(transaction, campaign_id, trial_id, amounts)
    BudgetLedger.settle(transaction, reservation.id, new_id(), amounts)


class ChildControl:
    def __init__(
        self,
        database: Database,
        create: Callable[[Transaction, CreateCampaign, str | None], JsonObject],
        validate: Callable[[Transaction, CampaignData], None],
    ) -> None:
        self.database, self.create_campaign, self.validate_campaign = database, create, validate

    def create(self, context: CommandContext, intent: CreateChildCampaign) -> JsonObject:
        from research_swarm.control.service import check_revision

        def create(transaction: Transaction) -> JsonObject:
            row = transaction.get("campaign", intent.parent_campaign_id)
            check_revision(context, row)
            parent = CampaignData.model_validate(row["data"])
            delegation = parent.contract.delegation
            if row["state"] != "running" or parent.contract_digest is None:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Parent must be sealed and running"
                )
            if parent.incumbent_eligibility != "eligible" or target_met(
                transaction, row["id"], parent
            ):
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Parent does not admit new research"
                )
            if delegation is None or intent.remaining_depth >= delegation.max_depth:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Child exceeds parent delegation depth"
                )
            maximum = (
                intent.max_trials
                if intent.max_trials is not None
                else parent.contract.stopping.max_trials
            )
            if maximum > parent.contract.stopping.max_trials:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Child cannot widen stopping limits"
                )
            if any(limit.kind == BudgetKind.CAPACITY for limit in intent.limits):
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE,
                    "Capacity delegation requires a separate transfer contract",
                )
            request = CreateCampaign(
                project_id=parent.contract.project_id,
                project_revision_id=parent.contract.project_revision_id,
                name=intent.name,
                primary_metric=parent.contract.primary_metric,
                limits=intent.limits,
                search=intent.search or parent.contract.search,
                promotion=parent.contract.promotion,
                stopping=parent.contract.stopping.model_copy(update={"max_trials": maximum}),
                input_artifacts=parent.contract.input_artifacts,
                delegation=DelegationConfiguration(max_depth=intent.remaining_depth)
                if intent.remaining_depth
                else None,
            )
            child = self.create_campaign(transaction, request, row["id"])
            self.validate_campaign(transaction, CampaignData.model_validate(child["data"]))
            trial_grant = next(
                limit.limit for limit in intent.limits if limit.dimension == "trial_admissions"
            )
            require_funded_trials(transaction, row["id"], parent, trial_grant)
            # This admission charge is consumed once, separate from the held allocation.
            amounts = {"child_campaigns": Decimal(1)}
            admission = BudgetLedger.reserve(transaction, row["id"], child["id"], amounts)
            BudgetLedger.settle(transaction, admission.id, new_id(), amounts)
            ancestor = (
                CampaignFunding.model_validate(
                    transaction.get("campaign_funding", row["id"])["data"]
                )
                if parent.parent_campaign_id is not None
                else None
            )
            funding = CampaignFunding(
                parent_campaign_id=row["id"],
                child_campaign_id=child["id"],
                root_campaign_id=ancestor.root_campaign_id if ancestor else row["id"],
                parent_contract_digest=parent.contract_digest,
                depth=ancestor.depth + 1 if ancestor else 1,
                remaining_depth=intent.remaining_depth,
                limits=intent.limits,
                allocated_by=context.principal.id,
            )
            recorded = FundingLedger.open(transaction, funding)
            return {
                "campaign": child,
                "funding": recorded,
                "allocation": transaction.get("funding_projection", child["id"]),
            }

        return self.database.execute(
            context,
            "campaign.create_child",
            intent.model_dump(mode="json"),
            "campaign.manage",
            create,
        )

    def settle(self, context: CommandContext, intent: SettleChildCampaign) -> JsonObject:
        from research_swarm.control.service import check_revision

        def settle(transaction: Transaction) -> JsonObject:
            allocation = transaction.get("funding_projection", intent.child_campaign_id)
            if allocation["state"] != "settled":
                check_revision(context, allocation)
            return FundingLedger.close(transaction, intent.child_campaign_id)

        return self.database.execute(
            context,
            "campaign.settle_child",
            intent.model_dump(mode="json"),
            "campaign.manage",
            settle,
        )

    def cancel(self, context: CommandContext, intent: CancelChildCampaign) -> JsonObject:
        from research_swarm.control.service import check_revision

        def cancel(transaction: Transaction) -> JsonObject:
            row = transaction.get("campaign", intent.child_campaign_id)
            check_revision(context, row)
            campaign = CampaignData.model_validate(row["data"])
            if campaign.parent_campaign_id is None or row["state"] not in {"draft", "ready"}:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION,
                    "Only an unstarted funded child can be cancelled here",
                )
            FundingLedger.require_children_closed(transaction, row["id"])
            if transaction.connection.execute(
                "SELECT 1 FROM records WHERE workspace_id=? AND campaign_id=? "
                "AND kind='trial' LIMIT 1",
                (transaction.workspace_id, row["id"]),
            ).fetchone():
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Child already has admitted work")
            campaign_transition(CampaignState(row["state"]), CampaignState.CANCELLED)
            report_id = new_id()
            updated = transaction.update(
                "campaign",
                row["id"],
                expected_revision=row["revision"],
                state="cancelled",
                data=campaign.model_copy(
                    update={"stopping_reason": intent.reason, "final_report_id": report_id}
                ).model_dump(mode="json"),
            )
            transaction.event(
                "campaign.cancelled", updated, {"reason": intent.reason, "report_id": report_id}
            )
            settlement = FundingLedger.close(transaction, row["id"])
            report = create_report(transaction, updated, report_id=report_id)
            return {"campaign": updated, "report": report, "funding_settlement": settlement}

        return self.database.execute(
            context,
            "campaign.cancel_child",
            intent.model_dump(mode="json"),
            "campaign.manage",
            cancel,
        )
