"""Charge one funded admission when a controller trial is submitted."""

from decimal import Decimal

from research_swarm.control.models import CampaignData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction
from research_swarm.state.ledger import BudgetLedger


def charge_trial(
    transaction: Transaction,
    campaign: CampaignData,
    campaign_id: str,
    trial_id: str,
) -> None:
    if not any(item.dimension == "trial_admissions" for item in campaign.contract.limits):
        return
    amounts = {"trial_admissions": Decimal(1)}
    if BudgetLedger.account(transaction, campaign_id, "trial_admissions").available < 1:
        raise ResearchError(ErrorCode.BUDGET_EXHAUSTED, "Insufficient funded trial allowance")
    reservation = BudgetLedger.reserve(transaction, campaign_id, trial_id, amounts)
    BudgetLedger.settle(transaction, reservation.id, new_id(), amounts)
