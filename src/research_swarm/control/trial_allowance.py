"""Check the funded allowance for trial admission and delegation."""

from decimal import Decimal

from research_swarm.control.models import CampaignData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction
from research_swarm.state.ledger import BudgetLedger


def require_funded_trials(
    transaction: Transaction,
    campaign_id: str,
    campaign: CampaignData,
    amount: Decimal,
) -> None:
    if not any(limit.dimension == "trial_admissions" for limit in campaign.contract.limits):
        return
    account = BudgetLedger.account(transaction, campaign_id, "trial_admissions")
    if account.available < amount:
        raise ResearchError(ErrorCode.BUDGET_EXHAUSTED, "Insufficient funded trial allowance")
