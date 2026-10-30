"""One transactional funded allowance for direct, proposed, and delegated trials."""

from decimal import Decimal

from research_swarm.control.models import CampaignData
from research_swarm.control.search_models import SearchState
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import amount_sum
from research_swarm.state.database import Transaction
from research_swarm.state.ledger import BudgetLedger


def require_funded_trials(
    transaction: Transaction,
    campaign_id: str,
    campaign: CampaignData,
    amount: Decimal,
    *,
    consuming_proposal_id: str | None = None,
) -> None:
    if not any(limit.dimension == "trial_admissions" for limit in campaign.contract.limits):
        return
    row = transaction.connection.execute(
        "SELECT payload FROM records WHERE workspace_id=? AND kind='search_state' AND id=?",
        (transaction.workspace_id, campaign_id),
    ).fetchone()
    pending = (
        () if row is None else SearchState.model_validate_json(row["payload"]).pending_request_ids
    )
    promised = sum(identifier != consuming_proposal_id for identifier in pending)
    account = BudgetLedger.account(transaction, campaign_id, "trial_admissions")
    if account.available < amount_sum(amount, Decimal(promised)):
        raise ResearchError(
            ErrorCode.BUDGET_EXHAUSTED, "No unpromised funded trial allowance remains"
        )
