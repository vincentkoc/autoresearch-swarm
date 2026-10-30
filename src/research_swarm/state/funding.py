"""Hierarchical funding remains inside the domain transaction and shared ledger."""

from __future__ import annotations

from decimal import Decimal

from research_swarm.domain.budgets import BudgetAccount, BudgetEntry
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.funding import (
    MAX_FUNDING_DEPTH,
    AllocationBalance,
    CampaignFunding,
    FundingChange,
    FundingProjection,
    FundingReason,
    FundingSettlement,
)
from research_swarm.domain.types import amount_difference, amount_sum, new_id
from research_swarm.state.database import JsonObject, Transaction
from research_swarm.state.ledger import BudgetLedger


def _funding(transaction: Transaction, child_id: str) -> CampaignFunding | None:
    exists = transaction.connection.execute(
        "SELECT 1 FROM records WHERE workspace_id=? AND kind='campaign_funding' AND id=?",
        (transaction.workspace_id, child_id),
    ).fetchone()
    if exists is None:
        return None
    return CampaignFunding.model_validate(transaction.get("campaign_funding", child_id)["data"])


def _accounts(transaction: Transaction, campaign_id: str) -> dict[str, BudgetAccount]:
    rows = transaction.connection.execute(
        "SELECT dimension,payload FROM budgets WHERE workspace_id=? "
        "AND campaign_id=? ORDER BY dimension",
        (transaction.workspace_id, campaign_id),
    )
    return {row["dimension"]: BudgetAccount.model_validate_json(row["payload"]) for row in rows}


class FundingLedger:
    @staticmethod
    def open(transaction: Transaction, funding: CampaignFunding) -> JsonObject:
        identifier = funding.child_campaign_id
        previous = _funding(transaction, identifier)
        if previous is not None:
            if previous != funding:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Campaign funding already differs"
                )
            return transaction.get("campaign_funding", identifier)
        parent = transaction.get("campaign", funding.parent_campaign_id)
        child = transaction.get("campaign", identifier)
        if (
            parent["state"] != "running"
            or parent["data"].get("contract_digest") != funding.parent_contract_digest
            or child["state"] != "draft"
            or child["data"].get("parent_campaign_id") != funding.parent_campaign_id
            or funding.allocated_by != transaction.context.principal.id
        ):
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Funding differs from campaign authority"
            )
        ancestor = _funding(transaction, funding.parent_campaign_id)
        if ancestor is None:
            valid_lineage = (
                funding.root_campaign_id == funding.parent_campaign_id and funding.depth == 1
            )
        else:
            valid_lineage = (
                funding.root_campaign_id == ancestor.root_campaign_id
                and funding.depth == ancestor.depth + 1
                and funding.remaining_depth < ancestor.remaining_depth
            )
        if not valid_lineage:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Funding expands or changes its lineage"
            )
        accounts = _accounts(transaction, identifier)
        if set(accounts) != {item.dimension for item in funding.limits}:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Child accounts differ from its funding"
            )
        for limit in funding.limits:
            account = accounts[limit.dimension]
            parent_account = BudgetLedger.account(
                transaction, funding.parent_campaign_id, limit.dimension
            )
            if (
                account.limit != limit.limit
                or account.kind != limit.kind
                or account.unit != limit.unit
                or parent_account.kind != limit.kind
                or parent_account.unit != limit.unit
                or account.consumed
                or account.reserved
                or account.uncertain
            ):
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "Child funding requires fresh matching accounts",
                )
            FundingLedger.check_admission(
                transaction, funding.parent_campaign_id, (limit.dimension,)
            )
            parent_account.reserve(
                limit.limit
            )  # Validate availability before creating any projection.
        row = transaction.insert(
            "campaign_funding",
            identifier,
            funding.model_dump(mode="json"),
            state="immutable",
            campaign_id=identifier,
            parent_id=funding.parent_campaign_id,
        )
        projection = FundingProjection(
            funding_id=identifier,
            balances={limit.dimension: AllocationBalance() for limit in funding.limits},
        )
        transaction.insert(
            "funding_projection",
            identifier,
            projection.model_dump(mode="json"),
            state="open",
            campaign_id=identifier,
            parent_id=funding.parent_campaign_id,
        )
        for limit in funding.limits:
            FundingLedger._change(
                transaction,
                funding,
                limit.dimension,
                AllocationBalance.live(limit.limit, accounts[limit.dimension]),
                "allocation_opened",
                (),
            )
        transaction.event(
            "campaign.funded", row, {"parent_campaign_id": funding.parent_campaign_id}
        )
        return row

    @staticmethod
    def check_admission(
        transaction: Transaction, campaign_id: str, dimensions: tuple[str, ...]
    ) -> None:
        visited: set[str] = set()
        current = campaign_id
        while (funding := _funding(transaction, current)) is not None:
            if current in visited or len(visited) >= MAX_FUNDING_DEPTH:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "Funding lineage is cyclic or exceeds its bound",
                )
            visited.add(current)
            if transaction.get("funding_projection", current)["state"] != "open":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Campaign funding is settled")
            for dimension in dimensions:
                if (
                    BudgetLedger.account(
                        transaction, funding.parent_campaign_id, dimension
                    ).available
                    < 0
                ):
                    raise ResearchError(
                        ErrorCode.BUDGET_EXHAUSTED, "An ancestor has an outstanding budget deficit"
                    )
            current = funding.parent_campaign_id

    @staticmethod
    def propagate(
        transaction: Transaction, child: BudgetAccount, lineage: tuple[str, ...] = ()
    ) -> None:
        funding = _funding(transaction, child.campaign_id)
        if funding is None:
            return
        if child.campaign_id in lineage or len(lineage) >= MAX_FUNDING_DEPTH:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Funding lineage is cyclic or exceeds its bound"
            )
        limit = next((item for item in funding.limits if item.dimension == child.dimension), None)
        if limit is None or (limit.kind, limit.unit, limit.limit) != (
            child.kind,
            child.unit,
            child.limit,
        ):
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Budget account differs from immutable funding"
            )
        FundingLedger._change(
            transaction,
            funding,
            child.dimension,
            AllocationBalance.live(limit.limit, child),
            "child_account_changed",
            (*lineage, child.campaign_id),
        )

    @staticmethod
    def _change(
        transaction: Transaction,
        funding: CampaignFunding,
        dimension: str,
        after: AllocationBalance,
        reason: FundingReason,
        lineage: tuple[str, ...],
    ) -> None:
        row = transaction.get("funding_projection", funding.child_campaign_id)
        if row["state"] != "open":
            raise ResearchError(
                ErrorCode.INVALID_TRANSITION, "Settled funding cannot regain an allocation"
            )
        projection = FundingProjection.model_validate(row["data"])
        before = projection.balances[dimension]
        if before == after:
            return
        if after.child_revision < before.child_revision or after.consumed < before.consumed:
            raise ResearchError(ErrorCode.REVISION_CONFLICT, "Child accounting moved backwards")
        changed = transaction.update(
            "funding_projection",
            row["id"],
            expected_revision=row["revision"],
            state="open",
            data=projection.model_copy(
                update={"balances": {**projection.balances, dimension: after}}
            ).model_dump(mode="json"),
        )
        deltas = {
            name: amount_difference(getattr(after, name), getattr(before, name))
            for name in ("consumed", "reserved", "uncertain")
        }
        if not any(deltas.values()):
            return
        release = max(Decimal(0), amount_difference(before.total, after.total))
        parent = BudgetLedger.account(transaction, funding.parent_campaign_id, dimension)
        updated = parent.model_copy(
            update={
                **{
                    name: amount_sum(getattr(parent, name), delta) for name, delta in deltas.items()
                },
                "released_total": amount_sum(parent.released_total, release),
                "revision": parent.revision + 1,
            }
        )
        receipt_id = new_id()
        receipt = transaction.insert(
            "funding_change",
            receipt_id,
            FundingChange(
                funding_id=funding.child_campaign_id,
                dimension=dimension,
                before=before,
                after=after,
                reason=reason,
            ).model_dump(mode="json"),
            state="immutable",
            campaign_id=funding.child_campaign_id,
            parent_id=funding.parent_campaign_id,
        )
        BudgetLedger._save(transaction, updated, lineage=lineage)
        BudgetLedger._entry(
            transaction,
            BudgetEntry(
                campaign_id=parent.campaign_id,
                dimension=dimension,
                settlement_id=receipt_id,
                consumed_delta=deltas["consumed"],
                reserved_delta=deltas["reserved"],
                uncertain_delta=deltas["uncertain"],
                released_delta=release,
                reason=reason,
            ),
        )
        transaction.event(
            "funding.projected", receipt, {"projection_revision": changed["revision"]}
        )

    @staticmethod
    def require_children_closed(transaction: Transaction, parent_id: str) -> None:
        open_child = transaction.connection.execute(
            "SELECT 1 FROM records WHERE workspace_id=? AND kind='funding_projection' "
            "AND parent_id=? AND state='open' LIMIT 1",
            (transaction.workspace_id, parent_id),
        ).fetchone()
        if open_child:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Campaign has an open child allocation"
            )

    @staticmethod
    def close(transaction: Transaction, child_id: str) -> JsonObject:
        funding = _funding(transaction, child_id)
        if funding is None:
            raise ResearchError(ErrorCode.NOT_FOUND, "Campaign has no parent funding")
        row = transaction.get("funding_projection", child_id)
        if row["state"] == "settled":
            return transaction.get("funding_settlement", child_id)
        child = transaction.get("campaign", child_id)
        report_id = child["data"].get("final_report_id")
        if child["state"] not in {"completed", "failed", "cancelled"} or report_id is None:
            raise ResearchError(
                ErrorCode.INVALID_TRANSITION, "Child has no terminal report identity"
            )
        FundingLedger.require_children_closed(transaction, child_id)
        accounts = _accounts(transaction, child_id)
        if set(accounts) != {item.dimension for item in funding.limits}:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Child accounts differ from its funding"
            )
        if any(account.reserved or account.uncertain for account in accounts.values()):
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Child accounting is unresolved")
        for limit in funding.limits:
            FundingLedger.propagate(transaction, accounts[limit.dimension])
        previous = FundingProjection.model_validate(
            transaction.get("funding_projection", child_id)["data"]
        )
        for dimension, account in accounts.items():
            FundingLedger._change(
                transaction,
                funding,
                dimension,
                AllocationBalance(consumed=account.consumed, child_revision=account.revision),
                "allocation_closed",
                (child_id,),
            )
        current = transaction.get("funding_projection", child_id)
        transaction.update(
            "funding_projection",
            child_id,
            expected_revision=current["revision"],
            data=current["data"],
            state="settled",
        )
        receipt = FundingSettlement(
            funding_id=child_id,
            parent_campaign_id=funding.parent_campaign_id,
            child_campaign_id=child_id,
            final_report_id=report_id,
            child_accounts_digest=record_digest(
                "research.child-final-accounts",
                {name: account.model_dump(mode="json") for name, account in accounts.items()},
            ),
            balances=previous.balances,
        )
        settled = transaction.insert(
            "funding_settlement",
            child_id,
            receipt.model_dump(mode="json"),
            state="immutable",
            campaign_id=child_id,
            parent_id=funding.parent_campaign_id,
        )
        transaction.event("campaign.funding_settled", settled, {"final_report_id": report_id})
        return settled
