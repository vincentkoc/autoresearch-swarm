"""Immutable campaign funding and explicit live allocation contributions."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, model_validator

from research_swarm.domain.budgets import BudgetAccount, BudgetKind, BudgetLimit
from research_swarm.domain.types import (
    ID,
    BalanceAmount,
    Digest,
    Dimension,
    Revision,
    Timestamp,
    VersionedRecord,
    amount_difference,
    amount_sum,
    utc_now,
)

MAX_FUNDING_DEPTH = 8
FundingReason = Literal["allocation_opened", "child_account_changed", "allocation_closed"]


class CampaignFunding(VersionedRecord):
    parent_campaign_id: ID
    child_campaign_id: ID
    root_campaign_id: ID
    parent_contract_digest: Digest
    depth: int = Field(strict=True, ge=1, le=MAX_FUNDING_DEPTH)
    remaining_depth: int = Field(strict=True, ge=0, lt=MAX_FUNDING_DEPTH)
    limits: tuple[BudgetLimit, ...] = Field(min_length=1, max_length=128)
    allocated_by: ID
    allocated_at: Timestamp = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def bounded_tree(self) -> Self:
        if self.parent_campaign_id == self.child_campaign_id:
            raise ValueError("A campaign cannot fund itself")
        if self.depth + self.remaining_depth > MAX_FUNDING_DEPTH:
            raise ValueError("Funding exceeds the supported recursion bound")
        if len({limit.dimension for limit in self.limits}) != len(self.limits):
            raise ValueError("Funding dimensions must be unique")
        if any(limit.kind == BudgetKind.CAPACITY for limit in self.limits):
            raise ValueError("Capacity requires its own transfer and reclaim contract")
        return self


class AllocationBalance(VersionedRecord):
    consumed: BalanceAmount = Decimal(0)
    reserved: BalanceAmount = Decimal(0)
    uncertain: BalanceAmount = Decimal(0)
    child_revision: Revision = 0

    @property
    def total(self) -> Decimal:
        return amount_sum(self.consumed, self.reserved, self.uncertain)

    @classmethod
    def live(cls, allocation: Decimal, child: BudgetAccount) -> AllocationBalance:
        return cls(
            consumed=child.consumed,
            uncertain=child.uncertain,
            reserved=max(
                child.reserved,
                amount_difference(amount_difference(allocation, child.consumed), child.uncertain),
                Decimal(0),
            ),
            child_revision=child.revision,
        )


class FundingProjection(VersionedRecord):
    funding_id: ID
    balances: dict[Dimension, AllocationBalance] = Field(min_length=1, max_length=128)


class FundingChange(VersionedRecord):
    funding_id: ID
    dimension: Dimension
    before: AllocationBalance
    after: AllocationBalance
    reason: FundingReason


class FundingSettlement(VersionedRecord):
    funding_id: ID
    parent_campaign_id: ID
    child_campaign_id: ID
    final_report_id: ID
    child_accounts_digest: Digest
    balances: dict[Dimension, AllocationBalance] = Field(min_length=1, max_length=128)
    settled_at: Timestamp = Field(default_factory=utc_now)
