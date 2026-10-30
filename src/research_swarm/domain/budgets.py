"""Exact, pure accounting. Storage occupancy and deadlines have separate models."""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum

from pydantic import Field, model_validator

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import (
    ID,
    Amount,
    BalanceAmount,
    Dimension,
    Name,
    Record,
    Revision,
    SignedAmount,
    amount_difference,
    amount_sum,
    new_id,
)


class BudgetKind(StrEnum):
    CUMULATIVE = "cumulative"
    CAPACITY = "capacity"
    EXPOSURE = "exposure"


class BudgetLimit(Record):
    dimension: Dimension
    limit: Amount
    kind: BudgetKind = BudgetKind.CUMULATIVE
    unit: Name


class BudgetAccount(Record):
    campaign_id: ID
    dimension: Dimension
    kind: BudgetKind
    unit: Name
    limit: Amount
    consumed: BalanceAmount = Decimal(0)
    reserved: BalanceAmount = Decimal(0)
    uncertain: BalanceAmount = Decimal(0)
    released_total: BalanceAmount = Decimal(0)
    revision: Revision = 0

    @property
    def available(self) -> Decimal:
        return amount_sum(
            self.limit,
            self.consumed.copy_negate(),
            self.reserved.copy_negate(),
            self.uncertain.copy_negate(),
        )

    def reserve(self, amount: Decimal) -> BudgetAccount:
        if amount < 0:
            raise ValueError("Cannot reserve a negative amount")
        if amount > self.available:
            raise ResearchError(
                ErrorCode.BUDGET_EXHAUSTED,
                f"Insufficient {self.dimension}",
                details={"available": str(self.available), "requested": str(amount)},
            )
        return self.model_copy(
            update={"reserved": amount_sum(self.reserved, amount), "revision": self.revision + 1}
        )

    def mark_uncertain(self, amount: Decimal) -> BudgetAccount:
        if amount < 0 or amount > self.reserved:
            raise ValueError("Uncertainty must transfer an outstanding reservation")
        return self.model_copy(
            update={
                "reserved": amount_difference(self.reserved, amount),
                "uncertain": amount_sum(self.uncertain, amount),
                "revision": self.revision + 1,
            }
        )

    def settle(
        self, reservation: Decimal, actual: Decimal, *, from_uncertain: bool = False
    ) -> BudgetAccount:
        balance = self.uncertain if from_uncertain else self.reserved
        if min(reservation, actual) < 0 or reservation > balance:
            raise ValueError("Invalid settlement")
        updates = {
            "consumed": amount_sum(self.consumed, actual),
            "released_total": amount_sum(
                self.released_total, max(Decimal(0), amount_difference(reservation, actual))
            ),
            "revision": self.revision + 1,
            "uncertain" if from_uncertain else "reserved": amount_difference(balance, reservation),
        }
        return self.model_copy(update=updates)

    def reclaim(self, amount: Decimal) -> BudgetAccount:
        if self.kind != BudgetKind.CAPACITY:
            raise ValueError("Cumulative consumption and exposure cannot be reclaimed")
        if amount < 0 or amount > self.consumed:
            raise ValueError("Invalid reclaim amount")
        return self.model_copy(
            update={
                "consumed": amount_difference(self.consumed, amount),
                "released_total": amount_sum(self.released_total, amount),
                "revision": self.revision + 1,
            }
        )


class BudgetEntry(Record):
    id: ID = Field(default_factory=new_id)
    campaign_id: ID
    dimension: Dimension
    settlement_id: ID
    consumed_delta: SignedAmount = Decimal(0)
    reserved_delta: SignedAmount = Decimal(0)
    uncertain_delta: SignedAmount = Decimal(0)
    released_delta: Amount = Decimal(0)
    reason: str


class DeadlinePolicy(Record):
    wall_limit_ms: int = Field(strict=True, gt=0)
    pause_counts: bool = True


class Reservation(Record):
    id: ID = Field(default_factory=new_id)
    campaign_id: ID
    owner_id: ID
    amounts: dict[Dimension, Amount]
    uncertain: bool = False

    @model_validator(mode="after")
    def nonempty(self) -> Reservation:
        if not self.amounts:
            raise ValueError("Reservation must name at least one budget dimension")
        return self
