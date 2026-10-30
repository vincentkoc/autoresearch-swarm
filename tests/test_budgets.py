from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from research_swarm.domain.budgets import BudgetAccount, BudgetKind
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id


def account(limit: int = 100, kind: BudgetKind = BudgetKind.CUMULATIVE) -> BudgetAccount:
    return BudgetAccount(
        campaign_id=new_id(), dimension="worker_ms", unit="ms", kind=kind, limit=limit
    )


@given(st.integers(min_value=0, max_value=100))
def test_uncertainty_transfers_balance_without_refund(amount: int) -> None:
    current = account().reserve(Decimal(amount))
    unknown = current.mark_uncertain(Decimal(amount))
    assert unknown.available == current.available
    assert unknown.reserved == 0
    assert unknown.uncertain == amount
    settled = unknown.settle(Decimal(amount), Decimal(amount), from_uncertain=True)
    assert settled.consumed == amount
    assert settled.reserved == settled.uncertain == 0
    assert settled.available == 100 - amount


def test_overshoot_is_visible_not_clipped() -> None:
    result = account(10).reserve(Decimal(10)).settle(Decimal(10), Decimal(12))
    assert result.available == -2
    assert result.consumed == 12
    with pytest.raises(ResearchError) as caught:
        result.reserve(Decimal(1))
    assert caught.value.code == ErrorCode.BUDGET_EXHAUSTED


def test_release_is_not_a_second_charge() -> None:
    result = account().reserve(Decimal(10)).settle(Decimal(10), Decimal(3))
    assert result.available == 97
    assert result.released_total == 7


def test_large_overshoot_does_not_round_at_the_global_decimal_precision() -> None:
    limit = Decimal("9999999999999999999.999999999")
    original = account().model_copy(
        update={"limit": limit, "consumed": Decimal("9999999999999999999.999999997")}
    )
    reservation = Decimal("0.000000001")
    result = original.reserve(reservation).settle(reservation, limit)
    assert result.consumed == Decimal("19999999999999999999.999999996")
    assert result.available == Decimal("-9999999999999999999.999999997")
    BudgetAccount.model_validate(result.model_dump())


def test_only_capacity_can_be_reclaimed() -> None:
    cumulative = account().reserve(Decimal(10)).settle(Decimal(10), Decimal(10))
    with pytest.raises(ValueError):
        cumulative.reclaim(Decimal(1))
    occupancy = (
        account(kind=BudgetKind.CAPACITY).reserve(Decimal(10)).settle(Decimal(10), Decimal(10))
    )
    assert occupancy.reclaim(Decimal(4)).consumed == 6


@pytest.mark.parametrize("amount", [1.2, True, "NaN", "Infinity", "-1"])
def test_amount_contract_rejects_ambiguous_or_invalid_values(amount: object) -> None:
    with pytest.raises(ValidationError):
        account().model_validate({**account().model_dump(), "limit": amount})
