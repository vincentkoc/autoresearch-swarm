from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, localcontext
from threading import Barrier
from typing import Any

import pytest
from pydantic import ValidationError

from research_swarm.domain.budgets import BudgetAccount, BudgetKind, BudgetLimit
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.funding import AllocationBalance, CampaignFunding
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database, Transaction
from research_swarm.state.funding import FundingLedger
from research_swarm.state.ledger import BudgetLedger
from tests.state.test_database import ContextFactory

CONTRACT = content_digest(b"synthetic-funding-contract")


class Tree:
    def __init__(self, database: Database, context: ContextFactory) -> None:
        self.database, self.context = database, context

    def command(
        self, operation: str, payload: dict[str, Any], callback: Any, key: str | None = None
    ) -> Any:
        return self.database.execute(self.context(key), operation, payload, "write", callback)[
            "result"
        ]

    def account(self, campaign: str) -> BudgetAccount:
        with self.database.connect() as connection:
            return BudgetLedger.account(
                Transaction(connection, self.context()), campaign, "worker_ms"
            )

    def record(self, kind: str, identifier: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            return Transaction(connection, self.context()).get(kind, identifier)

    def root(self, limit: int = 100) -> str:
        def create(transaction: Transaction) -> dict[str, str]:
            identifier = new_id()
            transaction.insert(
                "campaign",
                identifier,
                {"contract_digest": CONTRACT},
                state="running",
                campaign_id=identifier,
            )
            BudgetLedger.create(
                transaction,
                identifier,
                (BudgetLimit(dimension="worker_ms", limit=limit, unit="ms"),),
            )
            return {"id": identifier}

        return self.command("root", {"limit": limit}, create)["id"]

    def child(
        self,
        parent: str,
        limit: int,
        *,
        remaining_depth: int = 2,
        key: str | None = None,
        unit: str = "ms",
    ) -> str:
        def create(transaction: Transaction) -> dict[str, Any]:
            identifier = new_id()
            data = {"parent_campaign_id": parent, "contract_digest": CONTRACT}
            child = transaction.insert(
                "campaign", identifier, data, state="draft", campaign_id=identifier
            )
            limits = (BudgetLimit(dimension="worker_ms", limit=limit, unit=unit),)
            BudgetLedger.create(transaction, identifier, limits)
            try:
                previous = CampaignFunding.model_validate(
                    transaction.get("campaign_funding", parent)["data"]
                )
            except ResearchError as error:
                assert error.code == ErrorCode.NOT_FOUND
                previous = None
            funding = CampaignFunding(
                parent_campaign_id=parent,
                child_campaign_id=identifier,
                root_campaign_id=previous.root_campaign_id if previous else parent,
                parent_contract_digest=CONTRACT,
                depth=previous.depth + 1 if previous else 1,
                remaining_depth=remaining_depth,
                limits=limits,
                allocated_by=transaction.context.principal.id,
            )
            FundingLedger.open(transaction, funding)
            return transaction.update(
                "campaign",
                identifier,
                expected_revision=child["revision"],
                data=data,
                state="running",
            )

        return self.command(
            "child",
            {"parent": parent, "limit": limit, "depth": remaining_depth, "unit": unit},
            create,
            key,
        )["id"]

    def reserve(self, campaign: str, amount: int, *, key: str | None = None) -> str:
        return self.command(
            "reserve",
            {"campaign": campaign, "amount": amount},
            lambda transaction: BudgetLedger.reserve(
                transaction, campaign, new_id(), {"worker_ms": Decimal(amount)}
            ).model_dump(mode="json"),
            key,
        )["id"]

    def settle(
        self, reservation: str, actual: int, *, identity: str | None = None, key: str | None = None
    ) -> Any:
        identity = identity or new_id()
        return self.command(
            "settle",
            {"reservation": reservation, "actual": actual, "identity": identity},
            lambda transaction: BudgetLedger.settle(
                transaction, reservation, identity, {"worker_ms": Decimal(actual)}
            ),
            key,
        )

    def uncertain(self, reservation: str) -> Any:
        return self.command(
            "unknown",
            {"reservation": reservation},
            lambda transaction: BudgetLedger.mark_uncertain(transaction, reservation).model_dump(
                mode="json"
            ),
        )

    def close(self, campaign: str, *, key: str | None = None) -> Any:
        def finish(transaction: Transaction) -> Any:
            row = transaction.get("campaign", campaign)
            if row["state"] != "completed":
                FundingLedger.require_children_closed(transaction, campaign)
                transaction.update(
                    "campaign",
                    campaign,
                    expected_revision=row["revision"],
                    state="completed",
                    data={**row["data"], "final_report_id": new_id()},
                )
            return FundingLedger.close(transaction, campaign)

        return self.command("close", {"campaign": campaign}, finish, key)

    def evidence(self) -> tuple[Any, ...]:
        with self.database.connect() as connection:
            return tuple(
                (
                    table,
                    tuple(
                        tuple(row)
                        for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")
                    ),
                )
                for table in (
                    "records",
                    "budgets",
                    "budget_reservations",
                    "budget_settlements",
                    "budget_entries",
                    "events",
                    "outbox",
                )
            )


@pytest.fixture
def tree(database: Database, context: ContextFactory) -> Tree:
    return Tree(database, context)


def test_allocation_consumption_and_release_are_distinct_and_replay_once(tree: Tree) -> None:
    parent = tree.root()
    child = tree.child(parent, 60)
    assert (
        tree.account(parent).consumed,
        tree.account(parent).reserved,
        tree.account(parent).available,
    ) == (0, 60, 40)
    reservation = tree.reserve(child, 20)
    identity = new_id()
    tree.settle(reservation, 17, identity=identity)
    before = tree.account(parent)
    assert (before.consumed, before.reserved, before.available, before.released_total) == (
        17,
        43,
        40,
        0,
    )
    assert tree.account(child).released_total == 3
    assert tree.settle(reservation, 17, identity=identity)["settlement_id"] == identity
    assert tree.account(parent) == before
    receipt = tree.close(child, key="finish-child")
    final = tree.account(parent)
    assert (final.consumed, final.reserved, final.available, final.released_total) == (
        17,
        0,
        83,
        43,
    )
    assert receipt["data"]["balances"]["worker_ms"]["reserved"] == "43"
    evidence = tree.evidence()
    assert tree.close(child, key="finish-child") == receipt
    reopened = Tree(Database(tree.database.path), tree.context)
    assert reopened.close(child, key="another-notification") == receipt
    assert tree.evidence() == evidence
    assert tree.record("funding_projection", child)["state"] == "settled"
    with pytest.raises(ResearchError) as changed:
        tree.settle(reservation, 18, identity=identity)
    assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    with pytest.raises(ResearchError) as closed:
        tree.reserve(child, 1)
    assert closed.value.code == ErrorCode.INVALID_TRANSITION


def test_independent_child_creations_compete_for_one_allocation(tree: Tree) -> None:
    parent = tree.root()
    barrier = Barrier(2)

    def create(_: int) -> Any:
        separate = Tree(Database(tree.database.path), tree.context)
        barrier.wait(timeout=5)
        try:
            return separate.child(parent, 60)
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(create, range(2)))
    assert outcomes.count(ErrorCode.BUDGET_EXHAUSTED) == 1
    assert tree.account(parent).reserved == 60
    with tree.database.connect() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM records WHERE kind='campaign'").fetchone()[0]
            == 2
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='campaign_funding'"
            ).fetchone()[0]
            == 1
        )
        assert connection.execute("SELECT COUNT(*) FROM budgets").fetchone()[0] == 2


def test_overshoot_preserves_outstanding_commitments_and_blocks_sibling_admission(
    tree: Tree,
) -> None:
    parent = tree.root()
    first, second = tree.child(parent, 60), tree.child(parent, 40)
    original, pending = tree.reserve(first, 50), tree.reserve(first, 10)
    tree.settle(original, 65)
    account = tree.account(parent)
    assert (account.consumed, account.reserved, account.available) == (65, 50, -15)
    with pytest.raises(ResearchError) as deficit:
        tree.reserve(second, 1)
    assert deficit.value.code == ErrorCode.BUDGET_EXHAUSTED
    tree.uncertain(pending)
    account = tree.account(parent)
    assert (account.consumed, account.reserved, account.uncertain, account.available) == (
        65,
        40,
        10,
        -15,
    )
    tree.settle(pending, 5)
    account = tree.account(parent)
    assert (
        account.consumed,
        account.reserved,
        account.uncertain,
        account.available,
        account.released_total,
    ) == (70, 40, 0, -10, 5)
    tree.close(first)
    assert tree.account(parent) == account
    tree.close(second)
    assert (
        tree.account(parent).consumed,
        tree.account(parent).available,
        tree.account(parent).released_total,
    ) == (70, 30, 45)


def test_unknown_funding_cannot_be_released_until_original_settlement(tree: Tree) -> None:
    parent = tree.root()
    child = tree.child(parent, 60)
    reservation = tree.reserve(child, 30)
    tree.uncertain(reservation)
    account = tree.account(parent)
    assert (account.reserved, account.uncertain, account.available) == (30, 30, 40)
    before = tree.evidence()
    with pytest.raises(ResearchError) as unresolved:
        tree.close(child)
    assert unresolved.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert tree.evidence() == before
    tree.settle(reservation, 17)
    assert (
        tree.account(parent).consumed,
        tree.account(parent).reserved,
        tree.account(parent).uncertain,
    ) == (17, 43, 0)
    tree.close(child)
    assert tree.account(parent).available == 83


def test_nested_consumption_rolls_up_once_and_release_stays_with_direct_parent(tree: Tree) -> None:
    root = tree.root()
    child = tree.child(root, 80, remaining_depth=2)
    grandchild = tree.child(child, 50, remaining_depth=1)
    tree.settle(tree.reserve(grandchild, 20), 10)
    assert (tree.account(child).consumed, tree.account(child).reserved) == (10, 40)
    assert (tree.account(root).consumed, tree.account(root).reserved) == (10, 70)
    before = tree.account(root)
    tree.close(grandchild)
    assert tree.account(child).available == 70
    # Child retains its full funded allowance until its own explicit completion.
    assert tree.account(root).consumed == before.consumed
    assert tree.account(root).reserved == before.reserved
    assert tree.account(root).released_total == 0
    tree.close(child)
    assert (
        tree.account(root).consumed,
        tree.account(root).available,
        tree.account(root).released_total,
    ) == (10, 90, 70)


def test_zero_child_allocation_and_depth_still_require_explicit_closure(tree: Tree) -> None:
    root = tree.root()
    child = tree.child(root, 80, remaining_depth=1)
    grandchild = tree.child(child, 0, remaining_depth=0)
    with pytest.raises(ResearchError) as open_child:
        tree.close(child)
    assert open_child.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    with pytest.raises(ResearchError) as depth:
        tree.child(grandchild, 0, remaining_depth=0)
    assert depth.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
    tree.close(grandchild)
    tree.close(child)
    assert tree.account(root).available == 100


def test_contract_unit_mismatch_rolls_back_fresh_child_and_funding(tree: Tree) -> None:
    parent = tree.root()
    before = tree.evidence()
    with pytest.raises(ResearchError) as different:
        tree.child(parent, 60, unit="seconds")
    assert different.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
    assert tree.evidence() == before


def test_failure_during_ancestor_projection_rolls_back_the_entire_settlement(
    tree: Tree, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tree.root()
    child = tree.child(root, 80, remaining_depth=1)
    grandchild = tree.child(child, 50, remaining_depth=0)
    reservation = tree.reserve(grandchild, 20)
    before = tree.evidence()
    original = BudgetLedger._entry

    def fail(transaction: Transaction, entry: Any) -> None:
        if entry.campaign_id == root:
            raise RuntimeError("Synthetic ancestor ledger failure")
        original(transaction, entry)

    with monkeypatch.context() as patch:
        patch.setattr(BudgetLedger, "_entry", staticmethod(fail))
        with pytest.raises(RuntimeError, match="ancestor ledger"):
            tree.settle(reservation, 17)
    assert tree.evidence() == before
    tree.settle(reservation, 17)
    assert tree.account(root).consumed == tree.account(child).consumed == 17


def test_funding_rejects_capacity_and_precision_does_not_follow_decimal_context() -> None:
    identifier, parent = new_id(), new_id()
    with pytest.raises(ValidationError):
        CampaignFunding(
            parent_campaign_id=parent,
            child_campaign_id=identifier,
            root_campaign_id=parent,
            parent_contract_digest=CONTRACT,
            depth=1,
            remaining_depth=0,
            limits=(
                BudgetLimit(dimension="storage", limit=10, unit="bytes", kind=BudgetKind.CAPACITY),
            ),
            allocated_by=new_id(),
        )
    with localcontext() as context:
        context.prec = 3
        account = BudgetAccount(
            campaign_id=identifier,
            dimension="worker_ms",
            kind=BudgetKind.CUMULATIVE,
            unit="ms",
            limit="9999999999999999999.999999999",
            consumed="9999999999999999999.999999997",
            reserved="0.000000001",
        )
        projection = AllocationBalance.live(account.limit, account)
        assert projection.reserved == Decimal("0.000000002")
        assert projection.total == Decimal("9999999999999999999.999999999")


def test_deep_ancestor_deficit_blocks_leaf_even_when_immediate_parent_has_slack(tree: Tree) -> None:
    root = tree.root()
    middle = tree.child(root, 80, remaining_depth=1)
    leaf = tree.child(middle, 50, remaining_depth=0)
    sibling = tree.child(root, 20)
    tree.settle(tree.reserve(sibling, 20), 35)
    assert tree.account(root).available == -15
    assert tree.account(middle).available == 30
    assert tree.account(leaf).available == 50
    before = tree.evidence()
    with pytest.raises(ResearchError) as refused:
        tree.reserve(leaf, 1)
    assert refused.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert tree.evidence() == before


def test_direct_account_mutation_cannot_resurrect_settled_funding(tree: Tree) -> None:
    root = tree.root()
    child = tree.child(root, 60)
    tree.close(child)
    before = tree.evidence()
    account = tree.account(child).reserve(Decimal(1))
    with pytest.raises(ResearchError) as refused:
        tree.command(
            "invalid-late-save", {}, lambda transaction: BudgetLedger._save(transaction, account)
        )
    assert refused.value.code == ErrorCode.INVALID_TRANSITION
    assert tree.evidence() == before


def test_eight_level_lineage_propagates_once_and_cannot_grow_a_ninth_edge(tree: Tree) -> None:
    root = tree.root()
    chain = [root]
    for remaining in range(7, -1, -1):
        chain.append(tree.child(chain[-1], 80, remaining_depth=remaining))
    before = tree.evidence()
    with pytest.raises(ValidationError):
        tree.child(chain[-1], 10, remaining_depth=0)
    assert tree.evidence() == before
    tree.settle(tree.reserve(chain[-1], 10), 7)
    assert all(tree.account(identifier).consumed == 7 for identifier in chain)
    for child in reversed(chain[1:]):
        tree.close(child)
    assert tree.account(root).available == 93
