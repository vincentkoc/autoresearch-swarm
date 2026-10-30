from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from threading import Barrier

import pytest

from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database, Transaction
from research_swarm.state.ledger import BudgetLedger

ContextFactory = Callable[..., CommandContext]


def create_campaign(database: Database, context: CommandContext, limit: int = 10) -> dict:
    identifier = new_id()

    def handler(transaction: Transaction) -> dict:
        record = transaction.insert(
            "campaign",
            identifier,
            {"objective": "synthetic"},
            state="running",
            campaign_id=identifier,
        )
        BudgetLedger.create(
            transaction,
            identifier,
            (BudgetLimit(dimension="worker_ms", limit=limit, unit="ms"),),
        )
        transaction.event("campaign.created", record, {})
        return record

    return database.execute(context, "create", {"objective": "synthetic"}, "write", handler)


def test_duplicate_command_returns_original_ids(
    database: Database, context: ContextFactory
) -> None:
    first = create_campaign(database, context("same"))
    second = create_campaign(database, context("same"))
    assert first == second
    assert len(database.events(context())) == 1
    assert len(database.pending_notifications()) == 1


def test_changed_payload_conflicts(database: Database, context: ContextFactory) -> None:
    create_campaign(database, context("same"))
    with pytest.raises(ResearchError) as caught:
        database.execute(context("same"), "create", {"objective": "changed"}, "write", lambda t: {})
    assert caught.value.code == ErrorCode.IDEMPOTENCY_CONFLICT


def test_failure_rolls_back_effects_but_preserves_rejection_identity(
    database: Database, context: ContextFactory
) -> None:
    calls = []

    def handler(transaction: Transaction) -> dict:
        calls.append(1)
        record = transaction.insert("campaign", new_id(), {}, state="running")
        transaction.event("campaign.created", record, {})
        raise ResearchError(ErrorCode.BUDGET_EXHAUSTED, "synthetic refusal")

    for _ in range(2):
        with pytest.raises(ResearchError):
            database.execute(context("rejected"), "create", {}, "write", handler)
    assert len(calls) == 1
    assert database.snapshot(context())["records"] == []
    assert database.events(context()) == []
    assert database.pending_notifications() == []


def test_restarted_controller_has_pending_outbox(
    database: Database, context: ContextFactory
) -> None:
    create_campaign(database, context())
    restarted = Database(database.path)
    pending = restarted.pending_notifications()
    assert len(pending) == 1
    restarted.acknowledge_notification(pending[0].id)
    assert database.pending_notifications() == []
    assert len(database.events(context())) == 1


def test_independent_connections_cannot_oversubscribe_budget(
    database: Database, context: ContextFactory
) -> None:
    campaign_id = create_campaign(database, context(), limit=10)["result"]["id"]
    barrier = Barrier(2)

    def reserve() -> str:
        separate = Database(database.path)
        request = context()
        owner_id = new_id()
        barrier.wait()

        def handler(transaction: Transaction) -> dict:
            reservation = BudgetLedger.reserve(
                transaction, campaign_id, owner_id, {"worker_ms": Decimal(8)}
            )
            return reservation.model_dump(mode="json")

        try:
            separate.execute(request, "reserve", {"owner": owner_id}, "write", handler)
            return "reserved"
        except ResearchError as error:
            return error.code.value

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: reserve(), range(2)))
    assert sorted(outcomes) == ["BUDGET_EXHAUSTED", "reserved"]


def test_duplicate_settlement_and_uncertainty(database: Database, context: ContextFactory) -> None:
    campaign_id = create_campaign(database, context(), limit=10)["result"]["id"]
    owner_id = new_id()

    def reserve(transaction: Transaction) -> dict:
        return BudgetLedger.reserve(
            transaction, campaign_id, owner_id, {"worker_ms": Decimal(8)}
        ).model_dump(mode="json")

    reservation_id = database.execute(context(), "reserve", {}, "write", reserve)["result"]["id"]
    database.execute(
        context(),
        "uncertain",
        {},
        "write",
        lambda t: BudgetLedger.mark_uncertain(t, reservation_id).model_dump(mode="json"),
    )
    settlement_id = new_id()

    def settle(transaction: Transaction) -> dict:
        return BudgetLedger.settle(
            transaction, reservation_id, settlement_id, {"worker_ms": Decimal(3)}
        )

    first = database.execute(context(), "settle", {}, "write", settle)["result"]
    second = database.execute(context(), "settle", {}, "write", settle)["result"]
    assert first == second

    result = database.execute(
        context(),
        "balance",
        {},
        "read",
        lambda t: BudgetLedger.account(t, campaign_id, "worker_ms").model_dump(mode="json"),
    )["result"]
    assert result["consumed"] == "3"
    assert result["reserved"] == result["uncertain"] == "0"
    assert result["released_total"] == "5"


def test_scope_checked_before_cached_response(
    database: Database, context: ContextFactory, principal: Principal
) -> None:
    create_campaign(database, context("same"))
    reader = principal.model_copy(update={"scopes": frozenset({"read"})})
    with pytest.raises(ResearchError) as caught:
        create_campaign(database, CommandContext(principal=reader, idempotency_key="same"))
    assert caught.value.code == ErrorCode.AUTHORIZATION_DENIED


def test_snapshot_and_watermark_are_consistent(database: Database, context: ContextFactory) -> None:
    def writer() -> None:
        separate = Database(database.path)
        for _ in range(20):
            create_campaign(separate, context())

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(writer)
        for _ in range(20):
            snapshot = database.snapshot(context())
            assert len(snapshot["records"]) == snapshot["watermark"]
        future.result()
    assert database.snapshot(context())["watermark"] == 20


def test_consistent_backup(database: Database, context: ContextFactory, tmp_path: Path) -> None:
    create_campaign(database, context())
    destination = tmp_path / "backup.sqlite"
    database.backup(destination)
    restored = Database(destination)
    assert restored.snapshot(context()) == database.snapshot(context())
