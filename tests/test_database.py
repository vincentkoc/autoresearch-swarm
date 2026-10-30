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


def test_legacy_acknowledgment_replay_preserves_command_identity(
    database: Database, context: ContextFactory
) -> None:
    first = create_campaign(database, context("legacy"))
    with database.connect() as connection:
        connection.execute(
            "UPDATE commands SET response=json_remove(response,'$.disposition') "
            "WHERE idempotency_key='legacy'"
        )
    assert create_campaign(database, context("legacy")) == first
    assert first["disposition"] == "accepted"
    assert len(database.events(context())) == 1


def test_replay_precedes_transient_preflight_without_recording_a_probe(
    database: Database, context: ContextFactory, tmp_path: Path
) -> None:
    source = tmp_path / "preflight-source"
    source.write_text("available")
    checked = []

    def operation(key: str) -> dict:
        request = context(key)
        replay = database.replay(request, "preflight-command", {}, "write")
        if replay is not None:
            return replay
        checked.append(key)
        source.read_text()
        return database.execute(
            request, "preflight-command", {}, "write", lambda t: {"id": new_id()}
        )

    assert database.replay(context("first"), "preflight-command", {}, "write") is None
    first = operation("first")
    source.unlink()
    assert operation("first") == first
    with pytest.raises(FileNotFoundError):
        operation("retryable-preflight")
    source.write_text("restored")
    assert operation("retryable-preflight")["result"]["id"] != first["result"]["id"]
    assert checked == ["first", "retryable-preflight", "retryable-preflight"]
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM commands").fetchone()[0] == 2


def test_preflight_race_rechecks_idempotency_in_the_committing_transaction(
    database: Database, context: ContextFactory
) -> None:
    barrier = Barrier(2)
    effects = []

    def submit(_: int) -> dict:
        separate = Database(database.path)
        request = context("race")
        assert separate.replay(request, "submit", {}, "write") is None
        barrier.wait(timeout=5)

        def commit(transaction: Transaction) -> dict:
            identifier = new_id()
            effects.append(identifier)
            return transaction.insert("trial", identifier, {}, state="queued")

        return separate.execute(request, "submit", {}, "write", commit)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(submit, range(2)))
    assert results[0] == results[1]
    assert effects == [results[0]["result"]["id"]]


def test_replay_uses_the_same_authority_revision_and_payload_rules(
    database: Database, context: ContextFactory, principal: Principal
) -> None:
    original = create_campaign(database, context("same", revision=1))
    assert (
        Database(database.path).replay(
            context("same", revision=1), "create", {"objective": "synthetic"}, "write"
        )
        == original
    )
    with pytest.raises(ResearchError) as conflict:
        database.replay(context("same", revision=2), "create", {"objective": "synthetic"}, "write")
    assert conflict.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    reader = principal.model_copy(update={"scopes": frozenset({"read"})})
    with pytest.raises(ResearchError) as denied:
        database.replay(
            CommandContext(principal=reader, idempotency_key="same", expected_revision=1),
            "create",
            {"objective": "synthetic"},
            "write",
        )
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED


def test_replay_preserves_committed_rejections(database: Database, context: ContextFactory) -> None:
    def refused(transaction: Transaction) -> dict:
        raise ResearchError(ErrorCode.BUDGET_EXHAUSTED, "No remaining allowance")

    with pytest.raises(ResearchError, match="No remaining allowance"):
        database.execute(context("reject"), "claim", {}, "write", refused)
    with pytest.raises(ResearchError, match="No remaining allowance"):
        database.replay(context("reject"), "claim", {}, "write")


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
