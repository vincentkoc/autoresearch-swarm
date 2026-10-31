"""Snapshot profile bounds refuse partial projections before SQL can overfetch."""

from dataclasses import replace
from decimal import Decimal

import pytest

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.event_recovery import CreateRecoverySnapshot, RecoverySnapshot
from research_swarm.domain.types import new_id
from research_swarm.recovery import SnapshotReader
from research_swarm.state.event_recovery import EventRecovery, SnapshotLimits
from research_swarm.state.ledger import BudgetLedger
from research_swarm.state.sqlite import SQLiteSession
from tests.test_database import create_campaign
from tests.test_event_recovery import checkpoint, client, context, release
from tests.test_event_recovery import runtime as runtime


def create(runtime, limits, **overrides):
    service = EventRecovery(
        runtime.database, runtime.service.event_recovery.public_kinds, limits=limits
    )
    intent = CreateRecoverySnapshot(snapshot_id=new_id(), **overrides)
    return RecoverySnapshot.model_validate(service.create(context(runtime), intent)["result"])


def projection(runtime, snapshot):
    reader = SnapshotReader(client(runtime), snapshot)
    items = list(reader.items(page_size=1))
    assert reader.completed
    return items


def retained(runtime):
    return runtime.database.store.read(
        lambda sql: (
            sql.execute("SELECT COUNT(*) FROM recovery_items").one()[0],
            sql.execute(
                "SELECT COUNT(*) FROM recovery_snapshots "
                "WHERE released_at IS NULL AND expired_at IS NULL"
            ).one()[0],
            sql.execute("SELECT COUNT(*) FROM events").one()[0],
        )
    )


@pytest.fixture
def populated(runtime):
    campaign = create_campaign(runtime.database, context(runtime))["result"]
    create_campaign(runtime.database, context(runtime))

    def settle(transaction):
        reservation = BudgetLedger.reserve(
            transaction, campaign["id"], new_id(), {"worker_ms": Decimal(4)}
        )
        BudgetLedger.settle(transaction, reservation.id, new_id(), {"worker_ms": Decimal(3)})
        return {"reservation_id": reservation.id}

    runtime.database.execute(context(runtime), "fixture.settle", {}, "write", settle)
    baseline = checkpoint(runtime)
    items = projection(runtime, baseline)
    release(runtime, baseline.id)
    exact = SnapshotLimits(
        max_items=baseline.item_count,
        max_bytes=baseline.size_bytes,
        max_retained_bytes=baseline.size_bytes,
    )
    return runtime, baseline, items, exact


def test_exact_profile_preserves_snapshot_contents_scope_and_watermark(populated):
    runtime, baseline, items, exact = populated
    snapshot = create(runtime, exact)
    assert projection(runtime, snapshot) == items
    assert snapshot.scope_digest == baseline.scope_digest
    assert snapshot.watermark == baseline.watermark
    assert (snapshot.item_count, snapshot.size_bytes) == (baseline.item_count, baseline.size_bytes)


@pytest.mark.parametrize("field", ["max_items", "max_bytes", "max_retained_bytes"])
def test_overflow_rolls_back_all_items_and_retention_pins(populated, field):
    runtime, _, _, exact = populated
    before = retained(runtime)
    with pytest.raises(ResearchError) as refused:
        create(runtime, replace(exact, **{field: getattr(exact, field) - 1}))
    assert refused.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert retained(runtime) == before


def test_profile_cannot_expand_request_bounds_and_retained_bytes_include_other_snapshots(populated):
    runtime, _, _, exact = populated
    with pytest.raises(ResearchError) as refused:
        create(runtime, exact, max_items=exact.max_items - 1)
    assert refused.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    first = create(runtime, exact)
    before = retained(runtime)
    with pytest.raises(ResearchError) as retained_limit:
        create(runtime, exact)
    assert retained_limit.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert retained(runtime) == before
    release(runtime, first.id)
    assert create(runtime, exact).item_count == first.item_count


def test_sql_scans_bound_materialization_before_returning_a_cursor(populated, monkeypatch):
    runtime, _, _, exact = populated
    original = SQLiteSession.execute
    observed = []

    def execute(self, statement, parameters=()):
        if statement.startswith("SELECT kind,id FROM records") or statement.startswith(
            "SELECT payload FROM budget"
        ):
            assert statement.endswith(" LIMIT ?")
            assert 1 <= parameters[-1] <= exact.max_items + 1
            observed.append(parameters[-1])
        return original(self, statement, parameters)

    monkeypatch.setattr(SQLiteSession, "execute", execute)
    snapshot = create(runtime, exact)
    assert snapshot.item_count == exact.max_items
    assert len(observed) == 4 and observed[0] == exact.max_items + 1
    assert observed == sorted(observed, reverse=True)


@pytest.mark.parametrize(
    "values",
    [
        {"max_items": True},
        {"max_items": 100_001},
        {"max_bytes": 0},
        {"max_bytes": 256 * 1024 * 1024 + 1},
        {"max_retained_bytes": 1024**3 + 1},
        {"max_retained_bytes": -1},
    ],
)
def test_snapshot_profiles_only_reduce_native_ceilings(values):
    with pytest.raises(ValueError):
        SnapshotLimits(**values)
