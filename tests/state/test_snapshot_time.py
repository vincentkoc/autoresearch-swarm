"""Recovery lifetimes use one witnessed instant and cannot cross source epochs."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from research_swarm.control.time_authority import ClockAuthorityScope
from research_swarm.control.witness_commands import WitnessExecutionBoundary
from research_swarm.control.witness_time import WitnessAuthorityTimeProvider
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.event_recovery import (
    AdvanceEventFloor,
    CollectExpiredSnapshots,
    CreateRecoverySnapshot,
    RecoveryItem,
    RecoveryPage,
    RecoverySnapshot,
    RecoverySnapshotTarget,
)
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database
from research_swarm.state.event_recovery import EventRecovery, SnapshotInstant, SnapshotLimits
from tests.state.test_database import create_campaign
from tests.workers.test_time_providers import registry as registry
from tests.workers.test_time_witnesses import identifier, payload
from tests.workers.test_witness_time import response


@pytest.fixture
def recovery(registry):
    registry.qualify()
    provider = WitnessAuthorityTimeProvider(registry.database.authority_identity)
    boundary = WitnessExecutionBoundary(registry.providers, provider)
    guarded = Database.from_store(registry.database.store, execution_boundary=boundary)
    authority = registry.authority
    scope = ClockAuthorityScope(
        authority.workspace_id,
        authority.controller_id,
        authority.control_epoch,
        authority.assignment_key_id,
    )

    def clock(transaction):
        assert transaction.workspace_id == scope.workspace_id
        observed = provider.observe(scope)
        return SnapshotInstant(
            timestamp=observed.datetime.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            epoch=observed.epoch,
        )

    service = EventRecovery(guarded, ("campaign",), clock=clock)
    create_campaign(registry.database, registry.context())
    return SimpleNamespace(
        registry=registry,
        provider=provider,
        boundary=boundary,
        database=guarded,
        service=service,
        epoch=identifier(7),
        delta=0,
    )


def invoke(e, name, intent, method, *, context=None, **changes):
    context = context or e.registry.context()
    with e.boundary.prepare(
        context, e.registry.current(), name, intent.model_dump(mode="json")
    ) as turn:
        values = {
            "source_epoch": e.epoch,
            "authority_time_ms": payload(turn.request).authority_time_ms + e.delta,
            **changes,
        }
        turn.complete(response(turn, **values))
        return method(context, intent)


def snapshot(e, **overrides):
    request = CreateRecoverySnapshot(snapshot_id=new_id(), lifetime_seconds=30, **overrides)
    result = invoke(e, "stream.create_snapshot", request, e.service.create)
    return RecoverySnapshot.model_validate(result["result"])


def counts(e):
    return e.database.store.read(
        lambda sql: {
            table: sql.execute(f"SELECT COUNT(*) FROM {table}").one()[0]
            for table in ("commands", "events", "recovery_snapshots", "recovery_items")
        }
    )


def test_each_snapshot_read_uses_its_own_witness_without_command_acknowledgments(recovery):
    e = recovery
    saved = snapshot(e)
    before = counts(e)
    page = RecoveryPage(snapshot_id=saved.id)
    with pytest.raises(ResearchError) as missing:
        e.service.page(e.registry.context(), page)
    assert missing.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    result = invoke(e, "snapshot.page", page, e.service.page)
    assert result["snapshot"] == saved.model_dump(mode="json")
    assert len(result["items"]) == saved.item_count
    item = RecoveryItem(snapshot_id=saved.id, ordinal=1)
    result = invoke(e, "snapshot.item", item, e.service.item)
    assert result["next_offset"] == result["size_bytes"] > 0
    assert counts(e) == before
    # The native UTC reader must not silently reinterpret a witnessed deadline.
    native = EventRecovery(e.registry.database, ("campaign",))
    with pytest.raises(ResearchError) as wrong_domain:
        native.page(e.registry.context(), page)
    assert wrong_domain.value.code == ErrorCode.SNAPSHOT_EXPIRED


def test_source_change_fences_before_refusal_and_new_epoch_cannot_resurrect_snapshot(recovery):
    e = recovery
    saved = snapshot(e)
    page = RecoveryPage(snapshot_id=saved.id)
    before = counts(e)
    with pytest.raises(ResearchError) as changed:
        invoke(e, "snapshot.page", page, e.service.page, source_epoch=identifier(8))
    assert changed.value.code == ErrorCode.STALE_FENCE
    assert e.registry.current().state == "unqualified"
    assert counts(e)["events"] == before["events"] + 1
    assert counts(e)["commands"] == before["commands"]
    e.registry.qualify(epoch=identifier(8))
    e.epoch, e.delta = identifier(8), -5000
    with pytest.raises(ResearchError) as retired:
        invoke(e, "snapshot.page", page, e.service.page)
    assert retired.value.code == ErrorCode.SNAPSHOT_EXPIRED
    fresh = snapshot(e)
    assert fresh.created_at < saved.created_at
    assert invoke(e, "snapshot.page", RecoveryPage(snapshot_id=fresh.id), e.service.page)[
        "snapshot"
    ] == fresh.model_dump(mode="json")
    row = e.database.store.read(
        lambda sql: sql.execute(
            "SELECT expired_at FROM recovery_snapshots WHERE id=?", (saved.id,)
        ).one()
    )
    assert row[0] == fresh.created_at  # Create collected the old epoch without UTC fallback.


def test_expiry_collection_floor_pins_and_replay_share_the_same_clock(recovery):
    e = recovery
    request = CreateRecoverySnapshot(snapshot_id=new_id(), lifetime_seconds=30)
    context = e.registry.context()
    result = invoke(e, "stream.create_snapshot", request, e.service.create, context=context)
    saved = RecoverySnapshot.model_validate(result["result"])
    create_campaign(e.registry.database, e.registry.context())
    floor = AdvanceEventFloor(event_floor=saved.watermark + 2, previous_floor=1)
    e.delta = 29_999
    with pytest.raises(ResearchError) as pinned:
        invoke(e, "stream.advance_floor", floor, e.service.advance_floor)
    assert pinned.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    e.delta = 30_000
    with pytest.raises(ResearchError) as expired:
        invoke(e, "snapshot.item", RecoveryItem(snapshot_id=saved.id, ordinal=1), e.service.item)
    assert expired.value.code == ErrorCode.SNAPSHOT_EXPIRED
    invoke(e, "stream.advance_floor", floor, e.service.advance_floor)
    assert counts(e)["recovery_items"] == 0
    # Committed metadata is replayable without an active witness; TTL is never renewed.
    assert e.service.create(context, request) == result
    e.delta = 0
    with pytest.raises(ResearchError) as no_resurrection:
        invoke(e, "snapshot.page", RecoveryPage(snapshot_id=saved.id), e.service.page)
    assert no_resurrection.value.code == ErrorCode.SNAPSHOT_EXPIRED


def test_failed_capture_rolls_back_clock_binding_and_release_tombstone_survives(recovery):
    e = recovery
    e.service.limits = replace(SnapshotLimits(), max_bytes=1)
    request = CreateRecoverySnapshot(snapshot_id=new_id())
    before = counts(e)
    with pytest.raises(ResearchError) as overflow:
        invoke(e, "stream.create_snapshot", request, e.service.create)
    assert overflow.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert counts(e)["recovery_snapshots"] == before["recovery_snapshots"]
    assert (
        e.database.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='recovery_snapshot_clock'"
            ).one()[0]
        )
        == 0
    )
    e.service.limits = SnapshotLimits()
    saved = snapshot(e)
    target = RecoverySnapshotTarget(snapshot_id=saved.id)
    context = e.registry.context()
    released = invoke(e, "stream.release_snapshot", target, e.service.release, context=context)
    assert e.service.release(context, target) == released
    assert counts(e)["recovery_items"] == 0
    assert (
        invoke(e, "stream.collect_expired", CollectExpiredSnapshots(), e.service.collect)["result"][
            "expired_snapshot_ids"
        ]
        == []
    )
    with pytest.raises(ResearchError) as refused:
        invoke(e, "snapshot.page", RecoveryPage(snapshot_id=saved.id), e.service.page)
    assert refused.value.code == ErrorCode.SNAPSHOT_EXPIRED


def test_injected_clock_must_bind_an_epoch_and_legacy_views_are_not_reinterpreted(recovery):
    e = recovery
    native = EventRecovery(e.registry.database, ("campaign",))
    request = CreateRecoverySnapshot(snapshot_id=new_id())
    legacy = RecoverySnapshot.model_validate(native.create(e.registry.context(), request)["result"])
    with pytest.raises(ResearchError) as legacy_clock:
        invoke(e, "snapshot.page", RecoveryPage(snapshot_id=legacy.id), e.service.page)
    assert legacy_clock.value.code == ErrorCode.SNAPSHOT_EXPIRED
    e.service.clock = lambda tx: SnapshotInstant(timestamp=legacy.created_at)
    with pytest.raises(ResearchError) as unbound:
        snapshot(e)
    assert unbound.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
