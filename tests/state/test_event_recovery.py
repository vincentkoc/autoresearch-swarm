from __future__ import annotations

import base64
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from threading import Barrier, Event
from typing import Any

import httpx
import pytest
from mcp import Client

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
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
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.recovery import SnapshotReader
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import SCHEMA, Database, Transaction
from research_swarm.state.ledger import BudgetLedger
from tests.api.test_cli_mcp import cli, stdio
from tests.orchestration.test_http_delivery import serving
from tests.state.test_database import create_campaign


@pytest.fixture
def runtime(tmp_path: Path) -> LocalRuntime:
    return LocalRuntime.open(tmp_path / "event-runtime")


def context(runtime: LocalRuntime, key: str | None = None) -> CommandContext:
    return CommandContext(principal=runtime.principal(), idempotency_key=key or new_id())


def client(runtime: LocalRuntime) -> ResearchClient:
    return ResearchClient(LocalTransport(runtime))


def checkpoint(runtime: LocalRuntime, **overrides: Any) -> RecoverySnapshot:
    intent = CreateRecoverySnapshot(snapshot_id=new_id(), **overrides)
    return RecoverySnapshot.model_validate(
        client(runtime).command(
            "stream.create_snapshot",
            intent,
            idempotency_key=new_id(),
        )["result"]
    )


def advance(runtime: LocalRuntime, floor: int, previous: int = 1) -> dict[str, Any]:
    return client(runtime).command(
        "stream.advance_floor",
        AdvanceEventFloor(event_floor=floor, previous_floor=previous),
        idempotency_key=new_id(),
    )["result"]


def release(runtime: LocalRuntime, identifier: str) -> dict[str, Any]:
    return client(runtime).command(
        "stream.release_snapshot",
        RecoverySnapshotTarget(snapshot_id=identifier),
        idempotency_key=new_id(),
    )["result"]


def legacy(path: Path) -> str:
    workspace = new_id()
    with sqlite3.connect(path) as connection:
        connection.executescript(SCHEMA)
        connection.execute("PRAGMA user_version=1")
        connection.execute("INSERT INTO workspaces VALUES (?,?,1,29)", (workspace, new_id()))
        connection.execute(
            "INSERT INTO commands VALUES (?,?,?,?,?,?,?)",
            (
                workspace,
                new_id(),
                "preserved-key",
                "preserved-digest",
                new_id(),
                '{"result":{"historical":true}}',
                "2026-01-01T00:00:00.000Z",
            ),
        )
    return workspace


def test_schema_v1_migrates_once_under_concurrent_openers_and_preserves_commands(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy.sqlite"
    workspace = legacy(path)
    barrier = Barrier(2)

    def open_database() -> Database:
        barrier.wait(timeout=5)
        return Database(path)

    with ThreadPoolExecutor(max_workers=2) as pool:
        databases = list(pool.map(lambda _: open_database(), range(2)))
    with databases[0].connect() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        row = connection.execute("SELECT * FROM workspaces WHERE id=?", (workspace,)).fetchone()
        assert row["next_sequence"] == 29 and row["event_floor"] == 1
        assert (
            connection.execute("SELECT response FROM commands").fetchone()[0]
            == '{"result":{"historical":true}}'
        )


def test_failed_schema_migration_rolls_back_and_newer_schema_is_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from research_swarm.state import schema as migrations

    path = tmp_path / "rollback.sqlite"
    legacy(path)
    original = migrations.migrate_v2

    def fail(connection: sqlite3.Connection) -> None:
        original(connection)
        raise RuntimeError("Fixture interrupted schema upgrade")

    with monkeypatch.context() as patch:
        patch.setattr(migrations, "migrate_v2", fail)
        with pytest.raises(RuntimeError, match="interrupted schema"):
            Database(path)
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert "event_floor" not in {
            row[1] for row in connection.execute("PRAGMA table_info(workspaces)")
        }
        assert not connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name='recovery_items'"
        ).fetchall()
        connection.execute("PRAGMA user_version=99")
    with pytest.raises(ResearchError) as future:
        Database(path)
    assert future.value.code == ErrorCode.SCHEMA_UNSUPPORTED
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 99


def test_retention_boundary_restart_and_empty_stream_preserve_permanent_outbox(
    runtime: LocalRuntime,
) -> None:
    for _ in range(3):
        create_campaign(runtime.database, context(runtime))
    assert advance(runtime, 4) == {"event_floor": 4, "watermark": 3}
    reopened = LocalRuntime.open(runtime.root)
    with pytest.raises(ResearchError) as gap:
        client(reopened).events(after=2)
    assert gap.value.code == ErrorCode.EVENT_CURSOR_EXPIRED
    assert gap.value.details["recovery_command"] == "stream.create_snapshot"
    assert client(reopened).events(after=3) == {
        "events": [],
        "cursor": 3,
        "event_floor": 4,
        "watermark": 3,
    }
    assert len(reopened.database.pending_notifications()) == 3
    create_campaign(reopened.database, context(reopened))
    assert client(reopened).events(after=3)["events"][0]["sequence"] == 4
    with pytest.raises(ResearchError) as future:
        client(reopened).events(after=5)
    assert future.value.code == ErrorCode.PROJECT_CONTRACT_INVALID


def test_frozen_pages_and_budget_match_watermark_despite_independent_writer(
    runtime: LocalRuntime,
) -> None:
    campaign = create_campaign(runtime.database, context(runtime))["result"]
    snapshot = checkpoint(runtime)
    first = client(runtime).snapshot_page(RecoveryPage(snapshot_id=snapshot.id, limit=1))
    other = LocalRuntime.open(runtime.root)

    def change(transaction: Transaction) -> dict[str, Any]:
        row = transaction.update(
            "campaign",
            campaign["id"],
            expected_revision=1,
            state="paused",
            data={"objective": "Changed after the recovery checkpoint"},
        )
        reservation = BudgetLedger.reserve(
            transaction, campaign["id"], new_id(), {"worker_ms": Decimal(4)}
        )
        BudgetLedger.settle(transaction, reservation.id, new_id(), {"worker_ms": Decimal(3)})
        transaction.event("campaign.changed", row, {})
        return row

    other.database.execute(context(other), "fixture.change", {}, "write", change)
    reader = SnapshotReader(client(other), snapshot)
    items = list(reader.items(page_size=1))
    assert reader.completed
    frozen = next(item["value"] for item in items if item["type"] == "record")
    assert frozen == campaign and frozen["state"] == "running"
    account = next(item["value"] for item in items if item["type"] == "budget_account")
    assert account["consumed"] == account["reserved"] == "0"
    assert client(other).snapshot_page(RecoveryPage(snapshot_id=snapshot.id, limit=1)) == first
    events = client(other).events(after=snapshot.watermark)["events"]
    assert [event["type"] for event in events] == ["campaign.changed"]
    assert len({event["id"] for event in events}) == 1
    with pytest.raises(ResearchError) as pinned:
        advance(other, events[0]["sequence"] + 1)
    assert pinned.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    receipt = release(other, snapshot.id)
    assert release(LocalRuntime.open(runtime.root), snapshot.id) == receipt
    advance(other, events[0]["sequence"] + 1)
    with pytest.raises(ResearchError) as released:
        client(other).snapshot_page(RecoveryPage(snapshot_id=snapshot.id))
    assert released.value.code == ErrorCode.SNAPSHOT_EXPIRED


def test_writer_blocked_during_capture_resumes_strictly_after_snapshot_watermark(
    runtime: LocalRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_campaign(runtime.database, context(runtime))
    other = LocalRuntime.open(runtime.root)
    reading, writing = Event(), Event()
    original = Transaction.get

    def observe_read(self: Transaction, kind: str, identifier: str) -> dict[str, Any]:
        if kind == "campaign":
            reading.set()
            assert writing.wait(5)
        return original(self, kind, identifier)

    def writer() -> None:
        assert reading.wait(5)
        writing.set()
        create_campaign(other.database, context(other))

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(writer)
        with monkeypatch.context() as patch:
            patch.setattr(Transaction, "get", observe_read)
            snapshot = checkpoint(runtime)
        future.result(timeout=5)
    assert snapshot.watermark == 1
    assert (
        len(
            [
                item
                for item in SnapshotReader(client(runtime), snapshot).items()
                if item["type"] == "record"
            ]
        )
        == 1
    )
    assert [event["sequence"] for event in client(runtime).events(after=1)["events"]] == [2]
    with pytest.raises(ResearchError):
        advance(other, 3)


def test_expiry_drops_only_retention_pin_and_requires_new_snapshot(
    runtime: LocalRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_campaign(runtime.database, context(runtime))
    snapshot = checkpoint(runtime, lifetime_seconds=30)
    create_campaign(runtime.database, context(runtime))
    with monkeypatch.context() as patch:
        patch.setattr("research_swarm.state.event_recovery.utc_now", lambda: snapshot.expires_at)
        advance(runtime, 3)
        with pytest.raises(ResearchError) as expired:
            client(runtime).snapshot_page(RecoveryPage(snapshot_id=snapshot.id))
        assert expired.value.code == ErrorCode.SNAPSHOT_EXPIRED
    with pytest.raises(ResearchError) as rolled_back_clock:
        client(runtime).snapshot_page(RecoveryPage(snapshot_id=snapshot.id))
    assert rolled_back_clock.value.code == ErrorCode.SNAPSHOT_EXPIRED
    with runtime.database.connect() as connection:
        assert (
            connection.execute("SELECT expired_at FROM recovery_snapshots").fetchone()[0]
            == snapshot.expires_at
        )
        assert connection.execute("SELECT COUNT(*) FROM recovery_items").fetchone()[0] == 0
    release(runtime, snapshot.id)
    fresh = checkpoint(runtime)
    assert fresh.watermark == 2
    assert len(list(SnapshotReader(client(runtime), fresh).items())) == fresh.item_count


def test_snapshot_bounds_rollback_and_private_records_are_excluded(runtime: LocalRuntime) -> None:
    create_campaign(runtime.database, context(runtime))
    private_id = new_id()
    runtime.database.execute(
        context(runtime),
        "fixture.private",
        {},
        "write",
        lambda t: t.insert(
            "orchestration_owner",
            private_id,
            {"secret_fixture": "internal"},
            state="immutable",
        ),
    )
    with pytest.raises(ResearchError) as bounded:
        checkpoint(runtime, max_bytes=1)
    assert bounded.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    with runtime.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM recovery_snapshots").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM recovery_items").fetchone()[0] == 0
    snapshot = checkpoint(runtime)
    items = list(SnapshotReader(client(runtime), snapshot).items())
    assert private_id not in str(items) and "secret_fixture" not in str(items)
    outsider = runtime.principal().model_copy(update={"id": new_id()})
    with pytest.raises(ResearchError) as actor:
        runtime.service.event_recovery.page(
            CommandContext(principal=outsider, idempotency_key=new_id()),
            RecoveryPage(snapshot_id=snapshot.id),
        )
    assert actor.value.code == ErrorCode.NOT_FOUND
    second = checkpoint(runtime)
    page = client(runtime).snapshot_page(RecoveryPage(snapshot_id=snapshot.id, limit=1))
    with pytest.raises(ResearchError) as wrong:
        client(runtime).snapshot_page(
            RecoveryPage(snapshot_id=second.id, cursor=page["next_cursor"])
        )
    assert wrong.value.code == ErrorCode.PROJECT_CONTRACT_INVALID


def test_expired_cleanup_is_bounded_preserves_tombstones_and_replays(
    runtime: LocalRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_campaign(runtime.database, context(runtime))
    first = checkpoint(runtime)
    second = checkpoint(runtime)
    with monkeypatch.context() as patch:
        patch.setattr("research_swarm.state.event_recovery.utc_now", lambda: second.expires_at)
        result = client(runtime).command(
            "stream.collect_expired",
            CollectExpiredSnapshots(limit=1),
            idempotency_key="collect-one",
        )
        assert len(result["result"]["expired_snapshot_ids"]) == 1
        assert (
            client(runtime).command(
                "stream.collect_expired",
                CollectExpiredSnapshots(limit=1),
                idempotency_key="collect-one",
            )
            == result
        )
        with runtime.database.connect() as connection:
            assert (
                connection.execute(
                    "SELECT COUNT(*) FROM recovery_snapshots WHERE expired_at IS NOT NULL"
                ).fetchone()[0]
                == 1
            )
            assert (
                connection.execute(
                    "SELECT COUNT(DISTINCT snapshot_id) FROM recovery_items"
                ).fetchone()[0]
                == 1
            )
        result2 = client(runtime).command(
            "stream.collect_expired",
            CollectExpiredSnapshots(limit=1),
            idempotency_key="collect-second",
        )
        assert set(
            result["result"]["expired_snapshot_ids"] + result2["result"]["expired_snapshot_ids"]
        ) == {first.id, second.id}
    with runtime.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM recovery_items").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM recovery_snapshots").fetchone()[0] == 2


def test_large_frozen_item_uses_bounded_chunks_and_detects_corruption(
    runtime: LocalRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    large = "frozen-λ" * 30_000
    runtime.database.execute(
        context(runtime),
        "fixture.large",
        {},
        "write",
        lambda t: t.insert(
            "report",
            new_id(),
            {"large": large},
            state="immutable",
        ),
    )
    snapshot = checkpoint(runtime)
    consumer = client(runtime)
    part = consumer.snapshot_item(RecoveryItem(snapshot_id=snapshot.id, ordinal=1))
    assert len(base64.b64decode(part["content_base64"])) == 64 * 1024
    assert len(canonical_bytes(part)) < 100 * 1024
    assert list(SnapshotReader(consumer, snapshot).items())[0]["value"]["data"]["large"] == large
    original = consumer.snapshot_item

    def corrupt(request: RecoveryItem) -> dict[str, Any]:
        result = original(request)
        block = bytearray(base64.b64decode(result["content_base64"]))
        block[0] ^= 1
        return {**result, "content_base64": base64.b64encode(block).decode("ascii")}

    monkeypatch.setattr(consumer, "snapshot_item", corrupt)
    reader = SnapshotReader(consumer, snapshot)
    with pytest.raises(ResearchError) as invalid:
        list(reader.items())
    assert invalid.value.code == ErrorCode.ARTIFACT_UNVERIFIED and not reader.completed


def test_http_initial_and_midstream_gaps_and_revoked_snapshot_reader(runtime: LocalRuntime) -> None:
    create_campaign(runtime.database, context(runtime))
    actor = Principal(
        id=new_id(), workspace_id=runtime.identity.workspace_id, scopes=frozenset({"read"})
    )
    credential = runtime.credentials.issue(runtime.principal(), actor)
    with (
        serving(create_app(runtime.service, runtime.credentials)) as url,
        ResearchClient.connect(url, credential.token) as remote,
    ):
        snapshot = RecoverySnapshot.model_validate(
            remote.command(
                "stream.create_snapshot",
                CreateRecoverySnapshot(snapshot_id=new_id()),
                idempotency_key="remote-checkpoint",
            )["result"]
        )
        assert list(SnapshotReader(remote, snapshot).items())
        remote.command(
            "stream.release_snapshot",
            RecoverySnapshotTarget(snapshot_id=snapshot.id),
            idempotency_key="release-remote",
        )
        with httpx.Client(
            base_url=url, headers={"Authorization": "Bearer " + credential.token}, timeout=5
        ) as http:
            with http.stream("GET", "/v1/events/stream", params={"after": 1}) as response:
                lines = response.iter_lines()
                assert next(lines) == ": heartbeat"
                create_campaign(runtime.database, context(runtime))
                advance(runtime, 3)
                frames = list(lines)
                assert "event: gap" in frames
                assert not any(line.startswith("id:") for line in frames)
                data = next(line[6:] for line in frames if line.startswith("data: "))
                assert strict_json_loads(data)["error"]["code"] == "EVENT_CURSOR_EXPIRED"
            assert http.get("/v1/events/stream", params={"after": 0}).status_code == 410
            assert (
                http.post(
                    "/v1/snapshot/item",
                    json={
                        "snapshot_id": snapshot.id,
                        "ordinal": 1,
                        "offset": 2**63,
                    },
                ).status_code
                == 422
            )
        new_snapshot = remote.command(
            "stream.create_snapshot",
            CreateRecoverySnapshot(snapshot_id=new_id()),
            idempotency_key="remote-second",
        )["result"]
        runtime.credentials.revoke(runtime.principal(), credential.id)
        with pytest.raises(ResearchError) as denied:
            remote.snapshot_page(RecoveryPage(snapshot_id=new_snapshot["id"]))
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED


@pytest.mark.asyncio
async def test_cli_mcp_snapshot_pages_and_release_share_original_reader(
    runtime: LocalRuntime,
) -> None:
    create_campaign(runtime.database, context(runtime))
    request = CreateRecoverySnapshot(snapshot_id=new_id()).model_dump(mode="json")
    made = cli(
        runtime.root,
        "command",
        "stream.create_snapshot",
        "--key",
        "cli-checkpoint",
        payload=request,
    )
    identifier = made["result"]["id"]
    page = cli(runtime.root, "snapshot", "page", identifier, "--limit", "1")
    item = cli(runtime.root, "snapshot", "item", identifier, "1", "--limit", "16")
    assert len(base64.b64decode(item["content_base64"])) == 16
    async with Client(stdio(runtime.root), read_timeout_seconds=20, cache=None) as agent:
        replay = await agent.call_tool(
            "research_stream_create_snapshot",
            {"request": request, "idempotency_key": "cli-checkpoint"},
        )
        assert not replay.is_error and replay.structured_content["command_id"] == made["command_id"]
        same = await agent.call_tool(
            "research_snapshot_page", {"request": {"snapshot_id": identifier, "limit": 1}}
        )
        assert not same.is_error and same.structured_content["result"] == page
        read = await agent.call_tool(
            "research_snapshot_item",
            {"request": {"snapshot_id": identifier, "ordinal": 1, "limit": 16}},
        )
        assert not read.is_error and read.structured_content["result"] == item
    first_release = release(runtime, identifier)
    assert release(LocalRuntime.open(runtime.root), identifier) == first_release


def test_owned_expired_snapshots_are_reclaimed_before_new_admission(
    runtime: LocalRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_campaign(runtime.database, context(runtime))
    snapshots = [checkpoint(runtime) for _ in range(8)]
    with pytest.raises(ResearchError) as full:
        checkpoint(runtime)
    assert full.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    with monkeypatch.context() as patch:
        patch.setattr(
            "research_swarm.state.event_recovery.utc_now", lambda: snapshots[-1].expires_at
        )
        fresh = checkpoint(runtime)
    with runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM recovery_snapshots WHERE expired_at IS NOT NULL"
            ).fetchone()[0]
            == 8
        )
        assert (
            connection.execute("SELECT COUNT(DISTINCT snapshot_id) FROM recovery_items").fetchone()[
                0
            ]
            == 1
        )
    assert list(SnapshotReader(client(runtime), fresh).items())
