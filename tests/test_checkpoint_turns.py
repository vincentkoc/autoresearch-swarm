"""Original hosted agent actions reconcile or close without executing them in recovery."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from adapters.cloudflare.checkpoint_turns import reconcile_target_turn
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database
from tests.test_checkpoint_admission import admit, context
from tests.test_checkpoint_admission import elected as elected
from tests.test_checkpoint_admission_research import source as source
from tests.test_checkpoint_capture import sealed as sealed
from tests.test_checkpoint_import import recovery as recovery
from tests.test_checkpoint_import import snapshot
from tests.test_checkpoint_request import imported as imported
from tests.test_checkpoint_target import configured as configured
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.test_hosted_router import FixtureStore, Gate
from tests.test_manifest import project as project
from tests.test_production_delivery import production as production


def command(e, operation="orchestration.close_turn"):
    turn = e.source.turn
    payload = {"turn_id": turn["id"]}
    if operation == "orchestration.close_turn":
        payload.update(epoch=turn["data"]["epoch"], finding="Retire original fenced turn")
    return {
        "request_id": new_id(),
        "operation": operation,
        "expected_revision": turn["revision"],
        "payload": payload,
    }


async def call(e, request, *, store=None, **changes):
    def open_store():
        e.opens += 1
        return e.target if store is None else store

    return await reconcile_target_turn(
        Gate(),
        open_store,
        e.manifest,
        e.source.secrets,
        identity=e.target.identity,
        target_json=json.dumps(e.target_config),
        crypto=e.source.crypto,
        **{
            "credential": e.source.secrets.bootstrap,
            "request_json": json.dumps(request),
            **changes,
        },
    )


async def test_absent_ack_closes_without_dispatch_or_funding_release(elected):
    e = elected
    admitted = await admit(e)
    before = snapshot(e.target)
    with pytest.raises(ResearchError, match="unresolved"):
        await call(e, command(e, "orchestration.reconcile_turn"))
    assert snapshot(e.target) == before
    request = command(e)
    raw = await call(e, request)
    result = strict_json_loads(raw)
    assert result["disposition"] == "accepted"
    assert result["result"]["turn"]["state"] == "closed_unacknowledged"
    assert result["result"]["response"] is result["result"]["receipt"] is None
    assert await call(e, request) == raw
    assert await admit(e) == admitted
    after = snapshot(e.target)
    for table in ("budgets", "budget_reservations", "budget_settlements", "budget_entries"):
        assert after[table] == before[table]
    assert [r for r in after["records"] if r["kind"] == "report"] == [
        r for r in before["records"] if r["kind"] == "report"
    ]
    turn = e.source.turn["data"]
    assert not [
        r
        for r in after["commands"]
        if r["actor_id"] == turn["actor_id"]
        and r["idempotency_key"] == turn["action"]["idempotency_key"]
    ]
    database = Database.from_store(e.target)
    binding = database.read_record(context(e), "orchestration_binding", turn["campaign_id"])
    assert binding["state"] == "fenced" and binding["data"]["pending_turn_id"] is None


@pytest.mark.parametrize("source", ["acknowledged"], indirect=True)
async def test_retained_target_ack_reconciles_once_without_repeating_action(elected):
    e = elected
    await admit(e)
    before = snapshot(e.target)
    assert [r for r in before["records"] if r["kind"] == "report"]
    with pytest.raises(ResearchError) as refused:
        await call(e, command(e))
    assert refused.value.code == ErrorCode.INVALID_TRANSITION
    request = command(e, "orchestration.reconcile_turn")
    raw = await call(e, request)
    result = strict_json_loads(raw)["result"]
    assert result["turn"]["state"] == "settled" and result["receipt"]["state"] == "immutable"
    assert await call(e, request) == raw
    after = snapshot(e.target)
    assert [r for r in after["records"] if r["kind"] == "report"] == [
        r for r in before["records"] if r["kind"] == "report"
    ]
    assert after["budgets"] == before["budgets"]


async def test_parallel_connection_retry_and_lost_reply_keep_one_closure(elected):
    e = elected
    await admit(e)
    request, barrier = command(e), Barrier(2)

    def parallel(_):
        opened = FixtureStore(Database(e.target.delegate.path).store)
        opened.identity = e.target.identity
        barrier.wait(timeout=10)
        return asyncio.run(call(e, request, store=opened))

    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(parallel, range(2)))
    assert replies[0] == replies[1]
    before = snapshot(e.target)
    assert len([r for r in before["records"] if r["kind"] == "drive_closure"]) == 1

    class Lost:
        identity = e.target.identity

        def write(self, callback):
            e.target.write(callback)
            raise OSError("Synthetic lost closure reply")

    with pytest.raises(OSError, match="lost closure reply"):
        await call(e, request, store=Lost())
    assert await call(e, request) == replies[0]
    assert snapshot(e.target) == before


async def test_lost_initial_closure_reply_replays_after_reopening(elected):
    e = elected
    await admit(e)
    request, committed = command(e), []

    class Lost:
        identity = e.target.identity

        def write(self, callback):
            committed.append(e.target.write(callback))
            raise OSError("Synthetic lost initial reply")

    with pytest.raises(OSError, match="lost initial reply"):
        await call(e, request, store=Lost())
    before = snapshot(e.target)
    reopened = FixtureStore(Database(e.target.delegate.path).store)
    reopened.identity = e.target.identity
    assert await call(e, request, store=reopened) == committed[0]
    assert snapshot(e.target) == before


@pytest.mark.parametrize(
    "damage", ["execute", "prepare", "bad_payload", "bootstrap", "companion", "binding", "owner"]
)
async def test_recovery_refuses_execution_or_changed_current_authority(elected, damage):
    e = elected
    await admit(e)
    request, options = command(e), {}
    if damage in {"execute", "prepare"}:
        request["operation"] = "orchestration." + damage + "_turn"
    elif damage == "bad_payload":
        request["payload"]["dispatch"] = True
    elif damage == "bootstrap":
        await call(e, request)
        options["credential"] = "wrong"
    elif damage == "companion":
        await call(e, request)
        e.target.write(
            lambda sql: sql.execute("DELETE FROM records WHERE kind='logical_recovery_election'")
        )
    elif damage == "binding":
        await call(e, request)
        e.target.write(
            lambda sql: sql.execute(
                "UPDATE records SET state='active' WHERE kind='orchestration_binding'"
            )
        )
    else:
        await call(e, request)
        e.target.write(
            lambda sql: sql.execute(
                "UPDATE records SET state='revoked' WHERE kind='orchestration_owner'"
            )
        )
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await call(e, request, **options)
    assert snapshot(e.target) == before
