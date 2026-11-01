"""Private hosted route issuance retains atomic evidence without reopening execution."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from adapters.cloudflare import checkpoint_routes
from adapters.cloudflare.checkpoint_routes import issue_target_route
from research_swarm.control.recovery_routes import verify_logical_recovery_route
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.recovery_routes import SignedWorkerRecoveryRoute
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database
from research_swarm.state.recovery_routes import OPERATION, require_recovery_route
from research_swarm.state.sql import StoreIdentity
from tests.adapters.cloudflare.hosted.test_hosted_research import enroll_and_claim, sdk
from tests.adapters.cloudflare.hosted.test_hosted_router import FixtureStore, Gate
from tests.adapters.cloudflare.hosted.test_production_delivery import production as production
from tests.adapters.cloudflare.recovery.test_checkpoint_admission import admit, context
from tests.adapters.cloudflare.recovery.test_checkpoint_admission import elected as elected
from tests.adapters.cloudflare.recovery.test_checkpoint_capture import sealed as sealed
from tests.adapters.cloudflare.recovery.test_checkpoint_import import recovery as recovery
from tests.adapters.cloudflare.recovery.test_checkpoint_import import snapshot
from tests.adapters.cloudflare.recovery.test_checkpoint_request import imported as imported
from tests.adapters.cloudflare.recovery.test_checkpoint_target import configured as configured
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.projects.test_manifest import project as project


@pytest.fixture
async def source(production):
    e = production
    loop = asyncio.get_running_loop()

    def claim():
        with sdk(e, loop) as client:
            return enroll_and_claim(client, e.admitted, e.manifest.provisioning_actor_id)[0]

    e.claimed = await asyncio.to_thread(claim)

    def metadata(sql):
        sql.execute(
            "CREATE TABLE research_schema (singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "version INTEGER NOT NULL)"
        )
        sql.execute("INSERT INTO research_schema VALUES (1,3)")

    e.store.write(metadata)
    return e


@pytest.fixture
async def routed(elected):
    e = elected
    await admit(e)
    e.request = {
        "route_id": new_id(),
        "attempt_id": e.source.claimed["result"]["id"],
        "credential_id": e.source.manifest.credentials[0].id,
        "source_controller_url": "https://source.example",
        "destination_url": "https://target.example",
    }
    return e


async def call(e, *, store=None, **changes):
    options = {
        "identity": e.target.identity,
        "credential": e.source.secrets.bootstrap,
        "target_json": json.dumps(e.target_config),
        "request_json": json.dumps(e.request),
        "crypto": e.source.crypto,
        **changes,
    }
    return await issue_target_route(
        Gate(),
        lambda: e.target if store is None else store,
        e.manifest,
        e.source.secrets,
        **options,
    )


async def test_real_original_attempt_route_commits_once_without_research_changes(routed):
    e = routed
    before, calls = snapshot(e.target), len(e.source.witness.requests)
    e.source.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic offline")
    first = await call(e)
    assert await call(e) == first
    reply = strict_json_loads(first)
    assert reply["disposition"] == "accepted"
    signed = SignedWorkerRecoveryRoute.model_validate(reply["result"]["data"])
    verify_logical_recovery_route(
        signed, e.binding.expected_custody, crypto_provider=e.source.crypto
    )
    assert signed.route.attempt_id == e.request["attempt_id"]
    assert signed.route.credential_id == e.request["credential_id"]
    assert reply["result"]["parent_id"] == e.binding.intent.restore_id
    after = snapshot(e.target)
    for table in before.keys() - {"records", "events", "outbox", "commands", "workspaces"}:
        assert after[table] == before[table]
    assert [row for row in after["records"] if row["kind"] != "worker_recovery_route"] == before[
        "records"
    ]
    for table in ("events", "outbox", "commands"):
        assert len(after[table]) == len(before[table]) + 1
    assert len(e.source.witness.requests) == calls
    assert (
        Database.from_store(e.target).read_record(
            context(e), "workspace_gate", e.manifest.configuration.authority.workspace_id
        )["state"]
        == "recovering"
    )


async def test_independent_connection_race_and_restart_preserve_one_route(routed):
    e, barrier = routed, Barrier(2)
    before = snapshot(e.target)

    def race(_):
        store = FixtureStore(Database(e.target.delegate.path).store)
        store.identity = e.target.identity
        barrier.wait(timeout=10)
        return asyncio.run(call(e, store=store))

    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(race, range(2)))
    assert replies[0] == replies[1]
    after = snapshot(e.target)
    assert len(after["commands"]) == len(before["commands"]) + 1
    assert len(after["events"]) == len(before["events"]) + 1
    opened = FixtureStore(Database(e.target.delegate.path).store)
    opened.identity = e.target.identity
    assert await call(e, store=opened) == replies[0]
    assert snapshot(e.target) == after


@pytest.mark.parametrize(
    "damage",
    ["bootstrap", "destination", "credential", "proof", "revoked", "expires", "missing_route"],
)
async def test_current_custody_is_rechecked_before_saved_response(routed, damage):
    e, changes = routed, {}
    first = await call(e)
    if damage == "bootstrap":
        changes["credential"] = "wrong"
    elif damage in {"destination", "credential"}:
        request = {
            **e.request,
            "destination_url"
            if damage == "destination"
            else "credential_id": "https://other.example" if damage == "destination" else new_id(),
        }
        changes["request_json"] = json.dumps(request)
    elif damage == "proof":
        e.target.write(
            lambda sql: sql.execute("DELETE FROM records WHERE kind='logical_recovery_election'")
        )
    elif damage == "missing_route":
        e.target.write(
            lambda sql: sql.execute("DELETE FROM records WHERE kind='worker_recovery_route'")
        )
    else:
        column, value = (
            ("revoked", 1) if damage == "revoked" else ("expires_at", "2099-01-01T00:00:00Z")
        )
        e.target.write(
            lambda sql: sql.execute(
                f"UPDATE credentials SET {column}=? WHERE id=?", (value, e.request["credential_id"])
            )
        )
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await call(e, **changes)
    assert snapshot(e.target) == before
    assert any(row["response"] == first for row in before["commands"])


async def test_route_record_event_and_acknowledgment_roll_back_together(routed, monkeypatch):
    e = routed
    before = snapshot(e.target)
    original = checkpoint_routes.persist_route

    def fail(transaction, signed):
        result = original(transaction, signed)
        assert result["state"] == "immutable"
        raise RuntimeError("Synthetic failure after route event")

    with monkeypatch.context() as patch:
        patch.setattr(checkpoint_routes, "persist_route", fail)
        with pytest.raises(RuntimeError, match="after route event"):
            await call(e)
    assert snapshot(e.target) == before
    assert strict_json_loads(await call(e))["disposition"] == "accepted"


async def test_lost_first_committed_route_reply_replays_after_reopening(routed, monkeypatch):
    e, original = routed, routed.target.write
    before = snapshot(e.target)
    committed = []

    def lost(callback):
        committed.append(original(callback))
        raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic lost route reply")

    with monkeypatch.context() as patch:
        patch.setattr(e.target, "write", lost)
        with pytest.raises(ResearchError, match="lost route reply"):
            await call(e)
    after = snapshot(e.target)
    assert len(after["commands"]) == len(before["commands"]) + 1
    reopened = FixtureStore(Database(e.target.delegate.path).store)
    reopened.identity = e.target.identity
    assert await call(e, store=reopened) == committed[0]
    assert snapshot(e.target) == after


async def test_wrong_physical_target_cannot_issue_a_route(routed):
    e = routed
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await call(e, identity=StoreIdentity("cloudflare-do", "another/target"))
    assert snapshot(e.target) == before


async def test_guard_cannot_authorize_other_operations_and_fresh_key_does_not_duplicate_event(
    routed, monkeypatch
):
    e, captured = routed, []
    original = checkpoint_routes.require_recovery_route

    def record(sql, context, authority, operation, payload, digest):
        original(sql, context, authority, operation, payload, digest)
        captured.append((context.model_copy(), payload, digest))

    monkeypatch.setattr(checkpoint_routes, "require_recovery_route", record)
    result = strict_json_loads(await call(e))
    context, payload, digest = captured[0]
    signed = SignedWorkerRecoveryRoute.model_validate(result["result"]["data"])
    before = snapshot(e.target)
    for operation in ("project.create", "worker.submit_evidence", "orchestration.execute_turn"):
        with pytest.raises(ResearchError):
            e.target.read(
                lambda sql, operation=operation: require_recovery_route(
                    sql, context, e.target.identity, operation, payload, digest
                )
            )
    database = Database.from_store(e.target, recovery_verifier=e.verifier)
    bare = context.model_copy(update={"idempotency_key": new_id(), "command_id": new_id()})
    bare._recovery_route = None
    with pytest.raises(ResearchError):
        database.execute(
            bare, OPERATION, payload, "*", lambda _: pytest.fail("Missing guard passed")
        )
    fresh = context.model_copy(update={"idempotency_key": new_id(), "command_id": new_id()})
    replayed = database.execute(
        fresh, OPERATION, payload, "*", lambda tx: checkpoint_routes.persist_route(tx, signed)
    )
    assert replayed["result"] == result["result"]
    after = snapshot(e.target)
    for table in before.keys() - {"commands"}:
        assert after[table] == before[table]
    assert len(after["commands"]) == len(before["commands"]) + 1


async def test_credential_change_after_guard_minting_refuses_and_rolls_back(routed, monkeypatch):
    e, original = routed, checkpoint_routes.require_recovery_route
    before = snapshot(e.target)

    def changed(sql, context, authority, operation, payload, digest):
        sql.execute("UPDATE credentials SET revoked=1 WHERE id=?", (e.request["credential_id"],))
        return original(sql, context, authority, operation, payload, digest)

    monkeypatch.setattr(checkpoint_routes, "require_recovery_route", changed)
    with pytest.raises(ResearchError, match="credential is unavailable"):
        await call(e)
    assert snapshot(e.target) == before
