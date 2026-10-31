"""The ordinary SDK verifies and recovers successor state through the hosted API."""

import asyncio
from dataclasses import replace

import pytest

from adapters.cloudflare.delivery_router import DeliveryRegistration, HostedDelivery
from adapters.cloudflare.research_router import HostedStartupConfiguration
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DriveAction, OrchestrationBinding
from research_swarm.domain.types import new_id
from research_swarm.orchestration.delivery_contract import binding_reference
from research_swarm.orchestration.gateway_models import GatewayBinding
from research_swarm.orchestration.startup import recover_driver_start
from research_swarm.orchestration.turns import DurableTurnDriver
from research_swarm.state.database import encode
from tests.test_hosted_artifacts import current
from tests.test_hosted_http import send
from tests.test_hosted_research import sdk
from tests.test_hosted_transfers import CONFIG, command, issue, native_state, request, rows
from tests.test_hosted_transfers import hosted as hosted
from tests.test_hosted_transfers import project as project
from tests.test_hosted_transfers import queued as queued
from tests.test_hosted_transfers import research as research
from tests.test_time_witnesses import SEED, encoded, identifier
from tests.test_time_witnesses import payload as witness_payload


@pytest.fixture
async def startup(queued):
    e, h = queued, queued.h
    h.config = HostedStartupConfiguration(**h.config.model_dump(exclude={"profile"}))
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    payload, revision = await request(e)
    e.transferred = (await command(e, "orchestration.transfer", payload, revision=revision))[
        "result"
    ]
    e.starts = e.transferred["data"]["start_ids"]
    e.loop = asyncio.get_running_loop()
    return e


async def recover(e, *, route="control", modify=None, recovery_id="fixture-recovery"):
    credential = e.successor if route == "control" else e.next_evaluator

    def run():
        with sdk(e.h, e.loop, token=credential.token) as client:
            if modify is not None:
                modify(client)
            recover_driver_start(client, e.starts[route], recovery_id=recovery_id)

    return await asyncio.to_thread(run)


async def prepare(e, *, credential=None, epoch=2):
    credential = credential or e.successor

    def run():
        with sdk(e.h, e.loop, token=credential.token) as client:
            campaign_id = e.admitted.campaign["id"]
            driver = DurableTurnDriver(
                client, campaign_id, epoch=epoch, configuration_digest=CONFIG
            )
            return driver.prepare_turn(
                1,
                lambda: DriveAction(
                    operation="report.create",
                    payload={"campaign_id": campaign_id},
                    idempotency_key="successor-report",
                ),
            )

    return await asyncio.to_thread(run)


async def test_hosted_delivery_bootstrap_accepts_only_completed_external_successor_recovery(
    startup,
):
    e, h = startup, startup.h
    gateway = h.credentials.issue(
        h.principal,
        h.principal.model_copy(update={"id": new_id(), "scopes": {"orchestration.deliver"}}),
    )
    binding = OrchestrationBinding.model_validate(
        current(h, "orchestration_binding", e.admitted.campaign["id"])["data"]
    )
    reference = GatewayBinding.from_reference(binding_reference(binding))
    facade = HostedDelivery(
        h.router,
        {
            gateway.principal.id: DeliveryRegistration(
                reference,
                {"control": e.successor.token, "evaluator": e.next_evaluator.token},
            )
        },
    )

    async def send(operation, payload):
        async def read(limit):
            body = canonical_bytes(payload)
            assert len(body) <= limit
            return body

        response = await facade.handle(
            "POST", "/v1/delivery/" + operation, {"authorization": "Bearer " + gateway.token}, read
        )
        return response.status, strict_json_loads(response.body)

    request = {"binding": reference.model_dump(mode="json"), "recovery_id": new_id()}
    assert (await send("bootstrap", request))[0] == 501
    await recover(e)
    assert (await send("bootstrap", request))[0] == 501
    await recover(e, route="evaluator")
    witness_calls = len(h.witness.requests)
    assert await send("bootstrap", request) == (200, {"schema_version": "1.0", "state": "active"})
    assert len(h.witness.requests) == witness_calls
    turn = await prepare(e)
    status, pending = await send("inspect", reference.model_dump(mode="json"))
    assert status == 200 and pending["work"]["record_id"] == turn["id"]
    first = await send("execute", pending["work"])
    assert first[0] == 200 and first[1]["disposition"] == "accepted"
    assert await send("execute", pending["work"]) == first
    assert len(await rows(e, "report")) == 1


async def test_sdk_recovery_is_actor_scoped_and_enables_one_successor_action_after_restart(startup):
    e, h = startup, startup.h
    before = native_state(e)
    with pytest.raises(ResearchError) as pending:
        await prepare(e)
    assert pending.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    await recover(e)
    assert current(h, "orchestration_start", e.starts["control"])["state"] == "acknowledged"
    assert current(h, "orchestration_start", e.starts["evaluator"])["state"] == "pending"
    await recover(e, route="evaluator")
    turn = await prepare(e)
    receipt = await command(
        e,
        "orchestration.execute_turn",
        {"turn_id": turn["id"]},
        credential=e.successor,
        key="execute-successor",
    )
    assert receipt["result"]["response"]["disposition"] == "accepted"
    assert len(await rows(e, "report")) == 1
    assert native_state(e) == before
    assert (
        h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM recovery_items").one()[0]) == 0
    )
    assert len(await rows(e, "orchestration_start_ack")) == 2
    with pytest.raises(ResearchError) as retired:
        await prepare(e, credential=e.driver, epoch=1)
    assert retired.value.code == ErrorCode.STALE_FENCE


@pytest.mark.parametrize(
    "operation",
    ["stream.create_snapshot", "orchestration.acknowledge_start", "stream.release_snapshot"],
)
async def test_lost_startup_replies_recover_without_duplicate_ack_or_retained_items(
    startup, operation
):
    e, h = startup, startup.h
    captured = []

    def modify(client):
        original = client.command

        def command(name, request, **options):
            result = original(name, request, **options)
            if name == operation and not captured:
                captured.append((name, request, options, result))
                raise ResearchError(ErrorCode.DELIVERY_UNCERTAIN, "Synthetic lost response")
            return result

        client.command = command

    with pytest.raises(ResearchError) as lost:
        await recover(e, modify=modify)
    assert lost.value.code == ErrorCode.DELIVERY_UNCERTAIN
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    await recover(e)
    assert len(await rows(e, "orchestration_start_ack")) == 1
    assert (
        h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM recovery_items").one()[0]) == 0
    )
    name, intent, options, result = captured[0]
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Source unavailable")
    before = len(h.witness.requests)
    assert (
        await h.router.command(e.successor.token, name, intent.model_dump(mode="json"), **options)
        == result
    )
    assert len(h.witness.requests) == before


@pytest.mark.parametrize("fault", ["checkpoint", "item"])
async def test_unverified_checkpoint_or_snapshot_content_never_acknowledges_successor(
    startup, fault
):
    e = startup

    def modify(client):
        if fault == "checkpoint":
            original = client.get

            def get(kind, identifier):
                row = original(kind, identifier)
                if kind == "orchestration_checkpoint":
                    row["data"]["entry_count"] += 1
                return row

            client.get = get
        else:
            original = client.snapshot_item

            def item(request):
                row = original(request)
                row["content_base64"] = "AA=="
                return row

            client.snapshot_item = item

    with pytest.raises(ResearchError) as corrupt:
        await recover(e, modify=modify)
    assert corrupt.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert await rows(e, "orchestration_start_ack") == []
    assert current(e.h, "orchestration_start", e.starts["control"])["state"] == "pending"
    assert (
        e.h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM recovery_items").one()[0]) == 0
    )


async def test_snapshot_preflight_refuses_wrong_actor_and_invalid_cursor_before_witness(startup):
    e, h = startup, startup.h
    saved = (
        await command(
            e, "stream.create_snapshot", {"snapshot_id": new_id()}, credential=e.successor
        )
    )["result"]
    before = len(h.witness.requests)
    for credential, payload, expected in (
        (h.operator, {"snapshot_id": saved["id"]}, ErrorCode.NOT_FOUND),
        (
            e.successor,
            {"snapshot_id": saved["id"], "cursor": "invalid"},
            ErrorCode.PROJECT_CONTRACT_INVALID,
        ),
    ):
        with pytest.raises(ResearchError) as denied:
            await h.router.query(credential.token, "snapshot.page", payload)
        assert denied.value.code == expected
    start = current(h, "orchestration_start", e.starts["control"])
    with pytest.raises(ResearchError) as wrong_actor:
        await command(
            e,
            "orchestration.acknowledge_start",
            {
                "start_id": start["id"],
                "checkpoint_digest": start["data"]["checkpoint_digest"],
                "observed_watermark": start["data"]["watermark"],
            },
            revision=start["revision"],
        )
    assert wrong_actor.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert len(h.witness.requests) == before


async def test_epoch_change_fences_query_and_requires_a_new_snapshot(startup):
    e, h = startup, startup.h
    saved = (
        await command(
            e, "stream.create_snapshot", {"snapshot_id": new_id()}, credential=e.successor
        )
    )["result"]
    h.witness.epoch = identifier(8)
    with pytest.raises(ResearchError) as fenced:
        await h.router.query(e.successor.token, "snapshot.page", {"snapshot_id": saved["id"]})
    assert (
        fenced.value.code == ErrorCode.STALE_FENCE and h.registry.current().state == "unqualified"
    )
    h.registry.qualify(epoch=identifier(8))
    with pytest.raises(ResearchError) as expired:
        await h.router.query(e.successor.token, "snapshot.page", {"snapshot_id": saved["id"]})
    assert expired.value.code == ErrorCode.SNAPSHOT_EXPIRED
    await recover(e, recovery_id="after-source-change")
    assert len(await rows(e, "orchestration_start_ack")) == 1


async def test_capabilities_make_recovery_and_bootstrap_limits_explicit(startup):
    e = startup
    response = await send(e.h, "GET", "/v1/capabilities")
    capabilities = strict_json_loads(response.body)
    assert capabilities["recovery"]["clock"] == "authority-epoch"
    assert capabilities["recovery"]["max_items"] == 4096
    assert capabilities["recovery"]["automatic_delivery_bootstrap"] is False
    assert capabilities["recovery"]["delivery_after_acknowledgment"] is True
    assert capabilities["commands"]["orchestration.acknowledge_start"]["required_scopes"] == [
        "orchestration.drive",
        "read",
    ]
    assert "stream.advance_floor" not in capabilities["commands"]


async def test_transfer_profile_refuses_snapshot_body_without_consumption(queued):
    consumed = []
    response = await send(queued.h, "POST", "/v1/snapshot/page", body=b"{}", consume=consumed)
    assert response.status == 501 and consumed == []


@pytest.mark.parametrize("fault", ["revoke", "cancel"])
async def test_snapshot_cannot_commit_after_credential_revocation_or_cancellation(
    startup, monkeypatch, fault
):
    e, h = startup, startup.h
    acquire = h.router.acquire

    async def changed(request):
        response = await acquire(request)
        if fault == "revoke":
            h.credentials.revoke(h.principal, e.successor.id)
        else:
            asyncio.current_task().cancel()
        return response

    monkeypatch.setattr(h.router, "acquire", changed)
    task = asyncio.create_task(
        command(e, "stream.create_snapshot", {"snapshot_id": new_id()}, credential=e.successor)
    )
    with pytest.raises(ResearchError if fault == "revoke" else asyncio.CancelledError):
        await task
    assert (
        h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM recovery_snapshots").one()[0])
        == 0
    )
    assert await rows(e, "orchestration_start_ack") == []


async def test_operator_collects_expired_revoked_reader_snapshots_to_recover_workspace_quota(
    startup, monkeypatch
):
    e, h = startup, startup.h
    abandoned = issue(h, {"read"})
    saved = (
        await command(
            e,
            "stream.create_snapshot",
            {"snapshot_id": new_id(), "lifetime_seconds": 30},
            credential=abandoned,
        )
    )["result"]
    h.credentials.revoke(h.principal, abandoned.id)
    service = h.router.service.event_recovery
    service.limits = replace(service.limits, max_retained_bytes=saved["size_bytes"])
    with pytest.raises(ResearchError) as full:
        await command(
            e, "stream.create_snapshot", {"snapshot_id": new_id()}, credential=e.successor
        )
    assert full.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    acquire = h.router.acquire

    async def advanced(request):
        await acquire(request)
        value = witness_payload(request)
        value = value.model_copy(update={"authority_time_ms": value.authority_time_ms + 30_000})
        return encoded(WitnessSigner(h.config.source, SEED).sign(value))

    monkeypatch.setattr(h.router, "acquire", advanced)
    reclaimed = await command(e, "stream.collect_expired", {"limit": 1})
    assert reclaimed["result"]["expired_snapshot_ids"] == [saved["id"]]
    fresh = (
        await command(
            e, "stream.create_snapshot", {"snapshot_id": new_id()}, credential=e.successor
        )
    )["result"]
    assert fresh["item_count"] == saved["item_count"]
    assert fresh["size_bytes"] == saved["size_bytes"]


async def test_read_and_collection_scopes_are_required_before_body_or_cached_replay(startup):
    e, h = startup, startup.h
    request = {"snapshot_id": new_id()}
    await command(
        e, "stream.create_snapshot", request, credential=e.successor, key="saved-snapshot"
    )
    limited = h.credentials.issue(
        h.principal, e.successor.principal.model_copy(update={"scopes": {"orchestration.drive"}})
    )
    before = len(h.witness.requests)
    with pytest.raises(ResearchError) as replay:
        await command(
            e, "stream.create_snapshot", request, credential=limited, key="saved-snapshot"
        )
    assert replay.value.code == ErrorCode.AUTHORIZATION_DENIED
    for path, token in (
        ("/v1/snapshot/page", limited.token),
        ("/v1/commands/stream.collect_expired", e.successor.token),
    ):
        consumed = []
        response = await send(h, "POST", path, token=token, body=b"{}", consume=consumed)
        assert response.status == 403 and consumed == []
    assert len(h.witness.requests) == before
