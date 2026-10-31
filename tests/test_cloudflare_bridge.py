import asyncio
from dataclasses import replace
from threading import Event

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, Response

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.orchestration.cloudflare import (
    CloudflareBridge,
    WorkflowOptions,
    WorkflowStatus,
    workflow_id,
)
from tests.test_http_delivery import serving
from tests.test_local_delivery import connection
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_orchestration_transfer import pause, request, successor, transfer
from tests.test_orchestration_turns import bind
from tests.test_orchestration_waits import driver, record, report

TOKEN = "public-fixture-adapter-token-with-no-hosted-authority"


@pytest.mark.asyncio
async def test_explicit_workflow_recovery_replays_exact_intent_and_validates_successor(pipeline):
    drivers = bind(pipeline, adapter="cloudflare")
    binding = connection(pipeline, drivers).binding
    expected = WorkflowStatus(
        id=workflow_id(binding, 7), generation=7, status="errored", initial_pending=None
    )
    request_id = new_id()
    app, requests = FastAPI(), []
    receipt = {
        "request_id": request_id,
        "predecessor_id": expected.id,
        "predecessor_generation": 7,
        "id": workflow_id(binding, 8),
        "generation": 8,
    }
    replies = [Response("Lost accepted reply", status_code=503), receipt]

    @app.post("/adapter/recover")
    async def recover(request: Request):
        assert request.headers["authorization"] == "Bearer " + TOKEN
        requests.append(await request.json())
        return replies.pop(0)

    with serving(app) as url:
        async with CloudflareBridge(url, TOKEN) as bridge:
            arguments = {
                "request_id": request_id,
                "expected": expected,
                "reason": "Resume original work after fixing delivery",
            }
            with pytest.raises(ResearchError) as lost:
                await bridge.recover(binding, **arguments)
            assert lost.value.code == ErrorCode.DELIVERY_UNCERTAIN
            accepted = await bridge.recover(binding, **arguments)
            assert accepted.model_dump() == receipt
            assert requests[0] == requests[1]
            for field, value in (
                ("request_id", new_id()),
                ("predecessor_id", workflow_id(binding, 6)),
                ("predecessor_generation", 6),
                ("id", workflow_id(binding, 9)),
                ("generation", 9),
            ):
                replies.append({**receipt, field: value})
                with pytest.raises(ResearchError) as foreign:
                    await bridge.recover(binding, **arguments)
                assert foreign.value.code == ErrorCode.DELIVERY_UNCERTAIN
            before = len(requests)
            for invalid in (
                {"expected": expected.model_copy(update={"status": "running"})},
                {"expected": expected.model_copy(update={"id": workflow_id(binding, 6)})},
                {"request_id": "invalid"},
                {"reason": " "},
            ):
                with pytest.raises(ValueError):
                    await bridge.recover(binding, **{**arguments, **invalid})
            assert len(requests) == before


@pytest.mark.asyncio
async def test_lost_provider_ack_retains_outbox_and_retry_only_repeats_same_hint(pipeline):
    drivers = bind(pipeline, adapter="cloudflare")
    binding = connection(pipeline, drivers).binding
    waited = record(pipeline, drivers)
    report(pipeline)
    waits = driver(pipeline, drivers)
    delivery_id = pipeline.client.get("orchestration_wait", waited["wait"]["id"])["id"]
    app, accepted = FastAPI(), []

    @app.post("/adapter/wake")
    async def wake(request: Request):
        assert request.headers["authorization"] == "Bearer " + TOKEN
        payload = await request.json()
        accepted.append(payload)
        if len(accepted) == 1:
            return Response("lost fixture acknowledgment", status_code=503)
        return {"accepted": True}

    with serving(app) as url:
        async with CloudflareBridge(url, TOKEN) as bridge:
            with pytest.raises(ResearchError) as uncertain:
                await bridge.deliver(binding, waits)
            assert uncertain.value.code == ErrorCode.DELIVERY_UNCERTAIN
            assert uncertain.value.retryable
            delivered = await bridge.deliver(binding, waits)
            assert len(delivered) == 1
            assert await bridge.deliver(binding, waits) == ()
            assert len(accepted) == 2 and accepted[0] == accepted[1]
            assert accepted[0]["wait_id"] == delivery_id
            assert pipeline.client.get("wakeup_delivery", delivered[0])["state"] == "delivered"
            assert pipeline.client.get("orchestration_wait", delivery_id)["state"] == "ready"
            with pytest.raises(ResearchError) as stale:
                await bridge.deliver(replace(binding, epoch=binding.epoch + 1), waits)
            assert stale.value.code == ErrorCode.STALE_FENCE
            assert len(accepted) == 2


@pytest.mark.asyncio
async def test_redirect_never_forwards_private_adapter_credential(pipeline):
    drivers = bind(pipeline, adapter="cloudflare")
    binding = connection(pipeline, drivers).binding
    target, app, seen = FastAPI(), FastAPI(), []

    @target.post("/target")
    async def receive():
        seen.append(True)
        return {"id": workflow_id(binding)}

    with serving(target) as target_url:

        @app.post("/adapter/start")
        async def redirect():
            return RedirectResponse(target_url + "/target", status_code=307)

        with serving(app) as url:
            async with CloudflareBridge(url, TOKEN) as bridge:
                with pytest.raises(ResearchError) as rejected:
                    await bridge.start(binding)
                assert rejected.value.code == ErrorCode.DELIVERY_UNCERTAIN
    assert seen == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reply",
    [
        '{"accepted":true,"accepted":true}',
        '{"accepted":1}',
        '{"accepted":true,"private":"fixture"}',
        '{"accepted":false}',
        "[" * 1100 + "0" + "]" * 1100,
        "x" * (16 * 1024 + 1),
    ],
)
async def test_unverified_response_cannot_acknowledge_domain_outbox(pipeline, reply):
    drivers = bind(pipeline, adapter="cloudflare")
    binding = connection(pipeline, drivers).binding
    record(pipeline, drivers)
    report(pipeline)
    waits, app = driver(pipeline, drivers), FastAPI()

    @app.post("/adapter/wake")
    async def wake():
        return Response(reply, media_type="application/json")

    with serving(app) as url:
        async with CloudflareBridge(url, TOKEN) as bridge:
            with pytest.raises(ResearchError) as rejected:
                await bridge.deliver(binding, waits)
            assert rejected.value.code == ErrorCode.DELIVERY_UNCERTAIN
            received = []

            async def observe(delivery):
                received.append(delivery.wait_id)

            assert len(await waits.deliver_once(observe)) == len(received) == 1


def test_adapter_configuration_requires_explicit_bounded_origin_and_options():
    for url in ("http://example.test", "https://example.test/path", "https://x:y@example.test"):
        with pytest.raises(ValueError):
            CloudflareBridge(url, TOKEN)
    for field, value in (
        ("rotate_after", 129),
        ("poll_seconds", True),
        ("request_timeout_seconds", 0),
    ):
        with pytest.raises(ValueError):
            WorkflowOptions(**{field: value})


@pytest.mark.asyncio
async def test_cancellation_drains_admitted_native_acknowledgment(pipeline, monkeypatch):
    drivers = bind(pipeline, adapter="cloudflare")
    binding = connection(pipeline, drivers).binding
    record(pipeline, drivers)
    report(pipeline)
    waits, app = driver(pipeline, drivers), FastAPI()
    entered, release, finished = Event(), Event(), Event()
    command = drivers.control.command

    def held(operation, *args, **kwargs):
        if operation == "orchestration.acknowledge_wakeup":
            entered.set()
            assert release.wait(10)
        result = command(operation, *args, **kwargs)
        finished.set()
        return result

    monkeypatch.setattr(drivers.control, "command", held)

    @app.post("/adapter/wake")
    async def wake():
        return {"accepted": True}

    with serving(app) as url:
        async with CloudflareBridge(url, TOKEN) as bridge:
            task = asyncio.create_task(bridge.deliver(binding, waits))
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                task.cancel()
                await asyncio.sleep(0)
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done() and not finished.is_set()
            finally:
                release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert finished.is_set()
            assert await bridge.deliver(binding, waits) == ()


@pytest.mark.asyncio
async def test_full_binding_refusal_and_historical_delivery_after_transfer(pipeline):
    drivers = bind(pipeline, adapter="cloudflare")
    binding = connection(pipeline, drivers).binding
    waited = record(pipeline, drivers)
    report(pipeline)
    waits, app, sent = driver(pipeline, drivers), FastAPI(), []

    @app.post("/adapter/wake")
    async def wake(request: Request):
        sent.append(await request.json())
        return {"accepted": True}

    with serving(app) as url:
        async with CloudflareBridge(url, TOKEN) as bridge:
            for wrong in (
                replace(binding, workspace_id=new_id()),
                replace(binding, configuration_digest=content_digest(b"another configuration")),
                replace(binding, actors={**binding.actors, "evaluator": new_id()}),
            ):
                with pytest.raises(ResearchError) as denied:
                    await bridge.deliver(wrong, waits)
                assert denied.value.code == ErrorCode.STALE_FENCE
                assert sent == []
                assert (
                    pipeline.client.get("wakeup_delivery", waited["wait"]["id"])["state"]
                    == "pending"
                )
            pause(pipeline)
            next_driver = successor(pipeline)
            intent, revision = request(pipeline, next_driver)
            transfer(pipeline, intent, revision)
            assert await bridge.deliver(binding, waits) == (waited["wait"]["id"],)
            assert len(sent) == 1 and sent[0]["binding"]["epoch"] == 1
