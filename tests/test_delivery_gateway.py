from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import httpx
import pytest
from fastapi.testclient import TestClient

from research_swarm.api.delivery import GATEWAY_SCOPE, MAX_DELIVERY_BYTES, create_delivery_app
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.orchestration.delivery import DeliveryKernel
from research_swarm.orchestration.gateway_models import GatewayBinding
from research_swarm.orchestration.turns import DurableTurnDriver
from tests.test_drive_closures import report_action
from tests.test_http_delivery import serving
from tests.test_local_delivery import connection
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_orchestration_turns import CONFIG, bind, fence
from tests.test_orchestration_waits import record, report


def setup(pipeline, *, scopes=frozenset({GATEWAY_SCOPE})):
    drivers = bind(pipeline)
    connected = connection(pipeline, drivers)
    runtime = pipeline.runtime
    actor = Principal(id=new_id(), workspace_id=connected.binding.workspace_id, scopes=scopes)
    issued = runtime.credentials.issue(runtime.principal(), actor)
    app = create_delivery_app(runtime.credentials, {actor.id: connected})
    binding = GatewayBinding.from_reference(connected.binding).model_dump(mode="json")
    return app, drivers, connected, issued, binding


def headers(issued):
    return {"authorization": "Bearer " + issued.token}


def prepare(pipeline, drivers):
    return DurableTurnDriver(
        drivers.control, pipeline.campaign["id"], epoch=1, configuration_digest=CONFIG
    ).prepare_turn(1, lambda: report_action(pipeline))


def test_real_http_lost_ack_and_parallel_retries_deliver_one_saved_turn(pipeline, monkeypatch):
    app, drivers, connected, issued, binding = setup(pipeline)
    prepared = prepare(pipeline, drivers)
    execute = DeliveryKernel.execute
    calls = []

    def lost(self, work):
        result = execute(self, work)
        calls.append(work.record_id)
        if len(calls) == 1:
            raise TimeoutError("private-fixture-must-not-enter-provider-history")
        return result

    monkeypatch.setattr(DeliveryKernel, "execute", lost)
    with (
        serving(app) as url,
        httpx.Client(base_url=url, headers=headers(issued), trust_env=False) as client,
    ):
        boot = client.post(
            "/v1/delivery/bootstrap", json={"binding": binding, "recovery_id": new_id()}
        )
        assert boot.json() == {"schema_version": "1.0", "state": "active"}
        pending = client.post("/v1/delivery/inspect", json=binding).json()
        work = pending["work"]
        assert work["record_id"] == prepared["id"]
        failed = client.post("/v1/delivery/execute", json=work)
        assert failed.status_code == 503
        assert failed.json()["error"] == {"code": "DELIVERY_UNCERTAIN", "retryable": True}
        assert "private-fixture" not in failed.text
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(
                pool.map(lambda _: client.post("/v1/delivery/execute", json=work), range(6))
            )
        assert all(response.status_code == 200 for response in results)
        assert all(response.json() == results[0].json() for response in results)
        assert results[0].json()["disposition"] == "accepted"
        assert client.post("/v1/delivery/inspect", json=binding).json()["state"] == "idle"
    assert calls == [prepared["id"]] * 7
    row = pipeline.client.get("orchestration_binding", connected.binding.campaign_id)
    assert row["data"]["next_ordinal"] == 2


def test_wait_is_authoritative_and_binding_fence_retires_delivery(pipeline):
    app, drivers, _, issued, binding = setup(pipeline)
    waited = record(pipeline, drivers)
    with TestClient(app, headers=headers(issued)) as client:
        work = client.post("/v1/delivery/inspect", json=binding).json()["work"]
        assert work["kind"] == "wait" and work["record_id"] == waited["wait"]["id"]
        assert client.post("/v1/delivery/wait", json=work).json()["state"] == "pending"
        report(pipeline)
        ready = client.post("/v1/delivery/wait", json=work)
        assert ready.status_code == 200 and ready.json()["state"] == "settled"
        assert client.post("/v1/delivery/wait", json=work).json() == ready.json()
        fence(pipeline)
        assert client.post("/v1/delivery/inspect", json=binding).json()["state"] == "retired"


def test_gateway_authentication_scope_registration_and_revocation(pipeline):
    app, _, connected, issued, binding = setup(pipeline)
    runtime = pipeline.runtime
    with TestClient(app) as client:
        assert client.post("/v1/delivery/inspect", json=binding).status_code == 401
        assert (
            client.post(
                "/v1/delivery/inspect", json=binding, headers={"authorization": "Bearer invalid"}
            ).status_code
            == 401
        )
        wrong = dict(binding, campaign_id=new_id())
        assert (
            client.post("/v1/delivery/inspect", json=wrong, headers=headers(issued)).status_code
            == 403
        )
        for scopes in ({"read"}, {GATEWAY_SCOPE}):
            actor = Principal(
                id=new_id(), workspace_id=connected.binding.workspace_id, scopes=scopes
            )
            credential = runtime.credentials.issue(runtime.principal(), actor)
            assert (
                client.post(
                    "/v1/delivery/inspect", json=binding, headers=headers(credential)
                ).status_code
                == 403
            )
        runtime.credentials.revoke(runtime.principal(), issued.id)
        assert (
            client.post("/v1/delivery/inspect", json=binding, headers=headers(issued)).status_code
            == 401
        )
    with pytest.raises(ValueError, match="distinct"):
        create_delivery_app(runtime.credentials, {connected.binding.actors["control"]: connected})


def test_registered_actor_still_needs_gateway_scope(pipeline):
    app, _, _, issued, binding = setup(pipeline, scopes=frozenset({"read"}))
    with TestClient(app, headers=headers(issued)) as client:
        assert client.post("/v1/delivery/inspect", json=binding).status_code == 403


def test_registered_binding_does_not_authorize_a_different_route_or_epoch(pipeline):
    app, drivers, _, issued, binding = setup(pipeline)
    prepare(pipeline, drivers)
    with TestClient(app, headers=headers(issued)) as client:
        work = client.post("/v1/delivery/inspect", json=binding).json()["work"]
        altered = dict(work, binding=dict(work["binding"], epoch=2))
        assert client.post("/v1/delivery/execute", json=altered).status_code == 403
        altered = dict(work, actor_id=binding["actors"]["evaluator"])
        denied = client.post("/v1/delivery/execute", json=altered)
        assert denied.status_code == 403
        assert denied.json()["error"]["code"] == "AUTHORIZATION_DENIED"
        assert client.post("/v1/delivery/inspect", json=binding).json()["work"] == work


@pytest.mark.parametrize("code", [ErrorCode.TRANSPORT_UNAVAILABLE, ErrorCode.DELIVERY_UNCERTAIN])
def test_gateway_keeps_transport_uncertainty_retryable(pipeline, monkeypatch, code):
    app, _, _, issued, binding = setup(pipeline)

    def unavailable(*args):
        raise ResearchError(code, "private provider details", retryable=False)

    monkeypatch.setattr(DeliveryKernel, "inspect", unavailable)
    with TestClient(app, headers=headers(issued)) as client:
        response = client.post("/v1/delivery/inspect", json=binding)
        assert response.status_code == 503
        assert response.json()["error"] == {"code": code.value, "retryable": True}
        assert "private provider" not in response.text


def test_gateway_refuses_unbounded_ambiguous_and_extra_payloads_without_echo(pipeline, monkeypatch):
    app, _, _, issued, binding = setup(pipeline)
    sentinel = "private-fixture-payload"
    calls = []
    monkeypatch.setattr(DeliveryKernel, "inspect", lambda *args: calls.append(True))
    bodies = [
        '{"workspace_id":"a","workspace_id":"b"}',
        "x" * (MAX_DELIVERY_BYTES + 1),
        "[" * 2000 + "0" + "]" * 2000,
        json.dumps(dict(binding, token=sentinel)),
        json.dumps(dict(binding, epoch=True)),
        json.dumps(dict(binding, actors={"control": new_id(), "foreign": new_id()})),
    ]
    with TestClient(app, headers=headers(issued)) as client:
        for body in bodies:
            response = client.post("/v1/delivery/inspect", content=body)
            assert response.status_code == 422
            assert sentinel not in response.text
        response = client.post("/v1/delivery/" + sentinel, json=binding)
        assert response.status_code == 422 and sentinel not in response.text
    assert calls == []


@pytest.mark.asyncio
async def test_cancelled_gateway_request_drains_admitted_native_call(pipeline, monkeypatch):
    app, drivers, _, issued, binding = setup(pipeline)
    prepared = prepare(pipeline, drivers)
    original = DeliveryKernel.execute
    started, release = Event(), Event()

    def held(self, work):
        result = original(self, work)
        started.set()
        assert release.wait(5)
        return result

    monkeypatch.setattr(DeliveryKernel, "execute", held)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gateway.invalid",
        headers=headers(issued),
    ) as client:
        work = (await client.post("/v1/delivery/inspect", json=binding)).json()["work"]
        task = asyncio.create_task(client.post("/v1/delivery/execute", json=work))
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            await asyncio.sleep(0.02)
            assert not task.done(), "Cancellation abandoned an admitted native API call"
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        replay = await client.post("/v1/delivery/execute", json=work)
        assert replay.status_code == 200 and replay.json()["record_id"] == prepared["id"]
