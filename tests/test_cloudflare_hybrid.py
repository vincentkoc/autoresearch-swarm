"""Real local Workflows and native research authority; no hosted deployment."""

import asyncio
import subprocess
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse

from research_swarm.domain.types import new_id
from research_swarm.orchestration.cloudflare import CloudflareBridge, WorkflowOptions
from research_swarm.orchestration.delivery import DeliveryKernel
from tests.test_cloudflare_native import ROOT, build_current_sources, server
from tests.test_cloudflare_native import pytestmark as pytestmark
from tests.test_delivery_gateway import prepare, setup
from tests.test_http_delivery import serving
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_orchestration_turns import fence
from tests.test_orchestration_waits import driver as wait_driver
from tests.test_orchestration_waits import record
from tests.test_search_observations import evaluate

CONTROL = "public-local-fixture-control-token-with-no-hosted-authority"


def until(predicate, *, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError("Local Workflow did not reach the requested durable state")


def post(client, path, payload):
    response = client.post(path, json=payload)
    assert response.status_code == 200, response.text[-2000:]
    return response.json()


def test_local_workflow_retries_rotates_and_recovers_around_real_native_evaluation(
    pipeline, tmp_path, monkeypatch
):
    build_current_sources()
    for command in (["npm", "run", "check:hybrid"], ["npm", "run", "test:hybrid"]):
        completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=60)
        assert completed.returncode == 0, (completed.stdout + completed.stderr)[-3000:]
    app, drivers, connected, issued, binding = setup(pipeline, adapter="cloudflare")
    prepared = prepare(pipeline, drivers)
    original = DeliveryKernel.execute
    original_wait = DeliveryKernel.wait
    calls = []
    delivered_waits = []

    def lost(self, work):
        outcome = original(self, work)
        calls.append(work.record_id)
        if len(calls) == 1:
            raise TimeoutError("synthetic loss after native command acceptance")
        return outcome

    monkeypatch.setattr(DeliveryKernel, "execute", lost)

    def observed_wait(self, work):
        result = original_wait(self, work)
        delivered_waits.append((work.record_id, result.state))
        return result

    monkeypatch.setattr(DeliveryKernel, "wait", observed_wait)
    start = {
        "binding": binding,
        "options": {"poll_seconds": 1, "rotate_after": 2, "request_timeout_seconds": 10},
    }
    with serving(app) as native_url:
        variables = {
            "NATIVE_DELIVERY_ORIGIN": native_url,
            "GATEWAY_TOKEN": issued.token,
            "CONTROL_TOKEN": CONTROL,
            "ALLOW_LOCAL_GATEWAY": "true",
        }
        with (
            server(tmp_path, sequence=1, variables=variables) as url,
            httpx.Client(
                base_url=url,
                headers={"authorization": "Bearer " + CONTROL},
                trust_env=False,
                timeout=30,
            ) as client,
        ):

            async def native_start():
                async with CloudflareBridge(url, CONTROL) as bridge:
                    identifiers = await asyncio.gather(
                        *(
                            bridge.start(connected.binding, WorkflowOptions(**start["options"]))
                            for _ in range(6)
                        )
                    )
                    status = await bridge.status(connected.binding)
                    assert status.generation >= 0
                    return [{"id": identifier} for identifier in identifiers]

            starts = asyncio.run(native_start())
            assert all(item == starts[0] for item in starts)
            conflict = {**start, "options": {**start["options"], "poll_seconds": 2}}
            assert client.post("/adapter/start", json=conflict).status_code == 409
            assert (
                client.post(
                    "/adapter/start", json=start, headers={"authorization": "Bearer wrong"}
                ).status_code
                == 401
            )
            until(lambda: len(calls) >= 2)
            assert calls == [prepared["id"]] * 2
            until(
                lambda: (
                    pipeline.client.get("orchestration_binding", connected.binding.campaign_id)[
                        "data"
                    ]["pending_turn_id"]
                    is None
                )
            )
            waited = record(pipeline, drivers, types=("evaluation.recorded",))["wait"]

            def carried():
                status = post(client, "/adapter/status", {"binding": binding})
                assert status["status"] != "errored", "Workflow errored during generation handoff"
                return (
                    status
                    if status["generation"] >= 1 and status["initial_pending"] is not None
                    else None
                )

            before = until(carried)
            assert before["initial_pending"]["record_id"] == waited["id"]
            hint = {
                "binding": binding,
                "actor_id": binding["actors"]["control"],
                "wait_id": waited["id"],
            }
            assert post(client, "/adapter/wake", hint) == {"accepted": True}
            # A hint cannot satisfy the native evaluation condition.
            assert pipeline.client.get("orchestration_wait", waited["id"])["state"] == "waiting"

        # Only this test's workerd process group closes; native research remains running.
        with (
            server(tmp_path, sequence=2, variables=variables) as url,
            httpx.Client(
                base_url=url,
                headers={"authorization": "Bearer " + CONTROL},
                trust_env=False,
                timeout=30,
            ) as client,
        ):
            after = post(client, "/adapter/status", {"binding": binding})
            assert after["generation"] >= before["generation"]
            assert after["initial_pending"]["record_id"] == waited["id"]
            assert post(client, "/adapter/start", start) == starts[0]

            def resumed():
                status = post(client, "/adapter/status", {"binding": binding})
                assert status["status"] != "errored", "Workflow failed to resume its saved history"
                return status if status["generation"] > after["generation"] else None

            resumed_status = until(resumed)
            assert resumed_status["initial_pending"]["record_id"] == waited["id"]
            receipt = evaluate(pipeline, pipeline.trial)
            assert receipt["state"] == "valid"
            # Deliberately omit a wakeup; durable polling must discover committed evidence.
            until(
                lambda: pipeline.client.get("orchestration_wait", waited["id"])["state"] == "ready"
            )
            until(lambda: (waited["id"], "settled") in delivered_waits)

            async def deliver_hint():
                async with CloudflareBridge(url, CONTROL) as bridge:
                    delivered = await bridge.deliver(
                        connected.binding, wait_driver(pipeline, drivers)
                    )
                    assert len(delivered) == 1
                    assert (
                        pipeline.client.get("wakeup_delivery", delivered[0])["state"] == "delivered"
                    )

            asyncio.run(deliver_hint())
            fence(pipeline)
            final = until(
                lambda: (
                    status
                    if (status := post(client, "/adapter/status", {"binding": binding}))["status"]
                    == "complete"
                    else None
                )
            )
            assert final["generation"] >= before["generation"]
            trial = pipeline.client.get("trial", pipeline.trial["id"])
            assert trial["state"] == "succeeded" and trial["data"]["resource_status"] == "closed"
            assert calls == [prepared["id"]] * 2

        unsupported = {**variables, "AUTHORITY_PROFILE": "durable-object-authority-v1"}
        with (
            server(tmp_path, sequence=3, variables=unsupported) as url,
            httpx.Client(
                base_url=url,
                headers={"authorization": "Bearer " + CONTROL},
                trust_env=False,
                timeout=30,
            ) as client,
        ):
            response = client.post("/adapter/start", json=start)
            assert response.status_code == 409
            assert response.json() == {"error": "ADAPTER_CAPABILITY_UNSUPPORTED"}

        redirects, forwarded = [], []
        target, redirector = FastAPI(), FastAPI()

        @target.post("/credential-target")
        async def receive():
            forwarded.append(True)
            return {}

        with serving(target) as target_url:

            @redirector.post("/v1/delivery/bootstrap")
            async def redirect(request: Request):
                assert request.headers["authorization"] == "Bearer " + issued.token
                redirects.append(True)
                return RedirectResponse(target_url + "/credential-target", status_code=307)

            with serving(redirector) as redirect_url:
                redirected_binding = {**binding, "campaign_id": new_id()}
                redirected_start = {**start, "binding": redirected_binding}
                redirected_vars = {**variables, "NATIVE_DELIVERY_ORIGIN": redirect_url}
                with (
                    server(tmp_path, sequence=4, variables=redirected_vars) as url,
                    httpx.Client(
                        base_url=url,
                        headers={"authorization": "Bearer " + CONTROL},
                        trust_env=False,
                        timeout=30,
                    ) as client,
                ):
                    post(client, "/adapter/start", redirected_start)
                    until(
                        lambda: (
                            post(client, "/adapter/status", {"binding": redirected_binding})[
                                "status"
                            ]
                            == "errored"
                        )
                    )
                assert redirects == [True] and forwarded == []
