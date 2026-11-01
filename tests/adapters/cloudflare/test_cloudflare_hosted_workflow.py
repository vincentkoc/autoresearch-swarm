"""Two real workerd processes deliver saved work through a witnessed DO authority."""

import asyncio
from contextlib import ExitStack

import httpx

from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import content_digest, record_digest
from research_swarm.domain.orchestration import DriveAction, OrchestrationBinding
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.orchestration.cloudflare import CloudflareBridge, WorkflowOptions
from research_swarm.orchestration.delivery_contract import binding_reference
from research_swarm.orchestration.gateway_models import GatewayBinding
from research_swarm.orchestration.turns import DurableTurnDriver
from research_swarm.orchestration.waits import DurableWaitDriver
from research_swarm.workers.clocks import native_clock
from tests.adapters.cloudflare.hosted.test_hosted_research import admit
from tests.adapters.cloudflare.test_cloudflare_hybrid import CONTROL, post, until
from tests.adapters.cloudflare.test_cloudflare_native import build_current_sources, port, server
from tests.adapters.cloudflare.test_cloudflare_native_source import TOKEN, control
from tests.adapters.cloudflare.test_cloudflare_orchestration import OPTIONS, ReceiptOutage
from tests.adapters.cloudflare.test_cloudflare_research_admission import client_at, inspect
from tests.adapters.cloudflare.test_cloudflare_router import pytestmark as pytestmark
from tests.orchestration.test_http_delivery import serving
from tests.projects.test_manifest import project as project
from tests.workers.test_time_witnesses import source

CONFIG = content_digest(b"workerd-workflow-delivery")


def connect(lifetime, url, setup, role):
    return lifetime.enter_context(client_at(url, setup[f"{role}_token"], **OPTIONS))


def workflow_client(lifetime, url):
    return lifetime.enter_context(
        httpx.Client(
            base_url=url,
            headers={"authorization": "Bearer " + CONTROL},
            trust_env=False,
            timeout=30,
        )
    )


def test_workflow_and_authority_restart_preserve_original_turn_and_wait(tmp_path, project):
    build_current_sources()
    authority, workflow = tmp_path / "authority", tmp_path / "workflow"
    authority.mkdir()
    workflow.mkdir()
    stable_port = port()
    descriptor = source().model_copy(update={"native_profile": native_clock().profile})
    with ExitStack() as lifetime:
        with (
            server(authority, sequence=1, listen_port=stable_port) as authority_url,
            ExitStack() as connections,
        ):
            description = control(
                authority_url, "describe", source=descriptor.model_dump(mode="json"), **OPTIONS
            )
            endpoint = ReceiptOutage(
                WitnessAuthorityScope.model_validate(description["authority"]), descriptor
            )
            origin = lifetime.enter_context(
                serving(create_time_witness_app(endpoint, witness_consumer_digest(TOKEN)))
            )
            setup = control(
                authority_url,
                "provision",
                source=descriptor.model_dump(mode="json"),
                origin=origin,
                **OPTIONS,
            )
            assert "error" not in setup, setup
            operator = connect(connections, authority_url, setup, "operator")
            driver = connect(connections, authority_url, setup, "driver")
            admitted = admit(operator, project)
            campaign = operator.get("campaign", admitted.campaign["id"])
            bound = operator.command(
                "orchestration.bind",
                {
                    "campaign_id": campaign["id"],
                    "adapter": "cloudflare-workflows",
                    "configuration_digest": CONFIG,
                    "routes": {"control": setup["driver_id"], "evaluator": setup["evaluator_id"]},
                },
                idempotency_key="binding",
                expected_revision=campaign["revision"],
            )["result"]
            reference = binding_reference(OrchestrationBinding.model_validate(bound["data"]))
            binding = GatewayBinding.from_reference(reference).model_dump(mode="json")
            saved = DurableTurnDriver(
                driver, campaign["id"], epoch=1, configuration_digest=CONFIG
            ).prepare_turn(
                1,
                lambda: DriveAction(
                    operation="report.create",
                    payload={"campaign_id": campaign["id"]},
                    idempotency_key="original-report",
                ),
            )
            with httpx.Client(base_url=authority_url, trust_env=False, timeout=30) as delivery:
                # Caller profile headers cannot redirect this fixed fixture authority.
                headers = {
                    "authorization": "Bearer " + setup["gateway_token"],
                    "x-fixture-native-source": "false",
                    "x-fixture-scientific-profile": "true",
                }
                response = delivery.post("/v1/delivery/inspect", json=binding, headers=headers)
                assert response.status_code == 200, response.text
                assert response.json()["work"]["record_id"] == saved["id"]
                assert (
                    delivery.post(
                        "/v1/delivery/inspect", json={**binding, "epoch": 2}, headers=headers
                    ).status_code
                    == 403
                )
                assert (
                    delivery.post(
                        "/v1/delivery/inspect",
                        json=binding,
                        headers={"authorization": "Bearer " + setup["operator_token"]},
                    ).status_code
                    == 403
                )

            variables = {
                "AUTHORITY_PROFILE": "hosted-do-witness-v1",
                "HOSTED_DELIVERY_ORIGIN": authority_url,
                "GATEWAY_TOKEN": setup["gateway_token"],
                "CONTROL_TOKEN": CONTROL,
                "ALLOW_LOCAL_GATEWAY": "true",
            }
            options = WorkflowOptions(poll_seconds=1, rotate_after=2, request_timeout_seconds=10)
            before = len(endpoint.requests)
            endpoint.fail_at = before + 2  # Report commits; its delivery receipt does not.
            with server(workflow, sequence=1, variables=variables) as url, ExitStack() as clients:
                client = workflow_client(clients, url)

                async def start():
                    async with CloudflareBridge(url, CONTROL) as bridge:
                        return await asyncio.gather(
                            *(bridge.start(reference, options) for _ in range(4))
                        )

                identifiers = asyncio.run(start())
                assert identifiers == [identifiers[0]] * 4
                until(lambda: driver.get("drive_turn", saved["id"])["state"] != "prepared")
                receipts = operator.list(RecordQuery(kind="drive_receipt"))["items"]
                assert len(receipts) == 1
                original_receipt = operator.get("drive_receipt", receipts[0]["id"])
                assert original_receipt["data"]["disposition"] == "accepted"
                assert len(operator.list(RecordQuery(kind="report"))["items"]) == 1
                assert endpoint.fail_at is None and len(endpoint.requests) == before + 3
                bound = driver.get("orchestration_binding", campaign["id"])
                watermark = driver.context(campaign["id"])["watermark"]
                waited = driver.command(
                    "orchestration.record_wait",
                    {
                        "wait_id": new_id(),
                        "campaign_id": campaign["id"],
                        "epoch": 1,
                        "configuration_digest": CONFIG,
                        "condition": {
                            "observed_watermark": watermark,
                            "event_types": ["report.created"],
                        },
                    },
                    idempotency_key="event-wait",
                    expected_revision=bound["revision"],
                )["result"]["wait"]

                def carried():
                    status = post(client, "/adapter/status", {"binding": binding})
                    assert status["status"] != "errored", status
                    pending = status["initial_pending"]
                    return status if pending and pending["record_id"] == waited["id"] else None

                checkpoint = until(carried)
            state_before = inspect(authority_url, **OPTIONS)

        # Both owned processes closed. The witness source and origin stay unchanged.
        with (
            server(authority, sequence=2, listen_port=stable_port) as reopened_url,
            ExitStack() as connections,
        ):
            assert reopened_url == authority_url
            operator = connect(connections, reopened_url, setup, "operator")
            driver = connect(connections, reopened_url, setup, "driver")
            assert inspect(reopened_url, **OPTIONS) == state_before
            with server(workflow, sequence=2, variables=variables) as url, ExitStack() as clients:
                client = workflow_client(clients, url)
                restarted = post(client, "/adapter/status", {"binding": binding})
                assert restarted["generation"] >= checkpoint["generation"]
                assert restarted["initial_pending"]["record_id"] == waited["id"]
                assert (
                    post(
                        client,
                        "/adapter/start",
                        {"binding": binding, "options": options.model_dump(mode="json")},
                    )["id"]
                    == identifiers[0]
                )
                hint = {
                    "binding": binding,
                    "actor_id": setup["driver_id"],
                    "wait_id": waited["id"],
                }
                assert post(client, "/adapter/wake", hint) == {"accepted": True}
                assert driver.get("orchestration_wait", waited["id"])["state"] == "waiting"
                hinted = post(client, "/adapter/status", {"binding": binding})

                def hint_retired():
                    status = post(client, "/adapter/status", {"binding": binding})
                    assert status["status"] != "errored", status
                    assert driver.get("orchestration_wait", waited["id"])["state"] == "waiting"
                    pending = status["initial_pending"]
                    return (
                        status
                        if status["generation"] > hinted["generation"]
                        and pending is not None
                        and pending["record_id"] == waited["id"]
                        else None
                    )

                # Retire the generation that received the false hint before the
                # real event, so a queued notification cannot explain recovery.
                unhinted = until(hint_retired)
                operator.command(
                    "report.create", {"campaign_id": campaign["id"]}, idempotency_key="event"
                )
                # No matching hint: durable polling must find the committed event.
                until(lambda: driver.get("orchestration_wait", waited["id"])["state"] == "ready")

                def polled():
                    status = post(client, "/adapter/status", {"binding": binding})
                    assert status["status"] != "errored", status
                    return (
                        status
                        if status["generation"] > unhinted["generation"]
                        and status["initial_pending"] is None
                        else None
                    )

                # Readiness itself is created by the event transaction. A later
                # generation must also prove the Workflow settled its carried wait.
                until(polled)
                ready = driver.get("wait_readiness", waited["id"])
                assert ready["data"]["cause"] == "event" and ready["data"]["watermark"] > watermark
                wakeup = driver.get("wakeup_delivery", waited["id"])

                async def deliver():
                    async with CloudflareBridge(url, CONTROL) as bridge:
                        waits = DurableWaitDriver(
                            driver, campaign["id"], epoch=1, actor_id=setup["driver_id"]
                        )
                        assert await bridge.deliver(reference, waits) == (waited["id"],)
                        assert await bridge.deliver(reference, waits) == ()

                asyncio.run(deliver())
                driver.command(
                    "orchestration.acknowledge_wakeup",
                    {"delivery_id": waited["id"], "campaign_id": campaign["id"], "epoch": 1},
                    idempotency_key=record_digest(
                        "research.wakeup-delivery-v1", {"delivery_id": waited["id"]}
                    ),
                    expected_revision=wakeup["revision"],
                )
                assert driver.get("orchestration_wait", waited["id"])["state"] == "ready"
                bound = operator.get("orchestration_binding", campaign["id"])
                operator.command(
                    "orchestration.fence",
                    {"campaign_id": campaign["id"], "epoch": 1, "reason": "Fixture complete"},
                    idempotency_key="fence",
                    expected_revision=bound["revision"],
                )
                until(
                    lambda: (
                        post(client, "/adapter/status", {"binding": binding})["status"]
                        == "complete"
                    )
                )
                assert operator.get("drive_receipt", original_receipt["id"]) == original_receipt
                assert len(operator.list(RecordQuery(kind="drive_receipt"))["items"]) == 1
                assert len(operator.list(RecordQuery(kind="report"))["items"]) == 2

            changed = {**variables, "HOSTED_DELIVERY_ORIGIN": "https://changed.example.invalid"}
            with server(workflow, sequence=3, variables=changed) as url, ExitStack() as clients:
                client = workflow_client(clients, url)
                assert (
                    client.post(
                        "/adapter/start",
                        json={"binding": binding, "options": options.model_dump(mode="json")},
                    ).status_code
                    == 409
                )
                assert operator.get("drive_receipt", original_receipt["id"]) == original_receipt
