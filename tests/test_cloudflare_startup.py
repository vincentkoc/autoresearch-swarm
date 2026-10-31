"""Actual workerd successor recovery uses the ordinary checkpoint/snapshot SDK."""

import asyncio
from contextlib import ExitStack

import httpx
import pytest

from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DriveAction, OrchestrationBinding
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.orchestration.cloudflare import CloudflareBridge, WorkflowOptions
from research_swarm.orchestration.delivery_contract import binding_reference, delivery_key
from research_swarm.orchestration.gateway_models import GatewayBinding
from research_swarm.orchestration.startup import recover_driver_start
from research_swarm.orchestration.turns import DurableTurnDriver
from research_swarm.workers.clocks import native_clock
from tests.test_cloudflare_hybrid import CONTROL, until
from tests.test_cloudflare_native import build_current_sources, server
from tests.test_cloudflare_native_source import TOKEN, Endpoint, control
from tests.test_cloudflare_research_admission import client_at, inspect
from tests.test_cloudflare_router import pytestmark as pytestmark
from tests.test_hosted_research import admit
from tests.test_http_delivery import serving
from tests.test_manifest import project as project
from tests.test_time_witnesses import source

OPTIONS = {"startup_profile": True}
CONFIG = content_digest(b"workerd-startup-driver")


def clients(lifetime, url, setup):
    return tuple(
        lifetime.enter_context(client_at(url, setup[f"{role}_token"], **OPTIONS))
        for role in ("operator", "driver", "successor", "next_evaluator")
    )


def prepare(client, campaign_id, *, epoch=2):
    driver = DurableTurnDriver(client, campaign_id, epoch=epoch, configuration_digest=CONFIG)
    return driver.prepare_turn(
        1,
        lambda: DriveAction(
            operation="report.create",
            payload={"campaign_id": campaign_id},
            idempotency_key="report",
        ),
    )


def test_successor_sdk_recovers_lost_snapshot_then_drives_after_authority_restart(
    tmp_path, project
):
    build_current_sources()
    descriptor = source().model_copy(update={"native_profile": native_clock().profile})
    with ExitStack() as lifetime:
        with server(tmp_path, sequence=1) as url, ExitStack() as connections:
            configuration = control(
                url, "describe", source=descriptor.model_dump(mode="json"), **OPTIONS
            )
            assert configuration["profile"] == "research-startup-witness-v1"
            endpoint = Endpoint(
                WitnessAuthorityScope.model_validate(configuration["authority"]), descriptor
            )
            origin = lifetime.enter_context(
                serving(create_time_witness_app(endpoint, witness_consumer_digest(TOKEN)))
            )
            setup = control(
                url,
                "provision",
                source=descriptor.model_dump(mode="json"),
                origin=origin,
                **OPTIONS,
            )
            assert "error" not in setup, setup
            operator, _, _, _ = clients(connections, url, setup)
            assert operator.capabilities()["recovery"]["clock"] == "authority-epoch"
            admitted = admit(operator, project)
            campaign = operator.get("campaign", admitted.campaign["id"])
            operator.command(
                "orchestration.bind",
                {
                    "campaign_id": campaign["id"],
                    "adapter": "cloudflare-workflows",
                    "configuration_digest": CONFIG,
                    "routes": {"control": setup["driver_id"], "evaluator": setup["evaluator_id"]},
                },
                idempotency_key="bind",
                expected_revision=campaign["revision"],
            )
            campaign = operator.command(
                "campaign.pause",
                {
                    "campaign_id": campaign["id"],
                    "mode": "drain",
                },
                idempotency_key="pause",
                expected_revision=campaign["revision"],
            )["result"]
            binding = operator.get("orchestration_binding", campaign["id"])
            before = inspect(url, **OPTIONS)
            trial = operator.get("trial", admitted.trial["result"]["id"])
            job = operator.get("job", admitted.job["id"])
            transferred = operator.command(
                "orchestration.transfer",
                {
                    "transfer_id": new_id(),
                    "campaign_id": campaign["id"],
                    "campaign_revision": campaign["revision"],
                    "epoch": 1,
                    "adapter": "cloudflare-workflows",
                    "configuration_digest": CONFIG,
                    "routes": {
                        "control": setup["successor_id"],
                        "evaluator": setup["next_evaluator_id"],
                    },
                    "reason": "Recover the queued fixture through the ordinary SDK",
                },
                idempotency_key="transfer",
                expected_revision=binding["revision"],
            )["result"]
            starts = transferred["data"]["start_ids"]

        with (
            server(tmp_path, sequence=2, fixture_delivery_profile="successor") as url,
            ExitStack() as connections,
        ):
            operator, driver, successor, evaluator = clients(connections, url, setup)
            with pytest.raises(ResearchError) as pending:
                prepare(successor, campaign["id"])
            assert pending.value.code == ErrorCode.RESOURCE_UNAVAILABLE
            successor.transport.http.headers["x-fixture-drop-command"] = "stream.create_snapshot"
            try:
                with pytest.raises(ResearchError) as lost:
                    recover_driver_start(
                        successor, starts["control"], recovery_id="stable-recovery"
                    )
                assert lost.value.code == ErrorCode.DELIVERY_UNCERTAIN
            finally:
                del successor.transport.http.headers["x-fixture-drop-command"]
            assert inspect(url, **OPTIONS)["recovery_counts"]["active"] == 1
            recover_driver_start(successor, starts["control"], recovery_id="stable-recovery")
            assert operator.get("orchestration_start", starts["evaluator"])["state"] == "pending"
            reference = binding_reference(
                OrchestrationBinding.model_validate(
                    operator.get("orchestration_binding", campaign["id"])["data"]
                )
            )
            with httpx.Client(base_url=url, trust_env=False, timeout=30) as delivery:
                response = delivery.post(
                    "/v1/delivery/bootstrap",
                    json={
                        "binding": GatewayBinding.from_reference(reference).model_dump(mode="json"),
                        "recovery_id": new_id(),
                    },
                    headers={"authorization": "Bearer " + setup["gateway_token"]},
                )
                assert response.status_code == 501
                assert response.json()["error"]["code"] == "CAPABILITY_UNAVAILABLE"
            recover_driver_start(evaluator, starts["evaluator"], recovery_id="evaluator-recovery")
            turn = prepare(successor, campaign["id"])
            workflow = tmp_path / "workflow"
            workflow.mkdir()
            variables = {
                "AUTHORITY_PROFILE": "hosted-do-witness-v1",
                "HOSTED_DELIVERY_ORIGIN": url,
                "GATEWAY_TOKEN": setup["gateway_token"],
                "CONTROL_TOKEN": CONTROL,
                "ALLOW_LOCAL_GATEWAY": "true",
            }
            with server(workflow, sequence=1, variables=variables) as workflow_url:

                async def start():
                    async with CloudflareBridge(workflow_url, CONTROL) as bridge:
                        return await asyncio.gather(
                            *(
                                bridge.start(
                                    reference,
                                    WorkflowOptions(
                                        poll_seconds=1, rotate_after=2, request_timeout_seconds=10
                                    ),
                                )
                                for _ in range(3)
                            )
                        )

                ids = asyncio.run(start())
                assert ids == [ids[0]] * 3
                until(lambda: successor.get("drive_turn", turn["id"])["state"] != "prepared")
                delivered = operator.list(
                    RecordQuery(kind="drive_receipt", campaign_id=campaign["id"])
                )["items"]
                assert len(delivered) == 1 and delivered[0]["data"]["disposition"] == "accepted"
            execution_key = delivery_key("turn", turn["id"])
            witness_calls = len(endpoint.requests)
            receipt = successor.command(
                "orchestration.execute_turn", {"turn_id": turn["id"]}, idempotency_key=execution_key
            )
            assert len(endpoint.requests) == witness_calls
            assert receipt["result"]["response"]["disposition"] == "accepted"
            assert len(operator.list(RecordQuery(kind="report"))["items"]) == 1
            assert len(operator.list(RecordQuery(kind="orchestration_start_ack"))["items"]) == 2
            assert operator.get("trial", trial["id"]) == trial
            assert operator.get("job", job["id"]) == job
            with pytest.raises(ResearchError) as retired:
                prepare(driver, campaign["id"], epoch=1)
            assert retired.value.code == ErrorCode.STALE_FENCE
            snapshot = inspect(url, **OPTIONS)
            assert snapshot["transfer_invariants"] == before["transfer_invariants"]
            assert snapshot["recovery_counts"] == {"snapshots": 2, "items": 0, "active": 0}
            assert snapshot["settlements"] == before["settlements"] == []

        with server(tmp_path, sequence=3) as url, ExitStack() as connections:
            operator, _, successor, evaluator = clients(connections, url, setup)
            endpoint.mode = "unavailable"
            successor.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
            count = len(endpoint.requests)
            recover_driver_start(successor, starts["control"], recovery_id="already-started")
            recover_driver_start(evaluator, starts["evaluator"], recovery_id="already-started")
            assert (
                successor.command(
                    "orchestration.execute_turn",
                    {"turn_id": turn["id"]},
                    idempotency_key=execution_key,
                )
                == receipt
            )
            assert len(endpoint.requests) == count and inspect(url, **OPTIONS) == snapshot
            assert operator.get("campaign", campaign["id"])["state"] == "paused"
