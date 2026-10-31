"""Actual workerd transfer preserves queued research through lost replies and restart."""

from contextlib import ExitStack

import pytest

from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.workers.clocks import native_clock
from tests.test_cloudflare_evaluations import lost_reply
from tests.test_cloudflare_native import build_current_sources, server
from tests.test_cloudflare_native_source import TOKEN, control
from tests.test_cloudflare_orchestration import CONFIG, ReceiptOutage, prepare
from tests.test_cloudflare_research_admission import client_at, inspect
from tests.test_cloudflare_router import pytestmark as pytestmark
from tests.test_hosted_research import admit
from tests.test_http_delivery import serving
from tests.test_manifest import project as project
from tests.test_time_witnesses import source

OPTIONS = {"transfer_profile": True}


def clients(lifetime, url, setup):
    return tuple(
        lifetime.enter_context(client_at(url, setup[f"{role}_token"], **OPTIONS))
        for role in ("operator", "driver", "successor")
    )


def test_transfer_recovers_original_target_and_replays_offline_after_workerd_restart(
    tmp_path, project
):
    build_current_sources()
    descriptor = source().model_copy(update={"native_profile": native_clock().profile})
    with ExitStack() as lifetime:
        with server(tmp_path, sequence=1) as url, ExitStack() as connections:
            configuration = control(
                url, "describe", source=descriptor.model_dump(mode="json"), **OPTIONS
            )
            assert configuration["profile"] == "research-transfer-witness-v1"
            endpoint = ReceiptOutage(
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
            operator, driver, _ = clients(connections, url, setup)
            capabilities = operator.capabilities()
            assert capabilities["orchestration"]["transfer"]["successor_startup"] is False
            assert "orchestration.acknowledge_start" not in capabilities["commands"]
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
                idempotency_key="binding",
                expected_revision=campaign["revision"],
            )
            turn = prepare(driver, campaign["id"], "report.create", {"campaign_id": campaign["id"]})
            endpoint.fail_at = len(endpoint.requests) + 2
            with pytest.raises(ResearchError) as interrupted:
                driver.command(
                    "orchestration.execute_turn", {"turn_id": turn["id"]}, idempotency_key="execute"
                )
            assert interrupted.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
            assert operator.list(RecordQuery(kind="drive_receipt"))["items"] == []
            assert len(operator.list(RecordQuery(kind="report"))["items"]) == 1
            assert driver.get("drive_turn", turn["id"])["state"] == "prepared"
            campaign = operator.command(
                "campaign.pause",
                {"campaign_id": campaign["id"], "mode": "drain"},
                idempotency_key="pause",
                expected_revision=campaign["revision"],
            )["result"]
            assert campaign["state"] == "paused"
            binding = operator.get("orchestration_binding", campaign["id"])
            revision = binding["revision"]
            payload = {
                "transfer_id": new_id(),
                "campaign_id": campaign["id"],
                "campaign_revision": campaign["revision"],
                "epoch": 1,
                "adapter": "local-successor",
                "configuration_digest": content_digest(b"workerd-successor"),
                "routes": {
                    "control": setup["successor_id"],
                    "evaluator": setup["next_evaluator_id"],
                },
                "reason": "Move the queued fixture without moving execution authority",
            }
            before = inspect(url, **OPTIONS)
            trial = operator.get("trial", admitted.trial["result"]["id"])
            job = operator.get("job", admitted.job["id"])
            witness_count = len(endpoint.requests)
            operator.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
            with pytest.raises(ResearchError) as unavailable:
                operator.command(
                    "orchestration.transfer",
                    payload,
                    idempotency_key="transfer",
                    expected_revision=revision,
                )
            assert unavailable.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
            assert len(endpoint.requests) == witness_count
            assert operator.get("orchestration_binding", campaign["id"]) == binding
            assert operator.list(RecordQuery(kind="orchestration_checkpoint"))["items"] == []
            del operator.transport.http.headers["x-fixture-r2-fault"]
            lost_reply(
                operator,
                "orchestration.transfer",
                payload,
                idempotency_key="transfer",
                expected_revision=revision,
            )
            transferred = operator.get("orchestration_transfer", payload["transfer_id"])
            assert transferred["data"]["pending_turn_outcome_kind"] == "drive_receipt"
            assert transferred["data"]["pending_turn_outcome_id"] == turn["id"]
            receipt = operator.get("drive_receipt", turn["id"])
            assert receipt["data"]["disposition"] == "accepted"
            assert len(operator.list(RecordQuery(kind="report"))["items"]) == 1
            assert len(operator.list(RecordQuery(kind="orchestration_checkpoint"))["items"]) == 1
            assert len(endpoint.requests) == witness_count + 1
            snapshot = inspect(url, **OPTIONS)
            assert snapshot["transfer_invariants"] == before["transfer_invariants"]
            assert snapshot["transfer_invariants"]["native_counts"] == {"worker": 0, "attempt": 0}
            assert snapshot["settlements"] == before["settlements"] == []

        with server(tmp_path, sequence=2) as url, ExitStack() as connections:
            operator, driver, successor = clients(connections, url, setup)
            assert inspect(url, **OPTIONS) == snapshot
            endpoint.mode = "unavailable"
            operator.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
            witness_count = len(endpoint.requests)
            accepted = operator.command(
                "orchestration.transfer",
                payload,
                idempotency_key="transfer",
                expected_revision=revision,
            )
            assert accepted["result"] == transferred
            assert len(endpoint.requests) == witness_count and inspect(url, **OPTIONS) == snapshot
            endpoint.mode = "normal"
            again = operator.command(
                "orchestration.transfer",
                payload,
                idempotency_key="same-transfer-new-key",
                expected_revision=revision,
            )
            assert again["result"] == transferred and len(endpoint.requests) == witness_count + 1
            assert operator.get("trial", trial["id"]) == trial
            assert operator.get("job", job["id"]) == job
            assert operator.get("campaign", campaign["id"])["state"] == "paused"
            binding = operator.get("orchestration_binding", campaign["id"])
            assert binding["data"]["epoch"] == 2
            for start_id in transferred["data"]["start_ids"].values():
                assert operator.get("orchestration_start", start_id)["state"] == "pending"
            with pytest.raises(ResearchError) as startup:
                successor.command(
                    "orchestration.prepare_turn",
                    {
                        "campaign_id": campaign["id"],
                        "epoch": 2,
                        "ordinal": 1,
                        "configuration_digest": payload["configuration_digest"],
                        "planning_watermark": 0,
                        "action": {
                            "operation": "report.create",
                            "payload": {"campaign_id": campaign["id"]},
                            "idempotency_key": "next-report",
                        },
                    },
                    idempotency_key="startup-unavailable",
                    expected_revision=binding["revision"],
                )
            assert startup.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
            assert operator.get("drive_receipt", turn["id"]) == receipt
            final = inspect(url, **OPTIONS)
            assert final["transfer_invariants"] == before["transfer_invariants"]
            assert final["commands"] == snapshot["commands"] + 1
            assert final["events"] == snapshot["events"]
