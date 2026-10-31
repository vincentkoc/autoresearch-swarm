"""Saved evaluator actions and event waits survive actual authority-process restart."""

from contextlib import ExitStack

import pytest

from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.control.queries import RecordQuery
from research_swarm.control.workers import WorkerData
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DriveAction
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.orchestration.turns import DurableTurnDriver
from research_swarm.workers.clocks import native_clock
from research_swarm.workers.local import LocalWorker
from tests.test_cloudflare_evaluations import closed, lost_reply
from tests.test_cloudflare_native import build_current_sources, server
from tests.test_cloudflare_native_source import TOKEN, Endpoint, control
from tests.test_cloudflare_research_admission import client_at, inspect
from tests.test_cloudflare_router import pytestmark as pytestmark
from tests.test_hosted_research import admit, enroll_and_claim
from tests.test_hosted_result_publication import EVALUATE, TRAIN
from tests.test_http_delivery import serving
from tests.test_manifest import project as project
from tests.test_time_witnesses import source

CONFIG = content_digest(b"workerd-saved-evaluator")
OPTIONS = {"orchestration_profile": True}


class ReceiptOutage(Endpoint):
    fail_at = None

    def observe(self, request):
        if self.fail_at == len(self.requests) + 1:
            self.mode = "unavailable"
            try:
                return super().observe(request)
            finally:
                self.mode, self.fail_at = "normal", None
        return super().observe(request)


def clients(lifetime, url, setup):
    return tuple(
        lifetime.enter_context(client_at(url, setup[f"{role}_token"], **OPTIONS))
        for role in ("operator", "worker", "driver", "evaluator")
    )


def prepare(client, campaign_id, operation, payload, *, revision=None):
    binding = client.get("orchestration_binding", campaign_id)
    ordinal = binding["data"]["next_ordinal"]
    return DurableTurnDriver(
        client, campaign_id, epoch=1, configuration_digest=CONFIG
    ).prepare_turn(
        ordinal,
        lambda: DriveAction(
            operation=operation,
            payload=payload,
            expected_revision=revision,
            idempotency_key=f"target-{ordinal}",
        ),
    )


def test_saved_evaluator_receipts_and_event_waits_survive_workerd_restart(tmp_path, project):
    build_current_sources()
    (project / "train.py").write_text(TRAIN)
    (project / "evaluate.py").write_text(EVALUATE)
    manifest = project / "research.toml"
    manifest.write_text(
        manifest.read_text()
        .replace("timeout_ms = 1000", "timeout_ms = 10000")
        .replace("[evaluation]", '[evaluation]\nexpected_members = ["fixture-panel"]')
    )
    descriptor = source().model_copy(update={"native_profile": native_clock().profile})
    with ExitStack() as lifetime:
        with server(tmp_path, sequence=1) as url, ExitStack() as connections:
            config = control(url, "describe", source=descriptor.model_dump(mode="json"), **OPTIONS)
            assert config["profile"] == "research-orchestration-witness-v1"
            endpoint = ReceiptOutage(
                WitnessAuthorityScope.model_validate(config["authority"]), descriptor
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
            operator, worker_client, driver, evaluator = clients(connections, url, setup)
            assert driver.capabilities()["orchestration"]["waits"] == "event-only"
            admitted = admit(operator, project)
            claimed, _ = enroll_and_claim(
                operator,
                admitted,
                setup["enrollment"]["principal_id"],
                worker_client=worker_client,
                project_ids=[admitted.project["id"]],
            )
            worker = WorkerData.model_validate(
                operator.get("worker", claimed["result"]["data"]["dispatch"]["worker_id"])["data"]
            )
            runner = LocalWorker(
                worker_client,
                worker,
                root=tmp_path / "worker",
                machine_registry=tmp_path / "machine" / "resources.sqlite",
            )
            trained = runner.execute(claimed["result"]["id"])
            assert trained["state"] == "succeeded"
            train_entry = closed(runner, trained["id"])
            campaign = operator.get("campaign", admitted.campaign["id"])
            operator.command(
                "orchestration.bind",
                {
                    "campaign_id": campaign["id"],
                    "configuration_digest": CONFIG,
                    "adapter": "cloudflare-workflows",
                    "routes": {"control": setup["driver_id"], "evaluator": setup["evaluator_id"]},
                },
                idempotency_key="binding",
                expected_revision=campaign["revision"],
            )
            jobs = operator.list(RecordQuery(kind="job", state="ready"))["items"]
            assert len(jobs) == 1
            job = operator.get("job", jobs[0]["id"])
            saved_authorization = prepare(
                driver,
                campaign["id"],
                "evaluation.authorize",
                {"job_id": job["id"], "evaluator_id": setup["evaluator_id"]},
                revision=job["revision"],
            )
            auth_target = {"turn_id": saved_authorization["id"]}
            before = len(endpoint.requests)
            endpoint.fail_at = before + 4
            with pytest.raises(ResearchError) as interrupted:
                driver.command(
                    "orchestration.execute_turn", auth_target, idempotency_key="authorize-delivery"
                )
            assert interrupted.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
            assert len(endpoint.requests) == before + 4
            assert driver.get("drive_turn", saved_authorization["id"])["state"] == "prepared"
            assert driver.list(RecordQuery(kind="drive_receipt"))["items"] == []
            requests = operator.list(RecordQuery(kind="evaluation_request"))["items"]
            assert len(requests) == 1
            authorized = operator.get("evaluation_request", requests[0]["id"])

        with server(tmp_path, sequence=2) as url, ExitStack() as connections:
            operator, worker_client, driver, evaluator = clients(connections, url, setup)

            def cannot_plan():
                raise AssertionError("Restart must recover the original saved action")

            recovered = DurableTurnDriver(
                driver, campaign["id"], epoch=1, configuration_digest=CONFIG
            ).prepare_turn(1, cannot_plan)
            assert recovered == saved_authorization
            driver.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
            before = len(endpoint.requests)
            auth_receipt = driver.command(
                "orchestration.execute_turn", auth_target, idempotency_key="authorize-delivery"
            )
            assert auth_receipt["result"]["response"]["result"] == authorized
            assert len(endpoint.requests) == before + 1
            del driver.transport.http.headers["x-fixture-r2-fault"]
            runner.client = worker_client
            scored_attempt = runner.claim(operator.get("job", job["id"]))
            scored = runner.execute(scored_attempt["id"])
            assert scored["state"] == "succeeded"
            score_entry = closed(runner, scored["id"])
            saved_completion = prepare(
                evaluator,
                campaign["id"],
                "evaluation.complete",
                {"request_id": authorized["id"], "attempt_id": scored["id"]},
            )
            completion_target = {"turn_id": saved_completion["id"]}
            lost_reply(
                evaluator,
                "orchestration.execute_turn",
                completion_target,
                idempotency_key="complete-delivery",
            )
            completion_receipt = evaluator.command(
                "orchestration.execute_turn", completion_target, idempotency_key="complete-delivery"
            )
            receipt = completion_receipt["result"]["response"]["result"]
            assert receipt["state"] == "valid"
            assert receipt["data"]["receipt"]["evaluator_id"] == setup["evaluator_id"]
            assert len(driver.list(RecordQuery(kind="drive_receipt"))["items"]) == 2

            observed = driver.context(campaign["id"])["watermark"]
            binding = driver.get("orchestration_binding", campaign["id"])
            wait_request = {
                "wait_id": new_id(),
                "campaign_id": campaign["id"],
                "epoch": 1,
                "configuration_digest": CONFIG,
                "condition": {"observed_watermark": observed, "event_types": ["report.created"]},
            }
            waited = driver.command(
                "orchestration.record_wait",
                wait_request,
                idempotency_key="wait",
                expected_revision=binding["revision"],
            )
            assert waited["result"]["wait"]["state"] == "waiting"
            report = operator.command(
                "report.create", {"campaign_id": campaign["id"]}, idempotency_key="wake-report"
            )["result"]
            ready = driver.command(
                "orchestration.reconcile_wait",
                {"wait_id": wait_request["wait_id"]},
                idempotency_key="ready",
            )
            delivery = ready["result"]["delivery"]
            assert ready["result"]["readiness"]["data"]["cause"] == "event"
            ack_request = {"delivery_id": delivery["id"], "campaign_id": campaign["id"], "epoch": 1}
            acknowledgment = driver.command(
                "orchestration.acknowledge_wakeup",
                ack_request,
                idempotency_key="ack",
                expected_revision=delivery["revision"],
            )
            assert driver.get("orchestration_wait", wait_request["wait_id"])["state"] == "ready"
            before, calls = inspect(url, **OPTIONS), len(endpoint.requests)
            assert before["evidence_counts"] == {
                "run_result": 2,
                "evaluation_receipt": 1,
                "promotion": 0,
                "artifact_read_session": 0,
            }
            assert len(before["settlements"]) == 2
            endpoint.mode = "unavailable"

        with server(tmp_path, sequence=3) as url, ExitStack() as connections:
            operator, worker_client, driver, evaluator = clients(connections, url, setup)
            for client in (operator, worker_client, driver, evaluator):
                client.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
            assert (
                driver.command(
                    "orchestration.execute_turn", auth_target, idempotency_key="authorize-delivery"
                )
                == auth_receipt
            )
            assert (
                evaluator.command(
                    "orchestration.execute_turn",
                    completion_target,
                    idempotency_key="complete-delivery",
                )
                == completion_receipt
            )
            assert (
                driver.command(
                    "orchestration.record_wait",
                    wait_request,
                    idempotency_key="wait",
                    expected_revision=binding["revision"],
                )
                == waited
            )
            assert (
                driver.command(
                    "orchestration.reconcile_wait",
                    {"wait_id": wait_request["wait_id"]},
                    idempotency_key="ready",
                )
                == ready
            )
            assert (
                driver.command(
                    "orchestration.acknowledge_wakeup",
                    ack_request,
                    idempotency_key="ack",
                    expected_revision=delivery["revision"],
                )
                == acknowledgment
            )
            assert operator.get("report", report["id"]) == report
            assert evaluator.get("evaluation_receipt", receipt["id"]) == receipt
            assert driver.get("orchestration_wait", wait_request["wait_id"])["state"] == "ready"
            assert (
                driver.get("orchestration_binding", campaign["id"])["data"]["active_wait_id"]
                == wait_request["wait_id"]
            )
            runner.client = worker_client
            assert runner.execute(trained["id"]) == trained
            assert runner.execute(scored["id"]) == scored
            assert closed(runner, trained["id"]) == train_entry
            assert closed(runner, scored["id"]) == score_entry
            assert inspect(url, **OPTIONS) == before
            assert len(endpoint.requests) == calls
