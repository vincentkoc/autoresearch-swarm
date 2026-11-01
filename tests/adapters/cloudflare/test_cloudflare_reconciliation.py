"""Production workerd reconciliation preserves original CPU evidence across restart."""

import asyncio
import copy
from contextlib import ExitStack
from datetime import UTC, datetime

import httpx
import pytest

from adapters.cloudflare.delivery.wait_delivery import wait_delivery_key
from adapters.cloudflare.runtime.deployment import (
    DeploymentCredential,
    authority_id,
    credential_manifest_digest,
)
from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.client import ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.control.workers import WorkerData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import RunResult
from research_swarm.domain.orchestration import OrchestrationBinding
from research_swarm.domain.time_providers import RequalifyTimeProvider
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.orchestration.cloudflare import CloudflareBridge, WorkflowOptions
from research_swarm.orchestration.delivery_contract import binding_reference
from research_swarm.orchestration.gateway_models import GatewayBinding
from research_swarm.orchestration.waits import DurableWaitDriver
from research_swarm.workers.clocks import native_clock
from research_swarm.workers.journal_records import JournalState
from research_swarm.workers.local import LocalWorker
from tests.adapters.cloudflare.hosted.test_hosted_research import admit, enroll_and_claim
from tests.adapters.cloudflare.hosted.test_hosted_result_publication import TRAIN
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_hybrid import CONTROL, until
from tests.adapters.cloudflare.test_cloudflare_hybrid import post as workflow_post
from tests.adapters.cloudflare.test_cloudflare_native import build_current_sources, port, server
from tests.adapters.cloudflare.test_cloudflare_native_source import Endpoint
from tests.adapters.cloudflare.test_cloudflare_production import (
    fixture_config,
    fixture_secrets,
    post,
    provider_call,
)
from tests.adapters.cloudflare.test_cloudflare_router import pytestmark as pytestmark
from tests.orchestration.test_http_delivery import serving, tls_certificate
from tests.projects.test_manifest import project as project
from tests.workers.test_time_witnesses import source


def test_production_result_and_timer_survive_clock_requalification_and_restarts(
    tmp_path, deployment, project
):
    build_current_sources(production=True)
    manifest, secrets = (copy.deepcopy(item) for item in deployment[:2])
    manifest["deliveries"] = []
    config_data = manifest["configuration"]
    config_data["profile"] = "research-timers-witness-v1"
    worker_credential = manifest["credentials"][2]
    worker_credential["principal"]["scopes"] = [
        "worker.query",
        "worker.claim",
        "worker.inputs",
        "worker.outputs",
        "worker.publish",
    ]
    config_data["credential_manifest_digest"] = credential_manifest_digest(
        tuple(DeploymentCredential.model_validate(item) for item in manifest["credentials"])
    )
    private = tmp_path / "bindings"
    config = fixture_config(private)
    options = {
        "config": "wrangler.operator-test.jsonc",
        "extra_configs": (config,),
        "listen_port": port(),
    }
    with (
        server(tmp_path, sequence=1, **options) as url,
        httpx.Client(base_url=url, trust_env=False, timeout=30) as http,
    ):
        object_id = http.get("/fixture/identity").json()["object_id"]

    manifest["object_id"] = object_id
    config_data["store_location"] = manifest["namespace"] + "/" + object_id
    config_data["authority"]["state_authority_id"] = authority_id(manifest["namespace"], object_id)
    descriptor = source().model_copy(update={"native_profile": native_clock().profile})
    config_data["source"] = descriptor.model_dump(mode="json")
    endpoint = Endpoint(WitnessAuthorityScope.model_validate(config_data["authority"]), descriptor)
    certificate, key = tls_certificate(tmp_path)
    app = create_time_witness_app(endpoint, witness_consumer_digest(secrets["witness_consumer"]))
    operator_token = secrets["credentials"][manifest["credentials"][0]["id"]]
    worker_token = secrets["credentials"][worker_credential["id"]]
    (project / "train.py").write_text(TRAIN)
    project_manifest = project / "research.toml"
    project_manifest.write_text(
        project_manifest.read_text().replace("timeout_ms = 1000", "timeout_ms = 10000")
    )
    captured = {}

    with serving(app, ssl_certfile=str(certificate), ssl_keyfile=str(key)) as origin:
        manifest["witness_origin"] = origin
        redactions = fixture_secrets(private, manifest, secrets)
        options.update(
            child_environment={"NODE_EXTRA_CA_CERTS": str(certificate)},
            diagnostic_secrets=redactions,
        )
        with server(tmp_path, sequence=2, **options) as url, ExitStack() as clients:
            http = clients.enter_context(httpx.Client(base_url=url, trust_env=False, timeout=30))
            provisioned = post(http, secrets["bootstrap"]).json()
            assert "error" not in provisioned
            operator = clients.enter_context(ResearchClient.connect(url, operator_token))
            client = clients.enter_context(ResearchClient.connect(url, worker_token))
            assert "attempt.reconcile" in operator.capabilities()["commands"]
            assert operator.capabilities()["orchestration"]["timer_waits"] is True
            admitted = admit(operator, project)
            claimed, _ = enroll_and_claim(
                operator,
                admitted,
                worker_credential["principal"]["id"],
                worker_client=client,
                project_ids=[admitted.project["id"]],
            )
            attempt_id = claimed["result"]["id"]
            worker = WorkerData.model_validate(
                operator.get("worker", claimed["result"]["data"]["dispatch"]["worker_id"])["data"]
            )
            runner = LocalWorker(
                client,
                worker,
                root=tmp_path / "worker",
                machine_registry=tmp_path / "machine" / "resources.sqlite",
            )
            original = client.command

            def capture(operation, payload, **kwargs):
                if operation == "worker.publish_attempt":
                    captured.update(request=payload.model_dump(mode="json"), **kwargs)
                    raise ResearchError(ErrorCode.DELIVERY_UNCERTAIN, "Fixture delays publication")
                return original(operation, payload, **kwargs)

            client.command = capture
            try:
                with pytest.raises(ResearchError) as held:
                    runner.execute(attempt_id)
            finally:
                client.command = original
            assert held.value.code == ErrorCode.DELIVERY_UNCERTAIN
            entry = runner.journal.get(attempt_id)
            assert entry.state == JournalState.CLOSED
            assert entry.outcome is not None and entry.outcome.exit_code == 0
            assert entry.outcome.closure.scope_closed
            assert captured["request"]["report_artifact"] is not None
            original_attempt = operator.get("attempt", attempt_id)
            assert original_attempt["data"]["publication_status"] == "pending"

            endpoint.restart()
            with pytest.raises(ResearchError) as discontinuity:
                client.command("worker.publish_attempt", **captured)
            assert discontinuity.value.code == ErrorCode.STALE_FENCE
            fenced = provider_call(http, secrets["bootstrap"]).json()["provider"]
            assert fenced["state"] == "unqualified"
            replacement = RequalifyTimeProvider(
                request_id=new_id(),
                expected_provider_revision=fenced["revision"],
                expected_provider_generation=fenced["data"]["scope"]["provider_generation"],
                reason="Recover the controller clock without replacing original native evidence",
            )
            assert (
                provider_call(
                    http, secrets["bootstrap"], replacement.model_dump(mode="json")
                ).json()["disposition"]
                == "accepted"
            )
            calls_before_replay = len(endpoint.requests)
            with pytest.raises(ResearchError) as retained_rejection:
                client.command("worker.publish_attempt", **captured)
            assert retained_rejection.value.code == ErrorCode.STALE_FENCE
            assert len(endpoint.requests) == calls_before_replay
            with pytest.raises(ResearchError) as expired:
                client.command(
                    "worker.publish_attempt",
                    captured["request"],
                    idempotency_key="publication-after-clock-requalification",
                )
            assert expired.value.code == ErrorCode.ACCEPTANCE_EXPIRED
            lease = operator.get("acceptance_lease", attempt_id)
            assert lease["state"] == "unqualified"

            client.transport.http.headers["x-fixture-drop-command"] = "worker.submit_evidence"
            with pytest.raises(ResearchError) as lost_submission:
                client.command(
                    "worker.submit_evidence", captured["request"], idempotency_key="late-evidence"
                )
            assert lost_submission.value.code == ErrorCode.DELIVERY_UNCERTAIN
            del client.transport.http.headers["x-fixture-drop-command"]
            submission = client.command(
                "worker.submit_evidence", captured["request"], idempotency_key="late-evidence"
            )
            assert submission["result"]["state"] == "quarantined"
            assert operator.list(RecordQuery(kind="run_result"))["items"] == []
            pending = operator.get("attempt", attempt_id)
            request = {
                "attempt_id": attempt_id,
                "submission_id": submission["result"]["id"],
                "expected_controller_id": config_data["authority"]["controller_id"],
                "expected_control_epoch": config_data["authority"]["control_epoch"],
                "reason": "Accept original closure after controller clock replacement",
            }
            kwargs = {
                "idempotency_key": "late-acceptance",
                "expected_revision": pending["revision"],
            }
            operator.transport.http.headers["x-fixture-drop-command"] = "attempt.reconcile"
            with pytest.raises(ResearchError) as lost_acceptance:
                operator.command("attempt.reconcile", request, **kwargs)
            assert lost_acceptance.value.code == ErrorCode.DELIVERY_UNCERTAIN
            del operator.transport.http.headers["x-fixture-drop-command"]
            accepted = operator.command("attempt.reconcile", request, **kwargs)
            assert accepted["result"]["disposition"] == "reconciled"
            accepted_attempt = accepted["result"]["attempt"]
            assert accepted_attempt["state"] == "succeeded"
            for field in ("dispatch", "signed_assignment", "assignment_time"):
                assert accepted_attempt["data"][field] == original_attempt["data"][field]
            receipt = accepted["result"]["reconciliation"]["data"]
            assert receipt["acceptance_lease"] == lease["data"]
            assert receipt["acceptance_disposition"] == "unqualified"
            assert len(operator.list(RecordQuery(kind="run_result"))["items"]) == 1
            assert operator.list(RecordQuery(kind="evaluation_receipt"))["items"] == []
            budget = operator.context(admitted.campaign["id"])["budget"]
            assert all(item["reserved"] == "0" and item["uncertain"] == "0" for item in budget)
            assert (
                next(item for item in budget if item["dimension"] == "attempts")["consumed"] == "1"
            )
            result = RunResult.model_validate(operator.get("run_result", attempt_id)["data"])
            result.verify()
            assert (
                int(
                    next(item for item in budget if item["dimension"] == "execution_ms")["consumed"]
                )
                == result.elapsed_ms
            )
            assert (
                result.process_closure_ref.model_dump(mode="json")
                == captured["request"]["journal_artifact"]
            )
            settled = operator.get("acceptance_lease", attempt_id)
            assert settled["state"] == "settled"
            assert {
                name: value for name, value in settled["data"].items() if name != "disposition"
            } == {name: value for name, value in lease["data"].items() if name != "disposition"}

            gateway, timer_actor = manifest["credentials"][1], manifest["credentials"][3]
            configuration_digest = "sha256:" + "7" * 64
            campaign = operator.get("campaign", admitted.campaign["id"])
            binding_row = operator.command(
                "orchestration.bind",
                {
                    "campaign_id": campaign["id"],
                    "adapter": "cloudflare-workflows",
                    "configuration_digest": configuration_digest,
                    "routes": {"control": timer_actor["principal"]["id"]},
                },
                idempotency_key="timer-binding",
                expected_revision=campaign["revision"],
            )["result"]
            operator.command(
                "orchestration.register_delivery",
                {
                    "campaign_id": campaign["id"],
                    "epoch": 1,
                    "configuration_digest": configuration_digest,
                    "gateway_credential_id": gateway["id"],
                    "route_credentials": {"control": timer_actor["id"]},
                },
                idempotency_key="timer-route",
                expected_revision=0,
            )
            reference = binding_reference(OrchestrationBinding.model_validate(binding_row["data"]))
            gateway_binding = GatewayBinding.from_reference(reference).model_dump(mode="json")
            variables = {
                "AUTHORITY_PROFILE": "hosted-do-witness-v1",
                "HOSTED_DELIVERY_ORIGIN": url,
                "GATEWAY_TOKEN": secrets["credentials"][gateway["id"]],
                "CONTROL_TOKEN": CONTROL,
                "ALLOW_LOCAL_GATEWAY": "true",
            }
            workflow_options = WorkflowOptions(
                poll_seconds=1, rotate_after=2, request_timeout_seconds=10
            )
            workflow_root = tmp_path / "workflow"
            workflow_root.mkdir()
            timer_token = secrets["credentials"][timer_actor["id"]]
            with (
                server(workflow_root, sequence=1, variables=variables) as workflow_url,
                httpx.Client(
                    base_url=workflow_url,
                    headers={"authorization": "Bearer " + CONTROL},
                    trust_env=False,
                    timeout=30,
                ) as workflow_http,
                ResearchClient.connect(url, timer_token) as driver,
            ):

                async def start():
                    async with CloudflareBridge(workflow_url, CONTROL) as bridge:
                        return await bridge.start(reference, workflow_options)

                workflow_id = asyncio.run(start())
                initial = workflow_post(
                    workflow_http, "/adapter/status", {"binding": gateway_binding}
                )
                operator.command(
                    "report.create",
                    {"campaign_id": campaign["id"]},
                    idempotency_key="timer-clock-anchor-report",
                )
                # Acquire fresh witnessed time after Workflow startup; idle polls do not sample it.
                wake_at = (
                    datetime.fromtimestamp(
                        (endpoint.responses[-1].payload.authority_time_ms + 60_000) / 1000, UTC
                    )
                    .isoformat(timespec="milliseconds")
                    .replace("+00:00", "Z")
                )
                timer_request = {
                    "wait_id": new_id(),
                    "campaign_id": campaign["id"],
                    "epoch": 1,
                    "configuration_digest": configuration_digest,
                    "condition": {
                        "observed_watermark": driver.context(campaign["id"])["watermark"],
                        "event_types": ["fixture.never"],
                        "wake_at": wake_at,
                    },
                }
                waiting = driver.command(
                    "orchestration.record_wait",
                    timer_request,
                    idempotency_key="timer-wait",
                    expected_revision=driver.get("orchestration_binding", campaign["id"])[
                        "revision"
                    ],
                )["result"]["wait"]
                assert waiting["state"] == "waiting"

                def carried():
                    status = workflow_post(
                        workflow_http, "/adapter/status", {"binding": gateway_binding}
                    )
                    assert status["status"] != "errored", status
                    assert driver.get("orchestration_wait", waiting["id"])["state"] == "waiting"
                    pending = status["initial_pending"]
                    return (
                        status
                        if status["generation"] > initial["generation"]
                        and pending
                        and pending["record_id"] == waiting["id"]
                        else None
                    )

                checkpoint = until(carried, timeout=30)
                timer_provider = provider_call(http, secrets["bootstrap"]).json()["provider"]
                timer_key = wait_delivery_key(
                    waiting["id"], timer_provider["data"]["scope"]["provider_generation"]
                )
                endpoint.restart()

                def clock_fenced():
                    provider = provider_call(http, secrets["bootstrap"]).json()["provider"]
                    return provider if provider["state"] == "unqualified" else None

                timer_fence = until(clock_fenced)
                with pytest.raises(ResearchError) as timer_rejection:
                    driver.command(
                        "orchestration.reconcile_wait",
                        {"wait_id": waiting["id"]},
                        idempotency_key=timer_key,
                    )
                assert timer_rejection.value.code == ErrorCode.STALE_FENCE
                initial = workflow_post(
                    workflow_http, "/adapter/status", {"binding": gateway_binding}
                )
                checkpoint = until(carried, timeout=30)
                assert provider_call(http, secrets["bootstrap"]).json()["provider"] == timer_fence
            endpoint.mode = "unavailable"
            calls = len(endpoint.requests)

        with (
            server(tmp_path, sequence=3, **options) as url,
            httpx.Client(base_url=url, trust_env=False, timeout=30) as http,
            ResearchClient.connect(url, operator_token) as operator,
            ResearchClient.connect(url, worker_token) as client,
        ):
            assert (
                client.command(
                    "worker.submit_evidence", captured["request"], idempotency_key="late-evidence"
                )
                == submission
            )
            assert operator.command("attempt.reconcile", request, **kwargs) == accepted
            assert operator.get("attempt", attempt_id) == accepted_attempt
            assert operator.context(admitted.campaign["id"])["budget"] == budget
            assert len(operator.list(RecordQuery(kind="attempt"))["items"]) == 1
            assert len(operator.list(RecordQuery(kind="run_result"))["items"]) == 1
            assert len(operator.list(RecordQuery(kind="attempt_reconciliation"))["items"]) == 1
            assert operator.list(RecordQuery(kind="evaluation_receipt"))["items"] == []
            assert runner.journal.get(attempt_id) == entry
            assert runner.resources.availability() == {"cpu": 1}
            assert len(endpoint.requests) == calls
            retained_wait = operator.get("orchestration_wait", waiting["id"])
            assert retained_wait == waiting
            with (
                server(workflow_root, sequence=2, variables=variables) as workflow_url,
                httpx.Client(
                    base_url=workflow_url,
                    headers={"authorization": "Bearer " + CONTROL},
                    trust_env=False,
                    timeout=30,
                ) as workflow_http,
                ResearchClient.connect(url, timer_token) as driver,
            ):
                restarted = workflow_post(
                    workflow_http, "/adapter/status", {"binding": gateway_binding}
                )
                assert restarted["generation"] >= checkpoint["generation"]
                assert restarted["initial_pending"]["record_id"] == waiting["id"]
                assert (
                    workflow_post(
                        workflow_http,
                        "/adapter/start",
                        {
                            "binding": gateway_binding,
                            "options": workflow_options.model_dump(mode="json"),
                        },
                    )["id"]
                    == workflow_id
                )
                endpoint.mode = "normal"
                timer_requalification = RequalifyTimeProvider(
                    request_id=new_id(),
                    expected_provider_revision=timer_fence["revision"],
                    expected_provider_generation=timer_fence["data"]["scope"][
                        "provider_generation"
                    ],
                    reason="Resume the original timer after its admitted clock restarted",
                )
                response = provider_call(
                    http, secrets["bootstrap"], timer_requalification.model_dump(mode="json")
                ).json()
                assert response["disposition"] == "accepted"
                with pytest.raises(ResearchError) as retained_timer_rejection:
                    driver.command(
                        "orchestration.reconcile_wait",
                        {"wait_id": waiting["id"]},
                        idempotency_key=timer_key,
                    )
                assert retained_timer_rejection.value.code == ErrorCode.STALE_FENCE
                until(
                    lambda: driver.get("orchestration_wait", waiting["id"])["state"] == "ready",
                    timeout=120,
                )

                def delivered():
                    status = workflow_post(
                        workflow_http, "/adapter/status", {"binding": gateway_binding}
                    )
                    assert status["status"] != "errored", status
                    return (
                        status
                        if status["generation"] > restarted["generation"]
                        and status["initial_pending"] is None
                        else None
                    )

                until(delivered)
                readiness = driver.get("wait_readiness", waiting["id"])
                assert readiness["data"]["cause"] == "timer"
                assert readiness["data"]["event_id"] is None
                assert readiness["data"]["ready_at"] >= wake_at
                assert (
                    driver.get("orchestration_wait", waiting["id"])["data"]["condition"]["wake_at"]
                    == wake_at
                )
                durable = DurableWaitDriver(
                    driver, campaign["id"], epoch=1, actor_id=timer_actor["principal"]["id"]
                )
                assert durable.reconcile(waiting["id"]) == durable.reconcile(waiting["id"])
                assert len(operator.list(RecordQuery(kind="wait_readiness"))["items"]) == 1
                assert len(operator.list(RecordQuery(kind="wakeup_delivery"))["items"]) == 1
                assert operator.get("attempt", attempt_id) == accepted_attempt
                assert operator.context(campaign["id"])["budget"] == budget
