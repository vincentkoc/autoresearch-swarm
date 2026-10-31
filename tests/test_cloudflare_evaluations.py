"""An ordinary two-stage trial with independent evaluation in actual workerd/R2."""

from contextlib import ExitStack

import pytest

from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.queries import RecordQuery
from research_swarm.control.worker_lifecycle import WorkerHeartbeat
from research_swarm.control.workers import WorkerData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.workers.clocks import native_clock
from research_swarm.workers.journal_records import JournalState
from research_swarm.workers.local import LocalWorker
from tests.test_cloudflare_native import build_current_sources, server
from tests.test_cloudflare_native_source import TOKEN, Endpoint, control
from tests.test_cloudflare_research_admission import client_at, inspect
from tests.test_cloudflare_router import pytestmark as pytestmark
from tests.test_hosted_research import admit, enroll_and_claim
from tests.test_hosted_result_publication import EVALUATE, TRAIN
from tests.test_http_delivery import serving
from tests.test_manifest import project as project
from tests.test_time_witnesses import source


def lost_reply(client, operation, payload, **kwargs):
    client.transport.http.headers["x-fixture-drop-command"] = operation
    try:
        with pytest.raises(ResearchError) as lost:
            client.command(operation, payload, **kwargs)
        assert lost.value.code == ErrorCode.DELIVERY_UNCERTAIN
    finally:
        del client.transport.http.headers["x-fixture-drop-command"]


def closed(runner, identifier):
    entry = runner.journal.get(identifier)
    assert entry.state == JournalState.CLOSED
    assert entry.native is not None and entry.timing is not None
    assert 0 < int(entry.timing.caps.launch_ns) <= int(entry.timing.caps.hard_ns)
    assert entry.outcome is not None and entry.outcome.exit_code == 0
    assert entry.outcome.released_at is not None and entry.outcome.closure.scope_closed
    assert runner.resources.availability() == {"cpu": 1}
    return entry


@pytest.mark.parametrize("lifecycle_profile", [False, True])
def test_two_stage_trial_independent_receipt_and_offline_restart_replay(
    tmp_path, project, lifecycle_profile
):
    options = {"evaluation_profile": True, "lifecycle_profile": lifecycle_profile}
    lifecycle_replays = []
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
        with server(tmp_path, sequence=1) as url:
            config = control(url, "describe", source=descriptor.model_dump(mode="json"), **options)
            assert config["profile"] == (
                "research-lifecycle-witness-v1"
                if lifecycle_profile
                else "research-evaluation-witness-v1"
            )
            endpoint = Endpoint(
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
                **options,
            )
            assert "error" not in setup, setup
            with (
                client_at(url, setup["operator_token"], **options) as operator,
                client_at(url, setup["worker_token"], **options) as worker_client,
                client_at(url, setup["evaluator_token"], **options) as evaluator,
            ):
                assert operator.capabilities()["evaluation"]["completion"] == "designated-evaluator"
                admitted = admit(operator, project)
                claimed, _ = enroll_and_claim(
                    operator,
                    admitted,
                    setup["enrollment"]["principal_id"],
                    worker_client=worker_client,
                    project_ids=[admitted.project["id"]],
                )
                worker = WorkerData.model_validate(
                    operator.get("worker", claimed["result"]["data"]["dispatch"]["worker_id"])[
                        "data"
                    ]
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
                trial_id = admitted.trial["result"]["id"]
                assert operator.get("trial", trial_id)["data"]["evaluation_status"] == "pending"
                jobs = operator.list(RecordQuery(kind="job", state="ready"))["items"]
                assert len(jobs) == 1 and jobs[0]["data"]["stage_kind"] == "evaluate"
                job = operator.get("job", jobs[0]["id"])

                def control_command(role, operation, request, key, revision=None, *, drop=False):
                    client = operator if role == "operator" else worker_client
                    kwargs = {"idempotency_key": key, "expected_revision": revision}
                    if drop:
                        lost_reply(client, operation, request, **kwargs)
                    accepted = client.command(operation, request, **kwargs)
                    lifecycle_replays.append((role, operation, request, kwargs, accepted))
                    return accepted["result"]

                if lifecycle_profile:
                    campaign = operator.get("campaign", admitted.campaign["id"])
                    paused = control_command(
                        "operator",
                        "campaign.pause",
                        {"campaign_id": campaign["id"], "mode": "drain"},
                        "pause",
                        campaign["revision"],
                    )
                    assert paused["state"] == "pausing"
                    with pytest.raises(ResearchError) as refused:
                        operator.command(
                            "trial.submit",
                            {**admitted.submission, "seed": 43},
                            idempotency_key="new-trial-while-pausing",
                            expected_revision=paused["revision"],
                        )
                    assert refused.value.code == ErrorCode.INVALID_TRANSITION
                    drain = {
                        "binding": worker.binding.model_dump(mode="json"),
                        "admission": "draining",
                        "reason": "Fixture drain",
                    }
                    draining = control_command(
                        "operator", "worker.set_admission", drain, "drain", 0
                    )
                    presence = control_command(
                        "worker",
                        "worker.heartbeat",
                        WorkerHeartbeat(
                            binding=worker.binding, driver_instance_id=new_id(), sequence=1
                        ),
                        "heartbeat",
                        drop=True,
                    )
                    assert presence["presence"]["data"]["heartbeat_sequence"] == 1
                authorization = AuthorizeEvaluation(
                    job_id=job["id"], evaluator_id=setup["evaluator_id"]
                )
                auth_args = {
                    "idempotency_key": "authorize-evaluation",
                    "expected_revision": job["revision"],
                }
                lost_reply(operator, "evaluation.authorize", authorization, **auth_args)
                before, calls = inspect(url, **options), len(endpoint.requests)
                endpoint.mode = "unavailable"
                operator.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
                authorized = operator.command("evaluation.authorize", authorization, **auth_args)
                assert inspect(url, **options) == before
                assert len(endpoint.requests) == calls
                endpoint.mode = "normal"
                del operator.transport.http.headers["x-fixture-r2-fault"]
                if lifecycle_profile:
                    ready = operator.get("job", job["id"])
                    with pytest.raises(ResearchError) as refused:
                        worker_client.command(
                            "worker.claim_job",
                            {
                                "worker_id": worker.binding.worker_id,
                                "session_id": worker.binding.worker_session_id,
                                "job_id": ready["id"],
                            },
                            idempotency_key="claim-while-draining",
                            expected_revision=ready["revision"],
                        )
                    assert refused.value.code == ErrorCode.INVALID_TRANSITION
                    assert len(operator.list(RecordQuery(kind="attempt"))["items"]) == 1
                    control_command(
                        "operator",
                        "worker.set_admission",
                        {**drain, "admission": "active"},
                        "reopen",
                        draining["revision"],
                    )
                score_attempt = runner.claim(operator.get("job", job["id"]))
                scored = runner.execute(score_attempt["id"])
                assert scored["state"] == "succeeded"
                score_entry = closed(runner, scored["id"])
                assert operator.get("trial", trial_id)["data"]["evaluation_status"] == "pending"
                if lifecycle_profile:
                    settled = operator.get("campaign", admitted.campaign["id"])
                    assert settled["state"] == "paused"
                completion = CompleteEvaluation(
                    request_id=authorized["result"]["id"], attempt_id=scored["id"]
                )
                calls = len(endpoint.requests)
                for client in (worker_client, operator):
                    with pytest.raises(ResearchError) as denied:
                        client.command(
                            "evaluation.complete", completion, idempotency_key="wrong-evaluator"
                        )
                    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
                assert len(endpoint.requests) == calls
                lost_reply(
                    evaluator,
                    "evaluation.complete",
                    completion,
                    idempotency_key="complete-evaluation",
                )
                if lifecycle_profile:
                    assert operator.get("campaign", settled["id"])["state"] == "paused"
                    assert operator.get("trial", trial_id)["data"]["evaluation_status"] == "valid"
                    resumed = control_command(
                        "operator",
                        "campaign.start",
                        {"campaign_id": settled["id"]},
                        "resume",
                        settled["revision"],
                    )
                    assert resumed["state"] == "running"
                before, calls = inspect(url, **options), len(endpoint.requests)
                endpoint.mode = "unavailable"
                evaluator.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
                receipt = evaluator.command(
                    "evaluation.complete", completion, idempotency_key="complete-evaluation"
                )
                result = receipt["result"]
                assert result["state"] == "valid"
                assert result["data"]["receipt"]["evaluator_id"] == setup["evaluator_id"]
                assert result["data"]["attempt_id"] == scored["id"]
                assert result["data"]["receipt"]["metrics"][0]["value"] == 0.25
                assert before["evidence_counts"] == {
                    "run_result": 2,
                    "evaluation_receipt": 1,
                    "promotion": 0,
                    "artifact_read_session": 0,
                }
                assert len(before["settlements"]) == 2
                assert inspect(url, **options) == before
                assert len(endpoint.requests) == calls

        with (
            server(tmp_path, sequence=2) as url,
            client_at(url, setup["operator_token"], **options) as operator,
            client_at(url, setup["worker_token"], **options) as worker_client,
            client_at(url, setup["evaluator_token"], **options) as evaluator,
        ):
            for client in (operator, worker_client, evaluator):
                client.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
            for role, operation, request, kwargs, accepted in lifecycle_replays:
                client = operator if role == "operator" else worker_client
                assert client.command(operation, request, **kwargs) == accepted
            assert (
                operator.command("evaluation.authorize", authorization, **auth_args) == authorized
            )
            assert (
                evaluator.command(
                    "evaluation.complete", completion, idempotency_key="complete-evaluation"
                )
                == receipt
            )
            assert evaluator.get("evaluation_receipt", result["id"]) == result
            assert len(evaluator.list(RecordQuery(kind="evaluation_receipt"))["items"]) == 1
            assert operator.get("trial", trial_id)["data"]["evaluation_status"] == "valid"
            runner.client = worker_client
            assert runner.execute(trained["id"]) == trained
            assert runner.execute(scored["id"]) == scored
            assert closed(runner, trained["id"]) == train_entry
            assert closed(runner, scored["id"]) == score_entry
            assert inspect(url, **options) == before
            assert len(endpoint.requests) == calls
