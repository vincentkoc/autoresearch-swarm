"""Ordinary SDK admission in workerd with real R2 and an actual native time source."""

import time
from contextlib import ExitStack

import pytest

from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.control.queries import RecordQuery
from research_swarm.control.worker_artifacts import WorkerArtifactRead
from research_swarm.control.workers import AttemptData, WorkerData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.workers.clock_estimates import ClockSampler
from research_swarm.workers.clock_signatures import ClockVerifier
from research_swarm.workers.clocks import native_clock
from tests.test_cloudflare_native import build_current_sources, server
from tests.test_cloudflare_native_source import TOKEN, Endpoint, connect, control
from tests.test_cloudflare_router import pytestmark as pytestmark
from tests.test_hosted_research import admit, enroll_and_claim
from tests.test_http_delivery import serving
from tests.test_manifest import project as project
from tests.test_time_witnesses import source


def client_at(url, token, input_profile=False):
    client = connect(url, token)
    client.transport.http.headers["x-fixture-research-profile"] = "true"
    if input_profile:
        client.transport.http.headers["x-fixture-input-profile"] = "true"
    return client


def inspect(url, input_profile=False):
    return control(url, "inspect", research_profile=True, input_profile=input_profile)


@pytest.mark.parametrize("input_profile", [False, True])
def test_ordinary_project_claim_replay_and_native_time_binding_in_workerd(
    tmp_path, project, input_profile
):
    build_current_sources()
    descriptor = source().model_copy(update={"native_profile": native_clock().profile})
    with ExitStack() as lifetime:
        with server(tmp_path, sequence=1) as url:
            config = control(
                url,
                "describe",
                source=descriptor.model_dump(mode="json"),
                research_profile=True,
                input_profile=input_profile,
            )
            assert config["profile"] == (
                "research-inputs-witness-v1" if input_profile else "research-admission-witness-v1"
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
                research_profile=True,
                input_profile=input_profile,
            )
            assert "error" not in setup, setup
            with (
                client_at(url, setup["operator_token"], input_profile) as operator,
                client_at(url, setup["worker_token"], input_profile) as worker_client,
            ):
                inputs = {}
                if input_profile:
                    manifest = project / "research.toml"
                    manifest.write_text(
                        manifest.read_text().replace(
                            'kind = "train"', 'kind = "train"\nrequired_artifacts = ["training"]'
                        )
                    )
                    data_path = tmp_path / "training.bin"
                    data_path.write_bytes(b"public synthetic input\x00\xff")
                    data = operator.upload_file(data_path, idempotency_key="training-input")
                    inputs = {"training": data.model_dump(mode="json")}
                # Lose the first trial acknowledgment after the actual shared commit.
                operator.transport.http.headers["x-fixture-drop-command"] = "trial.submit"
                with pytest.raises(ResearchError) as lost:
                    admit(operator, project, input_artifacts=inputs)
                assert lost.value.code == ErrorCode.DELIVERY_UNCERTAIN
                del operator.transport.http.headers["x-fixture-drop-command"]
                before, calls = inspect(url, input_profile), len(endpoint.requests)
                endpoint.mode = "unavailable"
                operator.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
                admitted = admit(operator, project, input_artifacts=inputs)
                assert inspect(url, input_profile) == before and len(endpoint.requests) == calls
                assert len(operator.list(RecordQuery(kind="project"))["items"]) == 1
                assert len(operator.list(RecordQuery(kind="trial"))["items"]) == 1
                del operator.transport.http.headers["x-fixture-r2-fault"]
                endpoint.mode = "normal"

                worker_client.transport.http.headers["x-fixture-drop-command"] = "worker.claim_job"
                with pytest.raises(ResearchError) as lost:
                    enroll_and_claim(
                        operator,
                        admitted,
                        setup["enrollment"]["principal_id"],
                        worker_client=worker_client,
                    )
                assert lost.value.code == ErrorCode.DELIVERY_UNCERTAIN
                del worker_client.transport.http.headers["x-fixture-drop-command"]
                row = operator.list(RecordQuery(kind="worker"))["items"][0]
                worker = WorkerData.model_validate(operator.get("worker", row["id"])["data"])
                claim = {
                    "worker_id": worker.binding.worker_id,
                    "session_id": worker.binding.worker_session_id,
                    "job_id": admitted.job["id"],
                }
                before, calls = inspect(url, input_profile), len(endpoint.requests)
                endpoint.mode = "unavailable"
                worker_client.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
                claimed = worker_client.command(
                    "worker.claim_job",
                    claim,
                    idempotency_key="claim",
                    expected_revision=admitted.job["revision"],
                )
                assert inspect(url, input_profile) == before and len(endpoint.requests) == calls
                with pytest.raises(ResearchError) as scoped:
                    worker_client.get("job", admitted.job["id"])
                assert scoped.value.code == ErrorCode.AUTHORIZATION_DENIED
                endpoint.mode = "normal"
                attempt = AttemptData.model_validate(claimed["result"]["data"])
                verified = ClockVerifier(worker.assignment_authority).binding(
                    attempt.assignment_time, attempt.signed_assignment
                )
                sampler = ClockSampler(
                    worker.assignment_authority, clock_admission=worker.clock_admission
                )
                waiting = sampler.begin()
                estimate = waiting.complete(worker_client.worker_clock(waiting.challenge))
                assert (
                    estimate.verified.reply.payload.clock_epoch
                    == verified.grant.payload.clock_epoch
                )
                # Give the timestamp guard a positive lower bound after the issued time.
                time.sleep(0.02)
                now = sampler.read()
                caps = estimate.deadlines(verified, now)
                assert now.continuous_ns < caps.launch_ns <= caps.hard_ns
                assert attempt.signed_assignment.payload.spec.seed == 42
                assert (
                    attempt.signed_assignment.payload.source_tree_digest
                    == admitted.revision["data"]["resolved"]["source"]["tree_digest"]
                )
                if input_profile:
                    del worker_client.transport.http.headers["x-fixture-r2-fault"]
                    target = WorkerArtifactRead(
                        worker_id=worker.binding.worker_id,
                        session_id=worker.binding.worker_session_id,
                        attempt_id=claimed["result"]["id"],
                        artifact_id=attempt.source_artifact.id,
                    )
                    before_input = inspect(url, input_profile)
                    for request, filename in (
                        (target, "source"),
                        (target.model_copy(update={"artifact_id": data.id}), "training"),
                    ):
                        reference = worker_client.worker_input(request)
                        destination = tmp_path / (filename + ".download")
                        assert worker_client.worker_download(request, destination) == reference
                        if filename == "training":
                            assert destination.read_bytes() == b"public synthetic input\x00\xff"
                    with pytest.raises(ResearchError) as forbidden:
                        worker_client.download(data.id, tmp_path / "global.download")
                    assert forbidden.value.code == ErrorCode.AUTHORIZATION_DENIED
                    assert inspect(url, input_profile) == before_input
                endpoint.mode = "unavailable"
                calls = len(endpoint.requests)
                before_restart = inspect(url, input_profile)

        # Preserve the native source origin while replacing the real workerd process.
        with (
            server(tmp_path, sequence=2) as url,
            client_at(url, setup["operator_token"], input_profile) as operator,
            client_at(url, setup["worker_token"], input_profile) as worker_client,
        ):
            operator.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
            assert (
                operator.command(
                    "project.register",
                    {"source_artifact": admitted.revision["data"]["source_artifact"]},
                    idempotency_key="source",
                )
                == admitted.registered
            )
            assert (
                operator.command(
                    "trial.submit",
                    admitted.submission,
                    idempotency_key="trial",
                    expected_revision=admitted.campaign["revision"],
                )
                == admitted.trial
            )
            assert (
                worker_client.command(
                    "worker.claim_job",
                    claim,
                    idempotency_key="claim",
                    expected_revision=admitted.job["revision"],
                )
                == claimed
            )
            assert inspect(url, input_profile) == before_restart and len(endpoint.requests) == calls
            assert len(operator.list(RecordQuery(kind="attempt"))["items"]) == 1
            assert len(operator.list(RecordQuery(kind="acceptance_lease"))["items"]) == 1

            # A fresh command needs bytes; a retained acknowledgment does not.
            with pytest.raises(ResearchError) as unavailable:
                operator.command(
                    "project.register",
                    {"source_artifact": admitted.revision["data"]["source_artifact"]},
                    idempotency_key="fresh-registration",
                )
            assert unavailable.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
            assert inspect(url, input_profile) == before_restart and len(endpoint.requests) == calls
            del operator.transport.http.headers["x-fixture-r2-fault"]
            endpoint.mode = "normal"

            (project / "evaluate.py").write_text("metric = 'forged'\n")
            changed = operator.capture_project(project, idempotency_key="protected-change")
            candidate = operator.command(
                "candidate.propose",
                {
                    "campaign_id": admitted.campaign["id"],
                    "source_artifact": changed.model_dump(mode="json"),
                    "hypothesis": "Exercise protected mutation refusal",
                },
                idempotency_key="changed-candidate",
                expected_revision=admitted.campaign["revision"],
            )["result"]
            validation = operator.command(
                "candidate.validate",
                {"candidate_id": candidate["id"]},
                idempotency_key="changed-validation",
                expected_revision=candidate["revision"],
            )["result"]
            assert validation["state"] == "invalid"
            with pytest.raises(ResearchError) as invalid:
                operator.command(
                    "trial.submit",
                    {
                        **admitted.submission,
                        "candidate_id": candidate["id"],
                        "validation_id": validation["id"],
                    },
                    idempotency_key="invalid-trial",
                    expected_revision=admitted.campaign["revision"],
                )
            assert invalid.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
            assert len(operator.list(RecordQuery(kind="trial"))["items"]) == 1
