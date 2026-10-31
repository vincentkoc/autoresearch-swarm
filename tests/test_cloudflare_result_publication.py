"""Real native CPU closure published through workerd/R2, including lost responses."""

from contextlib import ExitStack

import pytest

from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.control.queries import RecordQuery
from research_swarm.control.workers import WorkerData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.workers.clocks import native_clock
from research_swarm.workers.journal_records import JournalState
from research_swarm.workers.local import LocalWorker
from tests.test_cloudflare_native import build_current_sources, server
from tests.test_cloudflare_native_source import TOKEN, Endpoint, control
from tests.test_cloudflare_research_admission import client_at, inspect
from tests.test_cloudflare_router import pytestmark as pytestmark
from tests.test_hosted_research import admit, enroll_and_claim
from tests.test_hosted_result_publication import TRAIN
from tests.test_http_delivery import serving
from tests.test_manifest import project as project
from tests.test_time_witnesses import source


def test_native_cpu_publication_survives_lost_reply_and_workerd_restart(tmp_path, project):
    build_current_sources()
    descriptor = source().model_copy(update={"native_profile": native_clock().profile})
    (project / "train.py").write_text(TRAIN)
    manifest = project / "research.toml"
    manifest.write_text(manifest.read_text().replace("timeout_ms = 1000", "timeout_ms = 10000"))
    captured = {}
    with ExitStack() as lifetime:
        with server(tmp_path, sequence=1) as url:
            config = control(
                url, "describe", source=descriptor.model_dump(mode="json"), publication_profile=True
            )
            assert config["profile"] == "research-publication-witness-v1"
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
                publication_profile=True,
            )
            assert "error" not in setup, setup
            with (
                client_at(url, setup["operator_token"], publication_profile=True) as operator,
                client_at(url, setup["worker_token"], publication_profile=True) as client,
            ):
                caps = operator.capabilities()
                assert caps["execution"] == "cpu-result-publication"
                assert caps["publication"]["evaluation_receipts"] == "unavailable"
                admitted = admit(operator, project)
                claimed, _ = enroll_and_claim(
                    operator,
                    admitted,
                    setup["enrollment"]["principal_id"],
                    worker_client=client,
                    project_ids=[admitted.project["id"]],
                )
                attempt_id = claimed["result"]["id"]
                worker = WorkerData.model_validate(
                    operator.get("worker", claimed["result"]["data"]["dispatch"]["worker_id"])[
                        "data"
                    ]
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
                        captured.update(payload=payload.model_dump(mode="json"), **kwargs)
                    return original(operation, payload, **kwargs)

                client.command = capture
                client.transport.http.headers["x-fixture-drop-command"] = "worker.publish_attempt"
                try:
                    with pytest.raises(ResearchError) as lost:
                        runner.execute(attempt_id)
                finally:
                    client.command = original
                assert lost.value.code == ErrorCode.DELIVERY_UNCERTAIN
                del client.transport.http.headers["x-fixture-drop-command"]
                entry = runner.journal.get(attempt_id)
                assert entry.state == JournalState.CLOSED
                assert entry.native is not None and entry.timing is not None
                assert 0 < entry.timing.caps.launch_ns <= entry.timing.caps.hard_ns
                assert entry.outcome is not None and entry.outcome.exit_code == 0
                assert entry.outcome.released_at is not None and entry.outcome.closure.scope_closed
                assert captured["payload"]["report_artifact"] is not None
                assert runner.resources.availability() == {"cpu": 1}
                accepted_row = operator.get("attempt", attempt_id)
                assert accepted_row["state"] == "succeeded"
                assert accepted_row["data"]["publication_status"] == "accepted"
                trial = operator.get("trial", admitted.trial["result"]["id"])
                assert trial["data"]["evaluation_status"] == "pending"
                before = inspect(url, publication_profile=True)
                assert before["evidence_counts"] == {
                    "run_result": 1,
                    "evaluation_receipt": 0,
                    "promotion": 0,
                    "artifact_read_session": 0,
                }
                assert len(before["settlements"]) == 1
                endpoint.mode = "unavailable"
                client.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
                calls = len(endpoint.requests)
                accepted = client.command("worker.publish_attempt", **captured)
                assert accepted["result"] == accepted_row
                assert inspect(url, publication_profile=True) == before
                assert len(endpoint.requests) == calls

        # Keep the actual source epoch and native journal while replacing only workerd.
        with (
            server(tmp_path, sequence=2) as url,
            client_at(url, setup["operator_token"], publication_profile=True) as operator,
            client_at(url, setup["worker_token"], publication_profile=True) as client,
        ):
            client.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
            assert client.command("worker.publish_attempt", **captured) == accepted
            runner.client = client
            assert runner.execute(attempt_id) == accepted_row
            assert runner.journal.get(attempt_id) == entry
            assert runner.resources.availability() == {"cpu": 1}
            assert inspect(url, publication_profile=True) == before
            assert len(endpoint.requests) == calls
            assert len(operator.list(RecordQuery(kind="attempt"))["items"]) == 1
            assert operator.get("trial", admitted.trial["result"]["id"]) == trial
