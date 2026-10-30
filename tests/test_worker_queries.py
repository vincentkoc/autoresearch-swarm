from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import PauseCampaign, RegisterProject
from research_swarm.control.worker_queries import WorkerQuery, WorkerRead
from research_swarm.control.workers import ClaimJob, EnrollWorker, WorkerData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.state.sqlite import SQLiteSession
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline


def scoped_worker(
    pipeline: Pipeline,
    projects: tuple[str, ...] | None = None,
) -> tuple[ResearchClient, WorkerData, str]:
    runtime = pipeline.runtime
    principal = Principal(
        id=new_id(),
        workspace_id=runtime.identity.workspace_id,
        scopes=frozenset({"worker.query", "worker.claim"}),
    )
    issued = runtime.credentials.issue(runtime.principal(), principal)
    request = EnrollWorker(
        worker_id=new_id(),
        session_id=new_id(),
        principal_id=principal.id,
        name="Scoped fixture",
        profiles=pipeline.worker.worker.profiles,
        project_ids=projects or (pipeline.trial["data"]["spec"]["project_id"],),
    )
    row = pipeline.client.command("worker.enroll", request, idempotency_key=new_id())["result"]
    worker = WorkerData.model_validate(row["data"])
    client = ResearchClient(
        LocalTransport(runtime, lambda: runtime.credentials.resolve(issued.token))
    )
    return client, worker, issued.token


def query(worker: WorkerData, kind: str = "offers", **kwargs: Any) -> WorkerQuery:
    return WorkerQuery(
        worker_id=worker.binding.worker_id,
        session_id=worker.binding.worker_session_id,
        kind=kind,
        **kwargs,
    )


def claim(client: ResearchClient, worker: WorkerData, job: dict[str, Any]) -> dict[str, Any]:
    return client.command(
        "worker.claim_job",
        ClaimJob(
            worker_id=worker.binding.worker_id,
            session_id=worker.binding.worker_session_id,
            job_id=job["id"],
        ),
        idempotency_key=new_id(),
        expected_revision=job["revision"],
    )["result"]


def read(worker: WorkerData, kind: str, identifier: str) -> WorkerRead:
    return WorkerRead(
        worker_id=worker.binding.worker_id,
        session_id=worker.binding.worker_session_id,
        kind=kind,
        id=identifier,
    )


def test_scoped_worker_can_claim_and_recover_only_its_assignments(pipeline: Pipeline) -> None:
    client, worker, _ = scoped_worker(pipeline)
    other_client, other, _ = scoped_worker(pipeline)
    with pytest.raises(ResearchError) as broad:
        client.get("trial", pipeline.trial["id"])
    assert broad.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pytest.raises(ResearchError):
        client.worker_get(read(worker, "trial", pipeline.trial["id"]))
    offers = client.worker_list(query(worker))
    assert len(offers["items"]) == 1
    attempt = claim(client, worker, offers["items"][0])
    assert attempt["data"]["signed_assignment"] is not None
    assert client.worker_get(read(worker, "attempt", attempt["id"])) == attempt
    assert (
        client.worker_get(read(worker, "trial", pipeline.trial["id"]))["id"] == pipeline.trial["id"]
    )
    assert client.worker_list(query(worker, "pending"))["items"][0]["id"] == attempt["id"]
    assert not other_client.worker_list(query(other, "pending"))["items"]
    with pytest.raises(ResearchError, match="not assigned"):
        other_client.worker_get(read(other, "attempt", attempt["id"]))
    with pytest.raises(ResearchError, match="not assigned"):
        other_client.worker_get(read(other, "trial", pipeline.trial["id"]))
    with pytest.raises(ResearchError, match="does not own"):
        other_client.worker_list(query(worker))


def test_project_scope_blocks_unoffered_claim_before_reservation(pipeline: Pipeline) -> None:
    candidate = pipeline.client.get("candidate", pipeline.trial["data"]["candidate_id"])
    other = pipeline.client.command(
        "project.register",
        RegisterProject(source_artifact=candidate["data"]["source_artifact"]),
        idempotency_key="other-project",
    )["result"]["project"]
    client, worker, _ = scoped_worker(pipeline, (other["id"],))
    assert not client.worker_list(query(worker))["items"]
    with pytest.raises(ResearchError) as denied:
        claim(client, worker, pipeline.ready())
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pipeline.runtime.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM budget_reservations").fetchone()[0] == 0
        assert (
            connection.execute("SELECT COUNT(*) FROM records WHERE kind='attempt'").fetchone()[0]
            == 0
        )


@pytest.mark.parametrize("pipeline", [True], indirect=True)
def test_offer_paging_is_bounded_and_worker_bound(pipeline: Pipeline) -> None:
    client, worker, _ = scoped_worker(pipeline)
    other_client, other, _ = scoped_worker(pipeline)
    first = client.worker_list(query(worker, limit=1))
    assert first["scanned"] == 1 and first["next_cursor"] is not None
    second = client.worker_list(query(worker, limit=1, cursor=first["next_cursor"]))
    assert len(first["items"]) == len(second["items"]) == 1
    assert first["items"][0]["id"] != second["items"][0]["id"]
    assert second["next_cursor"] is None
    with pytest.raises(ResearchError, match="scope differs"):
        other_client.worker_list(query(other, cursor=first["next_cursor"]))
    with pytest.raises(ResearchError, match="scope differs"):
        client.worker_list(query(worker, "pending", cursor=first["next_cursor"]))
    with pytest.raises(ResearchError, match="Invalid worker cursor"):
        client.worker_list(query(worker, cursor="not-base64"))


def test_native_worker_checks_its_project_scope_before_journaling(pipeline: Pipeline) -> None:
    admitted = pipeline.worker.claim(pipeline.ready())
    pipeline.worker.worker = pipeline.worker.worker.model_copy(update={"project_ids": (new_id(),)})
    with pytest.raises(ResearchError, match="outside worker scope"):
        pipeline.worker.execute(admitted["id"])
    with pytest.raises(ResearchError) as absent:
        pipeline.worker.journal.get(admitted["id"])
    assert absent.value.code == ErrorCode.NOT_FOUND


def test_paused_campaign_and_stale_session_have_no_new_worker_authority(pipeline: Pipeline) -> None:
    client, worker, _ = scoped_worker(pipeline)
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "campaign.pause",
        PauseCampaign(campaign_id=campaign["id"]),
        idempotency_key="pause",
        expected_revision=campaign["revision"],
    )
    assert not client.worker_list(query(worker))["items"]
    with pytest.raises(ResearchError) as stale:
        client.worker_list(query(worker).model_copy(update={"session_id": new_id()}))
    assert stale.value.code == ErrorCode.STALE_FENCE


def test_worker_http_routes_do_not_grant_generic_read_or_evaluation(pipeline: Pipeline) -> None:
    local, worker, token = scoped_worker(pipeline)
    app = create_app(pipeline.runtime.service, pipeline.runtime.credentials)
    for name in ("get", "list"):
        schema = app.openapi()["paths"]["/v1/worker/" + name]["post"]["requestBody"]
        assert "session_id" in schema["content"]["application/json"]["schema"]["required"]
    with serving(app) as url, ResearchClient.connect(url, token) as remote:
        assert remote.worker_list(query(worker)) == local.worker_list(query(worker))
        enrollment = remote.worker_get(read(worker, "worker", worker.binding.worker_id))
        assert enrollment["data"]["assignment_authority"] is not None
        for method in (remote.capabilities, lambda: remote.get("trial", pipeline.trial["id"])):
            with pytest.raises(ResearchError) as denied:
                method()
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        attempt = claim(remote, worker, remote.worker_list(query(worker))["items"][0])
        assert remote.worker_get(read(worker, "attempt", attempt["id"])) == attempt
        with pytest.raises(ResearchError) as evaluator:
            remote.command("evaluation.complete", {}, idempotency_key="not-evaluator")
        assert evaluator.value.code == ErrorCode.AUTHORIZATION_DENIED


def test_empty_worker_poll_and_unowned_lookup_do_not_scan_completed_history(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, worker, _ = scoped_worker(pipeline)
    accepted = claim(client, worker, pipeline.ready())
    database = pipeline.runtime.database
    connect = database.connect
    instructions = 0

    def step() -> int:
        nonlocal instructions
        instructions += 1
        return 0

    @contextmanager
    def measured() -> Iterator[SQLiteSession]:
        with connect() as connection:
            connection.connection.set_progress_handler(step, 1)
            yield connection

    monkeypatch.setattr(database, "connect", measured)
    with connect() as connection:
        connection.execute(
            "UPDATE records SET payload=json_set(payload,'$.publication_status','accepted') "
            "WHERE kind='attempt' AND id=?",
            (accepted["id"],),
        )

    def observe() -> int:
        nonlocal instructions
        instructions = 0
        assert not client.worker_list(query(worker, "pending"))["items"]
        for kind in ("attempt", "trial"):
            with pytest.raises(ResearchError, match="not assigned"):
                client.worker_get(read(worker, kind, new_id()))
        return instructions

    before = observe()
    with connect() as connection:
        connection.connection.executemany(
            "INSERT INTO records SELECT workspace_id,kind,?,revision,state,campaign_id,parent_id,"
            "json_set(payload,'$.dispatch.attempt_id',?),created_at,updated_at FROM records "
            "WHERE kind='attempt' AND id=?",
            [(new_id(), new_id(), accepted["id"]) for _ in range(2000)],
        )
    assert observe() <= before + 300
