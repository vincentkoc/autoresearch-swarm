from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace

import pytest
from pydantic import ValidationError

from research_swarm import ResearchClient
from research_swarm.api.http import create_app
from research_swarm.api.mcp import make_mcp
from research_swarm.client import LocalTransport
from research_swarm.control.queries import RecordQuery
from research_swarm.control.worker_locality import AdvertiseWorkerCache
from research_swarm.domain.clocks import ClockChallenge
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_queries import query, read


def advertisement(pipeline: Pipeline) -> AdvertiseWorkerCache:
    candidate = pipeline.client.get("candidate", pipeline.trial["data"]["candidate_id"])
    reference = ArtifactRef.model_validate(candidate["data"]["source_artifact"])
    return AdvertiseWorkerCache(
        binding=pipeline.worker.worker.binding,
        snapshot_id=new_id(),
        references=(reference,),
        examined_bytes=reference.size_bytes,
    )


def publish(client, request, *, revision=0, key=None):
    return client.command(
        "worker.cache_advertise",
        request,
        expected_revision=revision,
        idempotency_key=key or new_id(),
    )


def fresh(pipeline):
    service = pipeline.runtime.service
    context = CommandContext(principal=pipeline.runtime.principal(), idempotency_key=new_id())
    return service.database.store.read(
        lambda connection: service.worker_locality.fresh_references(
            Transaction(connection, context), pipeline.worker.worker
        )
    )


@pytest.mark.parametrize("remote", [False, True])
@pytest.mark.parametrize("legacy_read", [False, True])
def test_cache_observation_is_own_session_metadata_without_execution_authority(
    pipeline: Pipeline, remote, legacy_read
):
    worker = pipeline.worker.worker
    actor = Principal(
        id=worker.principal_id,
        workspace_id=worker.binding.workspace_id,
        scopes=frozenset({"read", "worker.publish"} if legacy_read else {"worker.publish"}),
    )
    issued = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), actor)
    with ExitStack() as stack:
        if remote:
            url = stack.enter_context(
                serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials))
            )
            client = stack.enter_context(ResearchClient.connect(url, issued.token))
        else:
            client = ResearchClient(LocalTransport(pipeline.runtime, lambda: actor))
        request = read(worker, "worker_cache", worker.binding.worker_session_id)
        absent = client.worker_get(request)
        assert absent["revision"] == 0 and absent["state"] == "unobserved"
        assert pipeline.client.list(RecordQuery(kind="worker_cache"))["items"] == []
        budget = pipeline.client.budget(pipeline.campaign["id"])
        trial = pipeline.client.get("trial", pipeline.trial["id"])
        row = publish(client, advertisement(pipeline))["result"]
        assert row == client.worker_get(request)
        assert row["revision"] == 1
        assert row["data"]["observation"]["trust"] == "worker_reported"
        summary = pipeline.client.list(RecordQuery(kind="worker_cache"))["items"][0]
        assert summary["data"]["reference_count"] == 1
        assert "references" not in summary["data"]
        assert pipeline.client.budget(pipeline.campaign["id"]) == budget
        assert pipeline.client.get("trial", pipeline.trial["id"]) == trial
        assert pipeline.client.list(RecordQuery(kind="attempt"))["items"] == []
        if legacy_read:
            assert client.worker_list(query(worker))["items"]
        else:
            with pytest.raises(ResearchError) as offers_denied:
                client.worker_list(query(worker))
            assert offers_denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        for operation in (
            lambda: client.worker_list(query(worker, "pending")),
            lambda: client.worker_get(read(worker, "worker", worker.binding.worker_id)),
            lambda: client.worker_clock(
                ClockChallenge(
                    worker_id=worker.binding.worker_id,
                    session_id=worker.binding.worker_session_id,
                    nonce=new_id(),
                )
            ),
        ):
            with pytest.raises(ResearchError) as denied:
                operation()
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED


def test_cache_replay_preserves_receipt_time_and_cas_prevents_lost_observations(
    pipeline: Pipeline, monkeypatch
):
    control = pipeline.runtime.service.workers
    now = control.observation_time(pipeline.worker.worker)
    monkeypatch.setattr(control, "observation_time", lambda worker: now)
    client = pipeline.worker.client
    request = advertisement(pipeline)
    first = publish(client, request, key="first-cache")
    now = replace(now, milliseconds=now.milliseconds + 61_000)
    assert publish(client, request, key="first-cache") == first
    assert publish(client, request, revision=1)["result"] == first["result"]
    with pytest.raises(ResearchError) as changed_snapshot:
        publish(client, request.model_copy(update={"truncated": True}), revision=1)
    assert changed_snapshot.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert fresh(pipeline) == ()
    requests = [advertisement(pipeline), advertisement(pipeline)]

    def update(request):
        try:
            return publish(client, request, revision=1)
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(update, requests))
    assert sum(isinstance(result, dict) for result in results) == 1
    assert ErrorCode.REVISION_CONFLICT in results
    assert fresh(pipeline) == request.references


@pytest.mark.parametrize("age", [-1, 59_999, 60_000])
def test_cache_hint_expiry_uses_authority_elapsed_time(pipeline: Pipeline, monkeypatch, age):
    control = pipeline.runtime.service.workers
    origin = control.observation_time(pipeline.worker.worker)
    now = origin
    monkeypatch.setattr(control, "observation_time", lambda worker: now)
    request = advertisement(pipeline)
    publish(pipeline.worker.client, request)
    now = replace(origin, milliseconds=origin.milliseconds + age)
    assert fresh(pipeline) == (request.references if 0 <= age < 60_000 else ())


def test_cache_hint_ignores_replaced_clock_and_unavailable_authority(
    pipeline: Pipeline, monkeypatch
):
    control = pipeline.runtime.service.workers
    origin = control.observation_time(pipeline.worker.worker)
    monkeypatch.setattr(control, "observation_time", lambda worker: origin)
    publish(pipeline.worker.client, advertisement(pipeline))
    monkeypatch.setattr(control, "observation_time", lambda worker: replace(origin, epoch=new_id()))
    assert fresh(pipeline) == ()

    def unavailable(worker):
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Synthetic missing clock")

    monkeypatch.setattr(control, "observation_time", unavailable)
    assert fresh(pipeline) == ()
    with pytest.raises(ResearchError) as refused:
        publish(pipeline.worker.client, advertisement(pipeline), revision=1)
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    row = pipeline.client.get("worker_cache", pipeline.worker.worker.binding.worker_session_id)
    assert row["revision"] == 1


@pytest.mark.parametrize("change", ["missing", "media_type", "size_bytes"])
def test_cache_advertisement_requires_current_complete_artifact_identity(
    pipeline: Pipeline, change
):
    request = advertisement(pipeline)
    reference = request.references[0]
    fields = (
        {"id": new_id()}
        if change == "missing"
        else {change: "text/plain" if change == "media_type" else reference.size_bytes + 1}
    )
    invalid = request.model_copy(
        update={
            "references": (reference.model_copy(update=fields),),
            "examined_bytes": reference.size_bytes + 1,
        }
    )
    with pytest.raises(ResearchError) as refused:
        publish(pipeline.worker.client, invalid)
    assert refused.value.code in {ErrorCode.NOT_FOUND, ErrorCode.ARTIFACT_UNVERIFIED}
    assert pipeline.client.list(RecordQuery(kind="worker_cache"))["items"] == []


def test_cache_observation_cannot_be_published_or_read_for_another_owner(pipeline: Pipeline):
    worker = pipeline.worker.worker
    request = advertisement(pipeline)
    # Even an operator may not impersonate this enrollment's publisher.
    with pytest.raises(ResearchError) as wrong_actor:
        publish(pipeline.client, request)
    assert wrong_actor.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pytest.raises(ResearchError) as stale:
        publish(
            pipeline.worker.client,
            request.model_copy(
                update={"binding": request.binding.model_copy(update={"control_epoch": 99})}
            ),
        )
    assert stale.value.code == ErrorCode.STALE_FENCE
    with pytest.raises(ResearchError) as other_session:
        pipeline.worker.client.worker_get(read(worker, "worker_cache", new_id()))
    assert other_session.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert pipeline.client.list(RecordQuery(kind="worker_cache"))["items"] == []


def test_cache_hint_loses_a_deleted_artifact_without_refreshing_receipt(pipeline: Pipeline):
    request = advertisement(pipeline)
    row = publish(pipeline.worker.client, request)["result"]
    assert fresh(pipeline) == request.references
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "UPDATE records SET state='deleting',payload=json_set(payload,'$.state','deleting') "
            "WHERE workspace_id=? AND kind='artifact' AND id=?",
            (request.binding.workspace_id, request.references[0].id),
        )
    assert fresh(pipeline) == ()
    assert pipeline.client.get("worker_cache", request.binding.worker_session_id) == row


def test_cache_advertisement_validation_is_bounded(pipeline: Pipeline):
    request = advertisement(pipeline)
    for fields in (
        {"references": request.references * 2, "examined_bytes": request.examined_bytes * 2},
        {"examined_bytes": 0},
        {"examined_bytes": True},
        {"references": request.references * 129},
        {"truncated": "false"},
    ):
        with pytest.raises(ValidationError):
            AdvertiseWorkerCache.model_validate(request.model_copy(update=fields))


async def test_cache_advertisement_is_in_the_shared_agent_catalog(pipeline: Pipeline):
    tools = {tool.name: tool for tool in await make_mcp(pipeline.client).list_tools()}
    tool = tools["research_worker_cache_advertise"]
    assert "expected_revision" in tool.input_schema["required"]
    schema = create_app(pipeline.runtime.service, pipeline.runtime.credentials).openapi()
    assert "/v1/commands/worker.cache_advertise" in schema["paths"]
