from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import SubmitTrial
from research_swarm.control.queries import RecordQuery
from research_swarm.control.worker_lifecycle import SetWorkerAdmission, WorkerHeartbeat
from research_swarm.control.worker_records import WorkerData
from research_swarm.control.worker_turnover import FenceWorker, ReplaceWorkerSession
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.recovery import recover_worker_attempt
from research_swarm.workers.remote_enrollment import WORKER_SCOPES
from tests.test_closure import close_operationally
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_queries import claim, query, read, scoped_worker
from tests.test_worker_recovery import Recoverable
from tests.test_worker_recovery import recoverable as recoverable
from tests.test_worker_sessions import advance_epoch


def fence_request(pipeline: Pipeline, worker: WorkerData) -> FenceWorker:
    authority = pipeline.client.context(pipeline.campaign["id"])["authority"]
    return FenceWorker(
        binding=worker.binding,
        expected_controller_id=authority["controller_id"],
        expected_control_epoch=authority["control_epoch"],
        reason="Replace original session explicitly",
    )


def fence(pipeline: Pipeline, worker: WorkerData) -> dict[str, Any]:
    return pipeline.client.command(
        "worker.fence",
        fence_request(pipeline, worker),
        idempotency_key=new_id(),
        expected_revision=pipeline.client.get("worker", worker.binding.worker_id)["revision"],
    )["result"]


def replacement_request(
    pipeline: Pipeline, worker: WorkerData, fenced: dict[str, Any]
) -> ReplaceWorkerSession:
    return ReplaceWorkerSession(
        **fence_request(pipeline, worker).model_dump(exclude={"schema_version"}),
        fence_id=fenced["fence"]["id"],
        session_id=new_id(),
        principal_id=new_id(),
    )


def replace(
    pipeline: Pipeline, worker: WorkerData, fenced: dict[str, Any]
) -> tuple[dict[str, Any], ResearchClient]:
    request = replacement_request(pipeline, worker, fenced)
    principal = Principal(
        id=request.principal_id, workspace_id=worker.binding.workspace_id, scopes=WORKER_SCOPES
    )
    issued = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), principal)
    result = pipeline.client.command(
        "worker.replace_session",
        request,
        idempotency_key=new_id(),
        expected_revision=fenced["worker"]["revision"],
    )["result"]
    client = ResearchClient(
        LocalTransport(pipeline.runtime, lambda: pipeline.runtime.credentials.resolve(issued.token))
    )
    return result, client


def activate(pipeline: Pipeline, replaced: dict[str, Any]) -> None:
    worker = WorkerData.model_validate(replaced["worker"]["data"])
    pipeline.client.command(
        "worker.set_admission",
        SetWorkerAdmission(
            binding=worker.binding, admission="active", reason="Replacement installed"
        ),
        idempotency_key=new_id(),
        expected_revision=replaced["admission"]["revision"],
    )


def test_fence_replacement_resets_projections_and_retains_old_identity(pipeline: Pipeline) -> None:
    _, old, _ = scoped_worker(pipeline)
    principal = Principal(
        id=old.principal_id, workspace_id=old.binding.workspace_id, scopes=WORKER_SCOPES
    )
    credential = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), principal)
    old_client = ResearchClient(
        LocalTransport(
            pipeline.runtime, lambda: pipeline.runtime.credentials.resolve(credential.token)
        )
    )
    heartbeat = WorkerHeartbeat(binding=old.binding, driver_instance_id=new_id(), sequence=17)
    old_client.command("worker.heartbeat", heartbeat, idempotency_key=new_id())
    pipeline.client.command(
        "worker.set_admission",
        SetWorkerAdmission(
            binding=old.binding, admission="draining", reason="Drain before replacement"
        ),
        idempotency_key=new_id(),
        expected_revision=0,
    )
    original = pipeline.client.get("worker", old.binding.worker_id)
    fenced = fence(pipeline, old)
    assert fenced["worker"]["state"] == "fenced"
    assert fenced["worker"]["data"] == original["data"]
    assert fence(pipeline, old) == fenced
    replaced, new_client = replace(pipeline, old, fenced)
    new = WorkerData.model_validate(replaced["worker"]["data"])
    assert new.binding.worker_id == old.binding.worker_id
    assert new.binding.worker_session_id != old.binding.worker_session_id
    assert new.principal_id != old.principal_id
    assert new.profiles == old.profiles and new.project_ids == old.project_ids
    assert replaced["admission"]["state"] == "draining"
    assert replaced["admission"]["revision"] == 2
    assert replaced["presence"]["revision"] == 2
    assert replaced["presence"]["data"]["heartbeat_sequence"] == 0
    assert replaced["replacement"]["data"]["previous_presence"]["heartbeat_sequence"] == 17
    assert (
        old_client.worker_get(read(old, "worker_session", old.binding.worker_session_id))["data"]
        == original["data"]
    )
    assert new_client.worker_list(query(new))["items"] == []
    with pytest.raises(ResearchError) as denied:
        old_client.worker_get(read(new, "worker", new.binding.worker_id))
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pytest.raises(ResearchError):
        old_client.command(
            "worker.heartbeat",
            heartbeat.model_copy(update={"sequence": 18}),
            idempotency_key=new_id(),
        )
    with pytest.raises(ResearchError):
        fence(pipeline, old)
    activate(pipeline, replaced)
    assert len(new_client.worker_list(query(new))["items"]) == 1
    new_client.command(
        "worker.heartbeat",
        WorkerHeartbeat(binding=new.binding, driver_instance_id=new_id(), sequence=1),
        idempotency_key=new_id(),
    )
    assert (
        new_client.worker_get(read(new, "worker_presence", new.binding.worker_id))["data"][
            "heartbeat_sequence"
        ]
        == 1
    )


@pytest.mark.parametrize("identity", ["session", "principal"])
def test_replacement_rejects_reused_identity_atomically(pipeline: Pipeline, identity: str) -> None:
    _, old, _ = scoped_worker(pipeline)
    fenced = fence(pipeline, old)
    request = replacement_request(pipeline, old, fenced)
    request = request.model_copy(
        update={"session_id": old.binding.worker_session_id}
        if identity == "session"
        else {"principal_id": old.principal_id}
    )
    with pytest.raises(ResearchError) as refused:
        pipeline.client.command(
            "worker.replace_session",
            request,
            idempotency_key=new_id(),
            expected_revision=fenced["worker"]["revision"],
        )
    assert refused.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert pipeline.client.get("worker", old.binding.worker_id) == fenced["worker"]
    assert pipeline.client.list(RecordQuery(kind="worker_replacement"))["items"] == []


def test_two_replacements_from_one_revision_commit_one_session_and_replay(
    pipeline: Pipeline,
) -> None:
    _, old, _ = scoped_worker(pipeline)
    fenced = fence(pipeline, old)
    barrier = threading.Barrier(2)
    requests = [replacement_request(pipeline, old, fenced) for _ in range(2)]
    keys = [new_id(), new_id()]

    def compete(index: int) -> Any:
        barrier.wait(timeout=5)
        try:
            return pipeline.client.command(
                "worker.replace_session",
                requests[index],
                idempotency_key=keys[index],
                expected_revision=fenced["worker"]["revision"],
            )
        except ResearchError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(compete, range(2)))
    winners = [i for i, reply in enumerate(replies) if isinstance(reply, dict)]
    assert len(winners) == 1
    winner = winners[0]
    assert isinstance(replies[1 - winner], ResearchError)
    assert replies[1 - winner].code == ErrorCode.REVISION_CONFLICT
    advance_epoch(pipeline)
    replay = pipeline.client.command(
        "worker.replace_session",
        requests[winner],
        idempotency_key=keys[winner],
        expected_revision=fenced["worker"]["revision"],
    )
    assert replay == replies[winner]
    assert len(pipeline.client.list(RecordQuery(kind="worker_replacement"))["items"]) == 1


def test_epoch_drift_requires_current_operator_authority_and_can_fence_old_session(
    pipeline: Pipeline,
) -> None:
    _, old, _ = scoped_worker(pipeline)
    stale = fence_request(pipeline, old)
    advance_epoch(pipeline)
    original = pipeline.client.get("worker", old.binding.worker_id)
    with pytest.raises(ResearchError) as refused:
        pipeline.client.command(
            "worker.fence", stale, idempotency_key=new_id(), expected_revision=original["revision"]
        )
    assert refused.value.code == ErrorCode.STALE_FENCE
    fenced = fence(pipeline, old)
    replaced, _ = replace(pipeline, old, fenced)
    assert replaced["worker"]["data"]["binding"]["control_epoch"] == old.binding.control_epoch + 1
    assert (
        pipeline.client.get("worker_session", old.binding.worker_session_id)["data"]
        == original["data"]
    )


@pytest.mark.parametrize("pipeline", [{"search": {"max_parallel_trials": 2}}], indirect=True)
def test_replacement_preserves_original_native_and_controller_capacity_until_closure(
    pipeline: Pipeline, recoverable: Recoverable
) -> None:
    old = recoverable.enrollment.bundle.worker
    original = pipeline.client.get("attempt", recoverable.attempt_id)
    budget = pipeline.client.budget(pipeline.campaign["id"])
    fenced = fence(pipeline, old)
    replaced, new_client = replace(pipeline, old, fenced)
    new = WorkerData.model_validate(replaced["worker"]["data"])
    activate(pipeline, replaced)
    assert pipeline.client.get("attempt", recoverable.attempt_id) == original
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    with pytest.raises(ResearchError) as borrowed:
        new_client.worker_get(read(old, "historical_attempt", recoverable.attempt_id))
    assert borrowed.value.code == ErrorCode.AUTHORIZATION_DENIED
    new_native = LocalWorker(
        new_client,
        new,
        root=recoverable.root.parent / "replacement-native",
        machine_registry=recoverable.enrollment.worker.resources.path,
    )
    assert new_native.resources.availability() == {"cpu": 0}
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=pipeline.trial["data"]["spec"]["candidate_id"],
            validation_id=pipeline.trial["data"]["validation_id"],
            seed=13,
        ),
        idempotency_key=new_id(),
        expected_revision=campaign["revision"],
    )
    job = pipeline.ready()
    with pytest.raises(ResearchError) as occupied:
        claim(new_client, new, job)
    assert occupied.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    recovered = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert recovered.delivery_state == "quarantined" and recovered.custody_state == "released"
    assert new_native.resources.availability() == {"cpu": 1}
    with pytest.raises(ResearchError) as still_occupied:
        claim(new_client, new, job)
    assert still_occupied.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    submission = pipeline.client.get("attempt_submission", recovered.submission_id)
    close_operationally(pipeline, submission)
    admitted = claim(new_client, new, job)
    assert admitted["data"]["dispatch"]["worker_session_id"] == new.binding.worker_session_id
    assert pipeline.client.list(RecordQuery(kind="run_result"))["items"] == []


def test_claim_and_fence_race_preserves_any_issued_grant(pipeline: Pipeline) -> None:
    old_client, old, _ = scoped_worker(pipeline)
    job = pipeline.ready()
    barrier = threading.Barrier(2)

    def admit() -> Any:
        barrier.wait(timeout=5)
        try:
            return claim(old_client, old, job)
        except ResearchError as error:
            return error

    def stop() -> Any:
        barrier.wait(timeout=5)
        return fence(pipeline, old)

    with ThreadPoolExecutor(max_workers=2) as pool:
        admission, fencing = pool.submit(admit), pool.submit(stop)
        result, fenced = admission.result(), fencing.result()
    assert fenced["worker"]["state"] == "fenced"
    attempts = pipeline.client.list(RecordQuery(kind="attempt"))["items"]
    if isinstance(result, ResearchError):
        assert result.code == ErrorCode.STALE_FENCE
        assert attempts == []
    else:
        assert len(attempts) == 1 and attempts[0]["id"] == result["id"]
        assert pipeline.client.get("attempt", result["id"]) == result
        assert old_client.worker_get(read(old, "historical_attempt", result["id"])) == result
    with pytest.raises(ResearchError):
        claim(old_client, old, job)


def test_worker_credential_cannot_fence_or_replace_itself(pipeline: Pipeline) -> None:
    client, old, _ = scoped_worker(pipeline)
    current = pipeline.client.get("worker", old.binding.worker_id)
    with pytest.raises(ResearchError) as denied:
        client.command(
            "worker.fence",
            fence_request(pipeline, old),
            idempotency_key=new_id(),
            expected_revision=current["revision"],
        )
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    fenced = fence(pipeline, old)
    with pytest.raises(ResearchError) as denied:
        client.command(
            "worker.replace_session",
            replacement_request(pipeline, old, fenced),
            idempotency_key=new_id(),
            expected_revision=fenced["worker"]["revision"],
        )
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert pipeline.client.get("worker", old.binding.worker_id) == fenced["worker"]


@pytest.mark.parametrize("identity", ["session", "principal"])
def test_legacy_unarchived_worker_identity_cannot_be_borrowed(
    pipeline: Pipeline, identity: str
) -> None:
    _, old, _ = scoped_worker(pipeline)
    _, other, _ = scoped_worker(pipeline)
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "DELETE FROM records WHERE kind='worker_session' AND id=?",
            (other.binding.worker_session_id,),
        )
    fenced = fence(pipeline, old)
    request = replacement_request(pipeline, old, fenced).model_copy(
        update={"session_id": other.binding.worker_session_id}
        if identity == "session"
        else {"principal_id": other.principal_id}
    )
    with pytest.raises(ResearchError) as refused:
        pipeline.client.command(
            "worker.replace_session",
            request,
            idempotency_key=new_id(),
            expected_revision=fenced["worker"]["revision"],
        )
    assert refused.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert pipeline.client.get("worker", old.binding.worker_id) == fenced["worker"]


def test_stale_fence_receipt_and_unfenced_replacement_do_not_mutate_state(
    pipeline: Pipeline,
) -> None:
    _, old, _ = scoped_worker(pipeline)
    current = pipeline.client.get("worker", old.binding.worker_id)
    request = ReplaceWorkerSession(
        **fence_request(pipeline, old).model_dump(exclude={"schema_version"}),
        fence_id=new_id(),
        session_id=new_id(),
        principal_id=new_id(),
    )
    with pytest.raises(ResearchError) as unfenced:
        pipeline.client.command(
            "worker.replace_session",
            request,
            idempotency_key=new_id(),
            expected_revision=current["revision"],
        )
    assert unfenced.value.code == ErrorCode.INVALID_TRANSITION
    first_fence = fence(pipeline, old)
    replaced, _ = replace(pipeline, old, first_fence)
    new = WorkerData.model_validate(replaced["worker"]["data"])
    second_fence = fence(pipeline, new)
    stale = replacement_request(pipeline, new, second_fence).model_copy(
        update={"fence_id": first_fence["fence"]["id"]}
    )
    with pytest.raises(ResearchError) as mismatched:
        pipeline.client.command(
            "worker.replace_session",
            stale,
            idempotency_key=new_id(),
            expected_revision=second_fence["worker"]["revision"],
        )
    assert mismatched.value.code == ErrorCode.STALE_FENCE
    assert pipeline.client.get("worker", new.binding.worker_id) == second_fence["worker"]
    assert len(pipeline.client.list(RecordQuery(kind="worker_replacement"))["items"]) == 1


async def test_cli_mcp_http_and_sdk_share_replacement_acknowledgment(pipeline: Pipeline) -> None:
    from mcp import Client

    from research_swarm.api.http import create_app
    from research_swarm.runtime import read_private
    from tests.test_cli_mcp import cli, stdio
    from tests.test_http_delivery import serving

    _, old, _ = scoped_worker(pipeline)
    row = pipeline.client.get("worker", old.binding.worker_id)
    request = fence_request(pipeline, old)
    fenced = cli(
        pipeline.runtime.root,
        "command",
        "worker.fence",
        "--key",
        "turnover-fence",
        "--revision",
        str(row["revision"]),
        payload=request.model_dump(mode="json"),
    )["result"]
    replacement = replacement_request(pipeline, old, fenced)
    async with Client(stdio(pipeline.runtime.root), read_timeout_seconds=20, cache=None) as agent:
        result = await agent.call_tool(
            "research_worker_replace_session",
            {
                "request": replacement.model_dump(mode="json"),
                "idempotency_key": "turnover-replace",
                "expected_revision": fenced["worker"]["revision"],
            },
        )
        assert not result.is_error and result.structured_content is not None
        acknowledgment = result.structured_content
    token = read_private(pipeline.runtime.root, "operator.token", limit=256).decode("ascii")
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, token) as remote,
    ):
        replay = remote.command(
            "worker.replace_session",
            replacement,
            idempotency_key="turnover-replace",
            expected_revision=fenced["worker"]["revision"],
        )
    assert replay["command_id"] == acknowledgment["command_id"]
    assert replay["disposition"] == acknowledgment["disposition"] == "accepted"
    for kind in ("worker", "replacement", "presence", "admission"):
        assert replay["result"][kind]["id"] == acknowledgment["result"][kind]["id"]
        assert replay["result"][kind]["revision"] == acknowledgment["result"][kind]["revision"]
    assert (
        pipeline.client.command(
            "worker.replace_session",
            replacement,
            idempotency_key="turnover-replace",
            expected_revision=fenced["worker"]["revision"],
        )
        == replay
    )
    assert len(pipeline.client.list(RecordQuery(kind="worker_replacement"))["items"]) == 1
