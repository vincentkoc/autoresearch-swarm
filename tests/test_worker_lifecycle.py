from __future__ import annotations

import concurrent.futures
import threading
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.models import TrialData
from research_swarm.control.results import PublishAttempt
from research_swarm.control.worker_lifecycle import (
    ControlAcknowledgment,
    RequestAttemptCancellation,
    SetWorkerAdmission,
    WorkerHeartbeat,
)
from research_swarm.control.workers import AttemptData
from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.workers.journal import JournalState
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_queries import query, read, scoped_worker


def observer(pipeline: Pipeline) -> tuple[ResearchClient, str]:
    principal = Principal(
        id=pipeline.worker.worker.principal_id,
        workspace_id=pipeline.runtime.identity.workspace_id,
        scopes=frozenset({"worker.query", "worker.publish", "worker.claim"}),
    )
    issued = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), principal)
    return ResearchClient(LocalTransport(pipeline.runtime, lambda: principal)), issued.token


def admission(pipeline: Pipeline, state: str = "draining", revision: int = 0) -> dict[str, Any]:
    return pipeline.client.command(
        "worker.set_admission",
        SetWorkerAdmission(
            binding=pipeline.worker.worker.binding, admission=state, reason="Operator intent"
        ),
        idempotency_key=new_id(),
        expected_revision=revision,
    )["result"]


def cancel(pipeline: Pipeline, attempt: dict[str, Any]) -> dict[str, Any]:
    return pipeline.client.command(
        "attempt.request_cancel",
        RequestAttemptCancellation(attempt_id=attempt["id"], reason="Stop this attempt"),
        idempotency_key=new_id(),
        expected_revision=attempt["revision"],
    )["result"]


def heartbeat(pipeline: Pipeline, sequence: int, *controls: dict[str, Any]) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        binding=pipeline.worker.worker.binding,
        driver_instance_id=new_id(),
        sequence=sequence,
        acknowledgments=tuple(
            ControlAcknowledgment(
                attempt_id=row["id"],
                control_sequence=row["data"]["control_sequence"],
                dispatch_digest=row["data"]["dispatch_digest"],
            )
            for row in controls
        ),
    )


def beat(
    client: ResearchClient, request: WorkerHeartbeat, key: str | None = None
) -> dict[str, Any]:
    return client.command("worker.heartbeat", request, idempotency_key=key or new_id())


def test_presence_does_not_change_enrollment_or_admission_revision(pipeline: Pipeline) -> None:
    client, _ = observer(pipeline)
    worker = pipeline.worker.worker
    original = pipeline.client.get("worker", worker.binding.worker_id)
    initial = client.worker_get(read(worker, "worker_admission", worker.binding.worker_id))
    assert initial["revision"] == 0 and initial["state"] == "active"
    assert (
        client.worker_get(read(worker, "worker_presence", worker.binding.worker_id))["revision"]
        == 0
    )
    with pipeline.runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind IN ('worker_admission','worker_presence')"
            ).fetchone()[0]
            == 0
        )
    assert admission(pipeline, "active") == initial
    first = beat(client, heartbeat(pipeline, 1))["result"]
    assert first["presence"]["revision"] == 1 and first["admission"] == initial
    drained = admission(pipeline)
    assert drained["revision"] == 1
    second = beat(client, heartbeat(pipeline, 7))["result"]
    assert second["presence"]["data"]["heartbeat_sequence"] == 7
    assert second["admission"] == drained
    assert admission(pipeline, revision=1) == drained
    assert admission(pipeline, "active", 1)["revision"] == 2
    assert pipeline.client.get("worker", worker.binding.worker_id) == original
    with pytest.raises(ResearchError) as conflict:
        admission(pipeline, revision=1)
    assert conflict.value.code == ErrorCode.REVISION_CONFLICT


def test_heartbeat_replay_is_historical_and_sequence_survives_restart(pipeline: Pipeline) -> None:
    client, _ = observer(pipeline)
    request = heartbeat(pipeline, 10)
    response = beat(client, request, "lost-heartbeat-reply")
    admission(pipeline)
    runtime = LocalRuntime.open(pipeline.runtime.root)
    principal = Principal(
        id=pipeline.worker.worker.principal_id,
        workspace_id=runtime.identity.workspace_id,
        scopes=frozenset({"worker.publish", "worker.query"}),
    )
    restarted = ResearchClient(LocalTransport(runtime, lambda: principal))
    assert beat(restarted, request, "lost-heartbeat-reply") == response
    assert response["result"]["admission"]["state"] == "active"
    assert (
        restarted.worker_get(
            read(pipeline.worker.worker, "worker_admission", request.binding.worker_id)
        )["state"]
        == "draining"
    )
    for sequence in (1, 9, 10):
        with pytest.raises(ResearchError) as stale:
            beat(restarted, heartbeat(pipeline, sequence))
        assert stale.value.code == ErrorCode.REVISION_CONFLICT
    assert beat(restarted, heartbeat(pipeline, 11))["result"]["presence"]["revision"] == 2


def test_drain_blocks_fresh_claim_without_revoking_accepted_replay(pipeline: Pipeline) -> None:
    client, _ = observer(pipeline)
    job = pipeline.ready()
    attempt = pipeline.worker.claim(job)
    grant = attempt["data"]["dispatch"]
    before = pipeline.client.budget(pipeline.campaign["id"])
    admission(pipeline)
    beat(client, heartbeat(pipeline, 1))
    assert pipeline.worker.claim(job) == attempt
    assert not client.worker_list(query(pipeline.worker.worker))["items"]
    assert (
        client.worker_list(query(pipeline.worker.worker, "pending"))["items"][0]["id"]
        == attempt["id"]
    )
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    assert pipeline.client.get("attempt", attempt["id"])["data"]["dispatch"] == grant
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    with pytest.raises(ResearchError, match="draining"):
        pipeline.worker.claim(pipeline.ready())
    admission(pipeline, "active", 1)
    assert (
        client.worker_get(
            read(
                pipeline.worker.worker, "worker_admission", pipeline.worker.worker.binding.worker_id
            )
        )["state"]
        == "active"
    )


@pytest.mark.parametrize("drain_first", [False, True])
def test_claim_drain_race_linearizes_in_one_database_transaction(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
    drain_first: bool,
) -> None:
    import research_swarm.control.worker_lifecycle as lifecycle
    import research_swarm.control.workers as workers

    entered, release, second_started = threading.Event(), threading.Event(), threading.Event()
    target, name = (lifecycle, "write_worker_state") if drain_first else (workers, "admission_open")
    original = getattr(target, name)

    def hold(*args: Any, **kwargs: Any) -> Any:
        value = original(*args, **kwargs)
        entered.set()
        assert release.wait(5)
        return value

    monkeypatch.setattr(target, name, hold)
    job = pipeline.ready()

    def claim_job() -> dict[str, Any]:
        return pipeline.worker.claim(job)

    first, second = (
        (lambda: admission(pipeline), claim_job)
        if drain_first
        else (claim_job, lambda: admission(pipeline))
    )

    def waiting_second() -> dict[str, Any]:
        second_started.set()
        return second()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        first_result = pool.submit(first)
        assert entered.wait(5)
        second_result = pool.submit(waiting_second)
        assert second_started.wait(5)
        release.set()
        first_result.result(timeout=5)
        if drain_first:
            with pytest.raises(ResearchError, match="draining"):
                second_result.result(timeout=5)
        else:
            second_result.result(timeout=5)
            assert pipeline.worker.claim(job) == first_result.result()
    with pipeline.runtime.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM budget_reservations").fetchone()[0] == (
            0 if drain_first else 1
        )


def test_offline_cancel_is_sticky_and_acknowledgment_does_not_settle(pipeline: Pipeline) -> None:
    client, _ = observer(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    before = pipeline.client.budget(pipeline.campaign["id"])
    control = cancel(pipeline, attempt)["control"]
    assert cancel(pipeline, attempt)["control"] == control
    assert pipeline.client.get("attempt", attempt["id"]) == attempt
    assert client.worker_list(query(pipeline.worker.worker, "controls"))["items"] == [control]
    beat(client, heartbeat(pipeline, 1, control))
    acknowledged = pipeline.client.get("attempt_control", attempt["id"])
    assert acknowledged["state"] == "acknowledged"
    assert acknowledged["data"]["acknowledged_at"] is not None
    assert acknowledged["data"]["completed_at"] is None
    assert not client.worker_list(query(pipeline.worker.worker, "controls"))["items"]
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    assert pipeline.client.get("attempt", attempt["id"]) == attempt


def test_invalid_acknowledgment_rolls_back_entire_heartbeat(pipeline: Pipeline) -> None:
    client, _ = observer(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    control = cancel(pipeline, attempt)["control"]
    request = heartbeat(pipeline, 1, control)
    invalid = request.acknowledgments[0].model_copy(update={"attempt_id": new_id()})
    request = request.model_copy(update={"acknowledgments": (*request.acknowledgments, invalid)})
    with pytest.raises(ResearchError) as denied:
        beat(client, request)
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert pipeline.client.get("attempt_control", attempt["id"]) == control
    assert (
        client.worker_get(
            read(pipeline.worker.worker, "worker_presence", request.binding.worker_id)
        )["revision"]
        == 0
    )
    for changes in ({"control_sequence": 2}, {"dispatch_digest": content_digest(b"other")}):
        bad = request.acknowledgments[0].model_copy(update=changes)
        with pytest.raises(ResearchError) as stale:
            beat(client, request.model_copy(update={"acknowledgments": (bad,)}))
        assert stale.value.code == ErrorCode.STALE_FENCE


@pytest.mark.parametrize("native_cancel", [False, True])
def test_native_closure_resolves_control_without_rewriting_actual_outcome(
    pipeline: Pipeline,
    native_cancel: bool,
) -> None:
    client, _ = observer(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    control = cancel(pipeline, attempt)["control"]
    if native_cancel:
        pipeline.worker._prepare(
            AttemptData.model_validate(attempt["data"]),
            TrialData.model_validate(pipeline.trial["data"]),
        )
        pipeline.worker.journal.request_cancel(attempt["id"], 1)
    accepted = pipeline.worker.execute(attempt["id"])
    expected = "not_started" if native_cancel else "succeeded"
    assert accepted["state"] == expected
    closed = pipeline.client.get("attempt_control", attempt["id"])
    assert closed["state"] == "completed" and closed["data"]["attempt_outcome"] == expected
    assert closed["data"]["resolution"] == (
        "not_started" if native_cancel else "completed_without_cancellation"
    )
    assert closed["data"]["acknowledged_at"] is None
    beat(client, heartbeat(pipeline, 1, control))
    late = pipeline.client.get("attempt_control", attempt["id"])
    assert (
        late["state"] == "completed"
        and late["data"]["completed_at"] == closed["data"]["completed_at"]
    )
    assert late["data"]["acknowledged_at"] is not None
    assert cancel(pipeline, accepted)["control"] == late


def test_cancel_after_accepted_completion_creates_no_pending_control(pipeline: Pipeline) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    accepted = pipeline.worker.execute(attempt["id"])
    assert cancel(pipeline, accepted) == {
        "disposition": "already_terminal",
        "attempt_id": attempt["id"],
        "control": None,
    }
    with pytest.raises(ResearchError) as absent:
        pipeline.client.get("attempt_control", attempt["id"])
    assert absent.value.code == ErrorCode.NOT_FOUND


def test_unknown_publication_preserves_cancellation_and_uncertain_budget(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    control = cancel(pipeline, attempt)["control"]
    original = pipeline.worker.client.command

    def interrupted(operation: str, *args: Any, **kwargs: Any) -> Any:
        if operation == "worker.publish_attempt":
            raise RuntimeError("Synthetic missing publication")
        return original(operation, *args, **kwargs)

    monkeypatch.setattr(pipeline.worker.client, "command", interrupted)
    with pytest.raises(RuntimeError):
        pipeline.worker.execute(attempt["id"])
    closed = pipeline.worker.journal.get(attempt["id"])
    monkeypatch.setattr(pipeline.worker.client, "command", original)
    uncertain = closed.model_copy(update={"state": JournalState.UNKNOWN, "outcome": None})
    observation = tmp_path / "uncertain.json"
    observation.write_text(uncertain.model_dump_json(), encoding="utf-8")
    artifact = pipeline.worker.client.upload_file(observation, idempotency_key="uncertain")
    original(
        "worker.publish_attempt",
        PublishAttempt(attempt_id=attempt["id"], journal_artifact=artifact),
        idempotency_key="unknown",
    )
    assert pipeline.client.get("attempt_control", attempt["id"]) == control
    assert (
        next(
            a
            for a in pipeline.client.budget(pipeline.campaign["id"])["accounts"]
            if a["dimension"] == "attempts"
        )["uncertain"]
        == "1"
    )
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    assert pipeline.worker.journal.get(attempt["id"]) == closed
    assert pipeline.client.get("attempt_control", attempt["id"])["state"] == "completed"


def test_controls_page_over_hundred_records_with_stable_watermark(pipeline: Pipeline) -> None:
    client, _ = observer(pipeline)
    worker = pipeline.worker.worker
    attempt = pipeline.worker.claim(pipeline.ready())
    control = cancel(pipeline, attempt)["control"]
    # Synthetic historical controls exercise cursor behavior without allocating 100 native jobs.
    with pipeline.runtime.database.connect() as connection:
        for _ in range(104):
            identifier = new_id()
            data = {**control["data"], "attempt_id": identifier}
            connection.execute(
                "INSERT INTO records SELECT workspace_id,kind,?,revision,state,"
                "campaign_id,parent_id,?"
                ",created_at,updated_at FROM records WHERE kind='attempt_control' AND id=?",
                (identifier, canonical_bytes(data).decode(), attempt["id"]),
            )
    first = client.worker_list(query(worker, "controls", limit=100))
    assert len(first["items"]) == 100 and first["next_cursor"] is not None
    beat(client, heartbeat(pipeline, 1, *first["items"]))
    with pipeline.runtime.database.connect() as connection:
        late_id = new_id()
        connection.execute(
            "INSERT INTO records SELECT workspace_id,kind,?,revision,'requested',"
            "campaign_id,parent_id,?"
            ",created_at,updated_at FROM records WHERE kind='attempt_control' AND id=?",
            (
                late_id,
                canonical_bytes({**control["data"], "attempt_id": late_id}).decode(),
                attempt["id"],
            ),
        )
    last = client.worker_list(query(worker, "controls", limit=100, cursor=first["next_cursor"]))
    assert len(last["items"]) == 5 and last["next_cursor"] is None
    assert late_id not in {row["id"] for row in last["items"]}
    assert len(client.worker_list(query(worker, "controls", limit=100))["items"]) == 6
    other_client, other, _ = scoped_worker(pipeline)
    with pytest.raises(ResearchError, match="scope differs"):
        other_client.worker_list(query(other, "controls", cursor=first["next_cursor"]))
    assert other_client.worker_list(query(other, "controls"))["items"] == []


def test_scoped_http_control_access_and_operator_boundaries(pipeline: Pipeline) -> None:
    local, token = observer(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    cancel(pipeline, attempt)
    app = create_app(pipeline.runtime.service, pipeline.runtime.credentials)
    with serving(app) as url, ResearchClient.connect(url, token) as remote:
        assert remote.worker_list(query(pipeline.worker.worker, "controls")) == local.worker_list(
            query(pipeline.worker.worker, "controls")
        )
        response = beat(remote, heartbeat(pipeline, 1))
        assert response["result"]["presence"]["revision"] == 1
        for operation in ("worker.set_admission", "attempt.request_cancel"):
            with pytest.raises(ResearchError) as denied:
                remote.command(operation, {}, idempotency_key=new_id())
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        with pytest.raises(ResearchError, match="identity differs"):
            remote.worker_get(read(pipeline.worker.worker, "worker_presence", new_id()))
    other, foreign, _ = scoped_worker(pipeline)
    with pytest.raises(ResearchError):
        other.worker_get(
            read(
                pipeline.worker.worker, "worker_admission", pipeline.worker.worker.binding.worker_id
            )
        )
    wrong = heartbeat(pipeline, 2).model_copy(update={"binding": foreign.binding})
    with pytest.raises(ResearchError, match="does not own"):
        beat(local, wrong)
    stale = heartbeat(pipeline, 2).model_copy(
        update={"binding": pipeline.worker.worker.binding.model_copy(update={"control_epoch": 2})}
    )
    with pytest.raises(ResearchError) as denied:
        beat(local, stale)
    assert denied.value.code == ErrorCode.STALE_FENCE


def test_cancel_resolution_names_attempt_failure_without_fabricating_native_failure(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    cancel(pipeline, attempt)

    def invalid(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("Synthetic invalid workload report")

    monkeypatch.setattr("research_swarm.workers.local.read_report", invalid)
    accepted = pipeline.worker.execute(attempt["id"])
    assert accepted["state"] == "failed"
    native = pipeline.worker.journal.get(attempt["id"]).outcome
    assert native is not None and native.reason == "exited" and native.exit_code == 0
    control = pipeline.client.get("attempt_control", attempt["id"])
    assert control["data"]["attempt_outcome"] == "failed"
    assert control["data"]["resolution"] == "completed_without_cancellation"
    assert "native_outcome" not in control["data"]
