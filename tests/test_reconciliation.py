from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reconciliation import ReconcileAttempt, publication_digest
from research_swarm.control.results import PublishAttempt
from research_swarm.control.worker_outputs import WorkerOutputIntent
from research_swarm.control.workers import WorkerData
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import RunResult
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.projects.workload import WorkloadReport
from research_swarm.runtime import LocalRuntime
from research_swarm.workers.journal import JournalState
from research_swarm.workers.local import LocalWorker
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_outputs import output_client
from tests.test_worker_queries import scoped_worker
from tests.test_worker_sessions import advance_epoch


@dataclass
class ClosedAttempt:
    client: ResearchClient
    native: LocalWorker
    publication: PublishAttempt


def close_without_publication(
    client: ResearchClient,
    worker: WorkerData,
    attempt_id: str,
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> ClosedAttempt:
    native = LocalWorker(
        client, worker, root=root / "native", machine_registry=root / "machine" / "registry.sqlite"
    )
    original = client.command
    captured = []

    def disconnect(operation: str, request: Any, **kwargs: Any) -> Any:
        if operation == "worker.publish_attempt":
            captured.append(request)
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Synthetic lost controller")
        return original(operation, request, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(client, "command", disconnect)
        with pytest.raises(ResearchError, match="Synthetic lost controller"):
            native.execute(attempt_id)
    assert len(captured) == 1
    closed = native.journal.get(attempt_id)
    assert closed.state == JournalState.CLOSED and closed.outcome is not None
    assert closed.outcome.exit_code == 0 and closed.outcome.closure.scope_closed
    return ClosedAttempt(client, native, captured[0])


@pytest.fixture
def closed(pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ClosedAttempt:
    client, worker, _, target = output_client(pipeline)
    return close_without_publication(client, worker, target.attempt_id, tmp_path, monkeypatch)


def submit(closed: ClosedAttempt, publication: PublishAttempt | None = None) -> dict[str, Any]:
    return closed.client.command(
        "worker.submit_evidence", publication or closed.publication, idempotency_key=new_id()
    )["result"]


def intent(pipeline: Pipeline, submission: dict[str, Any]) -> ReconcileAttempt:
    authority = pipeline.client.context(pipeline.campaign["id"])["authority"]
    return ReconcileAttempt(
        attempt_id=submission["data"]["publication"]["attempt_id"],
        submission_id=submission["id"],
        expected_controller_id=authority["controller_id"],
        expected_control_epoch=authority["control_epoch"],
        reason="Recover original closure after disconnection",
    )


def reconcile(
    pipeline: Pipeline, submission: dict[str, Any], *, client: ResearchClient | None = None
) -> dict[str, Any]:
    request = intent(pipeline, submission)
    return (client or pipeline.client).command(
        "attempt.reconcile",
        request,
        idempotency_key=new_id(),
        expected_revision=pipeline.client.get("attempt", request.attempt_id)["revision"],
    )["result"]


def test_http_late_submission_is_quarantined_then_reconciled_once_after_restart(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, worker, credential, target = output_client(pipeline)
    app = create_app(pipeline.runtime.service, pipeline.runtime.credentials)
    with serving(app) as url, ResearchClient.connect(url, credential.token) as remote:
        closed = close_without_publication(remote, worker, target.attempt_id, tmp_path, monkeypatch)
        journal = closed.native.journal.get(target.attempt_id)
        budget = pipeline.client.budget(pipeline.campaign["id"])
        attempt = pipeline.client.get("attempt", target.attempt_id)
        jobs = pipeline.client.list(RecordQuery(kind="job", campaign_id=pipeline.campaign["id"]))
        advance_epoch(pipeline)
        with pytest.raises(ResearchError) as stale:
            remote.command("worker.publish_attempt", closed.publication, idempotency_key=new_id())
        assert stale.value.code == ErrorCode.STALE_FENCE
        submission = submit(closed)
        assert submission["state"] == "quarantined"
        assert submit(closed) == submission
        assert pipeline.client.get("attempt", target.attempt_id) == attempt
        assert pipeline.client.budget(pipeline.campaign["id"]) == budget
        assert (
            pipeline.client.list(RecordQuery(kind="job", campaign_id=pipeline.campaign["id"]))[
                "items"
            ]
            == jobs["items"]
        )
        assert pipeline.client.list(RecordQuery(kind="run_result"))["items"] == []
        assert pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"] == []
        pipeline.runtime.credentials.revoke(pipeline.runtime.principal(), credential.id)
        with pytest.raises(ResearchError) as revoked:
            remote.command("worker.submit_evidence", closed.publication, idempotency_key=new_id())
        assert revoked.value.code == ErrorCode.AUTHORIZATION_DENIED
        reopened = LocalRuntime.open(pipeline.runtime.root)
        operator = ResearchClient(LocalTransport(reopened))
        recovered = reconcile(pipeline, submission, client=operator)
        assert recovered["disposition"] == "reconciled"
        accepted = recovered["attempt"]
        assert accepted["state"] == "succeeded"
        assert accepted["data"]["dispatch"] == attempt["data"]["dispatch"]
        assert accepted["data"]["result_digest"] == publication_digest(closed.publication)
        result = RunResult.model_validate(
            pipeline.client.get("run_result", target.attempt_id)["data"]
        )
        result.verify()
        assert result.control_epoch == worker.binding.control_epoch
        assert result.process_closure_ref == closed.publication.journal_artifact
        receipt = recovered["reconciliation"]["data"]
        assert receipt["original_binding"] == worker.binding.model_dump(mode="json")
        assert receipt["control_epoch"] == worker.binding.control_epoch + 1
        assert receipt["run_result_digest"] == result.result_digest
        assert closed.native.journal.get(target.attempt_id) == journal
        assert closed.native.resources.availability() == {"cpu": 1}
        assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1
        accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
        assert all(item["reserved"] == "0" and item["uncertain"] == "0" for item in accounts)
        assert next(item for item in accounts if item["dimension"] == "attempts")["consumed"] == "1"
        assert (
            int(next(item for item in accounts if item["dimension"] == "execution_ms")["consumed"])
            == result.elapsed_ms
        )
        assert pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"] == []
        assert pipeline.client.get("attempt_submission", submission["id"]) == submission
        assert reconcile(pipeline, submission)["disposition"] == "already_accepted"
        assert len(pipeline.client.list(RecordQuery(kind="attempt_reconciliation"))["items"]) == 1


def test_unknown_and_closed_submissions_preserve_history_and_uncertainty(
    pipeline: Pipeline, closed: ClosedAttempt, tmp_path: Path
) -> None:
    entry = closed.native.journal.get(closed.publication.attempt_id)
    unknown = entry.model_copy(update={"state": JournalState.UNKNOWN, "outcome": None})
    path = tmp_path / "unknown.json"
    path.write_bytes(canonical_bytes(unknown.model_dump(mode="json")))
    reference = closed.client.worker_upload_file(
        WorkerOutputIntent(
            worker_id=entry.dispatch.worker_id,
            session_id=entry.dispatch.worker_session_id,
            attempt_id=entry.dispatch.attempt_id,
            role="journal",
        ),
        path,
        idempotency_key="unknown-observation",
    )
    publication = PublishAttempt(attempt_id=entry.dispatch.attempt_id, journal_artifact=reference)
    closed.client.command("worker.publish_attempt", publication, idempotency_key="publish-unknown")
    budget = pipeline.client.budget(pipeline.campaign["id"])
    assert (
        next(item for item in budget["accounts"] if item["dimension"] == "attempts")["uncertain"]
        == "1"
    )
    advance_epoch(pipeline)
    uncertain = submit(closed, publication)
    with pytest.raises(ResearchError) as blocked:
        reconcile(pipeline, uncertain)
    assert blocked.value.code == ErrorCode.EXECUTION_UNKNOWN
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    complete = submit(closed)
    assert complete["id"] != uncertain["id"]
    assert reconcile(pipeline, complete)["attempt"]["state"] == "succeeded"
    assert len(pipeline.client.list(RecordQuery(kind="attempt_submission"))["items"]) == 2
    assert pipeline.client.get("attempt_submission", uncertain["id"]) == uncertain


@pytest.mark.parametrize("change", ["logs", "outputs", "closure", "seed", "current_attempt"])
def test_invalid_or_superseded_observation_stays_quarantined(
    pipeline: Pipeline, closed: ClosedAttempt, tmp_path: Path, change: str
) -> None:
    request = closed.publication
    if change == "logs":
        request = request.model_copy(
            update={"logs": {"stdout": request.logs["stderr"], "stderr": request.logs["stderr"]}}
        )
        # A receipt for stderr cannot be reused as stdout unless the bytes are identical.
        if request.logs["stdout"] == closed.publication.logs["stdout"]:
            pytest.fail("Fixture needs distinguishable stdout and stderr")
        with pytest.raises(ResearchError) as absent:
            submit(closed, request)
        assert absent.value.code == ErrorCode.ARTIFACT_UNVERIFIED
        return
    if change == "outputs":
        request = request.model_copy(update={"outputs": {}})
    if change in {"closure", "seed"}:
        entry = closed.native.journal.get(request.attempt_id)
        if change == "closure":
            assert entry.outcome is not None
            native = entry.outcome.closure.identity.model_copy(update={"scope_id": new_id()})
            # Replace the journal's native identity while preserving the actual closure.
            altered = entry.model_copy(update={"native": native})
            role, data = "journal", altered.model_dump(mode="json")
        else:
            assert entry.prepared is not None
            report = WorkloadReport.model_validate_json(
                Path(entry.prepared.context.report_path).read_bytes()
            )
            role, data = (
                "report",
                report.model_copy(update={"observed_seed": report.observed_seed + 1}).model_dump(
                    mode="json"
                ),
            )
        path = tmp_path / (change + ".json")
        path.write_bytes(canonical_bytes(data))
        reference = closed.client.worker_upload_file(
            WorkerOutputIntent(
                worker_id=entry.dispatch.worker_id,
                session_id=entry.dispatch.worker_session_id,
                attempt_id=request.attempt_id,
                role=role,
            ),
            path,
            idempotency_key=change,
        )
        request = request.model_copy(update={role + "_artifact": reference})
    advance_epoch(pipeline)
    submission = submit(closed, request)
    if change == "current_attempt":
        attempt = pipeline.client.get("attempt", request.attempt_id)
        with pipeline.runtime.database.connect() as connection:
            connection.execute(
                "UPDATE records SET payload=json_set(payload,'$.current_attempt_id',?) "
                "WHERE kind='job' AND id=?",
                (new_id(), attempt["data"]["dispatch"]["job_id"]),
            )
    budget = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError):
        reconcile(pipeline, submission)
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    assert pipeline.client.get("attempt_submission", submission["id"]) == submission
    assert pipeline.client.list(RecordQuery(kind="run_result"))["items"] == []
    assert pipeline.client.list(RecordQuery(kind="attempt_reconciliation"))["items"] == []


def test_operator_scope_is_separate_from_original_worker_and_current_credentials(
    pipeline: Pipeline, closed: ClosedAttempt
) -> None:
    other, _, _ = scoped_worker(pipeline)
    with pytest.raises(ResearchError) as foreign:
        other.command("worker.submit_evidence", closed.publication, idempotency_key=new_id())
    assert foreign.value.code == ErrorCode.AUTHORIZATION_DENIED
    advance_epoch(pipeline)
    submission = submit(closed)
    with pytest.raises(ResearchError) as denied:
        reconcile(pipeline, submission, client=closed.client)
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    operator = Principal(
        id=new_id(),
        workspace_id=pipeline.runtime.identity.workspace_id,
        scopes=frozenset({"attempt.reconcile"}),
    )
    credential = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), operator)
    client = ResearchClient(
        LocalTransport(
            pipeline.runtime, lambda: pipeline.runtime.credentials.resolve(credential.token)
        )
    )
    assert (
        reconcile(pipeline, submission, client=client)["reconciliation"]["data"]["operator_id"]
        == operator.id
    )


@pytest.mark.parametrize("race", ["reconciler", "publication"])
def test_concurrent_acceptance_settles_original_once(
    pipeline: Pipeline, closed: ClosedAttempt, race: str
) -> None:
    submission = submit(closed)
    request = intent(pipeline, submission)
    revision = pipeline.client.get("attempt", request.attempt_id)["revision"]
    barrier = threading.Barrier(2)

    def compete(index: int) -> Any:
        barrier.wait(timeout=5)
        try:
            if index == 0 and race == "publication":
                return closed.client.command(
                    "worker.publish_attempt", closed.publication, idempotency_key="race-publication"
                )
            return pipeline.client.command(
                "attempt.reconcile",
                request,
                idempotency_key="race-" + str(index),
                expected_revision=revision,
            )
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(compete, range(2)))
    assert any(isinstance(value, dict) for value in results)
    assert all(isinstance(value, dict) or value == ErrorCode.REVISION_CONFLICT for value in results)
    assert len(pipeline.client.list(RecordQuery(kind="run_result"))["items"]) == 1
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert next(item for item in accounts if item["dimension"] == "attempts")["consumed"] == "1"
    with pipeline.runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM events WHERE "
                "json_extract(payload,'$.type')='attempt.observation_accepted'"
            ).fetchone()[0]
            == 1
        )
    assert len(pipeline.client.list(RecordQuery(kind="attempt_reconciliation"))["items"]) <= 1


def test_exact_command_replays_survive_epoch_change_but_not_changed_payloads(
    pipeline: Pipeline, closed: ClosedAttempt
) -> None:
    submitted = closed.client.command(
        "worker.submit_evidence", closed.publication, idempotency_key="lost-submit"
    )
    request = intent(pipeline, submitted["result"])
    revision = pipeline.client.get("attempt", request.attempt_id)["revision"]
    accepted = pipeline.client.command(
        "attempt.reconcile", request, idempotency_key="lost-reconcile", expected_revision=revision
    )
    publication = closed.client.command(
        "worker.publish_attempt", closed.publication, idempotency_key="lost-publication"
    )
    advance_epoch(pipeline)
    budget = pipeline.client.budget(pipeline.campaign["id"])
    assert (
        closed.client.command(
            "worker.submit_evidence", closed.publication, idempotency_key="lost-submit"
        )
        == submitted
    )
    assert (
        pipeline.client.command(
            "attempt.reconcile",
            request,
            idempotency_key="lost-reconcile",
            expected_revision=revision,
        )
        == accepted
    )
    assert (
        closed.client.command(
            "worker.publish_attempt", closed.publication, idempotency_key="lost-publication"
        )
        == publication
    )
    with pytest.raises(ResearchError) as changed:
        closed.client.command(
            "worker.publish_attempt",
            closed.publication.model_copy(update={"finding": "changed"}),
            idempotency_key="lost-publication",
        )
    assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    with pytest.raises(ResearchError) as stale:
        pipeline.client.command(
            "attempt.reconcile",
            request,
            idempotency_key="new-stale-reconcile",
            expected_revision=revision,
        )
    assert stale.value.code == ErrorCode.STALE_FENCE


def test_terminal_campaign_does_not_reopen_from_reconciliation(
    pipeline: Pipeline, closed: ClosedAttempt
) -> None:
    submission = submit(closed)
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "UPDATE records SET state='completed' WHERE kind='campaign' AND id=?",
            (pipeline.campaign["id"],),
        )
    before = pipeline.client.get("campaign", pipeline.campaign["id"])
    budget = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError) as terminal:
        reconcile(pipeline, submission)
    assert terminal.value.code == ErrorCode.INVALID_TRANSITION
    assert pipeline.client.get("campaign", pipeline.campaign["id"]) == before
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget


def test_dedicated_operator_can_reconcile_legacy_unscoped_worker(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    closed = close_without_publication(
        pipeline.worker.client, pipeline.worker.worker, attempt["id"], tmp_path, monkeypatch
    )
    advance_epoch(pipeline)
    submission = submit(closed)
    operator = Principal(
        id=new_id(),
        workspace_id=pipeline.runtime.identity.workspace_id,
        scopes=frozenset({"attempt.reconcile"}),
    )
    credential = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), operator)
    client = ResearchClient(
        LocalTransport(
            pipeline.runtime, lambda: pipeline.runtime.credentials.resolve(credential.token)
        )
    )
    assert reconcile(pipeline, submission, client=client)["attempt"]["state"] == "succeeded"


def test_normal_publication_ack_replays_without_reopening_artifacts_after_epoch_change(
    pipeline: Pipeline, closed: ClosedAttempt, monkeypatch: pytest.MonkeyPatch
) -> None:
    accepted = closed.client.command(
        "worker.publish_attempt", closed.publication, idempotency_key="original-acceptance"
    )
    assert accepted["result"]["state"] == "succeeded"
    advance_epoch(pipeline)
    budget = pipeline.client.budget(pipeline.campaign["id"])

    def unavailable(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("Exact accepted replay must not reopen evidence")

    monkeypatch.setattr(pipeline.runtime.service.results, "pin_evidence", unavailable)
    assert (
        closed.client.command(
            "worker.publish_attempt", closed.publication, idempotency_key="original-acceptance"
        )
        == accepted
    )
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget


@pytest.mark.parametrize(
    "operation", ["worker.publish_attempt", "worker.submit_evidence", "attempt.reconcile"]
)
def test_same_key_acknowledgment_wins_over_mutable_preflight_failure(
    pipeline: Pipeline, closed: ClosedAttempt, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    client = closed.client
    request: Any = closed.publication
    revision = None
    if operation == "attempt.reconcile":
        request = intent(pipeline, submit(closed))
        client = pipeline.client
        revision = pipeline.client.get("attempt", request.attempt_id)["revision"]
    key = "same-key-concurrent-ack"
    missed, committed = threading.Event(), threading.Event()
    original = pipeline.runtime.database.replay
    blocked_thread = []

    def replay(context: Any, command: str, payload: Any, scope: str) -> Any:
        response = original(context, command, payload, scope)
        if (
            command == operation
            and context.idempotency_key == key
            and response is None
            and threading.get_ident() in blocked_thread
        ):
            missed.set()
            assert committed.wait(10)
        return response

    monkeypatch.setattr(pipeline.runtime.database, "replay", replay)

    def invoke() -> Any:
        return client.command(operation, request, idempotency_key=key, expected_revision=revision)

    def delayed() -> Any:
        blocked_thread.append(threading.get_ident())
        return invoke()

    with ThreadPoolExecutor(max_workers=1) as executor:
        waiting = executor.submit(delayed)
        try:
            assert missed.wait(10)
            accepted = invoke()
            if operation == "worker.submit_evidence":
                with pipeline.runtime.database.connect() as connection:
                    connection.execute(
                        "UPDATE records SET state='revoked' WHERE kind='worker' AND id=?",
                        (closed.native.worker.binding.worker_id,),
                    )
            elif operation == "worker.publish_attempt":
                advance_epoch(pipeline)
        finally:
            committed.set()
        assert waiting.result(timeout=10) == accepted
    if operation != "worker.submit_evidence":
        assert len(pipeline.client.list(RecordQuery(kind="run_result"))["items"]) == 1
