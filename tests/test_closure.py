from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.commands import CompleteCampaign
from research_swarm.control.confirmation_models import ConfirmationData
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.models import TrialData
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import ReportCampaign
from research_swarm.control.worker_artifacts import WorkerAttemptTarget
from research_swarm.control.worker_outputs import WorkerOutputIntent
from research_swarm.control.workers import AttemptData
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.lifecycle import AttemptState, attempt_transition
from research_swarm.domain.types import new_id
from research_swarm.orchestration.local import LocalDriver
from research_swarm.workers.journal import JournalState
from research_swarm.workers.local import LocalWorker
from tests.test_confirmation import create_plan, exposure_budget, options, setup_plan
from tests.test_local_driver import driver
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_reconciliation import ClosedAttempt, intent, reconcile, submit
from tests.test_reconciliation import closed as closed
from tests.test_worker_outputs import begin
from tests.test_worker_sessions import advance_epoch


def close_operationally(pipeline: Pipeline, submission: dict[str, Any]) -> dict[str, Any]:
    request = intent(pipeline, submission)
    return pipeline.client.command(
        "attempt.reconcile_closure",
        request,
        idempotency_key=new_id(),
        expected_revision=pipeline.client.get("attempt", request.attempt_id)["revision"],
    )["result"]


def capture(worker: LocalWorker, attempt_id: str, monkeypatch: pytest.MonkeyPatch) -> ClosedAttempt:
    original = worker.client.command
    publications = []

    def hold(operation: str, request: Any, **kwargs: Any) -> Any:
        if operation == "worker.publish_attempt":
            publications.append(request)
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Synthetic held publication")
        return original(operation, request, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(worker.client, "command", hold)
        with pytest.raises(ResearchError, match="held publication"):
            worker.execute(attempt_id)
    assert len(publications) == 1
    assert worker.journal.get(attempt_id).state in {JournalState.CLOSED, JournalState.NOT_STARTED}
    return ClosedAttempt(worker.client, worker, publications[0])


def test_native_success_without_scientific_output_closes_without_result_or_successors(
    pipeline: Pipeline, closed: ClosedAttempt
) -> None:
    request = closed.publication.model_copy(update={"report_artifact": None, "outputs": {}})
    submission = submit(closed, request)
    result = close_operationally(pipeline, submission)
    attempt = result["attempt"]
    assert attempt["state"] == attempt["data"]["publication_status"] == "closed_unaccepted"
    assert attempt["data"]["result_digest"] is None
    assert attempt["data"]["closure_receipt_id"] == result["closure"]["id"]
    native = result["closure"]["data"]["native_outcome"]
    assert native["exit_code"] == 0 and native["reason"] == "exited"
    assert native["closure"]["scope_closed"]
    assert pipeline.client.list(RecordQuery(kind="run_result"))["items"] == []
    assert pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"] == []
    assert pipeline.client.list(RecordQuery(kind="job", state="ready"))["items"] == []
    trial = pipeline.client.get("trial", pipeline.trial["id"])
    assert trial["state"] == trial["data"]["execution_outcome"] == "closed_unaccepted"
    assert trial["data"]["resource_status"] == "closed"
    assert trial["data"]["evaluation_status"] == "incomplete"
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert next(item for item in accounts if item["dimension"] == "attempts")["consumed"] == "1"
    assert all(item["uncertain"] == "0" and item["reserved"] == "0" for item in accounts)
    assert close_operationally(pipeline, submission) == result
    assert closed.native.execute(request.attempt_id) == attempt
    with pytest.raises(ResearchError) as publication:
        closed.client.command(
            "worker.publish_attempt", closed.publication, idempotency_key=new_id()
        )
    assert publication.value.code == ErrorCode.INVALID_TRANSITION
    with pytest.raises(ResearchError) as upload:
        begin(
            closed.client,
            WorkerAttemptTarget(
                worker_id=closed.native.worker.binding.worker_id,
                session_id=closed.native.worker.binding.worker_session_id,
                attempt_id=request.attempt_id,
            ),
            b"late output",
        )
    assert upload.value.code == ErrorCode.INVALID_TRANSITION
    with pytest.raises(ResearchError) as upgrade:
        reconcile(pipeline, submission)
    assert upgrade.value.code == ErrorCode.INVALID_TRANSITION
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    completed = pipeline.client.command(
        "campaign.complete",
        CompleteCampaign(campaign_id=campaign["id"], reason="manual"),
        idempotency_key=new_id(),
        expected_revision=campaign["revision"],
    )["result"]
    assert completed["campaign"]["state"] == "completed"


def test_terminal_campaign_and_original_report_remain_unchanged(
    pipeline: Pipeline, closed: ClosedAttempt
) -> None:
    report = pipeline.client.command(
        "report.create",
        ReportCampaign(campaign_id=pipeline.campaign["id"]),
        idempotency_key=new_id(),
    )["result"]
    submission = submit(closed)
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "UPDATE records SET state='completed',payload=json_set(payload,'$.final_report_id',?) "
            "WHERE kind='campaign' AND id=?",
            (report["id"], pipeline.campaign["id"]),
        )
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    advance_epoch(pipeline)
    request = intent(pipeline, submission)
    revision = pipeline.client.get("attempt", request.attempt_id)["revision"]
    accepted = pipeline.client.command(
        "attempt.reconcile_closure",
        request,
        idempotency_key="closed-terminal",
        expected_revision=revision,
    )
    assert accepted["result"]["attempt"]["state"] == "closed_unaccepted"
    assert pipeline.client.get("campaign", pipeline.campaign["id"]) == campaign
    assert pipeline.client.get("report", report["id"]) == report
    advance_epoch(pipeline)
    assert (
        pipeline.client.command(
            "attempt.reconcile_closure",
            request,
            idempotency_key="closed-terminal",
            expected_revision=revision,
        )
        == accepted
    )
    refreshed = pipeline.client.command(
        "report.create",
        ReportCampaign(campaign_id=pipeline.campaign["id"]),
        idempotency_key=new_id(),
    )["result"]
    assert refreshed["id"] != report["id"]
    assert "closed_unaccepted" in str(refreshed["data"])


def test_scientific_corruption_does_not_block_native_closure(
    pipeline: Pipeline, closed: ClosedAttempt, tmp_path: Path
) -> None:
    entry = closed.native.journal.get(closed.publication.attempt_id)
    path = tmp_path / "invalid-report.json"
    path.write_bytes(b"not valid report JSON")
    reference = closed.client.worker_upload_file(
        WorkerOutputIntent(
            worker_id=entry.dispatch.worker_id,
            session_id=entry.dispatch.worker_session_id,
            attempt_id=entry.dispatch.attempt_id,
            role="report",
        ),
        path,
        idempotency_key="invalid-scientific-report",
    )
    submission = submit(
        closed, closed.publication.model_copy(update={"report_artifact": reference})
    )
    assert close_operationally(pipeline, submission)["attempt"]["state"] == "closed_unaccepted"
    assert pipeline.client.list(RecordQuery(kind="run_result"))["items"] == []


@pytest.mark.parametrize("mode", ["unknown", "wrong_identity", "stale_epoch"])
def test_unproven_closure_does_not_settle(
    pipeline: Pipeline, closed: ClosedAttempt, tmp_path: Path, mode: str
) -> None:
    request = closed.publication
    entry = closed.native.journal.get(request.attempt_id)
    if mode != "stale_epoch":
        assert entry.native is not None
        altered = (
            entry.model_copy(update={"state": JournalState.UNKNOWN, "outcome": None})
            if mode == "unknown"
            else entry.model_copy(
                update={"native": entry.native.model_copy(update={"scope_id": new_id()})}
            )
        )
        path = tmp_path / "unproven.json"
        path.write_bytes(canonical_bytes(altered.model_dump(mode="json")))
        reference = closed.client.worker_upload_file(
            WorkerOutputIntent(
                worker_id=entry.dispatch.worker_id,
                session_id=entry.dispatch.worker_session_id,
                attempt_id=request.attempt_id,
                role="journal",
            ),
            path,
            idempotency_key=new_id(),
        )
        request = request.model_copy(update={"journal_artifact": reference})
    submission = submit(closed, request)
    command = intent(pipeline, submission)
    if mode == "stale_epoch":
        advance_epoch(pipeline)
    budget = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError):
        pipeline.client.command(
            "attempt.reconcile_closure",
            command,
            idempotency_key=new_id(),
            expected_revision=pipeline.client.get("attempt", request.attempt_id)["revision"],
        )
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    assert pipeline.client.list(RecordQuery(kind="attempt_closure"))["items"] == []


@pytest.mark.parametrize("other", ["worker.publish_attempt", "attempt.reconcile"])
def test_closure_races_acceptance_with_one_final_disposition(
    pipeline: Pipeline, closed: ClosedAttempt, other: str
) -> None:
    submission = submit(closed)
    request = intent(pipeline, submission)
    revision = pipeline.client.get("attempt", request.attempt_id)["revision"]
    barrier = threading.Barrier(2)

    def contend(operation: str) -> Any:
        barrier.wait(10)
        try:
            if operation == "worker.publish_attempt":
                return closed.client.command(
                    operation, closed.publication, idempotency_key=new_id()
                )
            return pipeline.client.command(
                operation, request, idempotency_key=new_id(), expected_revision=revision
            )
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(contend, (other, "attempt.reconcile_closure")))
    assert sum(isinstance(item, dict) for item in responses) == 1
    assert any(
        item in {ErrorCode.REVISION_CONFLICT, ErrorCode.INVALID_TRANSITION}
        for item in responses
        if not isinstance(item, dict)
    )
    outcome = pipeline.client.get("attempt", request.attempt_id)["data"]["publication_status"]
    assert outcome in {"accepted", "closed_unaccepted"}
    assert len(pipeline.client.list(RecordQuery(kind="run_result"))["items"]) == (
        outcome == "accepted"
    )
    assert len(pipeline.client.list(RecordQuery(kind="attempt_closure"))["items"]) == (
        outcome == "closed_unaccepted"
    )
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert next(item for item in accounts if item["dimension"] == "attempts")["consumed"] == "1"


@pytest.mark.parametrize("pipeline", [True], indirect=True)
def test_parallel_sibling_custody_survives_and_cannot_restore_trial_eligibility(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    jobs = pipeline.client.list(RecordQuery(kind="job", state="ready"))["items"]
    attempts = [pipeline.worker.claim(job) for job in jobs]
    train = next(row for row in attempts if row["data"]["dispatch"]["stage_id"] == "train")
    sibling = next(row for row in attempts if row["id"] != train["id"])
    waiting = capture(pipeline.worker, train["id"], monkeypatch)
    close_operationally(pipeline, submit(waiting))
    trial = pipeline.client.get("trial", pipeline.trial["id"])
    assert trial["state"] == "running" and trial["data"]["resource_status"] == "active"
    assert trial["data"]["execution_outcome"] == "closed_unaccepted"
    assert pipeline.client.get("attempt", sibling["id"])["data"]["publication_status"] == "pending"
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert next(item for item in accounts if item["dimension"] == "attempts")["reserved"] == "1"
    sibling_closed = capture(pipeline.worker, sibling["id"], monkeypatch)
    entry = pipeline.worker.journal.get(sibling["id"])
    pipeline.worker._publish(
        entry.model_copy(update={"state": JournalState.UNKNOWN, "outcome": None})
    )
    uncertain = pipeline.client.get("trial", pipeline.trial["id"])
    assert uncertain["state"] == "execution_unknown"
    assert uncertain["data"]["resource_status"] == "unknown"
    assert uncertain["data"]["execution_outcome"] == "closed_unaccepted"
    sibling_closed.client.command(
        "worker.publish_attempt", sibling_closed.publication, idempotency_key=new_id()
    )
    trial = pipeline.client.get("trial", pipeline.trial["id"])
    assert trial["state"] == "closed_unaccepted" and trial["data"]["resource_status"] == "closed"
    assert trial["data"]["execution_outcome"] == "closed_unaccepted"
    assert trial["data"]["evaluation_status"] == "incomplete"
    assert pipeline.client.list(RecordQuery(kind="job", state="ready"))["items"] == []


@pytest.mark.parametrize("pipeline", [options()], indirect=True)
@pytest.mark.parametrize("phase", ["training", "scoring", "not_started_scoring"])
def test_closure_settles_original_exposure_without_scientific_receipt(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    proposal, campaign = setup_plan(pipeline)
    record = create_plan(pipeline, proposal, campaign)
    binding = ConfirmationData.model_validate(record["data"]).plan.bindings[0]
    trial = pipeline.client.get("trial", binding.trial_id)
    train = pipeline.client.get("job", trial["data"]["job_ids"][0])
    attempt = pipeline.worker.claim(train)
    authority = None
    if phase != "training":
        pipeline.worker.execute(attempt["id"])
        scoring = pipeline.client.get("job", binding.scoring_job_id)
        authority = pipeline.client.command(
            "evaluation.authorize",
            AuthorizeEvaluation(job_id=scoring["id"], evaluator_id=proposal.evaluator_id),
            idempotency_key=new_id(),
            expected_revision=scoring["revision"],
        )["result"]
        attempt = pipeline.worker.claim(pipeline.client.get("job", scoring["id"]))
        assert exposure_budget(pipeline)["uncertain"] == "1"
        if phase == "not_started_scoring":
            pipeline.worker._prepare(
                AttemptData.model_validate(attempt["data"]), TrialData.model_validate(trial["data"])
            )
            pipeline.worker.journal.request_local_cancel(attempt["id"])
    before = len(pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"])
    waiting = capture(pipeline.worker, attempt["id"], monkeypatch)
    result = close_operationally(pipeline, submit(waiting))
    expected = "consumed" if phase == "scoring" else "released"
    assert pipeline.client.get("evaluation_exposure", binding.exposure_id)["state"] == expected
    assert exposure_budget(pipeline)["uncertain"] == "0"
    assert exposure_budget(pipeline)["reserved"] == "9"
    assert exposure_budget(pipeline)["consumed"] == ("1" if phase == "scoring" else "0")
    assert result["closure"]["data"]["not_started"] == (phase == "not_started_scoring")
    if authority is not None:
        with pytest.raises(ResearchError):
            pipeline.client.command(
                "evaluation.complete",
                CompleteEvaluation(request_id=authority["id"], attempt_id=attempt["id"]),
                idempotency_key=new_id(),
            )
    assert len(pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"]) == before


def test_closed_unaccepted_is_an_operator_only_terminal_disposition() -> None:
    for state in (AttemptState.RESERVED, AttemptState.EXECUTION_UNKNOWN, AttemptState.RUNNING):
        with pytest.raises(ResearchError):
            attempt_transition(state, AttemptState.CLOSED_UNACCEPTED)
        attempt_transition(state, AttemptState.CLOSED_UNACCEPTED, reconciliation=True)
    for state in AttemptState:
        with pytest.raises(ResearchError):
            attempt_transition(AttemptState.CLOSED_UNACCEPTED, state, reconciliation=True)


@pytest.mark.parametrize("pipeline", [True], indirect=True)
@pytest.mark.parametrize("mode", ["closed", "race", "pending_receipt"])
async def test_resumed_driver_does_not_evaluate_closed_trial(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    run = driver(pipeline)
    jobs = pipeline.client.list(RecordQuery(kind="job", state="ready"))["items"]
    attempts = [pipeline.worker.claim(job) for job in jobs]
    train = next(row for row in attempts if row["data"]["dispatch"]["stage_id"] == "train")
    probe = next(row for row in attempts if row["id"] != train["id"])
    pipeline.worker.execute(train["id"])
    scoring = pipeline.ready()
    authorization = pipeline.client.command(
        "evaluation.authorize",
        AuthorizeEvaluation(job_id=scoring["id"], evaluator_id=run.enrollment.evaluator_id),
        idempotency_key=new_id(),
        expected_revision=scoring["revision"],
    )["result"]
    evaluation = pipeline.worker.claim(pipeline.client.get("job", scoring["id"]))
    pipeline.worker.execute(evaluation["id"])
    waiting = capture(pipeline.worker, probe["id"], monkeypatch)
    submission = submit(waiting)
    original = run.enrollment.evaluator.command
    if mode == "pending_receipt":
        with monkeypatch.context() as patch:

            def unavailable(*args: Any, **kwargs: Any) -> Any:
                raise OSError("Synthetic lost evaluator connection")

            patch.setattr(run.enrollment.evaluator, "command", unavailable)
            with pytest.raises(OSError):
                await run.drain(pipeline.campaign["id"])
    if mode == "race":

        def close_at_evaluation(operation: str, *args: Any, **kwargs: Any) -> Any:
            if operation == "evaluation.complete":
                close_operationally(pipeline, submission)
            return original(operation, *args, **kwargs)

        monkeypatch.setattr(run.enrollment.evaluator, "command", close_at_evaluation)
    else:
        close_operationally(pipeline, submission)
    resumed = await LocalDriver(pipeline.client, run.enrollment).drain(pipeline.campaign["id"])
    assert resumed.state == "idle" and not resumed.findings
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["execution_outcome"]
        == "closed_unaccepted"
    )
    assert pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"] == []
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 3
    assert pipeline.client.get("evaluation_request", authorization["id"])["state"] == "authorized"
    again = await LocalDriver(pipeline.client, run.enrollment).drain(pipeline.campaign["id"])
    assert again.state == "idle" and not again.findings and not again.executed_attempts


@pytest.mark.parametrize("phase", ["upload", "publication"])
def test_worker_observes_operator_closure_racing_its_terminal_delivery(
    pipeline: Pipeline, closed: ClosedAttempt, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    submission = submit(closed)
    journal = closed.native.journal.get(closed.publication.attempt_id)
    command = closed.client.transport.command
    changed = False

    def close_during_delivery(operation: str, *args: Any, **kwargs: Any) -> Any:
        nonlocal changed
        target = "worker.begin_upload" if phase == "upload" else "worker.publish_attempt"
        if operation == target and not changed:
            close_operationally(pipeline, submission)
            changed = True
        return command(operation, *args, **kwargs)

    monkeypatch.setattr(closed.client.transport, "command", close_during_delivery)
    result = (
        closed.native._publish(
            journal.model_copy(update={"finding": "Recovered terminal delivery"})
        )
        if phase == "upload"
        else closed.native.execute(closed.publication.attempt_id)
    )
    assert changed and result["state"] == "closed_unaccepted"
    assert closed.native.journal.get(closed.publication.attempt_id) == journal
    assert closed.native.resources.availability() == {"cpu": 1}
    assert pipeline.client.list(RecordQuery(kind="run_result"))["items"] == []
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1


def test_unknown_worker_observation_does_not_claim_local_closure_from_controller(
    pipeline: Pipeline, closed: ClosedAttempt
) -> None:
    close_operationally(pipeline, submit(closed))
    entry = closed.native.journal.get(closed.publication.attempt_id)
    with pytest.raises(ResearchError) as unknown:
        closed.native._publish(
            entry.model_copy(update={"state": JournalState.UNKNOWN, "outcome": None})
        )
    assert unknown.value.code == ErrorCode.INVALID_TRANSITION
