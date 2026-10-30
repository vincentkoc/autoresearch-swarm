from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import CampaignTarget, PauseCampaign
from research_swarm.control.models import TrialData
from research_swarm.control.queries import RecordQuery
from research_swarm.control.workers import AttemptData, ClaimJob
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.orchestration.intents import CommandIntents
from research_swarm.orchestration.local import LocalDriver
from research_swarm.workers.local_enrollment import LocalEnrollment
from tests.test_cli_mcp import cli
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline


def driver(pipeline: Pipeline) -> LocalDriver:
    evaluator = Principal(
        id=new_id(),
        workspace_id=pipeline.runtime.identity.workspace_id,
        scopes=frozenset({"read", "evaluation.publish"}),
    )
    credential = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), evaluator)
    client = ResearchClient(
        LocalTransport(
            pipeline.runtime, lambda: pipeline.runtime.credentials.resolve(credential.token)
        )
    )
    return LocalDriver(pipeline.client, LocalEnrollment(pipeline.worker, client, evaluator.id))


async def test_driver_executes_and_evaluates_without_selecting_or_completing(
    pipeline: Pipeline,
) -> None:
    run = driver(pipeline)
    result = await run.drain(pipeline.campaign["id"])
    assert result.state == "idle" and len(result.executed_attempts) == 2
    assert not result.findings
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == "valid"
    )
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    assert campaign["state"] == "running" and campaign["data"]["incumbent_candidate_id"] is None
    again = await LocalDriver(pipeline.client, run.enrollment).drain(campaign["id"])
    assert again.state == "idle" and not again.executed_attempts
    assert len(pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"]) == 1


async def test_restart_recovers_claim_whose_acknowledgment_was_lost(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = driver(pipeline)
    original = pipeline.worker.client.command

    def lost_ack(operation: str, *args: Any, **kwargs: Any) -> Any:
        result = original(operation, *args, **kwargs)
        if operation == "worker.claim_job":
            raise OSError("Synthetic lost claim acknowledgment")
        return result

    monkeypatch.setattr(pipeline.worker.client, "command", lost_ack)
    with pytest.raises(OSError, match="acknowledgment"):
        await run.drain(pipeline.campaign["id"])
    attempts = pipeline.client.list(RecordQuery(kind="attempt"))["items"]
    assert len(attempts) == 1 and attempts[0]["state"] == "reserved"
    monkeypatch.setattr(pipeline.worker.client, "command", original)
    restarted = await LocalDriver(pipeline.client, run.enrollment).drain(pipeline.campaign["id"])
    assert restarted.state == "idle" and attempts[0]["id"] in restarted.executed_attempts
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 2
    assert pipeline.worker.resources.availability() == {"cpu": 1}


async def test_restart_finishes_receipt_after_native_publication(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = driver(pipeline)
    original = run.enrollment.evaluator.command

    def interrupted(*args: Any, **kwargs: Any) -> Any:
        raise OSError("Synthetic evaluator interruption")

    monkeypatch.setattr(run.enrollment.evaluator, "command", interrupted)
    with pytest.raises(OSError, match="interruption"):
        await run.drain(pipeline.campaign["id"])
    assert pipeline.client.get("trial", pipeline.trial["id"])["state"] == "succeeded"
    monkeypatch.setattr(run.enrollment.evaluator, "command", original)
    result = await LocalDriver(pipeline.client, run.enrollment).drain(pipeline.campaign["id"])
    assert result.state == "idle" and not result.executed_attempts
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == "valid"
    )


async def test_task_cancellation_keeps_driver_lock_until_owned_thread_finishes(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = driver(pipeline)
    entered, release = threading.Event(), threading.Event()
    original = pipeline.worker.execute

    def held(identifier: str) -> dict[str, Any]:
        entered.set()
        assert release.wait(10), "Test did not release its execution thread"
        return original(identifier)

    monkeypatch.setattr(pipeline.worker, "execute", held)
    task = asyncio.create_task(run.drain(pipeline.campaign["id"]))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        with pytest.raises(ResearchError) as owned:
            await LocalDriver(pipeline.client, run.enrollment).drain(pipeline.campaign["id"])
        assert owned.value.code == ErrorCode.RESOURCE_UNAVAILABLE
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    monkeypatch.setattr(pipeline.worker, "execute", original)
    result = await LocalDriver(pipeline.client, run.enrollment).drain(pipeline.campaign["id"])
    assert result.state == "idle" and len(result.executed_attempts) == 1
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 2


@pytest.mark.parametrize("pipeline", [True], indirect=True)
async def test_driver_can_reduce_parallelism_without_changing_resource_admission(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    enrolled = driver(pipeline).enrollment
    for invalid in (0, 3, True):
        with pytest.raises(ResearchError, match="concurrency"):
            LocalDriver(pipeline.client, enrolled, concurrency=invalid)
    run = LocalDriver(pipeline.client, enrolled, concurrency=1)
    original = pipeline.worker.execute
    active = 0
    maximum = 0
    lock = threading.Lock()

    def observe(identifier: str) -> dict[str, Any]:
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(active, maximum)
        try:
            return original(identifier)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(pipeline.worker, "execute", observe)
    result = await run.drain(pipeline.campaign["id"])
    assert result.state == "idle" and len(result.executed_attempts) == 3
    assert maximum == 1 and pipeline.worker.worker.max_concurrency == 2
    assert pipeline.worker.resources.availability() == {"cpu": 2}


@pytest.mark.parametrize("pipeline", [True], indirect=True)
async def test_independent_ready_stages_execute_concurrently_and_keep_failure(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = driver(pipeline)
    barrier = threading.Barrier(2)
    original = pipeline.worker.execute
    entered: set[str] = set()
    lock = threading.Lock()

    def parallel(identifier: str) -> dict[str, Any]:
        with lock:
            entered.add(identifier)
            first_pair = len(entered) <= 2
        if first_pair:
            barrier.wait(timeout=10)
        return original(identifier)

    monkeypatch.setattr(pipeline.worker, "execute", parallel)
    result = await run.drain(pipeline.campaign["id"])
    assert result.state == "idle" and len(result.executed_attempts) == 3
    attempts = pipeline.client.list(RecordQuery(kind="attempt"))["items"]
    assert sum(row["state"] == "failed" for row in attempts) == 1
    assert pipeline.client.get("trial", pipeline.trial["id"])["state"] == "failed"
    assert pipeline.worker.resources.availability() == {"cpu": 2}


async def test_paused_campaign_stays_queued_until_explicit_resume(pipeline: Pipeline) -> None:
    run = driver(pipeline)
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    paused = pipeline.client.command(
        "campaign.pause",
        PauseCampaign(campaign_id=campaign["id"]),
        idempotency_key="pause",
        expected_revision=campaign["revision"],
    )["result"]
    assert (await run.drain(campaign["id"])).state == "paused"
    assert not pipeline.client.list(RecordQuery(kind="attempt"))["items"]
    pipeline.client.command(
        "campaign.start",
        CampaignTarget(campaign_id=campaign["id"]),
        idempotency_key="resume",
        expected_revision=paused["revision"],
    )
    assert (await run.drain(campaign["id"])).state == "idle"


def test_cli_automatically_enrolls_and_resumes_local_execution(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    arguments = (
        "worker",
        "run",
        pipeline.campaign["id"],
        "--machine-registry",
        str(tmp_path / "cli-machine" / "resources.sqlite"),
    )
    result = cli(pipeline.runtime.root, *arguments)
    assert result["state"] == "idle" and len(result["executed_attempts"]) == 2
    again = cli(pipeline.runtime.root, *arguments)
    assert again["state"] == "idle" and not again["executed_attempts"]
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == "valid"
    )


def test_rejected_claim_gets_new_key_only_after_known_resource_rejection(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Simulate the controller's concurrency gate, preserving its durable rejection ledger.
    binding = pipeline.worker.worker.binding
    job = pipeline.ready()
    request = ClaimJob(
        worker_id=binding.worker_id, session_id=binding.worker_session_id, job_id=job["id"]
    )
    intents = CommandIntents(tmp_path / "intents", {"worker": pipeline.worker.client})
    original = pipeline.runtime.service.workers.claim
    reject = True

    def capacity(context: Any, command: Any) -> Any:
        if not reject:
            return original(context, command)

        def unavailable(transaction: Any) -> Any:
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Synthetic occupied capacity")

        return pipeline.runtime.database.execute(
            context,
            "worker.claim_job",
            command.model_dump(mode="json"),
            "worker.claim",
            unavailable,
        )

    monkeypatch.setattr(pipeline.runtime.service.workers, "claim", capacity)
    with pytest.raises(ResearchError) as rejected:
        intents.command("claim", "worker", "worker.claim_job", request, revision=job["revision"])
    assert rejected.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    reject = False
    with pytest.raises(ResearchError):
        intents.command("claim", "worker", "worker.claim_job", request, revision=job["revision"])
    accepted = intents.command(
        "claim",
        "worker",
        "worker.claim_job",
        request,
        revision=job["revision"],
        retry_rejected=True,
    )
    assert (
        intents.command("claim", "worker", "worker.claim_job", request, revision=job["revision"])
        == accepted
    )
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1
    pipeline.worker.execute(accepted["result"]["id"])


async def test_execution_resource_refusal_resumes_same_admitted_attempt(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = driver(pipeline)
    reserve = pipeline.worker.resources.reserve

    def occupied(*args: Any, **kwargs: Any) -> Any:
        raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Synthetic native contention")

    monkeypatch.setattr(pipeline.worker.resources, "reserve", occupied)
    blocked = await run.drain(pipeline.campaign["id"])
    assert blocked.state == "blocked" and blocked.finding_count == 1
    attempts = pipeline.client.list(RecordQuery(kind="attempt"))["items"]
    assert len(attempts) == 1 and attempts[0]["state"] == "reserved"
    monkeypatch.setattr(pipeline.worker.resources, "reserve", reserve)
    completed = await LocalDriver(pipeline.client, run.enrollment).drain(pipeline.campaign["id"])
    assert completed.state == "idle" and attempts[0]["id"] in completed.executed_attempts
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 2


async def test_uncertain_journal_blocks_admission_and_keeps_native_capacity(
    pipeline: Pipeline,
) -> None:
    run = driver(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker._prepare(
        AttemptData.model_validate(attempt["data"]),
        TrialData.model_validate(pipeline.trial["data"]),
    )
    supervisor = new_id()
    pipeline.worker.journal.claim_launch(attempt["id"], supervisor)
    pipeline.worker.journal.unknown(attempt["id"], supervisor, "Synthetic lost launch observation")
    result = await run.drain(pipeline.campaign["id"])
    assert result.state == "blocked" and result.findings == ("EXECUTION_UNKNOWN:" + attempt["id"],)
    assert pipeline.worker.resources.availability() == {"cpu": 0}
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert next(item for item in accounts if item["dimension"] == "attempts")["uncertain"] == "1"


@pytest.mark.parametrize("pipeline", [True], indirect=True)
@pytest.mark.parametrize("refused_stage", ["train", "probe"])
async def test_same_completion_batch_retries_capacity_in_both_task_orders(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, refused_stage: str
) -> None:
    run = driver(pipeline)
    execute = pipeline.worker.execute
    wait = asyncio.wait
    refused: set[str] = set()

    def contention(identifier: str) -> dict[str, Any]:
        attempt = pipeline.client.get("attempt", identifier)
        if attempt["data"]["dispatch"]["stage_id"] == refused_stage and identifier not in refused:
            refused.add(identifier)
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Synthetic capacity refusal")
        return execute(identifier)

    async def whole_batch(tasks: Any, **kwargs: Any) -> Any:
        return await wait(tasks, return_when=asyncio.ALL_COMPLETED)

    monkeypatch.setattr(pipeline.worker, "execute", contention)
    monkeypatch.setattr("research_swarm.orchestration.local.asyncio.wait", whole_batch)
    result = await run.drain(pipeline.campaign["id"])
    assert result.state == "idle" and result.executed_attempt_count == 3
    assert len(refused) == 1 and refused <= set(result.executed_attempts)
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 3
