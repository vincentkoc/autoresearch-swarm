"""Opt-in real Temporal dev-server tests; no mocked Workflow runtime."""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from temporalio.client import WorkflowFailureError, WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from research_swarm.api.http import create_app
from research_swarm.client import ResearchClient
from research_swarm.control.orchestration_models import FenceOrchestration
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.event_recovery import RecoverySnapshotTarget
from research_swarm.domain.orchestration import OrchestrationBinding
from research_swarm.domain.orchestration_waits import WakeupDelivery
from research_swarm.domain.types import new_id
from research_swarm.orchestration.temporal.activities import (
    DriverConnection,
    TemporalActivities,
    binding_reference,
)
from research_swarm.orchestration.temporal.bridge import TemporalBridge
from research_swarm.orchestration.temporal.models import (
    WorkflowInput,
    WorkOutcome,
    WorkReference,
    workflow_id,
)
from research_swarm.orchestration.temporal.runtime import DATA_CONVERTER, create_worker
from research_swarm.orchestration.temporal.workflow import CampaignWorkflow
from research_swarm.orchestration.turns import DurableTurnDriver
from research_swarm.orchestration.waits import DurableWaitDriver
from research_swarm.workers.journal import JournalState
from tests.test_drive_closures import report_action
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_orchestration_transfer import pause, request, successor, transfer
from tests.test_orchestration_turns import CONFIG, Drivers, bind, fence
from tests.test_orchestration_waits import record, report

pytestmark = pytest.mark.skipif(
    os.environ.get("RESEARCH_TEMPORAL_TESTS") != "1",
    reason="Set RESEARCH_TEMPORAL_TESTS=1 to run a real disposable Temporal dev server",
)


def connect(pipeline: Pipeline, drivers: Drivers, stack: ExitStack, url: str) -> DriverConnection:
    evaluator_token = pipeline.runtime.credentials.issue(
        pipeline.runtime.principal(), drivers.evaluator_actor
    ).token
    return DriverConnection(
        binding_reference(
            OrchestrationBinding.model_validate(
                pipeline.client.get("orchestration_binding", pipeline.campaign["id"])["data"]
            )
        ),
        {
            "control": stack.enter_context(ResearchClient.connect(url, drivers.token)),
            "evaluator": stack.enter_context(ResearchClient.connect(url, evaluator_token)),
        },
    )


def worker(env: WorkflowEnvironment, queue: str, activities: TemporalActivities) -> Worker:
    return create_worker(
        env.client,
        task_queue=queue,
        activities=activities,
    )


async def until(predicate: Any, *, timeout: float = 15) -> None:
    async with asyncio.timeout(timeout):
        while not await asyncio.to_thread(predicate):
            await asyncio.sleep(0.02)


async def replay_chain(
    env: WorkflowEnvironment, handle: WorkflowHandle[CampaignWorkflow, str]
) -> list[str]:
    run_id = handle.first_execution_run_id
    histories = []
    replayer = Replayer(workflows=[CampaignWorkflow])
    while run_id:
        history = await env.client.get_workflow_handle(handle.id, run_id=run_id).fetch_history()
        await replayer.replay_workflow(history)
        histories.append(history.to_json())
        run_id = history.events[
            -1
        ].workflow_execution_continued_as_new_event_attributes.new_execution_run_id
    return histories


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [False, True])
async def test_real_activity_retry_recovers_one_saved_http_command_without_replanning(
    pipeline: Pipeline,
    timeout: bool,
) -> None:
    drivers = bind(pipeline)
    calls = []

    class LostReply(TemporalActivities):
        def _execute(self, work: WorkReference) -> WorkOutcome:
            calls.append(work.record_id)
            outcome = super()._execute(work)
            if len(calls) == 1:
                if timeout:
                    time.sleep(0.7)
                raise TimeoutError("Injected loss after durable domain acceptance")
            return outcome

    with ExitStack() as stack:
        url = stack.enter_context(
            serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials))
        )
        connection = connect(pipeline, drivers, stack, url)
        reference = connection.binding
        activities = LostReply({workflow_id(reference): connection})
        driver = DurableTurnDriver(
            connection.clients["control"],
            reference.campaign_id,
            epoch=1,
            configuration_digest=CONFIG,
        )
        plan_calls = []

        def plan() -> Any:
            plan_calls.append(1)
            return report_action(pipeline)

        prepared = driver.prepare_turn(1, plan)
        assert driver.prepare_turn(1, plan) == prepared
        assert plan_calls == [1]
        async with await WorkflowEnvironment.start_local(
            ui=False, data_converter=DATA_CONVERTER
        ) as env:
            queue = new_id()
            bridge = TemporalBridge(env.client, task_queue=queue)
            async with worker(env, queue, activities):
                workflow_input = WorkflowInput(
                    reference, poll_seconds=0.05, activity_timeout_seconds=0.3 if timeout else 120
                )
                handle = await bridge.start(workflow_input)
                duplicate = await bridge.start(workflow_input)
                assert duplicate.first_execution_run_id == handle.first_execution_run_id
                with pytest.raises(ResearchError) as changed:
                    await bridge.start(replace(workflow_input, poll_seconds=0.1))
                assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
                await until(lambda: len(calls) >= 2)
                fence(pipeline)
                assert await asyncio.wait_for(handle.result(), 15) == "retired"
            histories = await replay_chain(env, handle)
            assert len(histories) == 1
            assert drivers.token not in histories[0]
            assert "saved-report" not in histories[0]
            assert "Injected loss" not in histories[0]
            assert str(Path.home()) not in histories[0]
            assert len(calls) == 2 and set(calls) == {prepared["id"]}
            assert len(pipeline.client.list(RecordQuery(kind="report"))["items"]) == 1
            assert len(pipeline.client.list(RecordQuery(kind="drive_receipt"))["items"]) == 1
            assert (
                pipeline.client.get("orchestration_binding", reference.campaign_id)["data"][
                    "next_ordinal"
                ]
                == 2
            )


@pytest.mark.asyncio
async def test_real_wait_survives_worker_restart_signals_and_continue_as_new(
    pipeline: Pipeline,
) -> None:
    drivers = bind(pipeline)
    waited = record(pipeline, drivers)
    with ExitStack() as stack:
        url = stack.enter_context(
            serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials))
        )
        connection = connect(pipeline, drivers, stack, url)
        reference = connection.binding
        activities = TemporalActivities({workflow_id(reference): connection})
        async with await WorkflowEnvironment.start_local(
            ui=False, data_converter=DATA_CONVERTER
        ) as env:
            queue = new_id()
            bridge = TemporalBridge(env.client, task_queue=queue)
            async with worker(env, queue, activities):
                handle = await bridge.start(
                    WorkflowInput(reference, poll_seconds=0.05, rotate_after=2)
                )
                async with asyncio.timeout(15):
                    while (await handle.describe()).run_id == handle.first_execution_run_id:
                        await asyncio.sleep(0.05)
            report(pipeline)
            waiter = DurableWaitDriver(
                connection.clients["control"],
                reference.campaign_id,
                epoch=1,
                actor_id=drivers.actor.id,
            )
            # The server records a signal while no Activity/Workflow worker is running.
            delivered = await bridge.deliver(reference, waiter)
            readiness = waiter.reconcile(waited["wait"]["id"])
            assert delivered == (readiness["delivery"]["id"],)
            delivery = WakeupDelivery.model_validate(readiness["delivery"]["data"])
            await bridge.notify(reference, delivery)
            await bridge.notify(reference, delivery)
            async with worker(env, queue, activities):
                await asyncio.sleep(0.2)
                assert (
                    pipeline.client.get("orchestration_wait", waited["wait"]["id"])["state"]
                    == "ready"
                )
                assert (
                    len(pipeline.client.list(RecordQuery(kind="wakeup_acknowledgment"))["items"])
                    == 1
                )
                fence(pipeline)
                assert await asyncio.wait_for(handle.result(), 15) == "retired"
            histories = await replay_chain(env, handle)
            assert len(histories) >= 2
            assert pipeline.client.list(RecordQuery(kind="drive_turn"))["items"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("recovery", ["normal", "lost_creation_reply", "invalidated_snapshot"])
async def test_real_temporal_successor_recovers_checkpoint_and_preserves_paused_campaign(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
    recovery: str,
) -> None:
    bind(pipeline)
    pause(pipeline)
    new = successor(pipeline)
    intent, revision = request(pipeline, new)
    transferred = transfer(pipeline, intent.model_copy(update={"adapter": "temporal"}), revision)
    budget = pipeline.client.budget(pipeline.campaign["id"])
    with ExitStack() as stack:
        url = stack.enter_context(
            serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials))
        )
        connection = connect(pipeline, new, stack, url)
        reference = connection.binding
        activities = TemporalActivities({workflow_id(reference): connection})
        control = connection.clients["control"]
        command = control.command
        snapshots = []

        def interrupted(operation: str, payload: Any, **kwargs: Any) -> Any:
            result = command(operation, payload, **kwargs)
            if operation == "stream.create_snapshot":
                snapshots.append(payload.snapshot_id)
                if len(snapshots) == 1:
                    if recovery == "lost_creation_reply":
                        raise TimeoutError("Injected loss after snapshot creation")
                    if recovery == "invalidated_snapshot":
                        command(
                            "stream.release_snapshot",
                            RecoverySnapshotTarget(snapshot_id=payload.snapshot_id),
                            idempotency_key="fixture-invalidate",
                        )
            return result

        monkeypatch.setattr(control, "command", interrupted)
        async with await WorkflowEnvironment.start_local(
            ui=False, data_converter=DATA_CONVERTER
        ) as env:
            queue = new_id()
            bridge = TemporalBridge(env.client, task_queue=queue)
            async with worker(env, queue, activities):
                handle = await bridge.start(WorkflowInput(reference, poll_seconds=0.05))
                await until(
                    lambda: all(
                        pipeline.client.get("orchestration_start", identifier)["state"]
                        == "acknowledged"
                        for identifier in transferred["data"]["start_ids"].values()
                    )
                )
                assert pipeline.client.get("campaign", reference.campaign_id)["state"] == "paused"
                assert pipeline.client.budget(reference.campaign_id) == budget
                driver = DurableTurnDriver(
                    connection.clients["control"],
                    reference.campaign_id,
                    epoch=2,
                    configuration_digest=CONFIG,
                )
                prepared = driver.prepare_turn(1, lambda: report_action(pipeline))
                await until(
                    lambda: pipeline.client.get("drive_turn", prepared["id"])["state"] != "prepared"
                )
                row = pipeline.client.get("orchestration_binding", reference.campaign_id)
                pipeline.client.command(
                    "orchestration.fence",
                    FenceOrchestration(
                        campaign_id=reference.campaign_id,
                        epoch=2,
                        reason="Finish the provider fixture",
                    ),
                    idempotency_key=new_id(),
                    expected_revision=row["revision"],
                )
                assert await asyncio.wait_for(handle.result(), 15) == "retired"
            histories = await replay_chain(env, handle)
            if recovery == "lost_creation_reply":
                assert len(snapshots) == 2 and snapshots[0] == snapshots[1]
            elif recovery == "invalidated_snapshot":
                assert len(snapshots) == 2 and snapshots[0] != snapshots[1]
                assert len(histories) >= 2
            else:
                assert len(snapshots) == 1


@pytest.mark.asyncio
async def test_real_server_restart_reopens_original_workflow_history(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    drivers = bind(pipeline)
    waited = record(pipeline, drivers)
    database = str(tmp_path / "temporal.sqlite")
    seen = []

    class ObservedWait(TemporalActivities):
        def _wait(self, work: WorkReference) -> WorkOutcome:
            seen.append(work.record_id)
            return super()._wait(work)

    with ExitStack() as stack:
        url = stack.enter_context(
            serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials))
        )
        connection = connect(pipeline, drivers, stack, url)
        reference = connection.binding
        activities = ObservedWait({workflow_id(reference): connection})
        queue = new_id()
        parameters = WorkflowInput(reference, poll_seconds=0.1)
        async with (
            await WorkflowEnvironment.start_local(
                ui=False, dev_server_database_filename=database, data_converter=DATA_CONVERTER
            ) as first,
            worker(first, queue, activities),
        ):
            handle = await TemporalBridge(first.client, task_queue=queue).start(parameters)
            original_run = handle.first_execution_run_id
            await until(lambda: bool(seen))
        assert pipeline.client.get("orchestration_wait", waited["wait"]["id"])["state"] == "waiting"
        report(pipeline)
        async with await WorkflowEnvironment.start_local(
            ui=False, dev_server_database_filename=database, data_converter=DATA_CONVERTER
        ) as second:
            resumed = await TemporalBridge(second.client, task_queue=queue).start(parameters)
            assert (await resumed.describe()).run_id == original_run
            async with worker(second, queue, activities):
                await until(lambda: len(seen) >= 2)
                fence(pipeline)
                assert await asyncio.wait_for(resumed.result(), 15) == "retired"
            history = await second.client.get_workflow_handle(
                resumed.id, run_id=original_run
            ).fetch_history()
            await Replayer(workflows=[CampaignWorkflow]).replay_workflow(history)
            assert set(seen) == {waited["wait"]["id"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("pipeline", [{"source_finish_delay": 3}], indirect=True)
async def test_workflow_cancellation_does_not_cancel_or_refund_running_native_work(
    pipeline: Pipeline,
) -> None:
    drivers = bind(pipeline)
    with ExitStack() as stack:
        url = stack.enter_context(
            serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials))
        )
        connection = connect(pipeline, drivers, stack, url)
        reference = connection.binding
        activities = TemporalActivities({workflow_id(reference): connection})
        async with await WorkflowEnvironment.start_local(
            ui=False, data_converter=DATA_CONVERTER
        ) as env:
            queue = new_id()
            async with worker(env, queue, activities):
                handle = await TemporalBridge(env.client, task_queue=queue).start(
                    WorkflowInput(reference, poll_seconds=0.05)
                )
                attempt = pipeline.worker.claim(pipeline.ready())
                native = asyncio.create_task(
                    asyncio.to_thread(pipeline.worker.execute, attempt["id"])
                )
                try:

                    def running() -> bool:
                        try:
                            return (
                                pipeline.worker.journal.get(attempt["id"]).state
                                == JournalState.RUNNING
                            )
                        except ResearchError:
                            return False

                    await until(running)
                    budget = pipeline.client.budget(reference.campaign_id)
                    await handle.cancel()
                    with pytest.raises(WorkflowFailureError):
                        await asyncio.wait_for(handle.result(), 10)
                    assert pipeline.client.budget(reference.campaign_id) == budget
                    assert running()
                    assert (
                        pipeline.client.get("attempt", attempt["id"])["state"] == attempt["state"]
                    )
                finally:
                    outcome = await asyncio.wait_for(native, 15)
                assert outcome["state"] == "succeeded"
            await replay_chain(env, handle)
