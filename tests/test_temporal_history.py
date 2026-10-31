"""Offline compatibility with immutable histories from an earlier source revision."""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio import workflow
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from research_swarm.orchestration.temporal.runtime import DATA_CONVERTER
from research_swarm.orchestration.temporal.workflow import CampaignWorkflow
from tests.temporal_history import SCENARIOS, check_privacy, encoded, load_fixture, validate_bundle
from tests.temporal_incompatible import IncompatibleCampaignWorkflow


@pytest.mark.parametrize("scenario", SCENARIOS)
async def test_saved_v1_histories_replay_without_server_or_activities(scenario: str) -> None:
    saved = load_fixture()["scenarios"][scenario]
    with ThreadPoolExecutor(max_workers=2) as executor:
        replayer = Replayer(
            workflows=[CampaignWorkflow],
            data_converter=DATA_CONVERTER,
            workflow_task_executor=executor,
        )
        async with asyncio.timeout(15):
            for run in saved["runs"]:
                result = await replayer.replay_workflow(
                    WorkflowHistory.from_json(saved["workflow_id"], run["history"])
                )
                assert result.replay_failure is None


async def test_saved_history_rejects_an_incompatible_first_command() -> None:
    saved = load_fixture()["scenarios"]["command-retry"]
    history = WorkflowHistory.from_json(saved["workflow_id"], saved["runs"][0]["history"])
    with ThreadPoolExecutor(max_workers=2) as executor:
        replayer = Replayer(
            workflows=[IncompatibleCampaignWorkflow],
            data_converter=DATA_CONVERTER,
            workflow_task_executor=executor,
        )
        async with asyncio.timeout(15):
            result = await replayer.replay_workflow(history, raise_on_replay_failure=False)
    assert isinstance(result.replay_failure, workflow.NondeterminismError)


def test_saved_scenarios_include_retry_continuation_and_transferred_startup() -> None:
    scenarios = load_fixture()["scenarios"]
    for name, activity_type in (
        ("command-retry", "research.execute-v1"),
        ("transferred-checkpoint", "research.bootstrap-v1"),
    ):
        scheduled = {}
        retried = False
        for run in scenarios[name]["runs"]:
            history = WorkflowHistory.from_json(scenarios[name]["workflow_id"], run["history"])
            for event in history.events:
                if event.HasField("activity_task_scheduled_event_attributes"):
                    scheduled[event.event_id] = (
                        event.activity_task_scheduled_event_attributes.activity_type.name
                    )
                if event.HasField("activity_task_started_event_attributes"):
                    started = event.activity_task_started_event_attributes
                    retried |= (
                        started.attempt > 1
                        and scheduled[started.scheduled_event_id] == activity_type
                    )
        assert retried, name
    continued = scenarios["continued-wait"]
    assert len(continued["runs"]) >= 2
    first = WorkflowHistory.from_json(continued["workflow_id"], continued["runs"][0]["history"])
    carried = json.loads(
        first.events[-1].workflow_execution_continued_as_new_event_attributes.input.payloads[0].data
    )
    assert carried["pending"]["kind"] == "wait"
    assert carried["pending"]["binding"] == carried["binding"]
    signals = sum(
        event.HasField("workflow_execution_signaled_event_attributes")
        for run in continued["runs"]
        for event in WorkflowHistory.from_json(continued["workflow_id"], run["history"]).events
    )
    assert signals >= 2
    transferred = scenarios["transferred-checkpoint"]
    first = WorkflowHistory.from_json(transferred["workflow_id"], transferred["runs"][0]["history"])
    request = json.loads(
        first.events[0].workflow_execution_started_event_attributes.input.payloads[0].data
    )
    assert request["binding"]["epoch"] == 2


def test_fixture_integrity_rejects_changed_bytes_and_missing_continuation() -> None:
    bundle = load_fixture()
    changed = copy.deepcopy(bundle)
    changed["scenarios"]["command-retry"]["runs"][0]["history"]["events"][0]["eventTime"] = (
        "2020-01-01T00:00:00Z"
    )
    with pytest.raises(ValueError, match="History bytes"):
        validate_bundle(changed)
    incomplete = copy.deepcopy(bundle)
    incomplete["scenarios"]["continued-wait"]["runs"].pop()
    with pytest.raises(ValueError, match="continuation is missing"):
        validate_bundle(incomplete)
    suffix = copy.deepcopy(bundle)
    suffix["scenarios"]["continued-wait"]["runs"].pop(0)
    with pytest.raises(ValueError, match="workflow identity or predecessor"):
        validate_bundle(suffix)


@pytest.mark.parametrize(
    "field", ["firstExecutionRunId", "continuedExecutionRunId", "workflowId", "workflowType"]
)
def test_rehashed_history_still_requires_original_workflow_and_predecessor(field: str) -> None:
    bundle = load_fixture()
    run = bundle["scenarios"]["continued-wait"]["runs"][1]
    started = run["history"]["events"][0]["workflowExecutionStartedEventAttributes"]
    started[field] = {"name": "different-workflow"} if field == "workflowType" else "different-id"
    data = encoded(run["history"])
    run.update(sha256=hashlib.sha256(data).hexdigest(), size_bytes=len(data))
    with pytest.raises(ValueError, match="workflow identity or predecessor"):
        validate_bundle(bundle)


def test_privacy_scan_inspects_encoded_activity_payloads() -> None:
    payload = {
        "metadata": {"encoding": base64.b64encode(b"json/plain").decode()},
        "data": base64.b64encode(json.dumps({"token": "rsw_" + "x" * 43}).encode()).decode(),
    }
    with pytest.raises(ValueError, match="credential material"):
        check_privacy(payload)
    with pytest.raises(ValueError, match="fixed public identity"):
        check_privacy({"identity": "unexpected-provider-worker"})
