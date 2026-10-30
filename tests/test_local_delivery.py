from __future__ import annotations

import asyncio
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Event
from typing import Any

import pytest

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import OrchestrationBinding
from research_swarm.orchestration.delivery import DriverConnection, binding_reference
from research_swarm.orchestration.local_delivery import LocalDeliveryDriver
from research_swarm.orchestration.turns import DurableTurnDriver
from tests.test_drive_closures import report_action
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_orchestration_transfer import pause, request, successor, transfer
from tests.test_orchestration_turns import CONFIG, Drivers, bind
from tests.test_orchestration_waits import record, report


def connection(pipeline: Pipeline, drivers: Drivers) -> DriverConnection:
    binding = OrchestrationBinding.model_validate(
        pipeline.client.get("orchestration_binding", pipeline.campaign["id"])["data"]
    )
    return DriverConnection(
        binding_reference(binding), {"control": drivers.control, "evaluator": drivers.evaluator}
    )


def test_local_delivery_import_has_no_temporal_dependency() -> None:
    code = """
import sys
class RejectTemporal:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] == 'temporalio':
            raise AssertionError('Local delivery imported Temporal')
sys.meta_path.insert(0, RejectTemporal())
from research_swarm.orchestration.local_delivery import LocalDeliveryDriver
assert LocalDeliveryDriver.__name__ == 'LocalDeliveryDriver'
"""
    subprocess.run(
        [sys.executable, "-c", code], check=True, timeout=20, capture_output=True, text=True
    )


def test_local_checkpoint_recovers_exact_delivery_after_lost_acceptance(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    drivers = bind(pipeline)
    connected = connection(pipeline, drivers)
    root = tmp_path / "delivery"
    driver = LocalDeliveryDriver(root, connected)
    prepared = DurableTurnDriver(
        drivers.control, pipeline.campaign["id"], epoch=1, configuration_digest=CONFIG
    ).prepare_turn(1, lambda: report_action(pipeline))
    execute = driver.delivery.execute

    def lose(work: Any) -> Any:
        execute(work)
        raise TimeoutError("Fixture lost local acknowledgment")

    monkeypatch.setattr(driver.delivery, "execute", lose)
    with pytest.raises(TimeoutError):
        driver.step()
    assert driver._read().pending.record_id == prepared["id"]
    restarted = LocalDeliveryDriver(root, connected)
    result = restarted.step()
    assert result.state == "settled"
    assert result.outcome.record_id == prepared["id"]
    assert result.outcome.disposition == "accepted"
    assert restarted._read().pending is None
    assert restarted.step().state == "idle"
    assert (
        pipeline.client.get("orchestration_binding", pipeline.campaign["id"])["data"][
            "next_ordinal"
        ]
        == 2
    )


def test_local_restart_reuses_uncertain_startup_snapshot(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bind(pipeline)
    pause(pipeline)
    next_driver = successor(pipeline)
    intent, revision = request(pipeline, next_driver)
    transfer(pipeline, intent, revision)
    connected = connection(pipeline, next_driver)
    first = LocalDeliveryDriver(tmp_path / "delivery", connected)
    generation = first._read().recovery_id
    snapshots = []
    command = next_driver.control.command

    def lost(operation: str, payload: Any, **kwargs: Any) -> Any:
        response = command(operation, payload, **kwargs)
        if operation == "stream.create_snapshot":
            snapshots.append(payload.snapshot_id)
            if len(snapshots) == 1:
                raise TimeoutError("Fixture snapshot reply lost")
        return response

    monkeypatch.setattr(next_driver.control, "command", lost)
    with pytest.raises(TimeoutError):
        first.step()
    restarted = LocalDeliveryDriver(tmp_path / "delivery", connected)
    assert restarted._read().recovery_id == generation
    assert restarted.step().state == "idle"
    assert len(snapshots) == 2 and snapshots[0] == snapshots[1]
    assert pipeline.client.get("campaign", pipeline.campaign["id"])["state"] == "paused"


def test_local_checkpoint_rejects_identity_change_and_concurrent_step(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    drivers = bind(pipeline)
    connected = connection(pipeline, drivers)
    root = tmp_path / "delivery"
    first, second = LocalDeliveryDriver(root, connected), LocalDeliveryDriver(root, connected)
    before = (root / "delivery.json").read_bytes()
    with pytest.raises(ResearchError) as wrong:
        LocalDeliveryDriver(root, replace(connected, binding=replace(connected.binding, epoch=2)))
    assert wrong.value.code == ErrorCode.STALE_FENCE
    assert (root / "delivery.json").read_bytes() == before
    entered, release = Event(), Event()
    inspect = first.delivery.inspect

    def held(reference: Any) -> Any:
        entered.set()
        assert release.wait(10)
        return inspect(reference)

    monkeypatch.setattr(first.delivery, "inspect", held)
    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(first.step)
        try:
            assert entered.wait(10)
            with pytest.raises(ResearchError) as contended:
                second.step()
            assert contended.value.code == ErrorCode.RESOURCE_UNAVAILABLE
        finally:
            release.set()
        assert running.result().state == "idle"


@pytest.mark.asyncio
async def test_async_local_wait_stops_without_consuming_readiness_or_mutating_campaign(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    drivers = bind(pipeline)
    waited = record(pipeline, drivers)
    driver = LocalDeliveryDriver(tmp_path / "delivery", connection(pipeline, drivers))
    assert (await driver.step_async()).state == "pending"
    report(pipeline)
    result = await driver.step_async()
    assert result.state == "settled" and result.outcome.disposition == "event"
    assert (await driver.step_async()).state == "idle"
    assert pipeline.client.get("orchestration_wait", waited["wait"]["id"])["state"] == "ready"
    before = pipeline.client.get("campaign", pipeline.campaign["id"])
    stop = asyncio.Event()
    running = asyncio.create_task(driver.run(stop, poll_seconds=0.01))
    await asyncio.sleep(0.03)
    stop.set()
    assert (await running).state == "stopped"
    assert pipeline.client.get("campaign", pipeline.campaign["id"]) == before


@pytest.mark.asyncio
async def test_async_cancellation_drains_owned_delivery_before_releasing_checkpoint(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    drivers = bind(pipeline)
    connected = connection(pipeline, drivers)
    root = tmp_path / "delivery"
    first, second = LocalDeliveryDriver(root, connected), LocalDeliveryDriver(root, connected)
    prepared = DurableTurnDriver(
        drivers.control, pipeline.campaign["id"], epoch=1, configuration_digest=CONFIG
    ).prepare_turn(1, lambda: report_action(pipeline))
    entered, release = Event(), Event()
    execute = first.delivery.execute

    def held(work: Any) -> Any:
        entered.set()
        assert release.wait(10)
        return execute(work)

    monkeypatch.setattr(first.delivery, "execute", held)
    task = asyncio.create_task(first.step_async())
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        with pytest.raises(ResearchError) as contended:
            await second.step_async()
        assert contended.value.code == ErrorCode.RESOURCE_UNAVAILABLE
        assert first._read().pending.record_id == prepared["id"]
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert first._read().pending is None
    assert pipeline.client.get("drive_turn", prepared["id"])["state"] == "settled"
    assert second.step().state == "idle"
