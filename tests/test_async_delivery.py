"""Async delivery shares native outcomes and keys without importing native clients."""

import asyncio
import subprocess
import sys
from dataclasses import replace

import pytest

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.orchestration.async_delivery import AsyncDeliveryKernel, AsyncDriverConnection
from research_swarm.orchestration.delivery import DeliveryKernel
from research_swarm.orchestration.delivery_models import StartupRequest, delivery_id
from tests.test_delivery_gateway import prepare
from tests.test_local_delivery import connection
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_orchestration_transfer import pause, request, successor, transfer
from tests.test_orchestration_turns import bind, fence
from tests.test_orchestration_waits import record, report


class AsyncClient:
    """Yield transport scheduling around real local authority operations."""

    def __init__(self, client):
        self.client, self.lose_reply = client, False

    async def get(self, kind, identifier):
        await asyncio.sleep(0)
        return self.client.get(kind, identifier)

    async def command(self, operation, payload, *, idempotency_key):
        await asyncio.sleep(0)
        response = self.client.command(operation, payload, idempotency_key=idempotency_key)
        if self.lose_reply:
            self.lose_reply = False
            raise TimeoutError("Fixture reply lost after acceptance")
        return response


def kernels(pipeline, drivers):
    connected = connection(pipeline, drivers)
    asynchronous = AsyncDriverConnection(
        connected.binding,
        {route: AsyncClient(client) for route, client in connected.clients.items()},
    )
    key = delivery_id(connected.binding)
    return (
        AsyncDeliveryKernel({key: asynchronous}),
        DeliveryKernel({key: connected}),
        asynchronous,
    )


async def test_async_loss_recovery_matches_the_native_delivery_acknowledgment(pipeline):
    drivers = bind(pipeline)
    saved = prepare(pipeline, drivers)
    asynchronous, native, connected = kernels(pipeline, drivers)
    binding = connected.binding
    startup = StartupRequest(binding, new_id())
    assert await asynchronous.bootstrap(startup) == native.bootstrap(startup) == "active"
    pending = await asynchronous.inspect(binding)
    assert pending == native.inspect(binding)
    assert pending.work.record_id == saved["id"]
    connected.clients["control"].lose_reply = True
    with pytest.raises(TimeoutError, match="reply lost"):
        await asynchronous.execute(pending.work)
    outcomes = await asyncio.gather(*(asynchronous.execute(pending.work) for _ in range(3)))
    assert outcomes == [native.execute(pending.work)] * 3
    assert outcomes[0].disposition == "accepted"
    assert await asynchronous.inspect(binding) == native.inspect(binding)
    assert (await asynchronous.inspect(binding)).state == "idle"


async def test_async_wait_uses_authoritative_events_and_preserves_native_readiness(pipeline):
    drivers = bind(pipeline)
    waited = record(pipeline, drivers)
    asynchronous, native, connected = kernels(pipeline, drivers)
    pending = await asynchronous.inspect(connected.binding)
    assert pending == native.inspect(connected.binding)
    assert pending.work.record_id == waited["wait"]["id"]
    assert await asynchronous.wait(pending.work) == native.wait(pending.work)
    assert (await asynchronous.wait(pending.work)).state == "pending"
    report(pipeline)
    outcome = await asynchronous.wait(pending.work)
    assert outcome == native.wait(pending.work)
    assert outcome.state == "settled" and outcome.disposition == "event"
    assert pipeline.client.get("orchestration_wait", pending.work.record_id)["state"] == "ready"
    assert pipeline.client.get("wakeup_delivery", pending.work.record_id)["state"] == "pending"
    fence(pipeline)
    assert (await asynchronous.inspect(connected.binding)).state == "retired"
    assert await asynchronous.inspect(connected.binding) == native.inspect(connected.binding)


async def test_async_route_and_saved_work_identity_cannot_be_substituted(pipeline):
    drivers = bind(pipeline)
    prepare(pipeline, drivers)
    asynchronous, _, connected = kernels(pipeline, drivers)
    work = (await asynchronous.inspect(connected.binding)).work
    for forged in (
        replace(work, actor_id=new_id()),
        replace(work, actor_id=drivers.evaluator_actor.id),
        replace(work, binding=replace(work.binding, epoch=2)),
    ):
        with pytest.raises(ResearchError) as denied:
            await asynchronous.execute(forged)
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pytest.raises(ResearchError) as wrong_kind:
        await asynchronous.wait(work)
    assert wrong_kind.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
    assert (await asynchronous.inspect(connected.binding)).work == work


async def test_async_timer_waits_use_authority_and_replay_the_native_outcome(pipeline, monkeypatch):
    drivers = bind(pipeline)
    at = "2099-01-01T00:00:00.000Z"
    record(pipeline, drivers, types=("never.happens",), at=at)
    asynchronous, native, connected = kernels(pipeline, drivers)
    work = (await asynchronous.inspect(connected.binding)).work
    assert (await asynchronous.wait(work)).state == "pending"
    assert pipeline.client.get("orchestration_wait", work.record_id)["state"] == "waiting"
    monkeypatch.setattr("research_swarm.state.waits.utc_now", lambda: at)
    connected.clients["control"].lose_reply = True
    with pytest.raises(TimeoutError, match="reply lost"):
        await asynchronous.wait(work)
    result = await asynchronous.wait(work)
    assert result == native.wait(work)
    assert result.state == "settled" and result.disposition == "timer"
    assert pipeline.client.get("wait_readiness", work.record_id)["data"]["ready_at"] == at


async def test_async_successor_requires_its_separate_startup_adapter(pipeline):
    bind(pipeline)
    pause(pipeline)
    next_driver = successor(pipeline)
    intent, revision = request(pipeline, next_driver)
    transfer(pipeline, intent, revision)
    asynchronous, _, connected = kernels(pipeline, next_driver)
    with pytest.raises(ResearchError) as unsupported:
        await asynchronous.bootstrap(StartupRequest(connected.binding, new_id()))
    assert unsupported.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


def test_async_runtime_import_does_not_load_native_delivery_or_vendor_runtimes():
    code = """
import sys
class RejectNativeDelivery:
    def find_spec(self, fullname, path=None, target=None):
        blocked = {
            'research_swarm.client', 'research_swarm.orchestration.custody',
            'research_swarm.orchestration.startup', 'research_swarm.orchestration.delivery',
            'research_swarm.runtime', 'fastapi', 'temporalio',
        }
        if fullname in blocked:
            raise AssertionError('Async delivery imported ' + fullname)
sys.meta_path.insert(0, RejectNativeDelivery())
from research_swarm.orchestration.async_delivery import AsyncDeliveryKernel
assert AsyncDeliveryKernel.__name__ == 'AsyncDeliveryKernel'
"""
    subprocess.run(
        [sys.executable, "-c", code], check=True, timeout=20, capture_output=True, text=True
    )
