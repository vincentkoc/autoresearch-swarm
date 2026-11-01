"""Async delivery accepts completed external recovery without running recovery itself."""

import copy

import pytest

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.orchestration.delivery_models import StartupRequest
from research_swarm.orchestration.startup import recover_driver_start
from research_swarm.orchestration.turns import DurableTurnDriver
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_async_delivery import kernels
from tests.orchestration.test_drive_closures import report_action
from tests.orchestration.test_orchestration_transfer import pause, request, successor, transfer
from tests.orchestration.test_orchestration_turns import CONFIG, bind


@pytest.fixture
def transferred(pipeline):
    bind(pipeline)
    pause(pipeline)
    drivers = successor(pipeline)
    intent, revision = request(pipeline, drivers)
    transfer(pipeline, intent, revision)
    asynchronous, _, connection = kernels(pipeline, drivers)
    row = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    return pipeline, drivers, asynchronous, connection, row["data"]["start_ids"]


def recover(connection, starts, routes=("control", "evaluator")):
    for route in routes:
        recover_driver_start(connection.clients[route].client, starts[route], recovery_id=new_id())


async def test_completed_external_recovery_enables_read_only_bootstrap_across_revision_change(
    transferred, monkeypatch
):
    pipeline, drivers, kernel, connection, starts = transferred
    startup = StartupRequest(connection.binding, new_id())
    with pytest.raises(ResearchError) as pending:
        await kernel.bootstrap(startup)
    assert pending.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    recover(connection, starts, ("control",))
    with pytest.raises(ResearchError) as incomplete:
        await kernel.bootstrap(startup)
    assert incomplete.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    recover(connection, starts, ("evaluator",))
    prepared, reads = [], []
    control = connection.clients["control"]
    original = control.get

    async def advance(kind, identifier):
        row = await original(kind, identifier)
        reads.append(kind)
        if kind == "orchestration_start" and not prepared:
            prepared.append(
                DurableTurnDriver(
                    drivers.control, pipeline.campaign["id"], epoch=2, configuration_digest=CONFIG
                ).prepare_turn(1, lambda: report_action(pipeline))
            )
        return row

    async def no_commands(*args, **kwargs):
        pytest.fail("Async bootstrap must not create snapshots or acknowledge starts")

    with monkeypatch.context() as patch:
        patch.setattr(control, "get", advance)
        for client in connection.clients.values():
            patch.setattr(client, "command", no_commands)
        assert await kernel.bootstrap(startup) == "active"
    assert set(reads) == {"orchestration_binding", "orchestration_start", "orchestration_start_ack"}
    work = (await kernel.inspect(connection.binding)).work
    assert work.record_id == prepared[0]["id"]
    first = await kernel.execute(work)
    assert first.disposition == "accepted"
    assert await kernel.execute(work) == first


@pytest.mark.parametrize(
    "change",
    [
        "missing-route",
        "duplicate-start",
        "actor",
        "epoch",
        "configuration",
        "campaign",
        "crossed-id",
        "ack-actor",
        "ack-watermark",
    ],
)
async def test_inconsistent_startup_or_acknowledgment_cannot_enable_delivery(
    transferred, monkeypatch, change
):
    _, _, kernel, connection, starts = transferred
    recover(connection, starts)
    control = connection.clients["control"]
    original = control.get

    async def forged(kind, identifier):
        row = copy.deepcopy(await original(kind, identifier))
        if kind == "orchestration_binding":
            if change == "missing-route":
                row["data"]["start_ids"].pop("evaluator")
            if change == "duplicate-start":
                row["data"]["start_ids"]["evaluator"] = starts["control"]
        elif kind == "orchestration_start":
            if change == "actor":
                row["data"]["actor_id"] = new_id()
            elif change == "epoch":
                row["data"]["epoch"] += 1
            elif change == "configuration":
                row["data"]["configuration_digest"] = "sha256:" + "a" * 64
            elif change == "campaign":
                row["campaign_id"] = new_id()
            elif change == "crossed-id":
                row["id"] = starts["evaluator"]
        elif kind == "orchestration_start_ack":
            if change == "ack-actor":
                row["data"]["acknowledged_by"] = new_id()
            elif change == "ack-watermark":
                row["data"]["observed_watermark"] = 0
        return row

    monkeypatch.setattr(control, "get", forged)
    with pytest.raises(ResearchError):
        await kernel.bootstrap(StartupRequest(connection.binding, new_id()))


async def test_fence_during_awaited_start_read_retires_bootstrap(transferred, monkeypatch):
    pipeline, _, kernel, connection, starts = transferred
    recover(connection, starts)
    control = connection.clients["control"]
    original = control.get
    changed = False

    async def retire(kind, identifier):
        nonlocal changed
        row = await original(kind, identifier)
        if kind == "orchestration_start" and not changed:
            changed = True
            binding = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
            pipeline.client.command(
                "orchestration.fence",
                {"campaign_id": binding["id"], "epoch": 2, "reason": "Fixture successor ended"},
                idempotency_key=new_id(),
                expected_revision=binding["revision"],
            )
        return row

    monkeypatch.setattr(control, "get", retire)
    assert await kernel.bootstrap(StartupRequest(connection.binding, new_id())) == "retired"
