from __future__ import annotations

import subprocess
import sys
from typing import Any

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.child_models import CancelChildCampaign
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import ReportCampaign
from research_swarm.control.wait_models import (
    AcknowledgeWakeup,
    OrchestrationWaitTarget,
    RecordOrchestrationWait,
)
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DriveAction
from research_swarm.domain.orchestration_waits import WaitCondition, WakeupDelivery
from research_swarm.domain.types import new_id
from research_swarm.orchestration.turns import DurableTurnDriver
from research_swarm.orchestration.waits import DurableWaitDriver
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from tests.test_child_campaigns import OPTIONS, create
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_orchestration_turns import CONFIG, Drivers, bind, fence


def record(
    pipeline: Pipeline,
    drivers: Drivers,
    *,
    observed: int | None = None,
    types: tuple[str, ...] = ("report.created",),
    at: str | None = None,
    actor: ResearchClient | None = None,
) -> dict[str, Any]:
    binding = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    observed = (
        int(pipeline.client.context(pipeline.campaign["id"])["watermark"])
        if observed is None
        else observed
    )
    return (actor or drivers.control).command(
        "orchestration.record_wait",
        RecordOrchestrationWait(
            wait_id=new_id(),
            campaign_id=pipeline.campaign["id"],
            epoch=1,
            configuration_digest=CONFIG,
            condition=WaitCondition(observed_watermark=observed, event_types=types, wake_at=at),
        ),
        idempotency_key=new_id(),
        expected_revision=binding["revision"],
    )["result"]


def report(pipeline: Pipeline) -> dict[str, Any]:
    return pipeline.client.command(
        "report.create",
        ReportCampaign(campaign_id=pipeline.campaign["id"]),
        idempotency_key=new_id(),
    )["result"]


def driver(
    pipeline: Pipeline,
    drivers: Drivers,
    *,
    client: ResearchClient | None = None,
    actor_id: str | None = None,
) -> DurableWaitDriver:
    return DurableWaitDriver(
        client or drivers.control,
        pipeline.campaign["id"],
        epoch=1,
        actor_id=actor_id or drivers.actor.id,
    )


def test_registration_catches_event_between_snapshot_and_wait_without_advancing_observed_cursor(
    pipeline: Pipeline,
) -> None:
    drivers = bind(pipeline)
    observed = pipeline.client.context(pipeline.campaign["id"])["watermark"]
    trigger = report(pipeline)
    waited = record(pipeline, drivers, observed=observed)
    assert waited["wait"]["state"] == "ready"
    assert waited["wait"]["data"]["condition"]["observed_watermark"] == observed
    events = pipeline.client.events(after=observed)["events"]
    causal = next(event for event in events if event["aggregate_id"] == trigger["id"])
    assert waited["readiness"]["data"]["event_id"] == causal["id"]
    assert waited["readiness"]["data"]["watermark"] == causal["sequence"]
    assert waited["delivery"]["state"] == "pending"


@pytest.mark.asyncio
async def test_controller_exits_after_commit_and_duplicate_delivery_recovers_without_consuming_wait(
    pipeline: Pipeline,
) -> None:
    drivers = bind(pipeline)
    waited = record(pipeline, drivers)
    identifier = waited["wait"]["id"]
    code = (
        "import os,sys; from pathlib import Path; "
        "from research_swarm.runtime import LocalRuntime; "
        "from research_swarm.client import ResearchClient,LocalTransport; "
        "from research_swarm.control.reports import ReportCampaign; "
        "runtime=LocalRuntime.open(Path(sys.argv[1])); "
        "client=ResearchClient(LocalTransport(runtime)); "
        "client.command('report.create',ReportCampaign(campaign_id=sys.argv[2]),"
        "idempotency_key='crash-after-commit'); "
        "os._exit(77)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(pipeline.runtime.root), pipeline.campaign["id"]],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 77, result.stderr
    reopened = LocalRuntime.open(pipeline.runtime.root)
    client = ResearchClient(
        LocalTransport(reopened, lambda: reopened.credentials.resolve(drivers.token))
    )
    recovered = driver(pipeline, drivers, client=client)
    attempts: list[str] = []
    accepted: set[str] = set()

    async def deliver(wakeup: WakeupDelivery) -> None:
        attempts.append(wakeup.wait_id)
        accepted.add(wakeup.wait_id)
        if len(attempts) == 1:
            raise RuntimeError("Fixture lost reply after receiver accepted the wakeup")

    with pytest.raises(RuntimeError, match="lost reply"):
        await recovered.deliver_once(deliver)
    assert client.get("wakeup_delivery", identifier)["state"] == "pending"
    assert await recovered.deliver_once(deliver) == (identifier,)
    assert await recovered.deliver_once(deliver) == ()
    assert attempts == [identifier, identifier] and accepted == {identifier}
    assert client.get("orchestration_wait", identifier)["state"] == "ready"
    assert client.get("wakeup_delivery", identifier)["state"] == "delivered"
    assert len(client.list(RecordQuery(kind="wakeup_acknowledgment"))["items"]) == 1
    assert len(client.list(RecordQuery(kind="report"))["items"]) == 1


def test_persisted_timer_survives_reopen_and_same_pending_key_can_later_succeed(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drivers = bind(pipeline)
    at = "2099-01-01T00:00:00.000Z"
    waited = record(pipeline, drivers, types=("never.happens",), at=at)
    identifier = waited["wait"]["id"]
    with pytest.raises(ResearchError) as pending:
        driver(pipeline, drivers).reconcile(identifier)
    assert pending.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    reopened = LocalRuntime.open(pipeline.runtime.root)
    client = ResearchClient(
        LocalTransport(reopened, lambda: reopened.credentials.resolve(drivers.token))
    )
    monkeypatch.setattr("research_swarm.state.waits.utc_now", lambda: at)
    settled = driver(pipeline, drivers, client=client).reconcile(identifier)
    assert settled["readiness"]["data"]["cause"] == "timer"
    assert settled["wait"]["data"]["condition"]["wake_at"] == at
    assert driver(pipeline, drivers, client=client).reconcile(identifier) == settled
    assert len(client.list(RecordQuery(kind="wait_readiness"))["items"]) == 1


def test_consuming_wakeup_requires_observed_watermark_and_rejected_prepare_does_not_poison_retry(
    pipeline: Pipeline,
) -> None:
    drivers = bind(pipeline)
    waited = record(pipeline, drivers)
    turns = DurableTurnDriver(
        drivers.control, pipeline.campaign["id"], epoch=1, configuration_digest=CONFIG
    )

    def plan() -> DriveAction:
        return DriveAction(
            operation="report.create",
            payload=ReportCampaign(campaign_id=pipeline.campaign["id"]).model_dump(mode="json"),
            idempotency_key="report-after-wait",
        )

    with pytest.raises(ResearchError) as asleep:
        turns.advance_turn(1, plan)
    assert asleep.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    report(pipeline)
    with pytest.raises(ResearchError) as stale:
        turns.advance_turn(1, plan, planning_watermark=0)
    assert stale.value.code == ErrorCode.REVISION_CONFLICT
    assert pipeline.client.list(RecordQuery(kind="drive_turn"))["items"] == []
    assert turns.advance_turn(1, plan)["response"]["disposition"] == "accepted"
    assert pipeline.client.get("orchestration_wait", waited["wait"]["id"])["state"] == "consumed"
    assert (
        pipeline.client.get("orchestration_binding", pipeline.campaign["id"])["data"].get(
            "active_wait_id"
        )
        is None
    )


@pytest.mark.asyncio
async def test_actor_specific_delivery_does_not_starve_behind_another_route(
    pipeline: Pipeline,
) -> None:
    drivers = bind(pipeline)
    for _ in range(3):
        record(pipeline, drivers, actor=drivers.evaluator)
        report(pipeline)
    current = record(pipeline, drivers)
    report(pipeline)
    deliveries: list[str] = []

    async def accept(wakeup: WakeupDelivery) -> None:
        deliveries.append(wakeup.wait_id)

    assert await driver(pipeline, drivers).deliver_once(accept, limit=1) == (current["wait"]["id"],)
    assert deliveries == [current["wait"]["id"]]
    assert (
        len(pipeline.client.list(RecordQuery(kind="wakeup_delivery", state="pending"))["items"])
        == 3
    )


def test_bookkeeping_does_not_wake_itself_and_fence_preserves_destination(
    pipeline: Pipeline,
) -> None:
    drivers = bind(pipeline)
    waited = record(pipeline, drivers, types=())
    identifier = waited["wait"]["id"]
    assert waited["wait"]["state"] == "waiting"
    assert pipeline.client.list(RecordQuery(kind="wakeup_delivery"))["items"] == []
    fence(pipeline)
    outcome = drivers.control.command(
        "orchestration.reconcile_wait",
        OrchestrationWaitTarget(wait_id=identifier),
        idempotency_key="fenced-wait",
    )["result"]
    assert outcome["wait"]["state"] == "superseded"
    assert outcome["delivery"]["data"]["epoch"] == 1
    assert outcome["readiness"]["data"]["cause"] == "fenced"
    with pytest.raises(ResearchError) as wrong_epoch:
        drivers.control.command(
            "orchestration.acknowledge_wakeup",
            AcknowledgeWakeup(delivery_id=identifier, campaign_id=pipeline.campaign["id"], epoch=2),
            idempotency_key="wrong-destination",
            expected_revision=1,
        )
    assert wrong_epoch.value.code == ErrorCode.STALE_FENCE
    assert pipeline.client.get("wakeup_delivery", identifier)["state"] == "pending"


def test_wakeup_creation_failure_rolls_back_triggering_domain_event(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drivers = bind(pipeline)
    waited = record(pipeline, drivers)
    insert = Transaction.insert

    def fail(self: Transaction, kind: str, *args: Any, **kwargs: Any) -> Any:
        if kind == "wakeup_delivery":
            raise RuntimeError("Fixture wakeup storage failure")
        return insert(self, kind, *args, **kwargs)

    before = pipeline.client.context(pipeline.campaign["id"])["watermark"]
    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "insert", fail)
        with pytest.raises(RuntimeError, match="wakeup storage failure"):
            report(pipeline)
    assert pipeline.client.list(RecordQuery(kind="report"))["items"] == []
    assert pipeline.client.events(after=before)["events"] == []
    assert pipeline.client.get("orchestration_wait", waited["wait"]["id"])["state"] == "waiting"


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
@pytest.mark.parametrize("before_registration", [False, True])
def test_zero_funded_child_closure_wakes_parent_live_and_during_catchup(
    pipeline: Pipeline, before_registration: bool
) -> None:
    child = create(
        pipeline,
        limits=(
            BudgetLimit(dimension="trial_admissions", limit=0, unit="count"),
            BudgetLimit(dimension="attempts", limit=0, unit="count"),
            BudgetLimit(dimension="execution_ms", limit=0, unit="ms"),
        ),
    )["campaign"]
    drivers = bind(pipeline)
    observed = pipeline.client.context(pipeline.campaign["id"])["watermark"]

    def close() -> None:
        pipeline.client.command(
            "campaign.cancel_child",
            CancelChildCampaign(child_campaign_id=child["id"], reason="Close unused branch"),
            idempotency_key="close-zero-child",
            expected_revision=child["revision"],
        )

    if before_registration:
        close()
    waited = record(pipeline, drivers, observed=observed, types=("funding.child_closed",))
    if not before_registration:
        assert waited["wait"]["state"] == "waiting"
        close()
    outcome = driver(pipeline, drivers).reconcile(waited["wait"]["id"])
    assert outcome["wait"]["state"] == "ready"
    event = next(
        event
        for event in pipeline.client.events(after=observed)["events"]
        if event["id"] == outcome["readiness"]["data"]["event_id"]
    )
    assert event["campaign_id"] == pipeline.campaign["id"]
    assert event["type"] == "funding.child_closed"
    assert event["payload"]["child_campaign_id"] == child["id"]
