from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import PauseCampaign
from research_swarm.control.orchestration_models import CloseDriveTurn, DriveTurnTarget
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import ReportCampaign
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.orchestration import DriveAction
from research_swarm.domain.types import new_id
from research_swarm.orchestration.turns import DurableTurnDriver
from research_swarm.runtime import LocalRuntime
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_http_delivery import serving
from tests.orchestration.test_orchestration_turns import CONFIG, bind, execute, fence, prepare


def report_action(pipeline: Pipeline) -> DriveAction:
    return DriveAction(
        operation="report.create",
        payload=ReportCampaign(campaign_id=pipeline.campaign["id"]).model_dump(mode="json"),
        idempotency_key="saved-report",
    )


def close(
    pipeline: Pipeline,
    turn: dict[str, Any],
    *,
    client: ResearchClient | None = None,
    finding: str = "Retire the saved action before moving its driver",
    key: str | None = None,
) -> dict[str, Any]:
    return (client or pipeline.client).command(
        "orchestration.close_turn",
        CloseDriveTurn(turn_id=turn["id"], epoch=1, finding=finding),
        idempotency_key=key or new_id(),
        expected_revision=turn["revision"],
    )["result"]


def test_unacknowledged_closure_is_replayable_without_fabricating_target_outcome(
    pipeline: Pipeline,
) -> None:
    drivers = bind(pipeline)
    command = report_action(pipeline)
    turn = prepare(pipeline, drivers.control, command)
    before_budget = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError) as active:
        close(pipeline, turn)
    assert active.value.code == ErrorCode.STALE_FENCE
    fence(pipeline)
    closed = close(pipeline, turn, key="original-closure")
    assert closed["turn"]["state"] == "closed_unacknowledged"
    assert closed["receipt"] is closed["response"] is None
    assert closed["closure"]["data"]["reason"] == "fenced_without_target_ack"
    assert "target_command_id" not in closed["closure"]["data"]
    assert pipeline.client.list(RecordQuery(kind="drive_receipt"))["items"] == []
    assert pipeline.client.budget(pipeline.campaign["id"]) == before_budget
    binding = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    assert binding["state"] == "fenced" and binding["data"]["pending_turn_id"] is None
    reopened = LocalRuntime.open(pipeline.runtime.root)
    local = ResearchClient(LocalTransport(reopened))
    assert close(pipeline, turn, client=local, key="original-closure") == closed
    assert close(pipeline, turn, client=local) == closed
    assert (
        local.command(
            "orchestration.reconcile_turn",
            DriveTurnTarget(turn_id=turn["id"]),
            idempotency_key="read-closed-turn",
        )["result"]
        == closed
    )
    with pytest.raises(ResearchError) as changed:
        close(pipeline, turn, finding="A different closure claim")
    assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    with (
        serving(create_app(reopened.service, reopened.credentials)) as url,
        ResearchClient.connect(url, drivers.token) as remote,
    ):
        runner = DurableTurnDriver(
            remote, pipeline.campaign["id"], epoch=1, configuration_digest=CONFIG
        )

        def must_not_plan() -> DriveAction:
            raise AssertionError("Closed ordinal cannot replan")

        assert runner.advance_turn(1, must_not_plan) == closed
    assert local.list(RecordQuery(kind="report"))["items"] == []
    with reopened.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM commands WHERE actor_id=? AND idempotency_key=?",
                (drivers.actor.id, command.idempotency_key),
            ).fetchone()[0]
            == 0
        )


@pytest.mark.parametrize("rejected", [False, True])
def test_accepted_and_rejected_target_acknowledgments_require_reconciliation_not_closure(
    pipeline: Pipeline, rejected: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    drivers = bind(pipeline)
    command = (
        report_action(pipeline)
        if not rejected
        else DriveAction(
            operation="campaign.pause",
            payload=PauseCampaign(campaign_id=pipeline.campaign["id"], mode="drain").model_dump(
                mode="json"
            ),
            idempotency_key="rejected-original",
            expected_revision=0,
        )
    )
    turn = prepare(pipeline, drivers.control, command)

    def lose_receipt(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("Fixture lost outer receipt")

    with monkeypatch.context() as patch:
        patch.setattr(pipeline.runtime.service.orchestration, "_finish", lose_receipt)
        with pytest.raises(RuntimeError, match="lost outer receipt"):
            execute(drivers.control, turn)
    fence(pipeline)
    with pytest.raises(ResearchError) as acknowledged:
        close(pipeline, turn)
    assert acknowledged.value.code == ErrorCode.INVALID_TRANSITION
    assert pipeline.client.list(RecordQuery(kind="drive_closure"))["items"] == []
    result = pipeline.client.command(
        "orchestration.reconcile_turn",
        DriveTurnTarget(turn_id=turn["id"]),
        idempotency_key="reconcile-original",
    )["result"]
    assert result["receipt"]["data"]["disposition"] == ("rejected" if rejected else "accepted")
    assert result["turn"]["state"] == "settled"
    assert execute(drivers.control, turn) == result
    assert len(pipeline.client.list(RecordQuery(kind="drive_receipt"))["items"]) == 1


def test_delayed_original_dispatch_cannot_commit_after_closure_or_replace_missing_ack(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    drivers = bind(pipeline)
    command = report_action(pipeline)
    turn = prepare(pipeline, drivers.control, command)
    entered, resume = Event(), Event()
    original = pipeline.runtime.service.orchestration.dispatch

    def delayed(ctx: CommandContext, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        entered.set()
        assert resume.wait(10)
        return original(ctx, operation, payload)

    with monkeypatch.context() as patch, ThreadPoolExecutor(max_workers=1) as pool:
        patch.setattr(pipeline.runtime.service.orchestration, "dispatch", delayed)
        future = pool.submit(execute, drivers.control, turn)
        assert entered.wait(5)
        try:
            fence(pipeline)
            closed = close(pipeline, turn)
        finally:
            resume.set()
        assert future.result(timeout=5) == closed
    assert pipeline.client.list(RecordQuery(kind="report"))["items"] == []
    assert pipeline.client.list(RecordQuery(kind="drive_receipt"))["items"] == []
    with pipeline.runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM commands WHERE actor_id=? AND idempotency_key=?",
                (drivers.actor.id, command.idempotency_key),
            ).fetchone()[0]
            == 0
        )
    with pytest.raises(ResearchError):
        drivers.control.command(
            command.operation, command.payload, idempotency_key=command.idempotency_key
        )
    assert execute(drivers.control, turn) == closed


def test_parallel_management_closure_preserves_one_terminal_record(pipeline: Pipeline) -> None:
    drivers = bind(pipeline)
    turn = prepare(pipeline, drivers.control, report_action(pipeline))
    fence(pipeline)
    clients = [
        ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root))) for _ in range(2)
    ]
    barrier = Barrier(2)

    def compete(index: int) -> dict[str, Any]:
        barrier.wait(timeout=5)
        return close(pipeline, turn, client=clients[index])

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(compete, range(2)))
    assert outcomes[0] == outcomes[1]
    assert len(pipeline.client.list(RecordQuery(kind="drive_closure"))["items"]) == 1


def test_bound_actor_cannot_self_close_and_storage_failure_rolls_back(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    from research_swarm.state.database import Transaction

    drivers = bind(pipeline)
    turn = prepare(pipeline, drivers.control, report_action(pipeline))
    fence(pipeline)
    before = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    with pytest.raises(ResearchError) as self_close:
        close(pipeline, turn, client=drivers.control)
    assert self_close.value.code == ErrorCode.AUTHORIZATION_DENIED
    original = Transaction.event

    def fail(self: Transaction, name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "orchestration.turn_closed_unacknowledged":
            raise RuntimeError("Fixture closure event failure")
        return original(self, name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="closure event failure"):
            close(pipeline, turn, key="atomic-close")
    assert pipeline.client.get("orchestration_binding", pipeline.campaign["id"]) == before
    assert pipeline.client.get("drive_turn", turn["id"]) == turn
    assert pipeline.client.list(RecordQuery(kind="drive_closure"))["items"] == []
    assert close(pipeline, turn, key="atomic-close")["turn"]["state"] == "closed_unacknowledged"


def test_direct_service_requires_read_before_closure_and_exact_replay(pipeline: Pipeline) -> None:
    drivers = bind(pipeline)
    turn = prepare(pipeline, drivers.control, report_action(pipeline))
    fence(pipeline)
    request = CloseDriveTurn(turn_id=turn["id"], epoch=1, finding="Direct service closure")
    owner = pipeline.runtime.principal()
    restricted = owner.model_copy(update={"scopes": frozenset({"orchestration.manage"})})
    denied_context = CommandContext(
        principal=restricted,
        idempotency_key="direct-close",
        expected_revision=turn["revision"],
    )
    service = pipeline.runtime.service
    with pytest.raises(ResearchError) as before:
        service.close_drive_turn(denied_context, request)
    assert before.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert pipeline.client.get("drive_turn", turn["id"])["state"] == "prepared"
    accepted = service.close_drive_turn(
        denied_context.model_copy(update={"principal": owner}), request
    )
    assert accepted["result"]["turn"]["state"] == "closed_unacknowledged"
    with pytest.raises(ResearchError) as replay:
        service.close_drive_turn(denied_context, request)
    assert replay.value.code == ErrorCode.AUTHORIZATION_DENIED
