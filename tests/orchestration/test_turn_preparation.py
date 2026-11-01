"""Pausing delivery between selection and dispatch does not freeze its authority."""

import pytest

from research_swarm.control.orchestration_models import DriveTurnTarget
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.orchestration import DriveAction
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_orchestration_turns import bind, fence, prepare


def saved_report(pipeline):
    drivers = bind(pipeline)
    turn = prepare(
        pipeline,
        drivers.control,
        DriveAction(
            operation="report.create",
            payload={"campaign_id": pipeline.campaign["id"]},
            idempotency_key="saved-report",
        ),
    )
    context = CommandContext(principal=drivers.actor, idempotency_key="delivery")
    intent = DriveTurnTarget(turn_id=turn["id"])
    service = pipeline.runtime.service.orchestration
    prepared = service.prepare_execution(context, intent)
    assert prepared.previous is None and prepared.target_context is not None
    assert pipeline.client.list(RecordQuery(kind="report"))["items"] == []
    assert pipeline.client.list(RecordQuery(kind="drive_receipt"))["items"] == []
    return service, context, intent, prepared


def test_fencing_after_preparation_prevents_the_saved_target(pipeline):
    service, _, intent, prepared = saved_report(pipeline)
    fence(pipeline)
    with pytest.raises(ResearchError) as fenced:
        service.dispatch(
            prepared.target_context, prepared.turn.action.operation, prepared.turn.action.payload
        )
    assert fenced.value.code == ErrorCode.STALE_FENCE
    assert pipeline.client.list(RecordQuery(kind="report"))["items"] == []
    assert pipeline.client.list(RecordQuery(kind="drive_receipt"))["items"] == []
    assert pipeline.client.get("drive_turn", intent.turn_id)["state"] == "prepared"


def test_committed_target_recovers_only_its_missing_delivery_receipt(pipeline, monkeypatch):
    service, context, intent, prepared = saved_report(pipeline)
    report = service.dispatch(
        prepared.target_context, prepared.turn.action.operation, prepared.turn.action.payload
    )
    pending = service.prepare_execution(context, intent)
    assert pending.previous is None and pending.target_context is None
    assert pipeline.client.list(RecordQuery(kind="drive_receipt"))["items"] == []

    def cannot_dispatch(*args, **kwargs):
        raise AssertionError("A committed target must not be delivered again")

    monkeypatch.setattr(service, "dispatch", cannot_dispatch)
    completed = service.execute(context, intent)
    assert completed["result"]["response"] == report
    assert service.prepare_execution(context, intent).previous == completed
    assert service.execute(context, intent) == completed
    assert len(pipeline.client.list(RecordQuery(kind="report"))["items"]) == 1
    assert len(pipeline.client.list(RecordQuery(kind="drive_receipt"))["items"]) == 1
