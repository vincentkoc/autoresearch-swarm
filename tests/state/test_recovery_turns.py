"""Retire original saved actions during recovery without executing them again."""

from dataclasses import dataclass

import pytest

from research_swarm.checkpoints.admission import admit_recovery
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import PauseCampaign
from research_swarm.control.orchestration_models import CloseDriveTurn, DriveTurnTarget
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DriveAction
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_drive_closures import report_action
from tests.orchestration.test_orchestration_turns import bind, execute, prepare
from tests.state.test_recovery_admission import copy_pipeline


@dataclass
class Saved:
    runtime: LocalRuntime
    client: ResearchClient
    driver: ResearchClient
    turn: dict
    campaign_id: str


@pytest.fixture
def saved(pipeline, tmp_path, monkeypatch, request):
    outcome = getattr(request, "param", "absent")
    drivers = bind(pipeline)
    action = (
        report_action(pipeline)
        if outcome != "rejected"
        else DriveAction(
            operation="campaign.pause",
            payload=PauseCampaign(campaign_id=pipeline.campaign["id"]).model_dump(mode="json"),
            idempotency_key="rejected-pause",
            expected_revision=0,
        )
    )
    turn = prepare(pipeline, drivers.control, action)
    if outcome != "absent":

        def lost(*args, **kwargs):
            raise RuntimeError("Fixture target reply lost before receipt")

        with monkeypatch.context() as patch:
            patch.setattr(pipeline.runtime.service.orchestration, "_finish", lost)
            with pytest.raises(RuntimeError, match="before receipt"):
                execute(drivers.control, turn)
    runtime, context, backup, claim = copy_pipeline(pipeline, tmp_path)
    admit_recovery(runtime, context, backup, claim)
    return Saved(
        runtime,
        ResearchClient(LocalTransport(runtime)),
        ResearchClient(LocalTransport(runtime, lambda: runtime.credentials.resolve(drivers.token))),
        turn,
        pipeline.campaign["id"],
    )


def close(saved, key="close", client=None):
    return (client or saved.client).command(
        "orchestration.close_turn",
        CloseDriveTurn(
            turn_id=saved.turn["id"],
            epoch=saved.turn["data"]["epoch"],
            finding="Retire original fenced turn",
        ),
        idempotency_key=key,
        expected_revision=saved.turn["revision"],
    )


def reconcile(saved, key="reconcile", client=None):
    return (client or saved.client).command(
        "orchestration.reconcile_turn",
        DriveTurnTarget(turn_id=saved.turn["id"]),
        idempotency_key=key,
    )


def forbid_dispatch(saved, monkeypatch):
    def fail(*args, **kwargs):
        pytest.fail("Recovery executed a saved action")

    monkeypatch.setattr(saved.runtime.service.orchestration, "dispatch", fail)


def test_unacknowledged_turn_closes_without_inventing_target_ack(saved, monkeypatch):
    forbid_dispatch(saved, monkeypatch)
    budget = saved.client.budget(saved.campaign_id)
    with pytest.raises(ResearchError, match="unresolved"):
        reconcile(saved)
    result = close(saved)
    assert result["result"]["turn"]["state"] == "closed_unacknowledged"
    assert result["result"]["receipt"] is result["result"]["response"] is None
    assert close(saved) == result
    assert close(saved, key="another-close")["result"] == result["result"]
    assert reconcile(saved)["result"] == result["result"]
    reopened = ResearchClient(LocalTransport(LocalRuntime.open(saved.runtime.root)))
    assert close(saved, client=reopened) == result
    binding = saved.client.get("orchestration_binding", saved.campaign_id)
    assert binding["state"] == "fenced" and binding["data"]["pending_turn_id"] is None
    assert saved.client.budget(saved.campaign_id) == budget
    with saved.runtime.database.connect() as sql:
        assert (
            sql.execute(
                "SELECT COUNT(*) FROM commands WHERE actor_id=? AND idempotency_key=?",
                (saved.turn["data"]["actor_id"], saved.turn["data"]["action"]["idempotency_key"]),
            ).one()[0]
            == 0
        )


@pytest.mark.parametrize("saved", ["accepted", "rejected"], indirect=True)
def test_only_original_acknowledgment_determines_reconciliation(saved, monkeypatch):
    forbid_dispatch(saved, monkeypatch)
    with saved.runtime.database.connect() as sql:
        before = sql.execute(
            "SELECT response FROM commands WHERE actor_id=? AND idempotency_key=?",
            (saved.turn["data"]["actor_id"], saved.turn["data"]["action"]["idempotency_key"]),
        ).one()[0]
    with pytest.raises(ResearchError, match="acknowledgment"):
        close(saved)
    result = reconcile(saved)
    assert result["result"]["turn"]["state"] == "settled"
    from research_swarm.domain.canonical import strict_json_loads

    assert result["result"]["response"] == strict_json_loads(before)
    assert (
        result["result"]["receipt"]["data"]["disposition"]
        == strict_json_loads(before)["disposition"]
    )
    assert reconcile(saved) == result
    assert reconcile(saved, "second-reconcile")["result"] == result["result"]
    assert (
        saved.client.get("workspace_gate", saved.runtime.identity.workspace_id)["state"]
        == "recovering"
    )
    assert (
        saved.client.get("orchestration_binding", saved.campaign_id)["data"]["pending_turn_id"]
        is None
    )


def test_original_driver_cannot_close_its_turn_or_resume_execution(saved):
    with pytest.raises(ResearchError):
        close(saved, client=saved.driver)
    with pytest.raises(ResearchError):
        execute(saved.driver, saved.turn)
    assert saved.client.get("drive_turn", saved.turn["id"])["state"] == "prepared"


@pytest.mark.parametrize("changed", ["binding", "turn", "acknowledgment"])
def test_maintenance_rechecks_scope_and_target_ack_inside_write(saved, monkeypatch, changed):
    original = saved.runtime.database.execute

    def execute(ctx, operation, *args, **kwargs):
        if operation == "orchestration.close_turn":
            with saved.runtime.database.connect() as sql:
                if changed == "binding":
                    sql.execute(
                        "UPDATE records SET revision=revision+1 WHERE kind='orchestration_binding'"
                    )
                elif changed == "turn":
                    sql.execute("UPDATE records SET revision=revision+1 WHERE kind='drive_turn'")
                else:
                    from research_swarm.domain.canonical import canonical_bytes
                    from research_swarm.domain.orchestration import DriveTurn

                    turn = DriveTurn.model_validate(saved.turn["data"])
                    sql.execute(
                        "INSERT INTO commands VALUES (?,?,?,?,?,?,?)",
                        (
                            saved.runtime.identity.workspace_id,
                            turn.actor_id,
                            turn.action.idempotency_key,
                            turn.action.request_digest,
                            new_id(),
                            canonical_bytes(
                                {"command_id": new_id(), "disposition": "accepted", "result": {}}
                            ).decode(),
                            "2026-01-01T00:00:00.000Z",
                        ),
                    )
        return original(ctx, operation, *args, **kwargs)

    monkeypatch.setattr(saved.runtime.database, "execute", execute)
    with pytest.raises(ResearchError) as refused:
        close(saved)
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert saved.client.get("drive_turn", saved.turn["id"])["state"] == "prepared"
    assert (
        saved.client.get("orchestration_binding", saved.campaign_id)["data"]["pending_turn_id"]
        == saved.turn["id"]
    )


def test_turn_close_rolls_back_receipt_and_binding_on_storage_failure(saved, monkeypatch):
    from research_swarm.state.database import Transaction

    original = Transaction.event

    def fail(self, name, *args, **kwargs):
        if name == "orchestration.turn_closed_unacknowledged":
            raise OSError("Fixture interrupted closure event")
        return original(self, name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "event", fail)
        with pytest.raises(OSError, match="closure event"):
            close(saved)
    assert saved.client.get("drive_turn", saved.turn["id"])["state"] == "prepared"
    assert close(saved)["result"]["turn"]["state"] == "closed_unacknowledged"


@pytest.mark.parametrize("saved", ["accepted"], indirect=True)
def test_concurrent_acknowledgment_survives_later_preflight_refusal(saved, monkeypatch):
    from research_swarm.control import recovery_turns

    original = recovery_turns.recovery_turn_context
    replies = []
    competing = ResearchClient(LocalTransport(LocalRuntime.open(saved.runtime.root)))
    entered = False

    def committed_elsewhere(*args, **kwargs):
        nonlocal entered
        if entered:
            return original(*args, **kwargs)
        entered = True
        replies.append(reconcile(saved, client=competing))
        raise ResearchError(ErrorCode.STALE_FENCE, "Fixture authority changed after other commit")

    monkeypatch.setattr(recovery_turns, "recovery_turn_context", committed_elsewhere)
    assert reconcile(saved) == replies[0]
    assert saved.client.get("drive_turn", saved.turn["id"])["state"] == "settled"
