from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import CampaignTarget, PauseCampaign
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.models import TrialData
from research_swarm.control.orchestration_models import (
    AcknowledgeDriverStart,
    PrepareDriveTurn,
    TransferOrchestration,
)
from research_swarm.control.orchestration_transfer import manifest_chunks
from research_swarm.control.queries import RecordQuery
from research_swarm.control.results import PublishAttempt
from research_swarm.control.workers import AttemptData
from research_swarm.domain.artifacts import StoredBlob
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.event_recovery import CreateRecoverySnapshot, RecoverySnapshot
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.orchestration import DriveAction
from research_swarm.domain.orchestration_transfer import MAX_CHUNK_BYTES
from research_swarm.domain.types import new_id
from research_swarm.recovery import SnapshotReader
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from research_swarm.storage.filesystem import FilesystemBlobStore
from research_swarm.workers.journal import JournalState
from tests.control.test_child_campaigns import OPTIONS, create
from tests.control.test_confirmation import create_plan, options, setup_plan
from tests.control.test_evaluation_driver import role
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_drive_closures import report_action
from tests.orchestration.test_http_delivery import serving
from tests.orchestration.test_orchestration_turns import CONFIG, Drivers, bind, execute, prepare
from tests.orchestration.test_orchestration_waits import record, report


def pause(pipeline: Pipeline) -> dict[str, Any]:
    row = pipeline.client.get("campaign", pipeline.campaign["id"])
    return pipeline.client.command(
        "campaign.pause",
        PauseCampaign(campaign_id=row["id"], mode="drain"),
        idempotency_key=new_id(),
        expected_revision=row["revision"],
    )["result"]


def successor(pipeline: Pipeline) -> Drivers:
    actor, control, token = role(pipeline, "*")
    evaluator_actor, evaluator, _ = role(pipeline, "*")
    return Drivers(control, evaluator, actor, evaluator_actor, token)


def request(pipeline: Pipeline, drivers: Drivers) -> tuple[TransferOrchestration, int]:
    binding = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    return TransferOrchestration(
        transfer_id=new_id(),
        campaign_id=campaign["id"],
        epoch=binding["data"]["epoch"],
        campaign_revision=campaign["revision"],
        adapter="local-replacement",
        configuration_digest=CONFIG,
        routes={"control": drivers.actor.id, "evaluator": drivers.evaluator_actor.id},
        reason="Move the paused research loop to a fresh durable driver",
    ), binding["revision"]


def transfer(
    pipeline: Pipeline, intent: TransferOrchestration, revision: int, *, key: str | None = None
) -> dict[str, Any]:
    return pipeline.client.command(
        "orchestration.transfer",
        intent,
        idempotency_key=key or new_id(),
        expected_revision=revision,
    )["result"]


def acknowledge(pipeline: Pipeline, drivers: Drivers, route: str = "control") -> dict[str, Any]:
    binding = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    start = pipeline.client.get("orchestration_start", binding["data"]["start_ids"][route])
    return (drivers.control if route == "control" else drivers.evaluator).command(
        "orchestration.acknowledge_start",
        AcknowledgeDriverStart(
            start_id=start["id"],
            checkpoint_digest=start["data"]["checkpoint_digest"],
            observed_watermark=pipeline.client.context(pipeline.campaign["id"])["watermark"],
        ),
        idempotency_key=new_id(),
        expected_revision=start["revision"],
    )["result"]


def test_http_successor_recovers_checkpoint_without_changing_native_authority_or_queued_work(
    pipeline: Pipeline,
) -> None:
    original = bind(pipeline)
    pause(pipeline)
    next_driver = successor(pipeline)
    intent, revision = request(pipeline, next_driver)
    budget = pipeline.client.budget(pipeline.campaign["id"])
    trial = pipeline.client.get("trial", pipeline.trial["id"])
    worker_rows = pipeline.client.list(RecordQuery(kind="worker"))
    with pipeline.runtime.database.connect() as connection:
        authority = tuple(
            connection.execute("SELECT controller_id,control_epoch FROM workspaces").fetchone()
        )
    result = transfer(pipeline, intent, revision, key="handoff")
    assert transfer(pipeline, intent, revision, key="handoff") == result
    assert transfer(pipeline, intent, revision) == result
    assert result["data"]["successor"]["epoch"] == 2
    assert pipeline.client.get("campaign", pipeline.campaign["id"])["state"] == "paused"
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    assert pipeline.client.get("trial", trial["id"]) == trial
    assert pipeline.client.list(RecordQuery(kind="worker"))["items"] == worker_rows["items"]
    with pipeline.runtime.database.connect() as connection:
        assert (
            tuple(
                connection.execute("SELECT controller_id,control_epoch FROM workspaces").fetchone()
            )
            == authority
        )
    checkpoint = pipeline.client.get("orchestration_checkpoint", result["data"]["checkpoint_id"])
    assert (
        record_digest("research.transfer-checkpoint-v1", checkpoint["data"])
        == result["data"]["checkpoint_digest"]
    )
    entries = [
        entry
        for identifier in checkpoint["data"]["manifest_chunk_ids"]
        for entry in pipeline.client.get("orchestration_checkpoint_chunk", identifier)["data"][
            "entries"
        ]
    ]
    assert (
        record_digest("research.transfer-view-v1", {"entries": entries})
        == checkpoint["data"]["manifest_digest"]
    )
    binding = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    turn_request = PrepareDriveTurn(
        campaign_id=pipeline.campaign["id"],
        epoch=2,
        ordinal=1,
        configuration_digest=CONFIG,
        planning_watermark=checkpoint["data"]["watermark"],
        action=report_action(pipeline),
    )
    with pytest.raises(ResearchError) as not_started:
        next_driver.control.command(
            "orchestration.prepare_turn",
            turn_request,
            idempotency_key="before-start",
            expected_revision=binding["revision"],
        )
    assert not_started.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, next_driver.token) as remote,
    ):
        snapshot = RecoverySnapshot.model_validate(
            remote.command(
                "stream.create_snapshot",
                CreateRecoverySnapshot(snapshot_id=new_id()),
                idempotency_key="successor-snapshot",
            )["result"]
        )
        reader = SnapshotReader(remote, snapshot)
        assert list(reader.items()) and reader.completed
        assert remote.events(after=snapshot.watermark)["cursor"] >= snapshot.watermark
    acknowledge(pipeline, next_driver)
    acknowledge(pipeline, next_driver, "evaluator")
    prepared = next_driver.control.command(
        "orchestration.prepare_turn",
        turn_request,
        idempotency_key="after-start",
        expected_revision=binding["revision"],
    )["result"]
    assert execute(next_driver.control, prepared)["response"]["disposition"] == "accepted"
    with pytest.raises(ResearchError) as retired:
        original.control.command(
            "report.create", report_action(pipeline).payload, idempotency_key="retired-direct"
        )
    assert retired.value.code == ErrorCode.AUTHORIZATION_DENIED
    row = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "campaign.start",
        CampaignTarget(campaign_id=row["id"]),
        idempotency_key="resume-after-transfer",
        expected_revision=row["revision"],
    )
    training = pipeline.worker.claim(pipeline.ready())
    assert pipeline.worker.execute(training["id"])["state"] == "succeeded"
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1


@pytest.mark.parametrize("outcome", ["absent", "accepted", "rejected"])
def test_transfer_reconciles_original_pending_turn_without_reexecuting_it(
    pipeline: Pipeline, outcome: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = bind(pipeline)
    command = (
        report_action(pipeline)
        if outcome != "rejected"
        else DriveAction(
            operation="campaign.pause",
            payload=PauseCampaign(campaign_id=pipeline.campaign["id"]).model_dump(mode="json"),
            idempotency_key="rejected-pause",
            expected_revision=0,
        )
    )
    turn = prepare(pipeline, original.control, command)
    if outcome != "absent":

        def lost(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("Fixture target committed before controller reply")

        with monkeypatch.context() as patch:
            patch.setattr(pipeline.runtime.service.orchestration, "_finish", lost)
            with pytest.raises(RuntimeError, match="controller reply"):
                execute(original.control, turn)
    pause(pipeline)
    next_driver = successor(pipeline)
    intent, revision = request(pipeline, next_driver)
    result = transfer(pipeline, intent, revision)
    assert result["data"]["pending_turn_outcome_kind"] == (
        "drive_closure" if outcome == "absent" else "drive_receipt"
    )
    binding = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    recovered = execute(original.control, turn)
    if outcome == "absent":
        assert (
            recovered["response"] is None and recovered["turn"]["state"] == "closed_unacknowledged"
        )
    else:
        assert recovered["response"]["disposition"] == outcome
    assert pipeline.client.get("orchestration_binding", pipeline.campaign["id"]) == binding
    assert len(pipeline.client.list(RecordQuery(kind="report"))["items"]) == (
        1 if outcome == "accepted" else 0
    )


def test_competing_transfers_choose_one_epoch_and_preserve_wait_destination(
    pipeline: Pipeline,
) -> None:
    old = bind(pipeline)
    waited = record(pipeline, old)
    pause(pipeline)
    successors = [successor(pipeline), successor(pipeline)]
    intents = [request(pipeline, drivers) for drivers in successors]
    peers = [
        ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root))) for _ in range(2)
    ]
    barrier = Barrier(2)

    def compete(index: int) -> dict[str, Any] | ErrorCode:
        intent, revision = intents[index]
        barrier.wait(timeout=5)
        try:
            return peers[index].command(
                "orchestration.transfer",
                intent,
                idempotency_key=new_id(),
                expected_revision=revision,
            )["result"]
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(compete, range(2)))
    winners = [value for value in outcomes if isinstance(value, dict)]
    assert len(winners) == 1
    assert ErrorCode.REVISION_CONFLICT in outcomes
    assert len(pipeline.client.list(RecordQuery(kind="orchestration_checkpoint"))["items"]) == 1
    assert len(pipeline.client.list(RecordQuery(kind="orchestration_start"))["items"]) == 2
    assert pipeline.client.get("orchestration_wait", waited["wait"]["id"])["state"] == "superseded"
    delivery = pipeline.client.get("wakeup_delivery", waited["wait"]["id"])
    assert delivery["data"]["actor_id"] == old.actor.id and delivery["data"]["epoch"] == 1


def test_corrupt_required_bytes_block_transfer_without_fencing_old_owner(
    pipeline: Pipeline,
) -> None:
    old = bind(pipeline)
    pause(pipeline)
    next_driver = successor(pipeline)
    intent, revision = request(pipeline, next_driver)
    candidate = pipeline.client.get("candidate", pipeline.trial["data"]["candidate_id"])
    artifact = pipeline.runtime.artifacts.get(
        CommandContext(principal=pipeline.runtime.principal(), idempotency_key=new_id()),
        candidate["data"]["source_artifact"]["id"],
    )
    store = pipeline.runtime.artifacts.store
    assert isinstance(store, FilesystemBlobStore)
    path = store._object(
        artifact.workspace_id,
        StoredBlob(reference=artifact.reference, generation=artifact.storage_generation),
    )
    original = path.read_bytes()
    path.write_bytes(b"Fixture corrupted source")
    try:
        with pytest.raises(ResearchError) as corrupt:
            transfer(pipeline, intent, revision)
        assert corrupt.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    finally:
        path.write_bytes(original)
    binding = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    assert binding["state"] == "active" and binding["data"]["routes"]["control"] == old.actor.id
    assert pipeline.client.list(RecordQuery(kind="orchestration_checkpoint"))["items"] == []
    assert transfer(pipeline, intent, revision)["data"]["successor"]["epoch"] == 2


def test_evidence_changed_during_byte_verification_aborts_the_entire_transfer(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    bind(pipeline)
    pause(pipeline)
    next_driver = successor(pipeline)
    intent, revision = request(pipeline, next_driver)
    store = pipeline.runtime.artifacts.store
    verify = store.verify
    changed = False

    def verify_and_write(*args: Any, **kwargs: Any) -> Any:
        nonlocal changed
        value = verify(*args, **kwargs)
        if not changed:
            changed = True
            report(pipeline)
        return value

    with monkeypatch.context() as patch:
        patch.setattr(store, "verify", verify_and_write)
        with pytest.raises(ResearchError) as stale:
            transfer(pipeline, intent, revision)
    assert stale.value.code == ErrorCode.REVISION_CONFLICT
    assert (
        pipeline.client.get("orchestration_binding", pipeline.campaign["id"])["revision"]
        == revision
    )
    assert pipeline.client.list(RecordQuery(kind="orchestration_checkpoint"))["items"] == []
    assert pipeline.client.list(RecordQuery(kind="orchestration_start"))["items"] == []
    assert transfer(pipeline, intent, revision)["data"]["successor"]["epoch"] == 2


@pytest.mark.parametrize("finish_evaluation", [True, False])
def test_retiring_evaluator_blocks_successful_unscored_work_but_not_closed_failed_authorization(
    pipeline: Pipeline, finish_evaluation: bool
) -> None:
    old = bind(pipeline)
    pipeline.worker.execute(pipeline.worker.claim(pipeline.ready())["id"])
    job = pipeline.ready()
    authorized = pipeline.client.command(
        "evaluation.authorize",
        AuthorizeEvaluation(
            job_id=job["id"],
            evaluator_id=old.evaluator_actor.id,
        ),
        idempotency_key="original-evaluator",
        expected_revision=job["revision"],
    )["result"]
    attempt = pipeline.worker.claim(pipeline.ready())
    if not finish_evaluation:
        pipeline.worker._prepare(
            AttemptData.model_validate(attempt["data"]),
            TrialData.model_validate(pipeline.client.get("trial", pipeline.trial["id"])["data"]),
        )
        pipeline.worker.journal.request_cancel(attempt["id"], 1)
    result = pipeline.worker.execute(attempt["id"])
    assert result["state"] == ("succeeded" if finish_evaluation else "not_started")
    assert pause(pipeline)["state"] == "paused"
    next_driver = successor(pipeline)
    intent, revision = request(pipeline, next_driver)
    if finish_evaluation:
        with pytest.raises(ResearchError) as pending:
            transfer(pipeline, intent, revision)
        assert pending.value.code == ErrorCode.EVALUATION_INCOMPLETE
        assert pending.value.details["evaluation_request_id"] == authorized["id"]
        completed = execute(
            old.evaluator,
            prepare(
                pipeline,
                old.evaluator,
                DriveAction(
                    operation="evaluation.complete",
                    payload=CompleteEvaluation(
                        request_id=authorized["id"], attempt_id=attempt["id"]
                    ).model_dump(mode="json"),
                    idempotency_key="finish-retiring-evaluator",
                    expected_revision=authorized["revision"],
                ),
            ),
        )
        assert completed["response"]["disposition"] == "accepted"
        intent, revision = request(pipeline, next_driver)
    transferred = transfer(pipeline, intent, revision)
    assert transferred["data"]["successor"]["epoch"] == 2
    authority = pipeline.client.get("evaluation_request", authorized["id"])
    assert authority["data"]["request"]["evaluator_id"] == old.evaluator_actor.id


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_known_child_funding_and_consumed_counters_survive_transfer(pipeline: Pipeline) -> None:
    bind(pipeline)
    created = create(pipeline)
    allocation = pipeline.client.get("funding_projection", created["campaign"]["id"])
    pause(pipeline)
    budget = pipeline.client.budget(pipeline.campaign["id"])
    intent, revision = request(pipeline, successor(pipeline))
    transferred = transfer(pipeline, intent, revision)
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    assert pipeline.client.get("funding_projection", created["campaign"]["id"]) == allocation
    checkpoint = pipeline.client.get("orchestration_checkpoint", transferred["id"])
    entries = [
        entry
        for identifier in checkpoint["data"]["manifest_chunk_ids"]
        for entry in pipeline.client.get("orchestration_checkpoint_chunk", identifier)["data"][
            "entries"
        ]
    ]
    assert any(
        item.get("kind") == "funding_projection" and item["id"] == allocation["id"]
        for item in entries
    )


def test_successor_command_history_and_wrong_startup_actor_cannot_gain_authority(
    pipeline: Pipeline,
) -> None:
    old = bind(pipeline)
    pause(pipeline)
    used = successor(pipeline)
    used.control.command(
        "stream.create_snapshot",
        CreateRecoverySnapshot(snapshot_id=new_id()),
        idempotency_key="too-early-snapshot",
    )
    intent, revision = request(pipeline, used)
    with pytest.raises(ResearchError) as reused:
        transfer(pipeline, intent, revision)
    assert reused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert (
        pipeline.client.get("orchestration_binding", pipeline.campaign["id"])["revision"]
        == revision
    )
    new = successor(pipeline)
    intent, revision = request(pipeline, new)
    receipt = transfer(pipeline, intent, revision)
    identifier = receipt["data"]["start_ids"]["control"]
    row = pipeline.client.get("orchestration_start", identifier)
    acknowledgement = AcknowledgeDriverStart(
        start_id=identifier,
        checkpoint_digest=row["data"]["checkpoint_digest"],
        observed_watermark=pipeline.client.context(pipeline.campaign["id"])["watermark"],
    )
    with pytest.raises(ResearchError) as wrong:
        old.control.command(
            "orchestration.acknowledge_start",
            acknowledgement,
            idempotency_key="wrong-start-actor",
            expected_revision=1,
        )
    assert wrong.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pytest.raises(ResearchError) as stale:
        new.control.command(
            "orchestration.acknowledge_start",
            acknowledgement.model_copy(update={"observed_watermark": 0}),
            idempotency_key="stale-start",
            expected_revision=1,
        )
    assert stale.value.code == ErrorCode.REVISION_CONFLICT
    assert pipeline.client.get("orchestration_start", identifier)["state"] == "pending"
    assert acknowledge(pipeline, new)["data"]["acknowledged_by"] == new.actor.id


def test_transfer_event_failure_rolls_back_fence_checkpoint_and_wait_wakeup(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    old = bind(pipeline)
    waited = record(pipeline, old)
    pause(pipeline)
    next_driver = successor(pipeline)
    intent, revision = request(pipeline, next_driver)
    before = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    event = Transaction.event

    def fail(self: Transaction, name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "orchestration.transferred":
            raise RuntimeError("Fixture transfer event failure")
        return event(self, name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="transfer event failure"):
            transfer(pipeline, intent, revision, key="atomic-transfer")
    assert pipeline.client.get("orchestration_binding", pipeline.campaign["id"]) == before
    assert pipeline.client.get("orchestration_wait", waited["wait"]["id"])["state"] == "waiting"
    for kind in (
        "orchestration_checkpoint",
        "orchestration_checkpoint_chunk",
        "orchestration_transfer",
        "orchestration_start",
        "wakeup_delivery",
    ):
        assert pipeline.client.list(RecordQuery(kind=kind))["items"] == []
    assert (
        transfer(pipeline, intent, revision, key="atomic-transfer")["data"]["successor"]["epoch"]
        == 2
    )


@pytest.mark.parametrize("unknown", [False, True])
def test_native_or_unknown_custody_blocks_transfer_until_original_no_start_closure(
    pipeline: Pipeline, tmp_path: Path, unknown: bool
) -> None:
    bind(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    prepared = pipeline.worker._prepare(
        AttemptData.model_validate(attempt["data"]),
        TrialData.model_validate(pipeline.trial["data"]),
    )
    if unknown:
        path = tmp_path / "unresolved-native.json"
        path.write_text(
            prepared.model_copy(update={"state": JournalState.UNKNOWN}).model_dump_json(),
            encoding="utf-8",
        )
        reference = pipeline.worker.client.upload_file(
            path, idempotency_key="unknown-native-artifact"
        )
        pipeline.worker.client.command(
            "worker.publish_attempt",
            PublishAttempt(
                attempt_id=attempt["id"],
                journal_artifact=reference,
                finding="Native closure is unresolved",
            ),
            idempotency_key="unknown-native-publication",
        )
    assert pause(pipeline)["state"] == "pausing"
    next_driver = successor(pipeline)
    intent, revision = request(pipeline, next_driver)
    budget = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError) as active:
        transfer(pipeline, intent, revision)
    assert active.value.code == ErrorCode.INVALID_TRANSITION
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    assert pipeline.client.list(RecordQuery(kind="orchestration_transfer"))["items"] == []
    pipeline.worker.journal.request_cancel(attempt["id"], 1)
    assert pipeline.worker.execute(attempt["id"])["state"] == "not_started"
    assert pipeline.client.get("campaign", pipeline.campaign["id"])["state"] == "paused"
    intent, revision = request(pipeline, next_driver)
    assert transfer(pipeline, intent, revision)["data"]["successor"]["epoch"] == 2


def test_manifest_chunks_preserve_large_reservations_with_bounded_payloads() -> None:
    entries = tuple(
        {
            "category": "budget_reservation",
            "id": new_id(),
            "owner_id": new_id(),
            "state": "settled",
            "value": {"dimensions": {"dimension-" + str(i): "0.00001" for i in range(1000)}},
        }
        for _ in range(140)
    )
    chunks = list(manifest_chunks(new_id(), entries))
    assert len(chunks) > 2
    assert [item.ordinal for item in chunks] == list(range(1, len(chunks) + 1))
    assert tuple(entry for chunk in chunks for entry in chunk.entries) == entries
    assert all(
        len(canonical_bytes(chunk.model_dump(mode="json"))) <= MAX_CHUNK_BYTES for chunk in chunks
    )
    assert record_digest("research.transfer-view-v1", {"entries": entries}) == record_digest(
        "research.transfer-view-v1",
        {"entries": [entry for chunk in chunks for entry in chunk.entries]},
    )
    with pytest.raises(ResearchError) as oversized:
        list(manifest_chunks(new_id(), ({"large": "x" * MAX_CHUNK_BYTES},)))
    assert oversized.value.code == ErrorCode.RESOURCE_UNAVAILABLE


@pytest.mark.parametrize("pipeline", [options()], indirect=True)
def test_transfer_preserves_pending_panel_evaluator_identity(pipeline: Pipeline) -> None:
    panel_intent, campaign = setup_plan(pipeline)
    old = bind(pipeline)
    panel = create_plan(
        pipeline, panel_intent.model_copy(update={"evaluator_id": old.evaluator_actor.id}), campaign
    )
    pause(pipeline)
    next_driver = successor(pipeline)
    intent, revision = request(pipeline, next_driver)
    with pytest.raises(ResearchError) as pending:
        transfer(pipeline, intent, revision)
    assert pending.value.code == ErrorCode.EVALUATION_INCOMPLETE
    assert pending.value.details["plan_id"] == panel["id"]
    assert pipeline.client.get("confirmation_plan", panel["id"]) == panel
    assert pipeline.client.get("orchestration_binding", campaign["id"])["data"]["epoch"] == 1
