from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.control.worker_lifecycle import SetWorkerAdmission
from research_swarm.control.worker_queries import WorkerRead
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.orchestration.scoped import ScopedWorkerDriver
from research_swarm.orchestration.worker_controls import HeartbeatState, WorkerControlSession
from research_swarm.runtime import read_private
from research_swarm.workers.journal import JournalState
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_scoped_driver import enrollment
from tests.test_worker_lifecycle import cancel


def presence(connected: Any) -> dict[str, Any]:
    binding = connected.worker.worker.binding
    return connected.worker.client.worker_get(
        WorkerRead(
            worker_id=binding.worker_id,
            session_id=binding.worker_session_id,
            kind="worker_presence",
            id=binding.worker_id,
        )
    )


def session(connected: Any) -> WorkerControlSession:
    driver = ScopedWorkerDriver(connected)
    return WorkerControlSession(
        connected.worker.client,
        connected.worker.worker.binding,
        connected.worker.journal,
        driver.root,
    )


async def test_lost_heartbeat_reply_replays_before_admission_and_sequence_survives_restart(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        original = connected.worker.client.command
        delivered: list[tuple[str, dict[str, Any]]] = []
        lost = False

        def lose_reply(operation: str, payload: Any, **kwargs: Any) -> Any:
            nonlocal lost
            result = original(operation, payload, **kwargs)
            if operation == "worker.heartbeat":
                delivered.append((kwargs["idempotency_key"], payload.model_dump(mode="json")))
                if not lost:
                    lost = True
                    raise ResearchError(
                        ErrorCode.DELIVERY_UNCERTAIN, "Synthetic heartbeat reply loss"
                    )
            return result

        monkeypatch.setattr(connected.worker.client, "command", lose_reply)
        interrupted = await ScopedWorkerDriver(connected).run(once=True)
        assert interrupted.state == "disconnected"
        assert not connected.worker.journal.unresolved()
        assert presence(connected)["data"]["heartbeat_sequence"] == 1
        resumed = await ScopedWorkerDriver(connected).run(once=True)
        assert resumed.state == "idle" and resumed.observation_count == 1
        assert delivered[0] == delivered[1]
        with connected.worker.journal._connect() as connection:
            assert connection.execute("SELECT COUNT(*) FROM attempts").fetchone()[0] == 1
        state = HeartbeatState.model_validate_json(
            read_private(connected.worker.root / "driver", "heartbeat.json", limit=128 * 1024)
        )
        assert state.pending is None and state.sequence >= 1
        assert list((connected.worker.root / "driver").glob("heartbeat*")) == [
            connected.worker.root / "driver" / "heartbeat.json"
        ]


def test_ack_committed_locally_before_state_advance_recovers_exact_request(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        attempt = connected.worker.claim(pipeline.ready())
        cancel(pipeline, attempt)
        controls = session(connected)
        original = controls._save
        failed = False

        def interrupted(state: HeartbeatState) -> None:
            nonlocal failed
            if state.sequence == 2 and state.pending is None and not failed:
                failed = True
                raise OSError("Synthetic durable heartbeat state failure")
            original(state)

        monkeypatch.setattr(controls, "_save", interrupted)
        with pytest.raises(ResearchError) as failure:
            controls.synchronize()
        assert failure.value.code == ErrorCode.RESOURCE_UNAVAILABLE
        binding = connected.worker.worker.binding
        assert connected.worker.journal.pending_control_acks(binding) == ()
        stored = HeartbeatState.model_validate_json(
            read_private(controls.path.parent, controls.path.name, limit=128 * 1024)
        )
        assert stored.sequence == 1 and stored.pending is not None
        assert session(connected).synchronize() == "active"
        recovered = HeartbeatState.model_validate_json(
            read_private(controls.path.parent, controls.path.name, limit=128 * 1024)
        )
        assert recovered.sequence == 2 and recovered.pending is None
        assert connected.worker.execute(attempt["id"])["state"] == "not_started"


def test_missing_or_divergent_heartbeat_state_never_resets_observed_sequence(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        controls = session(connected)
        controls.synchronize()
        sequence = presence(connected)["data"]["heartbeat_sequence"]
        controls.path.unlink()
        with pytest.raises(ResearchError, match="missing from observed worker"):
            session(connected).synchronize()
        assert presence(connected)["data"]["heartbeat_sequence"] == sequence


async def test_cancel_while_claim_reply_is_held_is_applied_before_native_receipt(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        original = connected.worker.client.command
        accepted, release = threading.Event(), threading.Event()
        attempt: dict[str, Any] = {}

        def delayed(operation: str, *args: Any, **kwargs: Any) -> Any:
            result = original(operation, *args, **kwargs)
            if operation == "worker.claim_job":
                attempt.update(result["result"])
                accepted.set()
                assert release.wait(8)
            return result

        monkeypatch.setattr(connected.worker.client, "command", delayed)
        driver = ScopedWorkerDriver(connected, poll_ms=50)
        task = asyncio.create_task(driver.run(once=True))
        try:
            assert await asyncio.to_thread(accepted.wait, 5)
            cancel(pipeline, attempt)
            until = asyncio.get_running_loop().time() + 5
            while asyncio.get_running_loop().time() < until:
                if pipeline.client.get("attempt_control", attempt["id"])["state"] == "acknowledged":
                    break
                await asyncio.sleep(0.02)
            assert pipeline.client.get("attempt_control", attempt["id"])["state"] == "acknowledged"
            with pytest.raises(ResearchError) as absent:
                connected.worker.journal.get(attempt["id"])
            assert absent.value.code == ErrorCode.NOT_FOUND
        finally:
            release.set()
        result = await asyncio.wait_for(task, 10)
        assert result.state == "idle" and result.observation_count == 1
        journal = connected.worker.journal.get(attempt["id"])
        assert journal.state == JournalState.NOT_STARTED and journal.native is None
        assert pipeline.client.get("attempt_control", attempt["id"])["state"] == "completed"


@pytest.mark.parametrize("stop_before_cancel", [False, True])
@pytest.mark.parametrize("pipeline", [{"source_finish_delay": 2}], indirect=True)
async def test_controls_continue_during_native_execution_and_shutdown(
    pipeline: Pipeline,
    tmp_path: Path,
    stop_before_cancel: bool,
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        driver = ScopedWorkerDriver(connected, poll_ms=50, heartbeat_ms=100)
        task = asyncio.create_task(driver.run(once=not stop_before_cancel))
        entry = None
        until = asyncio.get_running_loop().time() + 8
        while asyncio.get_running_loop().time() < until:
            entries = connected.worker.journal.unresolved()
            if entries and entries[0].state == JournalState.RUNNING:
                entry = entries[0]
                break
            await asyncio.sleep(0.02)
        assert entry is not None
        if stop_before_cancel:
            driver.stop.set()
        row = pipeline.client.get("attempt", entry.dispatch.attempt_id)
        cancel(pipeline, row)
        result = await asyncio.wait_for(task, 10)
        assert result.state == ("stopped" if stop_before_cancel else "idle")
        closed = connected.worker.journal.get(row["id"])
        assert closed.native == entry.native and closed.outcome is not None
        assert closed.outcome.reason == "cancelled" and closed.outcome.closure.scope_closed
        assert connected.worker.resources.availability() == {"cpu": 1}
        assert (
            pipeline.client.get("attempt_control", row["id"])["data"]["attempt_outcome"]
            == "cancelled"
        )


async def test_drained_worker_stays_observable_and_resumes_without_new_identity(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        binding = connected.worker.worker.binding

        def set_admission(state: str, revision: int) -> dict[str, Any]:
            return pipeline.client.command(
                "worker.set_admission",
                SetWorkerAdmission(binding=binding, admission=state, reason="Fixture drain"),
                expected_revision=revision,
                idempotency_key=new_id(),
            )["result"]

        drained = set_admission("draining", 0)
        result = await ScopedWorkerDriver(connected).run(once=True)
        assert result.state == "idle" and result.observation_count == 0
        assert presence(connected)["data"]["heartbeat_sequence"] == 1
        set_admission("active", drained["revision"])
        resumed = await ScopedWorkerDriver(connected).run(once=True)
        assert resumed.state == "idle" and resumed.observation_count == 1
        assert connected.worker.worker.binding == binding
        assert presence(connected)["data"]["heartbeat_sequence"] == 2


async def test_cancelled_heartbeat_keeps_driver_and_single_writer_ownership_until_reply(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        original = connected.worker.client.command
        entered, release = threading.Event(), threading.Event()
        calls = 0

        def held(operation: str, *args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            result = original(operation, *args, **kwargs)
            if operation == "worker.heartbeat":
                calls += 1
                entered.set()
                assert release.wait(8)
            return result

        monkeypatch.setattr(connected.worker.client, "command", held)
        driver = ScopedWorkerDriver(connected, poll_ms=50)
        task = asyncio.create_task(driver.run(once=True))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(ResearchError, match="driver is owned"):
                await ScopedWorkerDriver(connected).run(once=True)
            assert not task.done() and calls == 1
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        monkeypatch.setattr(connected.worker.client, "command", original)
        assert (await ScopedWorkerDriver(connected).run(once=True)).observation_count == 1


async def test_control_page_exhaustion_blocks_launch_but_preserves_acknowledged_progress(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        attempt = connected.worker.claim(pipeline.ready())
        cancel(pipeline, attempt)
        original = connected.worker.client.worker_list
        calls = 0
        snapshot = None

        def endless(request: Any) -> Any:
            nonlocal calls, snapshot
            if request.kind != "controls":
                return original(request)
            calls += 1
            if snapshot is None:
                snapshot = original(request)
            return {**snapshot, "next_cursor": "repeating-provider-cursor"}

        monkeypatch.setattr(connected.worker.client, "worker_list", endless)
        result = await ScopedWorkerDriver(connected).run(once=True)
        assert result.state == "blocked" and result.observation_count == 0 and calls == 128
        assert pipeline.client.get("attempt_control", attempt["id"])["state"] == "acknowledged"
        assert connected.worker.journal.pending_control_acks(connected.worker.worker.binding) == ()
        with pytest.raises(ResearchError) as absent:
            connected.worker.journal.get(attempt["id"])
        assert absent.value.code == ErrorCode.NOT_FOUND
        monkeypatch.setattr(connected.worker.client, "worker_list", original)
        resumed = await ScopedWorkerDriver(connected).run(once=True)
        assert resumed.state == "idle" and resumed.observation_count == 1
        assert connected.worker.journal.get(attempt["id"]).state == JournalState.NOT_STARTED


async def test_control_failure_during_claim_blocks_native_launch_until_resynchronization(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        command, query = connected.worker.client.command, connected.worker.client.worker_list
        accepted, failed, release = threading.Event(), threading.Event(), threading.Event()
        attempt: dict[str, Any] = {}

        def held_claim(operation: str, *args: Any, **kwargs: Any) -> Any:
            result = command(operation, *args, **kwargs)
            if operation == "worker.claim_job":
                attempt.update(result["result"])
                accepted.set()
                assert release.wait(8)
            return result

        def unavailable(request: Any) -> Any:
            if request.kind == "controls" and accepted.is_set():
                failed.set()
                raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic control outage")
            return query(request)

        monkeypatch.setattr(connected.worker.client, "command", held_claim)
        monkeypatch.setattr(connected.worker.client, "worker_list", unavailable)
        task = asyncio.create_task(ScopedWorkerDriver(connected, poll_ms=50).run(once=True))
        try:
            assert await asyncio.to_thread(accepted.wait, 5)
            assert await asyncio.to_thread(failed.wait, 5)
        finally:
            release.set()
        result = await asyncio.wait_for(task, 10)
        assert result.state == "disconnected" and result.observation_count == 0
        with pytest.raises(ResearchError) as absent:
            connected.worker.journal.get(attempt["id"])
        assert absent.value.code == ErrorCode.NOT_FOUND
        monkeypatch.setattr(connected.worker.client, "command", command)
        monkeypatch.setattr(connected.worker.client, "worker_list", query)
        resumed = await ScopedWorkerDriver(connected).run(once=True)
        assert resumed.state == "idle" and resumed.observation_count == 1
        assert connected.worker.journal.get(attempt["id"]).state == JournalState.CLOSED


async def test_fatal_pump_failure_remains_sticky_after_a_held_claim_reply(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        command, query = connected.worker.client.command, connected.worker.client.worker_list
        accepted, failed, release = threading.Event(), threading.Event(), threading.Event()
        attempt: dict[str, Any] = {}

        def held_claim(operation: str, *args: Any, **kwargs: Any) -> Any:
            result = command(operation, *args, **kwargs)
            if operation == "worker.claim_job":
                attempt.update(result["result"])
                accepted.set()
                assert release.wait(8)
            return result

        def fail_once(request: Any) -> Any:
            if request.kind == "controls" and accepted.is_set() and not failed.is_set():
                failed.set()
                raise ResearchError(ErrorCode.STALE_FENCE, "Synthetic fatal control fence")
            return query(request)

        monkeypatch.setattr(connected.worker.client, "command", held_claim)
        monkeypatch.setattr(connected.worker.client, "worker_list", fail_once)
        driver = ScopedWorkerDriver(connected, poll_ms=50)
        task = asyncio.create_task(driver.run(once=True))
        try:
            assert await asyncio.to_thread(accepted.wait, 5)
            assert await asyncio.to_thread(failed.wait, 5)
            until = asyncio.get_running_loop().time() + 5
            while driver._pump_failure is None and asyncio.get_running_loop().time() < until:
                await asyncio.sleep(0.01)
            assert driver._pump_failure is not None
            # Subsequent reads now succeed, but the independent pump has ended.
            monkeypatch.setattr(connected.worker.client, "worker_list", query)
        finally:
            release.set()
        with pytest.raises(ResearchError, match="fatal control fence"):
            await asyncio.wait_for(task, 10)
        with pytest.raises(ResearchError) as absent:
            connected.worker.journal.get(attempt["id"])
        assert absent.value.code == ErrorCode.NOT_FOUND
        assert (
            pipeline.client.get("attempt", attempt["id"])["data"]["publication_status"] == "pending"
        )
        monkeypatch.setattr(connected.worker.client, "command", command)
        assert (await ScopedWorkerDriver(connected).run(once=True)).observation_count == 1
