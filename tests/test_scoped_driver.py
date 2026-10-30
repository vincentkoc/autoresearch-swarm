from __future__ import annotations

import asyncio
import json
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.control.commands import SubmitTrial
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.models import TrialData
from research_swarm.control.queries import RecordQuery
from research_swarm.control.worker_lifecycle import RequestAttemptCancellation
from research_swarm.control.workers import AttemptData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.orchestration.scoped import ScopedWorkerDriver
from research_swarm.workers.journal import JournalState, WorkerJournal
from research_swarm.workers.remote_enrollment import (
    RemoteEnrollment,
    connect_worker,
    describe_worker,
    provision_worker,
)
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline


@contextmanager
def enrollment(
    pipeline: Pipeline, root: Path, url: str, *, cpu_slots: int = 1
) -> Iterator[RemoteEnrollment]:
    description = describe_worker(
        root,
        cpu_slots=cpu_slots,
        machine_registry=root.parent / "remote-machine" / "resources.sqlite",
    )
    bundle = root / "bundle.json"
    provision_worker(
        pipeline.client,
        description,
        url=url,
        project_ids=(pipeline.campaign["data"]["contract"]["project_id"],),
        destination=bundle,
    )
    with connect_worker(root, bundle_path=bundle) as connected:
        yield connected


async def test_scoped_driver_drains_training_and_only_authorized_evaluation(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        result = await ScopedWorkerDriver(connected).run(once=True)
        assert result.state == "idle" and result.observation_count == 1
        assert not result.findings
        job = pipeline.ready()
        authorization = pipeline.client.command(
            "evaluation.authorize",
            AuthorizeEvaluation(job_id=job["id"], evaluator_id=pipeline.runtime.principal().id),
            idempotency_key="authorize-remote-evaluation",
            expected_revision=job["revision"],
        )["result"]
        result = await ScopedWorkerDriver(connected).run(once=True)
        assert result.state == "idle" and result.observation_count == 1
        assert (
            pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"]
            == "pending"
        )
        pipeline.client.command(
            "evaluation.complete",
            CompleteEvaluation(
                request_id=authorization["id"], attempt_id=result.recent_attempts[0]
            ),
            idempotency_key="complete-remote-evaluation",
        )
        assert (
            pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"]
            == "valid"
        )
        replay = await ScopedWorkerDriver(connected).run(once=True)
        assert replay.state == "idle" and replay.observation_count == 0
        assert pipeline.client.get("campaign", pipeline.campaign["id"])["state"] == "running"


@pytest.mark.parametrize("operation", ["worker.claim_job", "worker.publish_attempt"])
async def test_uncertain_acknowledgment_recovers_exact_attempt_without_reexecution(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        original = connected.worker.client.command
        lost = False

        def committed_then_lost(name: str, *args: Any, **kwargs: Any) -> Any:
            nonlocal lost
            reply = original(name, *args, **kwargs)
            if name == operation and not lost:
                lost = True
                raise ResearchError(
                    ErrorCode.DELIVERY_UNCERTAIN, "Synthetic lost acknowledgment", retryable=True
                )
            return reply

        monkeypatch.setattr(connected.worker.client, "command", committed_then_lost)
        interrupted = await ScopedWorkerDriver(connected).run(once=True)
        assert interrupted.state == "disconnected"
        attempts = pipeline.client.list(RecordQuery(kind="attempt"))["items"]
        assert len(attempts) == 1
        before = (
            connected.worker.journal.get(attempts[0]["id"])
            if operation == "worker.publish_attempt"
            else None
        )
        monkeypatch.setattr(connected.worker.client, "command", original)
        with connect_worker(tmp_path / "remote") as reopened:
            result = await ScopedWorkerDriver(reopened).run(once=True)
            assert result.state == "idle"
            if before is not None:
                assert reopened.worker.journal.get(attempts[0]["id"]) == before
            assert reopened.worker.resources.availability() == {"cpu": 1}
        assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1


@pytest.mark.parametrize("pipeline", [{"search": {"max_parallel_trials": 3}}], indirect=True)
async def test_free_slot_is_refilled_without_waiting_for_the_slowest_attempt(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for seed in (8, 9):
        campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
        pipeline.client.command(
            "trial.submit",
            SubmitTrial(
                campaign_id=campaign["id"],
                candidate_id=pipeline.trial["data"]["spec"]["candidate_id"],
                validation_id=pipeline.trial["data"]["validation_id"],
                seed=seed,
            ),
            idempotency_key=f"extra-trial-{seed}",
            expected_revision=campaign["revision"],
        )
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url, cpu_slots=2) as connected,
    ):
        original = connected.worker.execute
        release, first, third = threading.Event(), threading.Event(), threading.Event()
        lock = threading.Lock()
        calls = 0

        def delayed(identifier: str) -> Any:
            nonlocal calls
            with lock:
                calls += 1
                turn = calls
            if turn == 1:
                first.set()
                assert release.wait(10)
            elif turn == 3:
                assert not release.is_set()
                third.set()
            return original(identifier)

        monkeypatch.setattr(connected.worker, "execute", delayed)
        command = connected.worker.client.command
        claims = 0

        def slow_second_claim(name: str, *args: Any, **kwargs: Any) -> Any:
            nonlocal claims
            if name == "worker.claim_job":
                claims += 1
                if claims == 2:
                    assert first.wait(3), "Later claim blocked an already admitted native task"
            return command(name, *args, **kwargs)

        monkeypatch.setattr(connected.worker.client, "command", slow_second_claim)
        task = asyncio.create_task(ScopedWorkerDriver(connected, poll_ms=50).run(once=True))
        try:
            assert await asyncio.to_thread(first.wait, 5)
            assert await asyncio.to_thread(third.wait, 5), (
                "Free slot waited for a generation barrier"
            )
        finally:
            release.set()
        result = await task
        assert result.state == "idle" and result.observation_count == 3
        assert connected.worker.resources.availability() == {"cpu": 2}


async def test_stop_drains_owned_execution_and_keeps_driver_lock_until_completion(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        entered, release = threading.Event(), threading.Event()
        original = connected.worker.execute

        def delayed(identifier: str) -> Any:
            entered.set()
            assert release.wait(10)
            return original(identifier)

        monkeypatch.setattr(connected.worker, "execute", delayed)
        driver = ScopedWorkerDriver(connected, poll_ms=50)
        task = asyncio.create_task(driver.run())
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            driver.stop.set()
            with pytest.raises(ResearchError, match="driver is owned"):
                await ScopedWorkerDriver(connected).run(once=True)
            assert not task.done()
        finally:
            release.set()
        result = await task
        assert result.state == "stopped" and result.observation_count == 1
        assert connected.worker.resources.availability() == {"cpu": 1}


async def test_cancelled_control_call_keeps_ownership_until_its_durable_reply_is_recorded(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        accepted, release = threading.Event(), threading.Event()
        command = connected.worker.client.command

        def held_reply(name: str, *args: Any, **kwargs: Any) -> Any:
            reply = command(name, *args, **kwargs)
            if name == "worker.claim_job":
                accepted.set()
                assert release.wait(5)
            return reply

        monkeypatch.setattr(connected.worker.client, "command", held_reply)
        task = asyncio.create_task(ScopedWorkerDriver(connected).run(once=True))
        try:
            assert await asyncio.to_thread(accepted.wait, 5)
            task.cancel()
            await asyncio.sleep(0)
            with pytest.raises(ResearchError, match="driver is owned"):
                await ScopedWorkerDriver(connected).run(once=True)
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        monkeypatch.setattr(connected.worker.client, "command", command)
        result = await ScopedWorkerDriver(connected).run(once=True)
        assert result.state == "idle" and result.observation_count == 1
        assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1


@pytest.mark.parametrize("pipeline", [{"search": {"max_parallel_trials": 2}}], indirect=True)
async def test_publication_recovery_does_not_wait_for_another_active_attempt(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=pipeline.trial["data"]["spec"]["candidate_id"],
            validation_id=pipeline.trial["data"]["validation_id"],
            seed=8,
        ),
        idempotency_key="long-sibling",
        expected_revision=campaign["revision"],
    )
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url, cpu_slots=2) as connected,
    ):
        command, execute = connected.worker.client.command, connected.worker.execute
        lost, held, recovered, release = (threading.Event() for _ in range(4))
        calls, lock = 0, threading.Lock()

        def lose_first_publication(name: str, *args: Any, **kwargs: Any) -> Any:
            reply = command(name, *args, **kwargs)
            if name == "worker.publish_attempt" and not lost.is_set():
                lost.set()
                raise ResearchError(
                    ErrorCode.DELIVERY_UNCERTAIN, "Synthetic lost publication reply"
                )
            return reply

        def delay_sibling(identifier: str) -> Any:
            nonlocal calls
            with lock:
                calls += 1
                turn = calls
            if turn == 2:
                held.set()
                assert release.wait(10)
            result = execute(identifier)
            if turn == 3:
                assert not release.is_set()
                recovered.set()
            return result

        monkeypatch.setattr(connected.worker.client, "command", lose_first_publication)
        monkeypatch.setattr(connected.worker, "execute", delay_sibling)
        driver = ScopedWorkerDriver(connected, poll_ms=50, maximum_backoff_ms=100)
        task = asyncio.create_task(driver.run())
        try:
            assert await asyncio.to_thread(held.wait, 5)
            assert await asyncio.to_thread(lost.wait, 5)
            assert await asyncio.to_thread(recovered.wait, 5), "Recovery waited for all active work"
        finally:
            release.set()
            driver.stop.set()
        result = await task
        assert result.state == "stopped" and result.observation_count == 2
        assert connected.worker.resources.availability() == {"cpu": 2}


async def test_disconnect_replays_pending_intent_and_does_not_claim_another_job(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        original = connected.worker.client.command
        released = threading.Event()
        admitted = threading.Event()
        keys: list[str] = []

        def unavailable(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "worker.claim_job":
                keys.append(kwargs["idempotency_key"])
                if not admitted.is_set():
                    original(name, *args, **kwargs)
                    admitted.set()
                if not released.is_set():
                    raise ResearchError(
                        ErrorCode.DELIVERY_UNCERTAIN, "Synthetic disconnected worker"
                    )
            return original(name, *args, **kwargs)

        monkeypatch.setattr(connected.worker.client, "command", unavailable)
        driver = ScopedWorkerDriver(connected, poll_ms=50, maximum_backoff_ms=100)
        task = asyncio.create_task(driver.run())
        try:
            assert await asyncio.to_thread(admitted.wait, 5)
            await asyncio.sleep(0.2)
            assert len(keys) >= 2 and len(set(keys)) == 1
            assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1
            released.set()
            until = asyncio.get_running_loop().time() + 5
            while (
                pipeline.client.list(RecordQuery(kind="attempt"))["items"][0]["state"]
                != "succeeded"
            ):
                assert asyncio.get_running_loop().time() < until
                await asyncio.sleep(0.05)
        finally:
            released.set()
            driver.stop.set()
        result = await task
        assert result.state == "stopped" and result.observation_count == 1
        commands = list((driver.root / "commands").glob("*.json"))
        intents = [
            json.loads(path.read_text()) for path in commands if path.name != "identity.json"
        ]
        assert len(intents) == 1 and intents[0]["state"] == "accepted"


@pytest.mark.parametrize("pipeline", [{"search": {"max_parallel_trials": 2}}], indirect=True)
async def test_recovered_unknown_blocks_new_admission_despite_spare_capacity(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=pipeline.trial["data"]["spec"]["candidate_id"],
            validation_id=pipeline.trial["data"]["validation_id"],
            seed=8,
        ),
        idempotency_key="waiting-for-unknown",
        expected_revision=campaign["revision"],
    )
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url, cpu_slots=2) as connected,
    ):
        worker = connected.worker
        row = worker.claim(pipeline.ready())
        entry = worker._prepare(
            AttemptData.model_validate(row["data"]),
            TrialData.model_validate(pipeline.trial["data"]),
        )
        origin = worker._timed_origin(entry)
        identifier = new_id()
        worker.journal.claim_timed_launch(row["id"], identifier, origin)
        worker.journal.unknown(row["id"], identifier, "Synthetic lost native creation observation")
        result = await ScopedWorkerDriver(connected).run(once=True)
        assert result.state == "blocked" and result.observation_count == 1
        assert worker.resources.availability() == {"cpu": 1}
        attempts = pipeline.client.list(RecordQuery(kind="attempt"))["items"]
        assert len(attempts) == 1 and attempts[0]["state"] == "execution_unknown"


@pytest.mark.parametrize("pipeline", [{"search": {"max_parallel_trials": 2}}], indirect=True)
@pytest.mark.parametrize("boundary", ["offers", "claim"])
async def test_unknown_observed_during_http_wait_prevents_a_subsequent_native_launch(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=pipeline.trial["data"]["spec"]["candidate_id"],
            validation_id=pipeline.trial["data"]["validation_id"],
            seed=8,
        ),
        idempotency_key="awaited-unknown",
        expected_revision=campaign["revision"],
    )
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url, cpu_slots=2) as connected,
    ):
        worker = connected.worker
        execute, command, query = worker.execute, worker.client.command, worker.client.worker_list
        control_waiting, published, later_native = (threading.Event() for _ in range(3))
        executions, claims, offers = 0, 0, 0

        def lose_native_observation(identifier: str) -> Any:
            nonlocal executions
            executions += 1
            if executions != 1:
                later_native.set()
                return execute(identifier)
            attempt = AttemptData.model_validate(worker._get("attempt", identifier)["data"])
            trial = TrialData.model_validate(
                worker._get("trial", attempt.dispatch.trial_id)["data"]
            )
            entry = worker._prepare(attempt, trial)
            origin = worker._timed_origin(entry)
            supervisor = new_id()
            worker.journal.claim_timed_launch(identifier, supervisor, origin)
            assert control_waiting.wait(5)
            worker.journal.unknown(
                identifier, supervisor, "Synthetic unknown during remote scheduling"
            )
            result = execute(identifier)
            published.set()
            return result

        def held_claim(name: str, *args: Any, **kwargs: Any) -> Any:
            nonlocal claims
            result = command(name, *args, **kwargs)
            if name == "worker.claim_job":
                claims += 1
                if boundary == "claim" and claims == 2:
                    control_waiting.set()
                    assert published.wait(5)
            return result

        def held_offers(request: Any) -> Any:
            nonlocal offers
            result = query(request)
            if request.kind == "offers" and boundary == "offers":
                offers += 1
                if offers == 1:
                    return {**result, "items": result["items"][:1], "next_cursor": None}
                if offers == 2:
                    control_waiting.set()
                    assert published.wait(5)
            return result

        monkeypatch.setattr(worker, "execute", lose_native_observation)
        monkeypatch.setattr(worker.client, "command", held_claim)
        monkeypatch.setattr(worker.client, "worker_list", held_offers)
        result = await ScopedWorkerDriver(connected, poll_ms=50).run(once=True)
        assert result.state == "blocked" and result.observation_count == 1
        assert not later_native.is_set()
        attempts = pipeline.client.list(RecordQuery(kind="attempt"))["items"]
        assert len(attempts) == (1 if boundary == "offers" else 2)
        assert sum(row["state"] == "execution_unknown" for row in attempts) == 1
        assert worker.resources.availability() == {"cpu": 1}


@contextmanager
def controller_process(pipeline: Pipeline) -> Iterator[str]:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "research_swarm.cli",
            "--state-dir",
            str(pipeline.runtime.root),
            "serve",
            "--port",
            str(port),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        until = time.monotonic() + 10
        while True:
            if process.poll() is not None:
                _, stderr = process.communicate(timeout=1)
                pytest.fail(stderr.decode("utf-8", errors="replace"))
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    break
            except OSError:
                assert time.monotonic() < until, "Owned HTTP controller did not start"
                time.sleep(0.025)
        yield f"http://127.0.0.1:{port}"
    finally:
        if process.poll() is None:
            process.terminate()
        try:
            process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)


@pytest.mark.parametrize("pipeline", [{"source_finish_delay": 5}], indirect=True)
@pytest.mark.parametrize("cancel_offline", [False, True])
def test_independent_worker_process_restart_recovers_the_original_running_native_scope(
    pipeline: Pipeline, tmp_path: Path, cancel_offline: bool
) -> None:
    root = tmp_path / "remote"
    with controller_process(pipeline) as url, enrollment(pipeline, root, url) as connected:
        argv = [
            sys.executable,
            "-m",
            "research_swarm.cli",
            "worker",
            "start",
            "--worker-dir",
            str(root),
            "--once",
            "--poll-ms",
            "50",
        ]
        first = subprocess.Popen(
            argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        try:
            until = time.monotonic() + 10
            journal = WorkerJournal(root / "execution" / "journal.sqlite")
            running = None
            while running is None:
                entries = journal.unresolved()
                running = next(
                    (entry for entry in entries if entry.state == JournalState.RUNNING), None
                )
                assert first.poll() is None and time.monotonic() < until, entries
                time.sleep(0.01)
            assert running.native is not None
            first.kill()  # Only the driver process created by this test.
            first.communicate(timeout=5)
            if cancel_offline:
                attempt = pipeline.client.get("attempt", running.dispatch.attempt_id)
                requested = pipeline.client.command(
                    "attempt.request_cancel",
                    RequestAttemptCancellation(
                        attempt_id=attempt["id"], reason="Offline cancellation fixture"
                    ),
                    idempotency_key="offline-cancel",
                    expected_revision=attempt["revision"],
                )["result"]
                assert requested["disposition"] == "requested"
                assert requested["control"]["data"]["acknowledged_at"] is None
            resumed = subprocess.run(argv, capture_output=True, text=True, timeout=20, check=True)
            result = json.loads(resumed.stdout)
            assert result["state"] == "idle" and result["observation_count"] == 1
            closed = journal.get(running.dispatch.attempt_id)
            assert closed.state == JournalState.CLOSED and closed.native == running.native
            assert closed.outcome is not None and closed.outcome.closure.scope_closed
            if cancel_offline:
                assert closed.outcome.reason == "cancelled"
                assert (
                    pipeline.client.get("attempt_control", closed.dispatch.attempt_id)["state"]
                    == "completed"
                )
            assert connected.worker.resources.availability() == {"cpu": 1}
            assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1
            assert connected.bundle.token not in resumed.stdout + resumed.stderr
        finally:
            if first.poll() is None:
                first.kill()
            first.communicate(timeout=5)
