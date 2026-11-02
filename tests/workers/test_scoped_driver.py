from __future__ import annotations

import asyncio
import json
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import psutil
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
from tests.cli_process import (
    OwnedCliProcess,
    PipeTail,
    acknowledge_driver,
    require_ancestry,
    require_driver,
)
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_http_delivery import serving
from tests.paths import REPO_ROOT


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


async def driver_gate(
    event: asyncio.Event, task: asyncio.Task[Any], *, timeout: float, message: str
) -> None:
    waiter = asyncio.create_task(event.wait())
    try:
        done, _ = await asyncio.wait(
            (waiter, task), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if task in done:
            task.result()
        assert event.is_set(), message
    finally:
        waiter.cancel()
        with suppress(asyncio.CancelledError):
            await waiter


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
        loop = asyncio.get_running_loop()
        release, first = threading.Event(), threading.Event()
        second_completed, third = asyncio.Event(), asyncio.Event()
        lock = threading.Lock()
        calls = 0

        def delayed(identifier: str) -> Any:
            nonlocal calls
            with lock:
                calls += 1
                turn = calls
            if turn == 1:
                first.set()
                assert release.wait(40), "Held attempt was not released during cleanup"
            elif turn == 3:
                assert not release.is_set()
                loop.call_soon_threadsafe(third.set)
            result = original(identifier)
            if turn == 2:
                loop.call_soon_threadsafe(second_completed.set)
            return result

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
        driver = ScopedWorkerDriver(connected, poll_ms=50)
        task = asyncio.create_task(driver.run(once=True))
        try:
            assert await asyncio.to_thread(first.wait, 5)
            await driver_gate(
                second_completed,
                task,
                timeout=30,
                message="Second attempt execution and publication did not complete",
            )
            await driver_gate(
                third, task, timeout=5, message="Free slot waited for a generation barrier"
            )
        except BaseException:
            driver.stop.set()
            raise
        finally:
            release.set()
            result = await asyncio.wait_for(task, timeout=30)
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
        loop = asyncio.get_running_loop()
        lost, recovered = asyncio.Event(), asyncio.Event()
        held, release = threading.Event(), threading.Event()
        calls: dict[str, int] = {}
        publications: dict[str, Any] = {}
        journals: dict[str, Any] = {}
        lock = threading.Lock()

        def lose_first_publication(name: str, *args: Any, **kwargs: Any) -> Any:
            if name != "worker.publish_attempt":
                return command(name, *args, **kwargs)
            with lock:
                first = next(iter(calls))
            if args[0].attempt_id == first:
                assert held.wait(30), "Distinct sibling did not enter active execution"
            reply = command(name, *args, **kwargs)
            if reply["result"]["id"] == first:
                assert first not in publications, "Recovery published the same native result twice"
                publications[first] = reply["result"]
                journals[first] = connected.worker.journal.get(first)
                loop.call_soon_threadsafe(lost.set)
                raise ResearchError(
                    ErrorCode.DELIVERY_UNCERTAIN, "Synthetic lost publication reply"
                )
            return reply

        def delay_sibling(identifier: str) -> Any:
            with lock:
                calls[identifier] = calls.get(identifier, 0) + 1
                first, turn = next(iter(calls)), calls[identifier]
            if identifier != first:
                assert turn == 1
                held.set()
                assert release.wait(40), "Owned sibling was not released during cleanup"
            result = execute(identifier)
            if identifier == first and turn >= 2:
                assert not release.is_set()
                assert result == publications[identifier]
                assert connected.worker.journal.get(identifier) == journals[identifier]
                loop.call_soon_threadsafe(recovered.set)
            return result

        monkeypatch.setattr(connected.worker.client, "command", lose_first_publication)
        monkeypatch.setattr(connected.worker, "execute", delay_sibling)
        driver = ScopedWorkerDriver(connected, poll_ms=50, maximum_backoff_ms=100)
        task = asyncio.create_task(driver.run())
        try:
            await driver_gate(
                lost, task, timeout=30, message="Native publication did not reach the lost reply"
            )
            assert held.is_set()
            await driver_gate(
                recovered, task, timeout=5, message="Recovery waited for all active work"
            )
        finally:
            release.set()
            driver.stop.set()
            result = await asyncio.wait_for(task, timeout=30)
        assert result.state == "stopped" and result.observation_count == 2
        first, sibling = calls
        assert calls[first] >= 2 and calls[sibling] == 1
        assert len(publications) == 1
        assert connected.worker.resources.availability() == {"cpu": 2}


async def test_disconnect_replays_pending_intent_and_does_not_claim_another_job(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        original = connected.worker.client.command
        loop = asyncio.get_running_loop()
        released = threading.Event()
        admitted, replayed, published = (asyncio.Event() for _ in range(3))
        keys: list[str] = []

        def unavailable(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "worker.claim_job":
                keys.append(kwargs["idempotency_key"])
                if len(keys) == 1:
                    original(name, *args, **kwargs)
                    loop.call_soon_threadsafe(admitted.set)
                if not released.is_set():
                    if len(keys) >= 2:
                        loop.call_soon_threadsafe(replayed.set)
                    raise ResearchError(
                        ErrorCode.DELIVERY_UNCERTAIN, "Synthetic disconnected worker"
                    )
            reply = original(name, *args, **kwargs)
            if name == "worker.publish_attempt":
                loop.call_soon_threadsafe(published.set)
            return reply

        monkeypatch.setattr(connected.worker.client, "command", unavailable)
        driver = ScopedWorkerDriver(connected, poll_ms=50, maximum_backoff_ms=100)
        task = asyncio.create_task(driver.run())
        try:
            await driver_gate(admitted, task, timeout=5, message="Claim was not admitted")
            await driver_gate(replayed, task, timeout=5, message="Pending claim was not replayed")
            assert len(keys) >= 2 and len(set(keys)) == 1
            assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1
            pending = [
                json.loads(path.read_text())
                for path in (driver.root / "commands").glob("*.json")
                if path.name != "identity.json"
            ]
            assert len(pending) == 1 and pending[0]["state"] == "pending"
            assert pending[0]["key"] == keys[0]
            released.set()
            await driver_gate(
                published, task, timeout=30, message="Admitted native execution did not publish"
            )
            assert (
                pipeline.client.list(RecordQuery(kind="attempt"))["items"][0]["state"]
                == "succeeded"
            )
        finally:
            released.set()
            driver.stop.set()
            result = await asyncio.wait_for(task, timeout=30)
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
def controller_process(
    pipeline: Pipeline,
    *,
    port: int | None = None,
    processes: list[OwnedCliProcess] | None = None,
) -> Iterator[str]:
    if port is None:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
    nonce = new_id()
    witness = pipeline.runtime.root.parent / f"controller-{nonce}.json"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tests.cli_process",
            str(witness),
            nonce,
            "",
            "serve",
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
    assert process.stderr is not None
    stderr_tail = PipeTail(process.stderr)
    process.stderr = None  # The drain owns this pipe; communicate must not read it too.
    launcher = psutil.Process(process.pid)
    launcher.create_time()
    owned = None
    try:
        until = time.monotonic() + 10
        while True:
            if process.poll() is not None:
                process.communicate(timeout=1)
                stderr_tail.close()
                details = (
                    stderr_tail.tail()
                    .decode("utf-8", errors="replace")
                    .replace(str(Path.home()), "<home>")
                )
                pytest.fail("Owned HTTP controller exited before readiness: " + details[-4096:])
            if owned is None and witness.exists():
                owned = OwnedCliProcess(process, acknowledge_driver(launcher, witness, nonce))
            listening = witness.with_suffix(".listening")
            if owned is not None and listening.exists():
                assert json.loads(listening.read_text(encoding="utf-8")) == {
                    "nonce": nonce,
                    "pid": owned.process.pid,
                    "created_at_hex": owned.process.create_time().hex(),
                    "host": "127.0.0.1",
                    "port": port,
                }
                require_ancestry(owned.process, launcher)
                break
            assert time.monotonic() < until, "Owned HTTP controller did not start"
            time.sleep(0.025)
        if processes is not None:
            processes.append(owned)
        yield f"http://127.0.0.1:{port}"
    except BaseException as error:
        details = stderr_tail.tail().decode("utf-8", errors="replace")
        for path in (str(Path.home()), str(REPO_ROOT)):
            details = details.replace(path, "<redacted>")
        if details:
            error.add_note("Owned controller stderr: " + details[-8192:])
        try:
            with witness.with_suffix(".trace").open("rb") as stream:
                stream.seek(0, 2)
                stream.seek(max(0, stream.tell() - 8192))
                trace = stream.read(8192).decode("utf-8", errors="replace")
            for path in (str(Path.home()), str(REPO_ROOT)):
                trace = trace.replace(path, "<redacted>")
            if trace:
                error.add_note("Owned controller stack: " + trace)
        except Exception:
            pass
        raise
    finally:
        try:
            if owned is None and witness.exists() and launcher.is_running():
                with suppress(psutil.NoSuchProcess):
                    owned = OwnedCliProcess(process, require_driver(launcher, witness, nonce))
            if owned is not None:
                owned.close()
            else:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=5)
        finally:
            stderr_tail.close()


def test_controller_readiness_rejects_an_existing_unrelated_listener(pipeline: Pipeline) -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        address = listener.getsockname()
        with (
            pytest.raises(
                pytest.fail.Exception, match="Owned HTTP controller exited before readiness"
            ),
            controller_process(pipeline, port=address[1]),
        ):
            pytest.fail("An unrelated listener was mistaken for the new controller")
        assert listener.getsockname() == address


@pytest.mark.parametrize(
    # Cover the cumulative restart choreography; individual watchdogs stay short.
    "pipeline",
    [{"source_finish_gate": True, "source_train_timeout_ms": 120000}],
    indirect=True,
)
@pytest.mark.parametrize("cancel_offline", [False, True])
def test_independent_worker_process_restart_recovers_the_original_running_native_scope(
    pipeline: Pipeline, tmp_path: Path, cancel_offline: bool
) -> None:
    fixture_started = time.monotonic()
    root = tmp_path / "remote"
    first_witness, resumed_witness = (
        tmp_path / "first-driver.json",
        tmp_path / "resumed-driver.json",
    )
    first_nonce, resumed_nonce = new_id(), new_id()
    marker = resumed_witness.with_suffix(".acquired")
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

        def driver_argv(
            witness: Path, nonce: str, lock_path: Path | None = None, *, wait_for_ack: bool = False
        ) -> list[str]:
            return [
                sys.executable,
                "-m",
                "tests.cli_process",
                str(witness),
                nonce,
                str(lock_path) if lock_path else "",
                "ack" if wait_for_ack else "",
                *argv[3:],
            ]

        first = subprocess.Popen(
            driver_argv(first_witness, first_nonce),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        first_launcher = psutil.Process(first.pid)
        first_launcher.create_time()
        first_driver = resumed_driver = resumed_launcher = None
        resumed = None
        release = None
        supervisor = None

        def setup_diagnostics(
            process: Any, phase: str, captured: tuple[bytes, bytes] | None = None
        ) -> str:
            def bounded(value: Any, limit: int = 1024) -> str:
                text = value.decode(errors="replace") if isinstance(value, bytes) else str(value)
                for private in (
                    connected.bundle.token,
                    str(root),
                    str(pipeline.runtime.root),
                    sys.executable,
                    str(REPO_ROOT),
                    str(Path.home()),
                ):
                    text = text.replace(private, "<redacted>")
                return text[:limit]

            result: dict[str, Any] = {"phase": phase, "returncode": process.poll(), "attempts": []}
            for suffix in (".phase", ".trace"):
                try:
                    with marker.with_suffix(suffix).open("rb") as stream:
                        result["resumer" + suffix] = bounded(stream.read(8192), 8192)
                except FileNotFoundError:
                    pass
                except Exception as error:
                    result["resumer" + suffix] = {"error_type": type(error).__name__}
            if process.poll() is not None:
                try:
                    output, error_output = captured or process.communicate(timeout=2)
                    result["stderr"] = bounded(error_output)
                    if len(output) > 128 * 1024:
                        result["stdout"] = "CLI response exceeded diagnostic bound"
                    else:
                        reply = json.loads(output)
                        result["driver"] = {
                            "state": reply.get("state"),
                            "observation_count": reply.get("observation_count"),
                            "recent_attempts": reply.get("recent_attempts", [])[:8],
                            "findings": [bounded(item) for item in reply.get("findings", [])[:8]],
                        }
                except Exception as error:
                    result["output_error"] = type(error).__name__
            try:
                records = pipeline.client.list(
                    RecordQuery(kind="attempt", campaign_id=pipeline.campaign["id"], limit=8)
                )["items"]
                for row in records:
                    item: dict[str, Any] = {"id": row["id"], "controller_state": row["state"]}
                    try:
                        entry = journal.get(row["id"])
                        item.update(
                            {
                                "state": entry.state.value,
                                "finding": bounded(entry.finding),
                                "clock_mode": entry.clock_mode,
                                "native": entry.native is not None,
                                "prepared": entry.prepared is not None,
                                "outcome": None
                                if entry.outcome is None
                                else {
                                    "reason": entry.outcome.reason,
                                    "exit_code": entry.outcome.exit_code,
                                    "elapsed_ms": entry.outcome.elapsed_ms,
                                    "scope_closed": entry.outcome.closure.scope_closed,
                                    "findings": [
                                        bounded(item) for item in entry.outcome.closure.findings[:8]
                                    ],
                                },
                            }
                        )
                    except Exception as error:
                        item["journal_error"] = type(error).__name__
                    result["attempts"].append(item)
            except Exception as error:
                result["records_error"] = type(error).__name__
            return json.dumps(result, sort_keys=True)

        def diagnostics(phase: str) -> str:
            def read(call: Any) -> Any:
                try:
                    return call()
                except Exception as error:
                    return {"error_type": type(error).__name__}

            def process(pid: int, created: float | None = None) -> Any:
                current = psutil.Process(pid)
                return {
                    "pid": pid,
                    "running": current.is_running(),
                    "created_at": current.create_time(),
                    "identity_matches": current.create_time() == created if created else None,
                }

            def outcome_file() -> Any:
                assert running is not None
                path = (
                    journal.path.parent
                    / "executions"
                    / running.dispatch.attempt_id
                    / "outcome.json"
                )
                with path.open("rb") as stream:
                    payload = stream.read(8193)
                if len(payload) > 8192:
                    return {"finding": "Outcome exceeded diagnostic byte bound"}
                value = json.loads(payload)
                findings = []
                for item in value.get("closure", {}).get("findings", [])[:8]:
                    for private in (
                        connected.bundle.token,
                        str(root),
                        str(pipeline.runtime.root),
                        sys.executable,
                        str(REPO_ROOT),
                        str(Path.home()),
                    ):
                        item = item.replace(private, "<redacted>")
                    findings.append(item[:512])
                return {
                    "reason": value.get("reason"),
                    "elapsed_ms": value.get("elapsed_ms"),
                    "scope_closed": value.get("closure", {}).get("scope_closed"),
                    "findings": findings,
                }

            def journal_state() -> Any:
                assert running is not None
                entry = journal.get(running.dispatch.attempt_id)
                return {
                    "state": entry.state,
                    "cancel_requested": entry.cancel_requested,
                    "last_controller_sequence": entry.last_controller_sequence,
                    "supervisor_id": entry.supervisor_id,
                    "native_unchanged": entry.native == running.native,
                    "native": entry.native.model_dump(mode="json") if entry.native else None,
                    "outcome": {
                        "reason": entry.outcome.reason,
                        "exit_code": entry.outcome.exit_code,
                        "scope_closed": entry.outcome.closure.scope_closed,
                        "findings": [item[:512] for item in entry.outcome.closure.findings[:8]],
                    }
                    if entry.outcome
                    else None,
                    "finding": entry.finding[:512],
                }

            assert running is not None
            assert running.native is not None
            return json.dumps(
                {
                    "phase": phase,
                    "fixture_elapsed_ms": int((time.monotonic() - fixture_started) * 1000),
                    "native_timeout_ms": running.command.timeout_ms,
                    "hard_deadline_at": running.dispatch.hard_deadline_at,
                    "timing_caps": running.timing.caps.model_dump(mode="json")
                    if running.timing
                    else None,
                    "outcome_file": read(outcome_file),
                    "journal": read(journal_state),
                    "control": read(
                        lambda: pipeline.client.get("attempt_control", running.dispatch.attempt_id)
                    ),
                    "presence": read(
                        lambda: pipeline.client.get(
                            "worker_presence", connected.bundle.worker.binding.worker_id
                        )
                    ),
                    "root": read(lambda: process(running.native.pid)),
                    "supervisor": read(lambda: process(*supervisor))
                    if supervisor is not None
                    else None,
                    "resumed_returncode": resumed.poll() if resumed is not None else None,
                    "capacity": read(connected.worker.resources.availability),
                },
                sort_keys=True,
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
                assert first.poll() is None and time.monotonic() < until, setup_diagnostics(
                    first, "first driver before running"
                )
                time.sleep(0.01)
            assert running.native is not None
            assert running.prepared is not None
            native_root = psutil.Process(running.native.pid)
            native_root.create_time()
            native_parent = native_root.parent()
            assert native_parent is not None
            supervisor = (native_parent.pid, native_parent.create_time())
            output_root = Path(running.prepared.context.output_root)
            release = output_root / "payload-release"
            while not (output_root / "payload-finished").exists():
                assert first.poll() is None and time.monotonic() < until, setup_diagnostics(
                    first, "first driver before payload marker"
                )
                time.sleep(0.01)
            first_driver = require_driver(first_launcher, first_witness, first_nonce)
            require_ancestry(native_parent, first_driver)
            # Interrupt the interpreter, not a venv redirector that may outlive it.
            first_driver.kill()
            first_driver.wait(timeout=5)
            first.communicate(timeout=5)
            assert not first_driver.is_running()
            assert native_parent.is_running() and native_parent.create_time() == supervisor[1], (
                diagnostics("supervisor after driver exit")
            )
            assert native_root.is_running(), diagnostics("native root after driver exit")
            if sys.platform == "win32":
                current_start_token = f"native-created:{native_root.create_time().hex()}"
            else:
                from research_swarm.workers._posix import start_token

                current_start_token = start_token(native_root)
            assert running.native.start_token == current_start_token
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
            lock_path = root / "execution" / "locks" / f"{running.dispatch.attempt_id}.lock"
            resumed_argv = driver_argv(
                resumed_witness,
                resumed_nonce,
                None if cancel_offline else lock_path,
                wait_for_ack=True,
            )
            resumed = subprocess.Popen(
                resumed_argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            resumed_launcher = psutil.Process(resumed.pid)
            resumed_launcher.create_time()
            until = time.monotonic() + 10
            while not resumed_witness.exists():
                assert resumed.poll() is None and time.monotonic() < until, setup_diagnostics(
                    resumed, "resumed driver before identity"
                )
                time.sleep(0.01)
            resumed_driver = acknowledge_driver(resumed_launcher, resumed_witness, resumed_nonce)
            if not cancel_offline:
                while True:
                    assert resumed.poll() is None and time.monotonic() < until, setup_diagnostics(
                        resumed, "resumed driver before attempt lock"
                    )
                    try:
                        acquired = json.loads(marker.read_text(encoding="utf-8"))
                    except FileNotFoundError:
                        time.sleep(0.01)
                    else:
                        assert acquired == {
                            "attempt_id": running.dispatch.attempt_id,
                            "pid": resumed_driver.pid,
                            "created_at_hex": resumed_driver.create_time().hex(),
                            "nonce": resumed_nonce,
                        }
                        break
                assert journal.get(running.dispatch.attempt_id).state == JournalState.RUNNING
                release.touch()
            finish_by = time.monotonic() + 20
            if cancel_offline:
                apply_by = until
                while not journal.get(running.dispatch.attempt_id).cancel_requested:
                    if resumed.poll() is not None:
                        break
                    if time.monotonic() >= apply_by:
                        pytest.fail(
                            "Resumed driver did not apply cancellation: " + diagnostics("delivery")
                        )
                    time.sleep(0.025)
            try:
                stdout, stderr = resumed.communicate(
                    timeout=max(0.001, finish_by - time.monotonic())
                )
            except subprocess.TimeoutExpired as error:
                captured = error.stderr or b""
                if isinstance(captured, bytes):
                    captured = captured.decode("utf-8", errors="replace")
                captured = captured.replace(connected.bundle.token, "<redacted>")[-2048:]
                pytest.fail(
                    "Resumed driver did not finish: "
                    + diagnostics("closure")
                    + "; stderr="
                    + captured
                )
            assert resumed.returncode == 0, setup_diagnostics(
                resumed, "resumed driver result", (stdout, stderr)
            )
            result = json.loads(stdout)
            assert result["state"] == "idle" and result["observation_count"] == 1, (
                setup_diagnostics(resumed, "resumed driver observations", (stdout, stderr))
            )
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
            assert connected.bundle.token not in stdout + stderr
            assert (output_root / "payload-finished").read_text() == "1"
        except psutil.NoSuchProcess as error:
            if running is not None and running.native is not None:
                error.add_note(diagnostics("native process disappeared"))
            raise
        finally:
            if release is not None:
                release.touch()
            for process, launcher, driver, witness, nonce in (
                (resumed, resumed_launcher, resumed_driver, resumed_witness, resumed_nonce),
                (first, first_launcher, first_driver, first_witness, first_nonce),
            ):
                if process is None:
                    continue
                try:
                    if (
                        driver is None
                        and witness.exists()
                        and launcher is not None
                        and launcher.is_running()
                    ):
                        driver = require_driver(launcher, witness, nonce)
                    if driver is not None and driver.is_running():
                        driver.kill()
                        driver.wait(timeout=5)
                except psutil.NoSuchProcess:
                    pass
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=5)
