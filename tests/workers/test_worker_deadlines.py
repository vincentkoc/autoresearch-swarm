import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.workers import supervision
from research_swarm.workers.journal import JournalState, WorkerJournal
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.supervisor import run_attempt
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_http_delivery import serving
from tests.workers.test_supervision import command
from tests.workers.test_worker_queries import scoped_worker


def inline_supervisor(journal: WorkerJournal, attempt_id: str) -> None:
    """Run real supervised children in this process for release fault injection."""
    run_attempt(journal, attempt_id)


def native_worker(pipeline: Pipeline, tmp_path: Path) -> tuple[LocalWorker, str]:
    _, data, token = scoped_worker(pipeline)
    runtime = pipeline.runtime
    principal = runtime.credentials.resolve(token)
    issued = runtime.credentials.issue(
        runtime.principal(),
        principal.model_copy(
            update={
                "scopes": principal.scopes | {"worker.publish", "worker.inputs", "worker.outputs"}
            }
        ),
    )
    client = ResearchClient(
        LocalTransport(runtime, lambda: runtime.credentials.resolve(issued.token))
    )
    return LocalWorker(
        client,
        data,
        root=tmp_path / "deadline-worker",
        machine_registry=tmp_path / "deadline-machine" / "resources.sqlite",
    ), issued.token


def test_remote_worker_executes_with_a_local_deadline_and_replays_once(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    worker, token = native_worker(pipeline, tmp_path)
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, token) as remote,
    ):
        worker.client = remote
        row = worker.claim(pipeline.ready())
        completed = worker.execute(row["id"])
        entry = worker.journal.get(row["id"])
        assert completed["state"] == "succeeded", entry.finding
        assert entry.state == JournalState.CLOSED
        assert entry.native is not None and entry.outcome is not None
        assert entry.outcome.released_at is not None and entry.outcome.closure.scope_closed
        assert worker.execute(row["id"]) == completed
        assert worker.journal.get(row["id"]) == entry
    assert worker.resources.availability() == {"cpu": 1}


def test_launch_cap_is_rechecked_after_held_callback_independently_of_hard_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = supervision.continuous_time
    shift = 0
    monkeypatch.setattr(supervision, "continuous_time", lambda: real() + shift)
    marker = tmp_path / "never-released"

    def held(_: object) -> bool:
        nonlocal shift
        shift = 20
        return True

    outcome = supervision.run_native(
        command(
            tmp_path, f"from pathlib import Path; Path({str(marker)!r}).touch()", timeout_ms=90_000
        ),
        tmp_path / "output",
        on_held=held,
        launch_deadline_ns=int((real() + 10) * 1_000_000_000),
        hard_deadline_ns=int((real() + 80) * 1_000_000_000),
    )
    assert outcome.reason == "not_started" and outcome.released_at is None
    assert outcome.closure.scope_closed and not marker.exists()


def test_overdue_unclaimed_supervisor_publishes_not_started(pipeline, tmp_path, monkeypatch):
    worker, _ = native_worker(pipeline, tmp_path)
    row = worker.claim(pipeline.ready())
    clock = iter((0.0, 1_000_000.0))
    monkeypatch.setattr(
        "research_swarm.workers.local.time", SimpleNamespace(monotonic=lambda: next(clock))
    )
    monkeypatch.setattr(
        "research_swarm.workers.local.spawn_supervisor",
        lambda *_: SimpleNamespace(poll=lambda: None, wait=lambda timeout: None),
    )

    result = worker.execute(row["id"])
    entry = worker.journal.get(row["id"])
    assert result["state"] == "not_started"
    assert entry.state == JournalState.NOT_STARTED and entry.supervisor_id is None
    assert run_attempt(worker.journal, row["id"]) == entry
    assert worker.resources.availability() == {"cpu": 1}


@pytest.mark.parametrize("phase", ["claim", "release"])
def test_lock_wait_consumes_the_local_launch_deadline(tmp_path, monkeypatch, phase):
    from research_swarm.domain.types import new_id
    from research_swarm.workers.contracts import NativeIdentity
    from tests.worker_support import dispatch

    verified, invocation = dispatch(tmp_path)
    journal = WorkerJournal(tmp_path / "journal.sqlite")
    attempt_id = journal.receive(verified, invocation).dispatch.attempt_id
    supervisor = new_id()
    now = 0
    monkeypatch.setattr(
        "research_swarm.workers.journal.native_clock",
        lambda: SimpleNamespace(continuous_ns=lambda: now),
    )
    if phase == "release":
        journal.claim_launch(attempt_id, supervisor, 1)
        journal.held(
            attempt_id,
            supervisor,
            NativeIdentity(
                backend="posix_group", pid=123, start_token="fixture", scope_id=new_id()
            ),
        )
    transition = journal.claim_launch if phase == "claim" else journal.authorize_release
    connecting = threading.Event()
    connect = journal._connect

    def notify_connect():
        connecting.set()
        return connect()

    with ThreadPoolExecutor(max_workers=1) as pool:
        with journal._transaction():
            monkeypatch.setattr(journal, "_connect", notify_connect)
            result = pool.submit(transition, attempt_id, supervisor, 1)
            assert connecting.wait(5)
            now = 1
        outcome = result.result(timeout=5)
    if phase == "claim":
        assert outcome.state == JournalState.NOT_STARTED
    else:
        assert outcome is False
        assert journal.get(attempt_id).release_requested_at is None
