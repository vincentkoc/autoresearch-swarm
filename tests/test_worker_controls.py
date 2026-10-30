from __future__ import annotations

import concurrent.futures
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from research_swarm.domain.canonical import content_digest, record_digest
from research_swarm.domain.controls import CancellationDirective
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.domain.workers import WorkerBinding
from research_swarm.workers import control_inbox
from research_swarm.workers.journal import JournalState, WorkerJournal
from research_swarm.workers.supervisor import run_attempt
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_supervision import require_closed
from tests.test_timed_execution import native_worker
from tests.test_worker_lifecycle import cancel
from tests.worker_support import dispatch


def directive(envelope: Any) -> CancellationDirective:
    return CancellationDirective(
        binding=WorkerBinding.model_validate(
            {key: getattr(envelope, key) for key in WorkerBinding.model_fields}
        ),
        attempt_id=envelope.attempt_id,
        dispatch_digest=record_digest("research.dispatch", envelope.model_dump(mode="json")),
    )


def test_control_before_assignment_remains_effective_after_ack_and_restart(tmp_path: Path) -> None:
    verified, command = dispatch(tmp_path, "raise SystemExit('must not execute')")
    control = directive(verified.envelope)
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    ack = journal.apply_control(control.binding, control)
    assert journal.pending_control_acks(control.binding) == (ack,)
    with pytest.raises(ResearchError) as absent:
        journal.get(control.attempt_id)
    assert absent.value.code == ErrorCode.NOT_FOUND
    journal.acknowledge_controls(control.binding, (ack,))
    restarted = WorkerJournal(journal.path)
    assert restarted.pending_control_acks(control.binding) == ()
    received = restarted.receive(verified, command)
    assert received.cancel_requested and received.last_controller_sequence == 0
    assert restarted.receive(verified, command) == received
    assert restarted.apply_control(control.binding, control) == ack
    assert restarted.pending_control_acks(control.binding) == ()
    result = run_attempt(restarted, control.attempt_id)
    assert result.state == JournalState.NOT_STARTED
    assert result.outcome is None and result.native is None


def test_receipt_does_not_change_immutable_input_identity_or_consume_legacy_sequence(
    tmp_path: Path,
) -> None:
    verified, command = dispatch(tmp_path)
    control = directive(verified.envelope)
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    before = journal.receive(verified, command)
    with journal._connect() as connection:
        digest = connection.execute("SELECT input_digest FROM attempts").fetchone()[0]
    journal.request_local_cancel(control.attempt_id)
    journal.apply_control(control.binding, control)
    after = journal.get(control.attempt_id)
    assert after.last_controller_sequence == before.last_controller_sequence == 0
    assert after.cancel_requested
    assert after.dispatch == before.dispatch and after.spec == before.spec
    assert journal.receive(verified, command) == after
    with journal._connect() as connection:
        assert connection.execute("SELECT input_digest FROM attempts").fetchone()[0] == digest
    with pytest.raises(ResearchError, match="dispatch changed"):
        journal.receive(verified, command.model_copy(update={"output_limit_bytes": 128}))


@pytest.mark.parametrize("existing", [False, True])
def test_wrong_dispatch_and_binding_cannot_apply_or_release_control(
    tmp_path: Path, existing: bool
) -> None:
    verified, command = dispatch(tmp_path)
    control = directive(verified.envelope)
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    if existing:
        journal.receive(verified, command)
    with pytest.raises(ResearchError) as binding:
        journal.apply_control(
            control.binding.model_copy(update={"worker_session_id": new_id()}), control
        )
    assert binding.value.code == ErrorCode.STALE_FENCE
    wrong = control.model_copy(update={"dispatch_digest": content_digest(b"wrong")})
    if existing:
        with pytest.raises(ResearchError, match="differs from admitted dispatch"):
            journal.apply_control(control.binding, wrong)
        assert not journal.get(control.attempt_id).cancel_requested
        assert journal.pending_control_acks(control.binding) == ()
    else:
        journal.apply_control(control.binding, wrong)
        with pytest.raises(ResearchError, match="differs from admitted dispatch"):
            journal.receive(verified, command)
        with pytest.raises(ResearchError) as absent:
            journal.get(control.attempt_id)
        assert absent.value.code == ErrorCode.NOT_FOUND
        with pytest.raises(ResearchError, match="identity changed"):
            journal.apply_control(control.binding, control)


@pytest.mark.parametrize("control_first", [False, True])
def test_receipt_and_control_are_atomic_across_independent_connections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    control_first: bool,
) -> None:
    verified, command = dispatch(tmp_path)
    control = directive(verified.envelope)
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    independent = WorkerJournal(journal.path)
    entered, release, second_started = threading.Event(), threading.Event(), threading.Event()
    name = "store_control" if control_first else "merge_control"
    original = getattr(control_inbox, name)
    first_call = True

    def hold(*args: Any, **kwargs: Any) -> Any:
        nonlocal first_call
        result = original(*args, **kwargs)
        if first_call:
            first_call = False
            entered.set()
            assert release.wait(5)
        return result

    monkeypatch.setattr(control_inbox, name, hold)

    def receive() -> Any:
        return journal.receive(verified, command)

    def apply() -> Any:
        return independent.apply_control(control.binding, control)

    first, second = (apply, receive) if control_first else (receive, apply)

    def waiting_second() -> Any:
        second_started.set()
        return second()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        one = pool.submit(first)
        assert entered.wait(5)
        two = pool.submit(waiting_second)
        assert second_started.wait(5)
        release.set()
        one.result(timeout=5)
        two.result(timeout=5)
    assert journal.get(control.attempt_id).cancel_requested
    assert journal.receive(verified, command) == journal.get(control.attempt_id)
    assert journal.pending_control_acks(control.binding) == (control.acknowledgment(),)


def test_acknowledgment_batch_is_bounded_atomic_and_does_not_delete_controls(
    tmp_path: Path,
) -> None:
    verified, _ = dispatch(tmp_path)
    control = directive(verified.envelope)
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    controls = tuple(control.model_copy(update={"attempt_id": new_id()}) for _ in range(105))
    for item in controls:
        journal.apply_control(control.binding, item)
    first = journal.pending_control_acks(control.binding)
    assert len(first) == 100
    invalid = first[0].model_copy(update={"attempt_id": new_id()})
    with pytest.raises(ResearchError, match="differs from persisted control"):
        journal.acknowledge_controls(control.binding, (first[0], invalid))
    assert journal.pending_control_acks(control.binding) == first
    with pytest.raises(ValueError, match="distinct"):
        journal.acknowledge_controls(control.binding, (first[0], first[0]))
    for limit in (0, 101, True):
        with pytest.raises(ValueError):
            journal.pending_control_acks(control.binding, limit=limit)
    journal.acknowledge_controls(control.binding, first)
    journal.acknowledge_controls(control.binding, first)
    remaining = journal.pending_control_acks(control.binding)
    assert len(remaining) == 5
    assert not set(a.attempt_id for a in first) & set(a.attempt_id for a in remaining)
    with journal._connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM control_inbox").fetchone()[0] == 105


@pytest.mark.parametrize("unstarted", [False, True])
def test_late_control_preserves_terminal_journal_bytes_and_outcome(
    tmp_path: Path, unstarted: bool
) -> None:
    verified, command = dispatch(tmp_path)
    control = directive(verified.envelope)
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    journal.receive(verified, command)
    if unstarted:
        journal.request_local_cancel(control.attempt_id)
    closed = run_attempt(journal, control.attempt_id)
    assert closed.state == (JournalState.NOT_STARTED if unstarted else JournalState.CLOSED)
    events = journal.pending()
    ack = journal.apply_control(control.binding, control)
    assert journal.get(control.attempt_id).model_dump_json() == closed.model_dump_json()
    assert journal.pending() == events
    assert journal.request_local_cancel(control.attempt_id) == closed
    assert journal.receive(verified, command) == closed
    assert journal.pending_control_acks(control.binding) == (ack,)


@pytest.mark.parametrize("already_authorized", [False, True])
def test_control_during_native_barrier_prevents_project_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    already_authorized: bool,
) -> None:
    marker = tmp_path / "must-not-execute"
    verified, command = dispatch(tmp_path, f"open({str(marker)!r},'w').write('executed')")
    control = directive(verified.envelope)
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    journal.receive(verified, command)
    held, resume = threading.Event(), threading.Event()
    original = journal.authorize_release

    def paused(attempt_id: str, supervisor_id: str) -> bool:
        result = original(attempt_id, supervisor_id) if already_authorized else None
        held.set()
        assert resume.wait(5)
        return result if result is not None else original(attempt_id, supervisor_id)

    monkeypatch.setattr(journal, "authorize_release", paused)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(run_attempt, journal, control.attempt_id)
        try:
            assert held.wait(5)
            assert journal.get(control.attempt_id).state == (
                JournalState.RELEASING if already_authorized else JournalState.HELD
            )
            WorkerJournal(journal.path).apply_control(control.binding, control)
        finally:
            resume.set()
        result = task.result(timeout=10)
    assert result.state == JournalState.NOT_STARTED and result.outcome is not None
    assert result.outcome.closure.scope_closed and result.outcome.released_at is None
    assert not marker.exists()


def test_remote_control_closes_parent_and_descendants_without_touching_unrelated_process(
    tmp_path: Path,
) -> None:
    child_path, grandchild_path = tmp_path / "child.pid", tmp_path / "grandchild.pid"
    grandchild = (
        f"import os,time; open({str(grandchild_path)!r},'w').write(str(os.getpid())); "
        "time.sleep(60)"
    )
    child = (
        f"import os,sys,subprocess,time; open({str(child_path)!r},'w').write(str(os.getpid())); "
        f"subprocess.Popen([sys.executable,'-c',{grandchild!r}]); time.sleep(60)"
    )
    parent = (
        f"import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',{child!r}]); "
        "time.sleep(60)"
    )
    verified, command = dispatch(tmp_path, parent)
    control = directive(verified.envelope)
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    journal.receive(verified, command)
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            task = pool.submit(run_attempt, journal, control.attempt_id)
            until = time.monotonic() + 3
            while not grandchild_path.exists() and time.monotonic() < until:
                time.sleep(0.01)
            assert grandchild_path.exists()
            WorkerJournal(journal.path).apply_control(control.binding, control)
            result = task.result(timeout=10)
        assert result.outcome is not None and result.outcome.reason == "cancelled"
        assert result.outcome.closure.scope_closed
        require_closed(int(child_path.read_text()))
        require_closed(int(grandchild_path.read_text()))
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=5)


def test_cancellation_before_receipt_settles_pipeline_without_downloading_or_claiming(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    requested = cancel(pipeline, attempt)["control"]["data"]
    control = CancellationDirective.model_validate(
        {key: requested[key] for key in CancellationDirective.model_fields}
    )
    pipeline.worker.journal.apply_control(control.binding, control)

    def unexpected(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("Cancelled receipt must not download or claim")

    monkeypatch.setattr(pipeline.worker, "_download", unexpected)
    monkeypatch.setattr(pipeline.worker.resources, "reserve", unexpected)
    accepted = pipeline.worker.execute(attempt["id"])
    assert accepted["state"] == "not_started"
    assert (
        pipeline.client.get("attempt_control", attempt["id"])["data"]["resolution"] == "not_started"
    )
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert all(a["consumed"] == "0" and a["reserved"] == "0" for a in accounts)


def test_cancellation_during_preparation_preserves_dispatch_and_releases_unstarted_capacity(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    entered, release = threading.Event(), threading.Event()
    original = pipeline.worker._download
    once = True

    def paused(*args: Any, **kwargs: Any) -> Any:
        nonlocal once
        result = original(*args, **kwargs)
        if once:
            once = False
            entered.set()
            assert release.wait(5)
        return result

    monkeypatch.setattr(pipeline.worker, "_download", paused)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(pipeline.worker.execute, attempt["id"])
        try:
            assert entered.wait(5)
            requested = cancel(pipeline, attempt)["control"]["data"]
            control = CancellationDirective.model_validate(
                {key: requested[key] for key in CancellationDirective.model_fields}
            )
            pipeline.worker.journal.apply_control(control.binding, control)
        finally:
            release.set()
        accepted = task.result(timeout=10)
    assert accepted["state"] == "not_started"
    entry = pipeline.worker.journal.get(attempt["id"])
    assert entry.native is None and entry.prepared is not None
    assert entry.dispatch.model_dump(mode="json") == attempt["data"]["dispatch"]
    assert pipeline.worker.resources.availability() == {"cpu": 1}


def test_calibrated_native_release_honors_control_after_origin_authorization(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, _ = native_worker(pipeline, tmp_path)
    attempt = worker.claim(pipeline.ready())
    entered, release = threading.Event(), threading.Event()
    original = worker.journal.authorize_timed_release

    def paused(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        assert result
        entered.set()
        assert release.wait(5)
        return result

    monkeypatch.setattr(worker.journal, "authorize_timed_release", paused)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(worker.execute, attempt["id"])
        try:
            assert entered.wait(5)
            requested = cancel(pipeline, attempt)["control"]["data"]
            control = CancellationDirective.model_validate(
                {key: requested[key] for key in CancellationDirective.model_fields}
            )
            worker.journal.apply_control(control.binding, control)
        finally:
            release.set()
        accepted = task.result(timeout=10)
    assert accepted["state"] == "not_started"
    entry = worker.journal.get(attempt["id"])
    assert entry.outcome is not None and entry.outcome.closure.scope_closed
    assert entry.outcome.released_at is None
    assert worker.resources.availability() == {"cpu": 1}


def test_remote_control_is_independent_of_legacy_sequence_and_unknown_is_not_closed(
    tmp_path: Path,
) -> None:
    verified, command = dispatch(tmp_path)
    control = directive(verified.envelope)
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    journal.receive(verified, command)
    owner = new_id()
    journal.claim_launch(control.attempt_id, owner)
    unknown = journal.unknown(control.attempt_id, owner, "Synthetic missing native receipt")
    journal.request_cancel(control.attempt_id, 99)
    ack = journal.apply_control(control.binding, control)
    after = journal.get(control.attempt_id)
    assert after.state == JournalState.UNKNOWN and after.cancel_requested
    assert after.last_controller_sequence == 99 and ack.control_sequence == 1
    assert after.native == unknown.native and after.outcome is None
    assert after.finding == unknown.finding
    assert journal.pending_control_acks(control.binding) == (ack,)
    assert journal.unresolved() == (after,)
