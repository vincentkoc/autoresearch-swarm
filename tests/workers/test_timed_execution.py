from __future__ import annotations

import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.models import TrialData
from research_swarm.control.workers import AttemptData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.workers import supervision
from research_swarm.workers.clock_channel import OriginServer
from research_swarm.workers.clock_estimates import DeadlineCaps
from research_swarm.workers.clocks import native_clock
from research_swarm.workers.contracts import NativeIdentity
from research_swarm.workers.journal import JournalState, WorkerJournal
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.timed_launch import LiveTimingOrigin, TimingEvidence
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_http_delivery import serving
from tests.workers.test_authority_clock import SimulatedClock, timed_worker
from tests.workers.test_authority_clock import simulated as simulated
from tests.workers.test_supervision import command

ORIGIN_PROCESS = """
import sys, time
from pathlib import Path
from research_swarm.domain.clocks import AssignmentTimePayload, timed_assignment_digest
from research_swarm.workers.clock_estimates import ClockSampler
from research_swarm.workers.contracts import NativeCommand
from research_swarm.workers.journal import JournalState, WorkerJournal
from research_swarm.workers.timed_launch import LiveTimingOrigin, capture_timing
from research_swarm.workers.timed_supervision import start_timed_supervisor
from tests.domain.test_assignment_signatures import assignment_payload, signer
from tests.workers.test_clock_estimates import fixture

root, phase = Path(sys.argv[1]), sys.argv[2]
payload = assignment_payload()
argv = (sys.executable, '-c',
        "from pathlib import Path; import time; Path('ran').touch(); time.sleep(60)")
stage = payload.spec.stages[0].model_copy(update={'argv': argv, 'timeout_ms': 90000})
spec = payload.spec.model_copy(update={'stages': (stage,), 'spec_digest': None}).sealed()
dispatch = payload.dispatch.model_copy(update={
    'spec_digest': spec.spec_digest,
    'expires_at': '2026-01-01T00:00:08.000Z',
    'hard_deadline_at': '2026-01-01T00:00:09.000Z',
})
payload = payload.model_copy(update={'spec': spec, 'dispatch': dispatch})
signed = signer(payload).sign(payload)
estimate, _, clock, _ = fixture()
sampler = ClockSampler(clock.authority)
pending = sampler.begin()
reply = clock.reply(estimate.verified.reply.payload.model_copy(
    update={'nonce': pending.challenge.nonce}))
timed = sampler.verifier.binding(clock.bind(AssignmentTimePayload(
    binding=clock.authority.binding,
    clock_epoch=reply.payload.clock_epoch,
    assignment_digest=timed_assignment_digest(signed),
    policy_digest=reply.payload.policy.digest,
)), signed)
verified = capture_timing(timed, pending.complete(reply), sampler.read())
journal = WorkerJournal(root / 'journal.sqlite')
journal.receive(timed, NativeCommand(argv=argv, cwd=str(root), timeout_ms=90000))
journal.bind_timing(dispatch.attempt_id, verified)
if phase in {'claim', 'release'}:
    def block(*args):
        (root / 'ready').touch()
        while True:
            time.sleep(0.1)
    setattr(journal, 'claim_timed_launch' if phase == 'claim'
            else 'authorize_timed_release', block)
supervised = start_timed_supervisor(journal, dispatch.attempt_id,
                                  LiveTimingOrigin(sampler, verified))
if phase == 'running':
    while journal.get(dispatch.attempt_id).state != JournalState.RUNNING:
        if supervised.process.poll() is not None:
            raise RuntimeError('Supervisor ended before release')
        time.sleep(0.01)
    (root / 'ready').touch()
supervised.process.wait(timeout=15)
"""


def native_worker(pipeline: Pipeline, tmp_path: Path) -> tuple[LocalWorker, str]:
    _, data, token = timed_worker(pipeline)
    runtime = pipeline.runtime
    principal = runtime.credentials.resolve(token)
    issued = runtime.credentials.issue(
        runtime.principal(),
        principal.model_copy(update={"scopes": principal.scopes | {"worker.publish"}}),
    )
    client = ResearchClient(
        LocalTransport(runtime, lambda: runtime.credentials.resolve(issued.token))
    )
    return LocalWorker(
        client,
        data,
        root=tmp_path / "timed-worker",
        machine_registry=tmp_path / "timed-machine" / "resources.sqlite",
    ), issued.token


def test_scoped_http_timed_cpu_execution_uses_private_live_origin_and_replays_once(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, token = native_worker(pipeline, tmp_path)
    monkeypatch.setattr("research_swarm.workers.local.utc_now", lambda: "2999-01-01T00:00:00.000Z")
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
        assert entry.timing is not None and entry.assignment_time is not None
        assert entry.native is not None and entry.outcome is not None
        assert entry.outcome.released_at is not None and entry.outcome.closure.scope_closed
        assert worker.execute(row["id"]) == completed
        assert worker.journal.get(row["id"]) == entry
    assert worker.resources.availability() == {"cpu": 1}


def test_new_origin_cannot_revive_an_attached_unreleased_attempt(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    worker, _ = native_worker(pipeline, tmp_path)
    row = worker.claim(pipeline.ready())
    attempt = AttemptData.model_validate(row["data"])
    trial = TrialData.model_validate(pipeline.trial["data"])
    entry = worker._prepare(attempt, trial)
    worker._timed_origin(entry)
    frozen = worker.journal.get(row["id"]).timing
    assert frozen is not None
    restarted = LocalWorker(
        worker.client, worker.worker, root=worker.root, machine_registry=worker.resources.path
    )
    completed = restarted.execute(row["id"])
    entry = restarted.journal.get(row["id"])
    assert completed["state"] == "not_started"
    assert entry.timing == frozen and entry.native is None and entry.outcome is None
    assert not (worker.root / "executions").exists()
    assert worker.resources.availability() == {"cpu": 1}


def test_clock_failure_after_preparation_quarantines_unstarted_and_releases_native_claim(
    pipeline: Pipeline,
    simulated: SimulatedClock,
    tmp_path: Path,
) -> None:
    from research_swarm.control.queries import RecordQuery
    from tests.control.test_reconciliation import reconcile

    worker, _ = native_worker(pipeline, tmp_path)
    row = worker.claim(pipeline.ready())
    worker._prepare(
        AttemptData.model_validate(row["data"]), TrialData.model_validate(pipeline.trial["data"])
    )
    assert worker.resources.availability() == {"cpu": 0}
    prior = simulated.reading
    simulated.reading = replace(prior, wall_ns=prior.wall_ns + 2_000_000_000)
    with pytest.raises(ResearchError) as unqualified:
        worker.execute(row["id"])
    assert unqualified.value.code == ErrorCode.ACCEPTANCE_EXPIRED
    entry = worker.journal.get(row["id"])
    assert entry.state == JournalState.NOT_STARTED and entry.native is None
    assert "Clock admission unavailable" in entry.finding
    assert worker.resources.availability() == {"cpu": 1}
    assert pipeline.client.get("attempt", row["id"])["state"] == "execution_unknown"
    submissions = pipeline.client.list(RecordQuery(kind="attempt_submission"))["items"]
    assert len(submissions) == 1
    submission = pipeline.client.get("attempt_submission", submissions[0]["id"])
    completed = reconcile(pipeline, submission)
    assert completed["attempt"]["state"] == "not_started"
    assert completed["reconciliation"]["data"]["acceptance_disposition"] == "unqualified"


def test_origin_failure_at_claim_cannot_leave_a_relaunchable_gap(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, _ = native_worker(pipeline, tmp_path)
    row = worker.claim(pipeline.ready())
    check = LiveTimingOrigin.check
    calls = 0

    def fails_after_start(self: LiveTimingOrigin, evidence: TimingEvidence) -> DeadlineCaps:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise OSError("Synthetic origin failure before native creation")
        return check(self, evidence)

    monkeypatch.setattr(LiveTimingOrigin, "check", fails_after_start)
    completed = worker.execute(row["id"])
    entry = worker.journal.get(row["id"])
    assert completed["state"] == "not_started"
    assert entry.supervisor_id is not None and entry.native is None
    assert "no native process creation" in entry.finding
    assert worker.execute(row["id"]) == completed
    assert worker.resources.availability() == {"cpu": 1}


@pytest.mark.parametrize("failure", ["expiry", "thread", "spawn"])
def test_startup_failure_before_claim_closes_channel_and_settles_resources(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    worker, _ = native_worker(pipeline, tmp_path)
    row = worker.claim(pipeline.ready())
    closed: list[OriginServer] = []
    original_close = OriginServer.close

    def close(server: OriginServer) -> None:
        original_close(server)
        closed.append(server)

    def fail(*_: Any, **__: Any) -> Any:
        if failure == "expiry":
            raise ResearchError(ErrorCode.STALE_FENCE, "Synthetic exhausted allowance")
        raise RuntimeError("Synthetic startup failure")

    monkeypatch.setattr(OriginServer, "close", close)
    target = {
        "expiry": "research_swarm.workers.timed_launch.LiveTimingOrigin.check",
        "thread": "research_swarm.workers.timed_supervision.threading.Thread.start",
        "spawn": "research_swarm.workers.supervisor.spawn_supervisor",
    }[failure]
    monkeypatch.setattr(target, fail)
    completed = worker.execute(row["id"])
    entry = worker.journal.get(row["id"])
    assert completed["state"] == "not_started"
    assert entry.state == JournalState.NOT_STARTED and entry.timing is not None
    assert entry.supervisor_id is None and entry.native is None
    assert worker.resources.availability() == {"cpu": 1}
    if failure != "expiry":
        assert closed and all(server._listener.fileno() == -1 for server in closed)


def test_lost_startup_handle_after_claim_cannot_certify_native_absence(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, _ = native_worker(pipeline, tmp_path)
    row = worker.claim(pipeline.ready())
    supervisor_id = new_id()

    def claimed(journal: WorkerJournal, attempt_id: str, origin: LiveTimingOrigin) -> Any:
        journal.claim_timed_launch(attempt_id, supervisor_id, origin)
        raise OSError("Synthetic loss after claim; native creation is unknown")

    ticks = iter([0, 20])
    monkeypatch.setattr("research_swarm.workers.local.start_timed_supervisor", claimed)
    monkeypatch.setattr("research_swarm.workers.local.continuous_time", lambda: next(ticks))
    completed = worker.execute(row["id"])
    entry = worker.journal.get(row["id"])
    assert completed["state"] == "execution_unknown"
    assert entry.state == JournalState.UNKNOWN and entry.supervisor_id == supervisor_id
    assert entry.cancel_requested and entry.outcome is None
    assert worker.resources.availability() == {"cpu": 0}


@pytest.mark.parametrize("phase", ["claim", "release"])
def test_clock_is_rechecked_inside_journal_transaction_after_lock_wait(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    worker, _ = native_worker(pipeline, tmp_path)
    row = worker.claim(pipeline.ready())
    entry = worker._prepare(
        AttemptData.model_validate(row["data"]), TrialData.model_validate(pipeline.trial["data"])
    )
    origin = worker._timed_origin(entry)
    identifier = new_id()
    if phase == "release":
        worker.journal.claim_timed_launch(row["id"], identifier, origin)
        worker.journal.held(
            row["id"],
            identifier,
            NativeIdentity(
                backend="posix_group", pid=123, start_token="fixture", scope_id=new_id()
            ),
        )
    before = worker.journal.get(row["id"])
    assert before.timing is not None
    current = origin.sampler.read()
    entered = threading.Event()
    expired = threading.Event()
    connect = worker.journal._connect

    def waiting() -> Any:
        entered.set()
        return connect()

    def read() -> Any:
        if expired.is_set():
            elapsed = int(before.timing.caps.launch_ns) - current.continuous_ns + 1_000_000_000
            return replace(
                current,
                continuous_ns=current.continuous_ns + elapsed,
                awake_ns=current.awake_ns + elapsed,
                wall_ns=current.wall_ns + elapsed,
            )
        return current

    monkeypatch.setattr(worker.journal, "_connect", waiting)
    monkeypatch.setattr(origin.sampler, "read", read)
    gate = (
        worker.journal.claim_timed_launch
        if phase == "claim"
        else worker.journal.authorize_timed_release
    )
    with connect() as blocker, ThreadPoolExecutor(max_workers=1) as pool:
        blocker.execute("BEGIN IMMEDIATE")
        future = pool.submit(gate, row["id"], identifier, origin)
        try:
            assert entered.wait(5)
            assert not future.done()
            expired.set()
        finally:
            blocker.rollback()
        with pytest.raises(ResearchError):
            future.result(timeout=5)
    assert worker.journal.get(row["id"]) == before


@pytest.mark.parametrize("phase", ["claim", "release"])
def test_lost_permit_reply_never_releases_work_or_relaunches(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    worker, _ = native_worker(pipeline, tmp_path)
    row = worker.claim(pipeline.ready())
    method = "claim_timed_launch" if phase == "claim" else "authorize_timed_release"
    authorize = getattr(WorkerJournal, method)

    def committed_then_lost(self: WorkerJournal, *args: Any) -> Any:
        authorize(self, *args)
        raise OSError("Synthetic lost permit acknowledgment")

    monkeypatch.setattr(WorkerJournal, method, committed_then_lost)
    completed = worker.execute(row["id"])
    entry = worker.journal.get(row["id"])
    assert completed["state"] in {"not_started", "failed"}
    assert entry.state in {JournalState.NOT_STARTED, JournalState.CLOSED}
    if phase == "claim":
        assert entry.native is None and entry.outcome is None
    else:
        assert entry.native is not None and entry.outcome is not None
        assert entry.outcome.released_at is None and entry.outcome.closure.scope_closed
    assert worker.execute(row["id"]) == completed
    assert worker.resources.availability() == {"cpu": 1}


def test_cancellation_at_release_keeps_real_native_child_held_until_closed(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, _ = native_worker(pipeline, tmp_path)
    row = worker.claim(pipeline.ready())
    authorize = WorkerJournal.authorize_timed_release

    def cancel_first(self: WorkerJournal, attempt_id: str, *args: Any) -> Any:
        self.request_cancel(attempt_id, 1)
        return authorize(self, attempt_id, *args)

    monkeypatch.setattr(WorkerJournal, "authorize_timed_release", cancel_first)
    completed = worker.execute(row["id"])
    entry = worker.journal.get(row["id"])
    assert completed["state"] == "not_started"
    assert entry.outcome is not None and entry.outcome.released_at is None
    assert entry.outcome.closure.scope_closed and entry.release_requested_at is None
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


@pytest.mark.parametrize("phase", ["claim", "release", "running"])
def test_actual_origin_process_death_preserves_native_phase_and_fixed_deadline(
    tmp_path: Path,
    phase: str,
) -> None:
    from tests.domain.test_assignment_signatures import identifier

    process = subprocess.Popen(
        [sys.executable, "-c", ORIGIN_PROCESS, str(tmp_path), phase],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        until = time.monotonic() + 10
        while not (tmp_path / "ready").exists() and process.poll() is None:
            assert time.monotonic() < until, "Origin never reached the requested phase"
            time.sleep(0.01)
        if process.poll() is not None:
            _, stderr = process.communicate(timeout=1)
            pytest.fail(stderr.decode("utf-8", errors="replace"))
        journal = WorkerJournal(tmp_path / "journal.sqlite")
        before = journal.get(identifier(10))
        assert (
            before.state
            == {
                "claim": JournalState.RECEIVED,
                "release": JournalState.HELD,
                "running": JournalState.RUNNING,
            }[phase]
        )
        process.kill()  # This test owns the origin; never signal a recovered native PID.
        process.communicate(timeout=5)
        assert before.timing is not None
        clock = native_clock()
        assert clock.profile == before.timing.caps.origin_profile
        # Closure may consume the declared cleanup allowance after the hard cap.
        # Two seconds allow the closed record to become observable to this process.
        observation_ns = (
            int(before.timing.caps.hard_ns) + before.command.cleanup_ms * 1_000_000 + 2_000_000_000
        )
        while True:
            entry = journal.get(identifier(10))
            if entry.state in {JournalState.CLOSED, JournalState.NOT_STARTED}:
                break
            assert clock.sample().continuous_ns < observation_ns, entry
            time.sleep(0.025)
        assert entry.timing == before.timing
        if phase == "claim":
            assert entry.state == JournalState.NOT_STARTED
            assert entry.native is None and entry.outcome is None
        else:
            assert entry.outcome is not None and entry.outcome.closure.scope_closed
            if phase == "release":
                assert entry.outcome.released_at is None and not (tmp_path / "ran").exists()
            else:
                assert (tmp_path / "ran").exists() and entry.outcome.reason == "deadline"
                assert entry.outcome.released_at is not None
                elapsed_bound_ms = (
                    observation_ns - int(before.timing.qualified_at.continuous_ns)
                ) / 1_000_000
                assert entry.outcome.elapsed_ms < elapsed_bound_ms
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)
