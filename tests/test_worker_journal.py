from __future__ import annotations

import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import (
    DispatchEnvelope,
    ExperimentSpec,
    MetricDefinition,
    SourceRef,
    StageSpec,
)
from research_swarm.domain.types import new_id
from research_swarm.workers.authority import (
    DispatchVerifier,
    VerifiedDispatch,
    WorkerBinding,
    authorize_dispatch,
)
from research_swarm.workers.contracts import NativeCommand
from research_swarm.workers.journal import JournalState, WorkerJournal
from research_swarm.workers.supervisor import reconcile_outcome, run_attempt, spawn_supervisor


def dispatch(
    tmp_path: Path, code: str = "print('workload')"
) -> tuple[VerifiedDispatch, NativeCommand]:
    digest = content_digest(b"synthetic")
    stage = StageSpec(id="train", kind="train", argv=(sys.executable, "-c", code), timeout_ms=4000)
    spec = ExperimentSpec(
        workspace_id=new_id(),
        project_id=new_id(),
        campaign_id=new_id(),
        candidate_id=new_id(),
        source=SourceRef(artifact_id=new_id(), tree_digest=digest),
        protocol_digest=digest,
        comparison_class="cpu-v1",
        environment_digest=digest,
        evaluator_contract_digest=digest,
        data_manifest_digest=digest,
        metrics=(MetricDefinition(name="loss", unit="mse", direction="minimize"),),
        stages=(stage,),
        seed=7,
        policy_version="test-v1",
    ).sealed()
    binding = WorkerBinding(
        workspace_id=spec.workspace_id,
        controller_id=new_id(),
        worker_id=new_id(),
        worker_session_id=new_id(),
        control_epoch=1,
    )
    expiry = (
        (datetime.now(UTC) + timedelta(minutes=5))
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    envelope = DispatchEnvelope(
        **binding.model_dump(exclude={"schema_version"}),
        campaign_id=spec.campaign_id,
        trial_id=spec.trial_id,
        job_id=new_id(),
        attempt_id=new_id(),
        spec_digest=spec.spec_digest,
        fencing_generation=1,
        reservation_ids=(new_id(),),
        stage_id=stage.id,
        expires_at=expiry,
        hard_deadline_at=expiry,
        launch_permit_digest=digest,
    )
    key = b"synthetic-test-authority-key-32-bytes"
    envelope = authorize_dispatch(envelope, key)
    verified = DispatchVerifier(binding, key).verify(envelope, spec)
    command = NativeCommand(argv=stage.argv, cwd=str(tmp_path), timeout_ms=4000, grace_ms=100)
    return verified, command


def test_dispatch_requires_authentication_and_enrolled_identity(tmp_path: Path) -> None:
    verified, _ = dispatch(tmp_path)
    envelope, spec = verified.envelope, verified.spec
    binding = WorkerBinding.model_validate(
        {key: getattr(envelope, key) for key in WorkerBinding.model_fields}
    )
    verifier = DispatchVerifier(binding, b"synthetic-test-authority-key-32-bytes")
    forged = envelope.model_copy(update={"worker_id": new_id()})
    with pytest.raises(ResearchError) as failure:
        verifier.verify(forged, spec)
    assert failure.value.code == ErrorCode.AUTHORIZATION_DENIED
    stale = DispatchVerifier(
        binding.model_copy(update={"control_epoch": 2}), b"synthetic-test-authority-key-32-bytes"
    )
    with pytest.raises(ResearchError, match="enrolled authority"):
        stale.verify(envelope, spec)
    with pytest.raises(ResearchError, match="expired"):
        verifier.verify(envelope, spec, authority_now="2999-01-01T00:00:00.000Z")


def test_duplicate_dispatch_preserves_original_record_and_changed_input_conflicts(
    tmp_path: Path,
) -> None:
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    verified, command = dispatch(tmp_path)
    original = journal.receive(verified, command)
    assert WorkerJournal(journal.path).receive(verified, command) == original
    with pytest.raises(ResearchError, match="changed"):
        journal.receive(verified, command.model_copy(update={"output_limit_bytes": 123}))
    assert len(journal.pending()) == 1


def test_racing_supervisors_execute_one_workload(tmp_path: Path) -> None:
    marker = tmp_path / "executions"
    verified, command = dispatch(tmp_path, f"open({str(marker)!r},'a').write('once\\n')")
    journal = WorkerJournal(tmp_path / "worker" / "worker.sqlite")
    entry = journal.receive(verified, command)
    first = spawn_supervisor(journal, entry.dispatch.attempt_id)
    second = spawn_supervisor(journal, entry.dispatch.attempt_id)
    assert sorted([first.wait(timeout=15), second.wait(timeout=15)]) == [0, 2]
    assert marker.read_text().splitlines() == ["once"]
    recovered = WorkerJournal(journal.path).get(entry.dispatch.attempt_id)
    assert recovered.state == JournalState.CLOSED and recovered.outcome is not None
    assert recovered.dispatch == entry.dispatch


def test_cancel_before_launch_has_no_fabricated_result(tmp_path: Path) -> None:
    verified, command = dispatch(tmp_path, "raise SystemExit('must not execute')")
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    entry = journal.receive(verified, command)
    journal.request_cancel(entry.dispatch.attempt_id, 10)
    result = run_attempt(journal, entry.dispatch.attempt_id)
    assert result.state == JournalState.NOT_STARTED
    assert result.outcome is None and result.native is None
    assert not journal.unresolved()


def test_launch_gap_never_restarts_the_attempt(tmp_path: Path) -> None:
    verified, command = dispatch(tmp_path)
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    entry = journal.receive(verified, command)
    journal.claim_launch(entry.dispatch.attempt_id, new_id())
    recovered = WorkerJournal(journal.path)
    unknown = reconcile_outcome(recovered, entry.dispatch.attempt_id)
    assert unknown.state == JournalState.LAUNCHING
    assert unknown.finding == "No durable native outcome is available"
    with pytest.raises(ResearchError, match="launched again"):
        run_attempt(recovered, entry.dispatch.attempt_id)


def test_outcome_recovers_after_publication_failure_without_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "once"
    verified, command = dispatch(tmp_path, f"open({str(marker)!r},'a').write('once\\n')")
    journal = WorkerJournal(tmp_path / "worker" / "worker.sqlite")
    entry = journal.receive(verified, command)
    original = journal.record_outcome

    def failed(*args: object, **kwargs: object) -> None:
        raise OSError("injected publication failure")

    monkeypatch.setattr(journal, "record_outcome", failed)
    assert run_attempt(journal, entry.dispatch.attempt_id).state == JournalState.UNKNOWN
    monkeypatch.setattr(journal, "record_outcome", original)
    closed = reconcile_outcome(journal, entry.dispatch.attempt_id)
    assert closed.state == JournalState.CLOSED
    assert marker.read_text().splitlines() == ["once"]
    assert reconcile_outcome(journal, entry.dispatch.attempt_id) == closed
    pending = journal.pending()
    for event in pending:
        journal.acknowledge(event.sequence)
        journal.acknowledge(event.sequence)
    assert not WorkerJournal(journal.path).pending()


def test_separate_supervisor_keeps_deadline_when_transport_process_dies(tmp_path: Path) -> None:
    marker = tmp_path / "started"
    verified, command = dispatch(
        tmp_path, f"import time; open({str(marker)!r},'w').write('started'); time.sleep(60)"
    )
    command = command.model_copy(update={"timeout_ms": 800})
    journal = WorkerJournal(tmp_path / "worker" / "worker.sqlite")
    entry = journal.receive(verified, command)
    code = (
        "import os; from pathlib import Path; "
        "from research_swarm.workers.journal import WorkerJournal; "
        "from research_swarm.workers.supervisor import spawn_supervisor; "
        f"spawn_supervisor(WorkerJournal(Path({str(journal.path)!r})), "
        f"{entry.dispatch.attempt_id!r}); "
        "os._exit(0)"
    )
    transport = subprocess.Popen([sys.executable, "-c", code])
    assert transport.wait(timeout=10) == 0
    until = time.monotonic() + 10
    while time.monotonic() < until:
        current = journal.get(entry.dispatch.attempt_id)
        if current.state in {JournalState.CLOSED, JournalState.UNKNOWN}:
            break
        time.sleep(0.05)
    assert marker.exists()
    assert current.state == JournalState.CLOSED
    assert current.outcome is not None and current.outcome.reason == "deadline"


def test_reconciliation_does_not_abort_a_live_supervisor_behind_its_barrier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    verified, command = dispatch(tmp_path)
    journal = WorkerJournal(tmp_path / "worker.sqlite")
    entry = journal.receive(verified, command)
    held = threading.Event()
    resume = threading.Event()
    original = journal.authorize_release
    completed = []

    def paused_release(attempt_id: str, supervisor_id: str) -> bool:
        held.set()
        assert resume.wait(5)
        return original(attempt_id, supervisor_id)

    monkeypatch.setattr(journal, "authorize_release", paused_release)
    runner = threading.Thread(
        target=lambda: completed.append(run_attempt(journal, entry.dispatch.attempt_id))
    )
    runner.start()
    try:
        assert held.wait(5)
        observed = reconcile_outcome(WorkerJournal(journal.path), entry.dispatch.attempt_id)
        assert observed.state == JournalState.HELD
    finally:
        resume.set()
        runner.join(timeout=10)
    assert not runner.is_alive()
    assert completed[0].state == JournalState.CLOSED
    assert completed[0].outcome.reason == "exited"
