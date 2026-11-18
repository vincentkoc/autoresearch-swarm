"""A dedicated local supervisor survives loss of the controller transport process."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.projects.native_files import SourceReader
from research_swarm.workers.clocks import native_clock
from research_swarm.workers.contracts import NativeIdentity, NativeOutcome
from research_swarm.workers.journal import JournalEntry, JournalState, WorkerJournal
from research_swarm.workers.supervision import base_environment, run_native


def _attempt_root(journal: WorkerJournal, attempt_id: str) -> Path:
    # Resolve the ID through the validated journal record before using it as a path.
    identifier = journal.get(attempt_id).dispatch.attempt_id
    root = journal.path.parent / "executions" / identifier
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return root


def _save_outcome(root: Path, outcome: NativeOutcome) -> None:
    destination = root / "outcome.json"
    payload = outcome.model_dump_json().encode("utf-8")
    if destination.exists():
        if NativeOutcome.model_validate_json(destination.read_bytes()) != outcome:
            raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Saved native outcome changed")
        return
    staged = root / "outcome.pending"
    with staged.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(staged, destination)
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def run_attempt(
    journal: WorkerJournal,
    attempt_id: str,
) -> JournalEntry:
    received = journal.settle_unstarted(attempt_id)
    if received.state == JournalState.NOT_STARTED:
        return received
    # Translate the dispatch timestamps once, before preparation or lock waits.
    clock = native_clock()
    anchor_ns = clock.continuous_ns()
    wall = datetime.now(UTC)

    def local_deadline(timestamp: str) -> int:
        remaining = datetime.fromisoformat(timestamp) - wall
        return anchor_ns + max(0, remaining // timedelta(microseconds=1)) * 1000

    hard_deadline_ns = local_deadline(received.dispatch.hard_deadline_at)
    launch_deadline_ns = min(hard_deadline_ns, local_deadline(received.dispatch.expires_at))
    received.spec.verify()
    environment = base_environment()
    resources = None
    if received.prepared is not None:
        from research_swarm.workers.resources import MachineResources, ResourceDemand

        received.prepared.verify_launch(received.dispatch, received.spec)
        received.prepared.verify_context_file()
        resources = MachineResources(Path(received.prepared.resource_registry))
        resources.verify_held(
            received,
            received.prepared.machine_claim_id,
            expected=ResourceDemand(amounts=received.prepared.resource_amounts(received.spec)),
        )
        environment.update(received.prepared.environment(received.spec))
        environment["RESEARCH_CONTEXT"] = received.prepared.context_path
    supervisor_id = new_id()
    entry = journal.claim_launch(attempt_id, supervisor_id, launch_deadline_ns)
    if entry.state == JournalState.NOT_STARTED:
        return entry
    root = _attempt_root(journal, attempt_id)
    command = entry.command
    if entry.prepared is not None:
        command = entry.prepared.command(command)

    def held(native: NativeIdentity) -> bool:
        journal.held(attempt_id, supervisor_id, native)
        if resources is not None and entry.prepared is not None:
            entry.prepared.verify_launch(entry.dispatch, entry.spec)
            resources.verify_held(
                entry,
                entry.prepared.machine_claim_id,
                expected=ResourceDemand(amounts=entry.prepared.resource_amounts(entry.spec)),
            )
        return journal.authorize_release(attempt_id, supervisor_id, launch_deadline_ns)

    def released(_: str) -> None:
        journal.running(attempt_id, supervisor_id)

    try:
        outcome = run_native(
            command,
            root / "output",
            on_held=held,
            on_released=released,
            cancelled=lambda: journal.get(attempt_id).cancel_requested,
            environment=environment,
            hard_deadline_ns=hard_deadline_ns,
            launch_deadline_ns=launch_deadline_ns,
        )
        _save_outcome(root, outcome)
        return journal.record_outcome(attempt_id, supervisor_id, outcome)
    except Exception as error:
        # Do not infer physical closure from a launch/publication exception.
        return journal.unknown(
            attempt_id, supervisor_id, f"Supervisor failure: {type(error).__name__}"
        )


def reconcile_outcome(journal: WorkerJournal, attempt_id: str) -> JournalEntry:
    """Recover a durable closure receipt without launching or signalling anything."""
    entry = journal.get(attempt_id)
    if entry.state in {JournalState.CLOSED, JournalState.NOT_STARTED, JournalState.RECEIVED}:
        return entry
    assert entry.supervisor_id is not None
    root = journal.path.parent / "executions" / entry.dispatch.attempt_id
    try:
        with SourceReader(root) as reader, reader.open("outcome.json") as stream:
            content = stream.read(1024 * 1024 + 1)
    except FileNotFoundError:
        return journal.note_reconciliation(attempt_id, "No durable native outcome is available")
    if len(content) > 1024 * 1024:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Native outcome exceeds its size limit")
    outcome = NativeOutcome.model_validate_json(content)
    return journal.record_outcome(attempt_id, entry.supervisor_id, outcome)


def spawn_supervisor(
    journal: WorkerJournal,
    attempt_id: str,
) -> subprocess.Popen[bytes]:
    entry = journal.get(attempt_id)
    entry.spec.verify()
    argv = [
        sys.executable,
        "-m",
        "research_swarm.workers.supervisor",
        "--journal",
        str(journal.path.resolve()),
        "--attempt",
        entry.dispatch.attempt_id,
    ]
    return subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=base_environment(),
        start_new_session=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--attempt", required=True)
    args = parser.parse_args()
    try:
        entry = run_attempt(WorkerJournal(args.journal), args.attempt)
    except (OSError, EOFError, ValueError, ResearchError):
        return 2
    return 0 if entry.state in {JournalState.CLOSED, JournalState.NOT_STARTED} else 3


if __name__ == "__main__":
    raise SystemExit(main())
