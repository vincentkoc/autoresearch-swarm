"""A dedicated local supervisor survives loss of the controller transport process."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.workers.clock_channel import Bootstrap, SupervisorChannel
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
    if sys.platform != "win32":
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def run_attempt(
    journal: WorkerJournal,
    attempt_id: str,
    *,
    channel: SupervisorChannel | None = None,
) -> JournalEntry:
    received = journal.settle_unstarted(attempt_id)
    if received.state == JournalState.NOT_STARTED:
        return received
    if received.clock_mode != "local-utc-v1":
        from research_swarm.workers.timed_supervision import channel_scope

        if channel is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "Calibrated supervisor requires a live origin channel",
            )
        if channel.scope != channel_scope(journal, received):
            raise ResearchError(ErrorCode.STALE_FENCE, "Supervisor channel differs from journal")
    elif channel is not None:
        raise ResearchError(
            ErrorCode.STALE_FENCE, "Local supervisor cannot consume a timed channel"
        )
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
    launch_deadline_ns = None
    if channel is not None:
        supervisor_id = channel.supervisor_id
        try:
            permit = channel.request("claim")
            entry = journal.get(attempt_id)
            if not permit.allowed and entry.state == JournalState.NOT_STARTED:
                return entry
            if (
                not permit.allowed
                or entry.state != JournalState.LAUNCHING
                or entry.supervisor_id != supervisor_id
                or entry.revision < permit.journal_revision
                or entry.timing is None
                or entry.timing.caps != permit.caps
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Timed launch acknowledgment differs")
            hard_deadline_ns = int(permit.caps.hard_ns)
            launch_deadline_ns = int(permit.caps.launch_ns)
        except (OSError, EOFError, ValueError, ResearchError):
            # No native creation has been attempted on this control path.
            journal.abort_unstarted_claim(
                attempt_id, supervisor_id, timing_digest=channel.scope.timing_digest
            )
            raise
    else:
        hard_deadline = datetime.fromisoformat(
            received.dispatch.hard_deadline_at.replace("Z", "+00:00")
        )
        anchor_ns = native_clock().continuous_ns()
        remaining_ms = max(0, int((hard_deadline - datetime.now(UTC)).total_seconds() * 1000))
        # Freeze local UTC conversion once; preparation and release consume this cap.
        hard_deadline_ns = anchor_ns + remaining_ms * 1_000_000
        supervisor_id = new_id()
        entry = journal.claim_launch(attempt_id, supervisor_id)
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
        if channel is None:
            return journal.authorize_release(attempt_id, supervisor_id)
        permit = channel.request("release")
        observed = journal.get(attempt_id)
        if not permit.allowed:
            return False
        if (
            observed.state != JournalState.RELEASING
            or observed.supervisor_id != supervisor_id
            or observed.revision < permit.journal_revision
            or observed.timing is None
            or observed.timing.caps != permit.caps
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Timed release acknowledgment differs")
        return True

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
    receipt = _attempt_root(journal, attempt_id) / "outcome.json"
    if not receipt.is_file():
        return journal.note_reconciliation(attempt_id, "No durable native outcome is available")
    if receipt.stat().st_size > 1024 * 1024:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Native outcome exceeds its size limit")
    outcome = NativeOutcome.model_validate_json(receipt.read_bytes())
    return journal.record_outcome(attempt_id, entry.supervisor_id, outcome)


def spawn_supervisor(
    journal: WorkerJournal,
    attempt_id: str,
    *,
    clock_stdin: BinaryIO | None = None,
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
    if clock_stdin is not None:
        argv.append("--clock-channel")
    creationflags = 0
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    return subprocess.Popen(
        argv,
        stdin=clock_stdin if clock_stdin is not None else subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=base_environment(),
        start_new_session=sys.platform != "win32",
        creationflags=creationflags,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--attempt", required=True)
    parser.add_argument("--clock-channel", action="store_true")
    args = parser.parse_args()
    channel = None
    try:
        journal = WorkerJournal(args.journal)
        if args.clock_channel:
            from research_swarm.workers.timed_supervision import channel_scope

            scope = channel_scope(journal, journal.get(args.attempt))
            channel = SupervisorChannel(
                Bootstrap.read(sys.stdin.buffer),
                scope,
                new_id(),
                deadline_ns=int(scope.caps.launch_ns),
            )
        entry = run_attempt(journal, args.attempt, channel=channel)
    except (OSError, EOFError, ValueError, ResearchError):
        return 2
    finally:
        if channel is not None:
            channel.close()
    return 0 if entry.state in {JournalState.CLOSED, JournalState.NOT_STARTED} else 3


if __name__ == "__main__":
    raise SystemExit(main())
