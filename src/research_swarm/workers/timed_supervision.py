"""Bind the live sampling process to journal decisions over the private channel."""

from __future__ import annotations

import subprocess
import threading
from dataclasses import dataclass

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.workers.clock_channel import (
    ChannelScope,
    GateDecision,
    GateRequest,
    OriginServer,
    bootstrap_stdin,
)
from research_swarm.workers.journal import JournalEntry, JournalState, WorkerJournal
from research_swarm.workers.timed_launch import LiveTimingOrigin


def channel_scope(journal: WorkerJournal, entry: JournalEntry) -> ChannelScope:
    if (
        entry.clock_mode != "bounded-continuous-v1"
        or entry.timing is None
        or entry.assignment_time is None
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Timed supervisor lacks frozen clock authority")
    return ChannelScope(
        binding=entry.assignment_time.payload.binding,
        attempt_id=entry.dispatch.attempt_id,
        assignment_digest=entry.timing.caps.assignment_digest,
        timing_digest=entry.timing.digest,
        journal_identity=journal.identity,
        origin_session_id=entry.timing.caps.origin_session_id,
        caps=entry.timing.caps,
    )


@dataclass
class TimedSupervisor:
    process: subprocess.Popen[bytes]
    server: OriginServer
    thread: threading.Thread
    errors: list[str]

    def close(self) -> None:
        self.server.close()
        # Journal acquisition is bounded by its ten-second SQLite busy timeout.
        self.thread.join(timeout=11)
        if self.thread.is_alive():
            raise ResearchError(
                ErrorCode.EXECUTION_UNKNOWN, "Clock admission thread remains active"
            )


def start_timed_supervisor(
    journal: WorkerJournal,
    attempt_id: str,
    origin: LiveTimingOrigin,
) -> TimedSupervisor:
    from research_swarm.workers.supervisor import spawn_supervisor

    entry = journal.get(attempt_id)
    scope = channel_scope(journal, entry)
    assert entry.timing is not None
    origin.check(entry.timing)
    server = OriginServer(scope, deadline_ns=int(scope.caps.launch_ns))
    errors: list[str] = []

    def authorize(request: GateRequest) -> GateDecision:
        if request.scope != scope:
            raise ResearchError(ErrorCode.STALE_FENCE, "Private clock admission differs")
        observed = (
            journal.claim_timed_launch(attempt_id, request.supervisor_id, origin)
            if request.phase == "claim"
            else journal.authorize_timed_release(attempt_id, request.supervisor_id, origin)
        )
        assert observed.timing is not None
        return GateDecision(
            allowed=observed.state
            == (JournalState.LAUNCHING if request.phase == "claim" else JournalState.RELEASING),
            journal_revision=observed.revision,
            caps=observed.timing.caps,
        )

    def serve() -> None:
        try:
            server.serve(authorize)
        except (OSError, EOFError, ValueError, ResearchError) as error:
            errors.append(
                error.code.value if isinstance(error, ResearchError) else type(error).__name__
            )

    thread = threading.Thread(target=serve, name="research-clock-origin", daemon=True)
    try:
        thread.start()
        with bootstrap_stdin(server.bootstrap) as stdin:
            process = spawn_supervisor(journal, attempt_id, clock_stdin=stdin)
    except BaseException:
        server.close()
        if thread.ident is not None:
            thread.join(timeout=11)
        raise
    return TimedSupervisor(process, server, thread, errors)
