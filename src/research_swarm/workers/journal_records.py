"""Portable worker evidence records, independent of the native journal database."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field, model_validator

from research_swarm.domain.assignments import SignedAssignment
from research_swarm.domain.clocks import ClockMode, SignedAssignmentTime, timed_assignment_digest
from research_swarm.domain.experiments import DispatchEnvelope, ExperimentSpec
from research_swarm.domain.types import ID, Revision, Timestamp, VersionedRecord, utc_now
from research_swarm.workers.contracts import NativeCommand, NativeIdentity, NativeOutcome
from research_swarm.workers.prepared import PreparedExecution
from research_swarm.workers.timed_launch import TimingEvidence


class JournalState(StrEnum):
    RECEIVED = "received"
    LAUNCHING = "launching"
    HELD = "held"
    RELEASING = "releasing"
    RUNNING = "running"
    CLOSED = "closed"
    NOT_STARTED = "not_started"
    UNKNOWN = "execution_unknown"


class JournalEntry(VersionedRecord):
    dispatch: DispatchEnvelope
    spec: ExperimentSpec
    command: NativeCommand
    signed_assignment: SignedAssignment | None = None
    clock_mode: ClockMode = "local-utc-v1"
    assignment_time: SignedAssignmentTime | None = None
    timing: TimingEvidence | None = None
    prepared: PreparedExecution | None = None
    preparation_required: bool = False
    revision: Revision = 1
    state: JournalState = JournalState.RECEIVED
    supervisor_id: ID | None = None
    native: NativeIdentity | None = None
    cancel_requested: bool = False
    received_at: Timestamp = Field(default_factory=utc_now)
    release_requested_at: Timestamp | None = None
    outcome: NativeOutcome | None = None
    last_controller_sequence: Revision = 0
    finding: str = ""

    @model_validator(mode="after")
    def assignment_matches(self) -> JournalEntry:
        if self.clock_mode == "local-utc-v1":
            if self.assignment_time is not None or self.timing is not None:
                raise ValueError("Local journal cannot carry calibrated clock authority")
        else:
            if self.signed_assignment is None or self.assignment_time is None:
                raise ValueError("Calibrated journal requires signed time authority")
            claimed = self.state in {
                JournalState.LAUNCHING,
                JournalState.HELD,
                JournalState.RELEASING,
                JournalState.RUNNING,
                JournalState.CLOSED,
            }
            native_evidence = self.native is not None or self.outcome is not None
            if (claimed or native_evidence) and self.supervisor_id is None:
                raise ValueError("Calibrated native evidence requires a supervisor identity")
            if (
                claimed or native_evidence or self.supervisor_id is not None
            ) and self.timing is None:
                raise ValueError("Calibrated supervisor requires frozen clock evidence")
            grant = self.assignment_time.payload
            if grant.assignment_digest != timed_assignment_digest(self.signed_assignment):
                raise ValueError("Journal time binding differs from its signed assignment")
            if self.timing is not None and (
                self.timing.caps.assignment_digest != grant.assignment_digest
                or self.timing.caps.clock_epoch != grant.clock_epoch
                or self.timing.reply.payload.clock_epoch != grant.clock_epoch
                or self.timing.reply.payload.binding != grant.binding
                or self.timing.reply.payload.policy.digest != grant.policy_digest
            ):
                raise ValueError("Journal timing differs from its admitted clock authority")
        if self.signed_assignment is None:
            return self
        payload = self.signed_assignment.payload
        if (
            self.prepared is not None
            and "registered-python-v1" in payload.profile.features
            and self.prepared.engine is None
        ):
            raise ValueError("Registered assignment requires its prepared engine binding")
        if payload.dispatch != self.dispatch or payload.spec != self.spec:
            raise ValueError("Journal differs from its signed assignment")
        if self.prepared is not None and (
            self.prepared.profile_digest != payload.profile.execution_fingerprint
            or {
                name: (value.digest, value.size_bytes)
                for name, value in self.prepared.context.inputs.items()
            }
            != {name: (value.digest, value.size_bytes) for name, value in payload.inputs.items()}
        ):
            raise ValueError("Prepared inputs or profile differ from signed assignment")
        return self


class JournalEvent(VersionedRecord):
    sequence: int
    attempt_id: ID
    type: str
    entry: JournalEntry
