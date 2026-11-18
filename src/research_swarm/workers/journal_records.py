"""Portable worker evidence records, independent of the native journal database."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import Field, model_validator

from research_swarm.domain.assignments import SignedAssignment
from research_swarm.domain.experiments import DispatchEnvelope, ExperimentSpec
from research_swarm.domain.types import ID, Revision, Timestamp, VersionedRecord, utc_now
from research_swarm.workers.contracts import NativeCommand, NativeIdentity, NativeOutcome
from research_swarm.workers.prepared import PreparedExecution


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
    clock_mode: Literal["local-utc-v1"] = "local-utc-v1"
    assignment_time: None = None
    timing: None = None
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
