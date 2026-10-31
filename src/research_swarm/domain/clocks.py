"""Authenticated authority-time records; time evidence alone never grants execution."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field

from research_swarm.domain.assignments import SignatureHex, SignedAssignment
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.types import ID, Digest, Name, VersionedRecord
from research_swarm.domain.workers import WorkerBinding

ClockMilliseconds = Annotated[int, Field(strict=True, ge=0, le=2**53 - 1)]
ClockMode = Literal["local-utc-v1", "bounded-continuous-v1"]


class ClockPolicy(VersionedRecord):
    """Admitted ratio bound: controller elapsed / worker elapsed is within 1 +/- ppm.

    This is not a per-clock accuracy claim or measured hardware qualification.
    """

    version: Literal["bounded-continuous-v1"] = "bounded-continuous-v1"
    maximum_roundtrip_ms: int = Field(default=1000, strict=True, ge=1, le=10_000)
    maximum_sample_age_ms: int = Field(default=30_000, strict=True, ge=1, le=300_000)
    maximum_uncertainty_ms: int = Field(default=2500, strict=True, ge=1, le=10_000)
    maximum_deadline_horizon_ms: int = Field(default=3_600_000, strict=True, ge=1, le=86_400_000)
    relative_drift_ppm: int = Field(default=1000, strict=True, ge=1, le=10_000)
    timestamp_guard_ms: int = Field(default=2, strict=True, ge=1, le=100)

    @property
    def digest(self) -> str:
        return record_digest("research.clock-policy", self.model_dump(mode="json"))


class AuthorityClockAdmission(VersionedRecord):
    """Enrollment-owned authority profile; never inferred from a sampled reply."""

    profile: Literal["hosted-native-witness-v1"] = "hosted-native-witness-v1"
    policy_digest: Digest


class ClockChallenge(VersionedRecord):
    worker_id: ID
    session_id: ID
    nonce: ID


class ClockReplyPayload(VersionedRecord):
    binding: WorkerBinding
    clock_epoch: ID
    nonce: ID
    authority_time_ms: ClockMilliseconds
    clock_profile: Name
    policy: ClockPolicy


class SignedClockReply(VersionedRecord):
    algorithm: Literal["ed25519-jcs-v1"] = "ed25519-jcs-v1"
    key_id: Digest
    payload: ClockReplyPayload
    signature: SignatureHex


class AssignmentTimePayload(VersionedRecord):
    binding: WorkerBinding
    clock_epoch: ID
    assignment_digest: Digest
    policy_digest: Digest


class SignedAssignmentTime(VersionedRecord):
    algorithm: Literal["ed25519-jcs-v1"] = "ed25519-jcs-v1"
    key_id: Digest
    payload: AssignmentTimePayload
    signature: SignatureHex


def timed_assignment_digest(assignment: SignedAssignment) -> str:
    return record_digest("research.timed-assignment", assignment.model_dump(mode="json"))
