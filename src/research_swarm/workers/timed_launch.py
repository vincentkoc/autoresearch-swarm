"""Portable, durable clock evidence. Reconstructed records never prove a live origin."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from pydantic import AfterValidator, Field, model_validator

from research_swarm.crypto import Ed25519Provider
from research_swarm.domain.assignments import AssignmentAuthority, SignedAssignment
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.clocks import ClockChallenge, SignedAssignmentTime, SignedClockReply
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Digest, Name, VersionedRecord
from research_swarm.workers.clock_estimates import ClockEstimate, ClockSampler, DeadlineCaps
from research_swarm.workers.clock_signatures import ClockVerifier, VerifiedAssignmentTime
from research_swarm.workers.clocks import ClockReading


def _unsigned_counter(value: str) -> str:
    if str(int(value)) != value:
        raise ValueError("Clock counter is not a canonical decimal string")
    if int(value) > 2**64 - 1:
        raise ValueError("Clock counter exceeds the unsigned 64-bit range")
    return value


# Nanosecond uptime crosses JCS's safe-integer ceiling after about 104 days.
Counter = Annotated[
    str,
    Field(strict=True, pattern=r"^(0|[1-9][0-9]*)$", max_length=20),
    AfterValidator(_unsigned_counter),
]


class ReadingRecord(VersionedRecord):
    continuous_ns: Counter
    awake_ns: Counter
    wall_ns: Counter
    read_uncertainty_ns: Counter

    @classmethod
    def capture(cls, reading: ClockReading) -> ReadingRecord:
        return cls(
            continuous_ns=str(reading.continuous_ns),
            awake_ns=str(reading.awake_ns),
            wall_ns=str(reading.wall_ns),
            read_uncertainty_ns=str(reading.read_uncertainty_ns),
        )

    def reading(self) -> ClockReading:
        return ClockReading(
            int(self.continuous_ns),
            int(self.awake_ns),
            int(self.wall_ns),
            int(self.read_uncertainty_ns),
        )


class CapsRecord(VersionedRecord):
    clock_epoch: ID
    assignment_digest: Digest
    origin_session_id: ID
    origin_profile: Name
    launch_ns: Counter
    hard_ns: Counter

    @model_validator(mode="after")
    def launch_before_hard(self) -> CapsRecord:
        if int(self.launch_ns) > int(self.hard_ns):
            raise ValueError("Launch deadline exceeds the hard deadline")
        return self

    @classmethod
    def capture(cls, caps: DeadlineCaps) -> CapsRecord:
        return cls(
            clock_epoch=caps.clock_epoch,
            assignment_digest=caps.assignment_digest,
            origin_session_id=caps.origin_session_id,
            origin_profile=caps.origin_profile,
            launch_ns=str(caps.launch_ns),
            hard_ns=str(caps.hard_ns),
        )

    def caps(self) -> DeadlineCaps:
        return DeadlineCaps(
            self.clock_epoch,
            self.assignment_digest,
            self.origin_session_id,
            self.origin_profile,
            int(self.launch_ns),
            int(self.hard_ns),
        )


class TimingEvidence(VersionedRecord):
    reply: SignedClockReply
    sent: ReadingRecord
    received: ReadingRecord
    qualified_at: ReadingRecord
    caps: CapsRecord

    @property
    def digest(self) -> str:
        return record_digest("research.worker-timing", self.model_dump(mode="json"))


@dataclass(frozen=True)
class VerifiedTiming:
    """Verified historical evidence. Fresh live-origin authorization is still required."""

    timed: VerifiedAssignmentTime
    estimate: ClockEstimate
    evidence: TimingEvidence


class LiveTimingOrigin:
    """Live sampler ownership; a historical record cannot supply this gate."""

    def __init__(self, sampler: ClockSampler, verified: VerifiedTiming) -> None:
        self.sampler, self.verified = sampler, verified
        if (
            sampler.origin_session_id != verified.estimate.origin_session_id
            or sampler.origin_profile != verified.estimate.origin_profile
            or sampler.verifier.authority.binding != verified.timed.grant.payload.binding
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock evidence has another live origin")

    def check(self, evidence: TimingEvidence) -> DeadlineCaps:
        if evidence != self.verified.evidence:
            raise ResearchError(ErrorCode.STALE_FENCE, "Frozen clock evidence changed")
        fresh = capture_timing(
            self.verified.timed,
            self.verified.estimate,
            self.sampler.read(),
            prior=evidence.caps.caps(),
        )
        return fresh.evidence.caps.caps()


def capture_timing(
    timed: VerifiedAssignmentTime,
    estimate: ClockEstimate,
    now: ClockReading,
    *,
    prior: DeadlineCaps | None = None,
) -> VerifiedTiming:
    computed = estimate.deadlines(timed, now)
    caps = computed if prior is None else prior.intersect(computed)
    if min(caps.launch_ns, caps.hard_ns) <= now.continuous_ns:
        raise ResearchError(ErrorCode.STALE_FENCE, "Frozen clock allowance is exhausted")
    return VerifiedTiming(
        timed,
        estimate,
        TimingEvidence(
            reply=estimate.verified.reply,
            sent=ReadingRecord.capture(estimate.sent),
            received=ReadingRecord.capture(estimate.received),
            qualified_at=ReadingRecord.capture(now),
            caps=CapsRecord.capture(caps),
        ),
    )


def verify_timing(
    evidence: TimingEvidence,
    authority: AssignmentAuthority,
    assignment_time: SignedAssignmentTime,
    assignment: SignedAssignment,
    *,
    crypto_provider: Ed25519Provider | None = None,
) -> VerifiedTiming:
    """Verify signature and conservative arithmetic, never current liveness or freshness."""
    evidence = TimingEvidence.model_validate(evidence)
    verifier = ClockVerifier(authority, crypto_provider=crypto_provider)
    timed = verifier.binding(assignment_time, assignment)
    challenge = ClockChallenge(
        worker_id=authority.binding.worker_id,
        session_id=authority.binding.worker_session_id,
        nonce=evidence.reply.payload.nonce,
    )
    estimate = ClockEstimate(
        verifier.reply(evidence.reply, challenge),
        evidence.sent.reading(),
        evidence.received.reading(),
        evidence.caps.origin_session_id,
        evidence.caps.origin_profile,
    )
    verified = capture_timing(
        timed, estimate, evidence.qualified_at.reading(), prior=evidence.caps.caps()
    )
    if verified.evidence != evidence:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Clock evidence overstates allowance")
    return verified
