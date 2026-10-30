"""Conservative controller-time intervals; these facts do not authorize native release."""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from datetime import UTC, datetime

from pydantic import TypeAdapter

from research_swarm.domain.assignments import AssignmentAuthority
from research_swarm.domain.clocks import ClockChallenge, SignedClockReply
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, new_id
from research_swarm.workers.clock_signatures import (
    ClockVerifier,
    VerifiedAssignmentTime,
    VerifiedClockReply,
)
from research_swarm.workers.clocks import (
    MAX_SAMPLE_UNCERTAINTY_NS,
    NATIVE_CLOCK_PROFILES,
    ClockReading,
    NativeClock,
    native_clock,
    require_continuity,
)

NS_PER_MS = 1_000_000
RATE_SCALE = 1_000_000


def timestamp_ms(value: str) -> int:
    delta = datetime.fromisoformat(value.replace("Z", "+00:00")) - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1000 + delta.microseconds // 1000


def ceiling_ratio(numerator: int, denominator: int) -> int:
    return -(-numerator // denominator)


@dataclass(frozen=True)
class TimeInterval:
    lower_ns: int
    upper_ns: int


@dataclass(frozen=True)
class DeadlineCaps:
    """Local counter facts. The native caller must still verify its live clock origin."""

    clock_epoch: str
    assignment_digest: str
    origin_session_id: str
    origin_profile: str
    launch_ns: int
    hard_ns: int

    def intersect(self, other: DeadlineCaps) -> DeadlineCaps:
        if (
            self.clock_epoch != other.clock_epoch
            or self.assignment_digest != other.assignment_digest
            or self.origin_session_id != other.origin_session_id
            or self.origin_profile != other.origin_profile
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Deadline authority differs")
        return DeadlineCaps(
            self.clock_epoch,
            self.assignment_digest,
            self.origin_session_id,
            self.origin_profile,
            min(self.launch_ns, other.launch_ns),
            min(self.hard_ns, other.hard_ns),
        )


@dataclass(frozen=True)
class ClockEstimate:
    verified: VerifiedClockReply
    sent: ClockReading
    received: ClockReading
    origin_session_id: str
    origin_profile: str

    def __post_init__(self) -> None:
        TypeAdapter(ID).validate_python(self.origin_session_id)
        if (
            self.verified.reply.payload.clock_profile not in NATIVE_CLOCK_PROFILES
            or self.origin_profile not in NATIVE_CLOCK_PROFILES
        ):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Authority clock profile is unsupported"
            )
        require_continuity(self.sent, self.received)
        elapsed = self.received.continuous_ns - self.sent.continuous_ns
        policy = self.verified.reply.payload.policy
        if elapsed > policy.maximum_roundtrip_ms * NS_PER_MS:
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock roundtrip exceeds its bound")
        self.interval(self.received)

    def interval(self, now: ClockReading) -> TimeInterval:
        require_continuity(self.received, now)
        payload = self.verified.reply.payload
        policy = payload.policy
        elapsed = now.continuous_ns - self.received.continuous_ns
        if elapsed > policy.maximum_sample_age_ms * NS_PER_MS:
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock sample is stale")
        roundtrip = self.received.continuous_ns - self.sent.continuous_ns
        guard = (
            policy.timestamp_guard_ms * NS_PER_MS
            + self.sent.read_uncertainty_ns
            + self.received.read_uncertainty_ns
            + now.read_uncertainty_ns
        )
        anchor = payload.authority_time_ms * NS_PER_MS
        bounds = TimeInterval(
            anchor - guard + elapsed * (RATE_SCALE - policy.relative_drift_ppm) // RATE_SCALE,
            anchor
            + guard
            + ceiling_ratio(
                (roundtrip + elapsed) * (RATE_SCALE + policy.relative_drift_ppm), RATE_SCALE
            ),
        )
        if bounds.upper_ns - bounds.lower_ns > policy.maximum_uncertainty_ms * NS_PER_MS:
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock interval is too uncertain")
        return bounds

    def deadlines(self, timed: VerifiedAssignmentTime, now: ClockReading) -> DeadlineCaps:
        payload, grant = self.verified.reply.payload, timed.grant.payload
        if (
            grant.binding != payload.binding
            or grant.clock_epoch != payload.clock_epoch
            or grant.policy_digest != payload.policy.digest
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Assignment time domain differs")
        assignment = timed.assignment.assignment.payload
        issued = timestamp_ms(assignment.issued_at) * NS_PER_MS
        expires = timestamp_ms(assignment.dispatch.expires_at) * NS_PER_MS
        hard = timestamp_ms(assignment.dispatch.hard_deadline_at) * NS_PER_MS
        current = self.interval(now)
        if current.lower_ns < issued or current.upper_ns >= min(expires, hard):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Assignment is outside its qualified interval"
            )
        if (
            hard - payload.authority_time_ms * NS_PER_MS
            > payload.policy.maximum_deadline_horizon_ms * NS_PER_MS
        ):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Assignment exceeds the qualified clock horizon"
            )
        # Recompute at the fixed receive anchor. Midpoint estimates and fresh
        # durations would overstate authority under asymmetric network delay.
        upper = self.interval(self.received).upper_ns + (
            MAX_SAMPLE_UNCERTAINTY_NS - self.received.read_uncertainty_ns
        )
        rate = RATE_SCALE + payload.policy.relative_drift_ppm

        def cap(deadline: int) -> int:
            return self.received.continuous_ns + (deadline - upper) * RATE_SCALE // rate

        launch = min(
            cap(expires),
            cap(hard),
            self.received.continuous_ns + payload.policy.maximum_sample_age_ms * NS_PER_MS,
        )
        hard_cap = cap(hard)
        if min(launch, hard_cap) <= now.continuous_ns:
            raise ResearchError(ErrorCode.STALE_FENCE, "No positive qualified launch allowance")
        return DeadlineCaps(
            payload.clock_epoch,
            grant.assignment_digest,
            self.origin_session_id,
            self.origin_profile,
            launch,
            hard_cap,
        )


class ClockSampler:
    """One live origin binds each fresh challenge to its actual counter readings."""

    def __init__(self, authority: AssignmentAuthority, clock: NativeClock | None = None) -> None:
        self.verifier = ClockVerifier(authority)
        self._clock = clock or native_clock()
        self.origin_session_id = new_id()
        self.origin_profile = self._clock.profile
        self._pid = os.getpid()

    def read(self) -> ClockReading:
        if os.getpid() != self._pid or self._clock.profile != self.origin_profile:
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock sampling origin changed")
        return self._clock.sample()

    def begin(self) -> PendingClockChallenge:
        binding = self.verifier.authority.binding
        request = ClockChallenge(
            worker_id=binding.worker_id, session_id=binding.worker_session_id, nonce=new_id()
        )
        return PendingClockChallenge(self, request, self.read())


class PendingClockChallenge:
    """Complete once. Replayed completion preserves the original receive anchor."""

    def __init__(self, origin: ClockSampler, challenge: ClockChallenge, sent: ClockReading) -> None:
        self._origin, self.challenge, self._sent = origin, challenge, sent
        self._used = False
        self._estimate: ClockEstimate | None = None
        self._lock = threading.Lock()

    def complete(self, reply: SignedClockReply) -> ClockEstimate:
        with self._lock:
            return self._complete(reply)

    def _complete(self, reply: SignedClockReply) -> ClockEstimate:
        if self._estimate is not None:
            if reply != self._estimate.verified.reply:
                raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Completed clock reply changed")
            return self._estimate
        if self._used:
            raise ResearchError(
                ErrorCode.INVALID_TRANSITION, "Clock challenge was already consumed"
            )
        self._used = True
        received = self._origin.read()
        verified = self._origin.verifier.reply(reply, self.challenge)
        self._estimate = ClockEstimate(
            verified,
            self._sent,
            received,
            self._origin.origin_session_id,
            self._origin.origin_profile,
        )
        return self._estimate
