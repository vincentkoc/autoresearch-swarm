"""Domain-separated public verification of time samples and assignment time bindings."""

from __future__ import annotations

from dataclasses import dataclass

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from research_swarm.domain.assignments import AssignmentAuthority, SignedAssignment, signing_key_id
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.clocks import (
    AssignmentTimePayload,
    ClockChallenge,
    ClockReplyPayload,
    SignedAssignmentTime,
    SignedClockReply,
    timed_assignment_digest,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import VersionedRecord
from research_swarm.domain.workers import WorkerBinding
from research_swarm.workers.signatures import AssignmentVerifier, VerifiedAssignment


def clock_signing_bytes(domain: str, key_id: str, payload: VersionedRecord) -> bytes:
    return canonical_bytes(
        {
            "domain": domain,
            "algorithm": "ed25519-jcs-v1",
            "key_id": key_id,
            "payload": payload.model_dump(mode="json"),
        }
    )


class ClockSigner:
    def __init__(self, binding: WorkerBinding, private_key: bytes) -> None:
        self._private = Ed25519PrivateKey.from_private_bytes(private_key)
        public = self._private.public_key().public_bytes_raw().hex()
        self.authority = AssignmentAuthority(
            binding=binding, public_key=public, key_id=signing_key_id(public)
        )

    def reply(self, payload: ClockReplyPayload) -> SignedClockReply:
        payload = ClockReplyPayload.model_validate(payload)
        if payload.binding != self.authority.binding:
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock sample authority differs")
        return SignedClockReply(
            key_id=self.authority.key_id,
            payload=payload,
            signature=self._private.sign(
                clock_signing_bytes("research.worker-clock", self.authority.key_id, payload)
            ).hex(),
        )

    def bind(self, payload: AssignmentTimePayload) -> SignedAssignmentTime:
        payload = AssignmentTimePayload.model_validate(payload)
        if payload.binding != self.authority.binding:
            raise ResearchError(ErrorCode.STALE_FENCE, "Assignment clock authority differs")
        return SignedAssignmentTime(
            key_id=self.authority.key_id,
            payload=payload,
            signature=self._private.sign(
                clock_signing_bytes("research.assignment-clock", self.authority.key_id, payload)
            ).hex(),
        )


@dataclass(frozen=True)
class VerifiedClockReply:
    """Internal verification product; never deserialize it from transport input."""

    reply: SignedClockReply


@dataclass(frozen=True)
class VerifiedAssignmentTime:
    """Authenticated static grant; clock freshness and live origin are separate gates."""

    grant: SignedAssignmentTime
    assignment: VerifiedAssignment


class ClockVerifier:
    def __init__(self, authority: AssignmentAuthority) -> None:
        self.authority = AssignmentAuthority.model_validate(authority)
        self._public = Ed25519PublicKey.from_public_bytes(bytes.fromhex(self.authority.public_key))

    def _verify(self, domain: str, record: SignedClockReply | SignedAssignmentTime) -> None:
        try:
            if record.key_id != self.authority.key_id:
                raise ValueError("Clock signing key differs")
            self._public.verify(
                bytes.fromhex(record.signature),
                clock_signing_bytes(domain, record.key_id, record.payload),
            )
        except (InvalidSignature, ValueError) as error:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Clock authentication failed"
            ) from error
        if record.payload.binding != self.authority.binding:
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock belongs to another authority")

    def reply(self, reply: SignedClockReply, challenge: ClockChallenge) -> VerifiedClockReply:
        reply, challenge = (
            SignedClockReply.model_validate(reply),
            ClockChallenge.model_validate(challenge),
        )
        self._verify("research.worker-clock", reply)
        if (
            reply.payload.nonce != challenge.nonce
            or challenge.worker_id != self.authority.binding.worker_id
            or challenge.session_id != self.authority.binding.worker_session_id
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock challenge differs")
        return VerifiedClockReply(reply)

    def binding(
        self, grant: SignedAssignmentTime, assignment: SignedAssignment
    ) -> VerifiedAssignmentTime:
        grant, assignment = (
            SignedAssignmentTime.model_validate(grant),
            SignedAssignment.model_validate(assignment),
        )
        self._verify("research.assignment-clock", grant)
        if grant.payload.assignment_digest != timed_assignment_digest(assignment):
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Timed assignment differs")
        verified = AssignmentVerifier(self.authority).verify(
            assignment, authority_now=assignment.payload.issued_at, allow_expired_observation=True
        )
        return VerifiedAssignmentTime(grant, verified)
