"""Session-scoped signing and public verification of complete worker assignments."""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import TypeAdapter

from research_swarm.crypto import Ed25519Provider, public_key, resolve_crypto, sign, verify
from research_swarm.domain.assignments import (
    AssignmentAuthority,
    AssignmentPayload,
    SignedAssignment,
    signing_key_id,
)
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import Timestamp
from research_swarm.domain.workers import WorkerBinding
from research_swarm.workers.authority import VerifiedDispatch, verify_dispatch_contract


def signing_bytes(payload: AssignmentPayload, key_id: str) -> bytes:
    return canonical_bytes(
        {
            "domain": "research.worker-assignment",
            "algorithm": "ed25519-jcs-v1",
            "key_id": key_id,
            "payload": payload.model_dump(mode="json"),
        }
    )


def require_binding(payload: AssignmentPayload, binding: WorkerBinding) -> None:
    if any(
        getattr(payload.dispatch, field) != getattr(binding, field)
        for field in WorkerBinding.model_fields
        if field != "schema_version"
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Assignment belongs to another worker authority")


class AssignmentSigner:
    """Only the controller owns this object and its private signing key."""

    def __init__(
        self,
        binding: WorkerBinding,
        private_key: bytes,
        *,
        crypto_provider: Ed25519Provider | None = None,
    ) -> None:
        self.crypto_provider = resolve_crypto(crypto_provider)
        self._private = private_key
        public = public_key(self.crypto_provider, private_key).hex()
        self.authority = AssignmentAuthority(
            binding=binding, public_key=public, key_id=signing_key_id(public)
        )

    def sign(self, payload: AssignmentPayload) -> SignedAssignment:
        payload = AssignmentPayload.model_validate(payload)
        require_binding(payload, self.authority.binding)
        return SignedAssignment(
            key_id=self.authority.key_id,
            payload=payload,
            signature=sign(
                self.crypto_provider, self._private, signing_bytes(payload, self.authority.key_id)
            ).hex(),
        )


@dataclass(frozen=True)
class VerifiedAssignment:
    """Internal verification product; never deserialize it from transport input."""

    assignment: SignedAssignment
    dispatch: VerifiedDispatch


class AssignmentVerifier:
    def __init__(
        self, authority: AssignmentAuthority, *, crypto_provider: Ed25519Provider | None = None
    ) -> None:
        self.crypto_provider = resolve_crypto(crypto_provider)
        self.authority = AssignmentAuthority.model_validate(authority)
        self._public = bytes.fromhex(self.authority.public_key)

    def verify(
        self,
        assignment: SignedAssignment,
        *,
        authority_now: Timestamp,
        allow_expired_observation: bool = False,
    ) -> VerifiedAssignment:
        authority_now = TypeAdapter(Timestamp).validate_python(authority_now)
        try:
            assignment = SignedAssignment.model_validate(assignment)
            if assignment.key_id != self.authority.key_id:
                raise ValueError("Assignment signing key differs")
            verify(
                self.crypto_provider,
                self._public,
                signing_bytes(assignment.payload, assignment.key_id),
                bytes.fromhex(assignment.signature),
            )
        except ValueError as error:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Assignment authentication failed"
            ) from error
        require_binding(assignment.payload, self.authority.binding)
        if authority_now < assignment.payload.issued_at:
            raise ResearchError(ErrorCode.STALE_FENCE, "Assignment is not yet valid")
        verified = verify_dispatch_contract(
            self.authority.binding,
            assignment.payload.dispatch,
            assignment.payload.spec,
            authority_now=authority_now,
            allow_expired_observation=allow_expired_observation,
        )
        return VerifiedAssignment(assignment, verified)
