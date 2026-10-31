"""Witness authentication and one-use responses, without a hosted authority adapter."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from uuid import UUID

from pydantic import TypeAdapter

from research_swarm.crypto import Ed25519Provider, public_key, resolve_crypto, sign, verify
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import (
    SignedTimeWitness,
    TimeWitnessPayload,
    TimeWitnessRequest,
    WitnessScope,
    WitnessSource,
)
from research_swarm.domain.types import ID, new_id

MAX_WITNESS_BYTES = 16 * 1024


def witness_signing_bytes(key_id: str, payload: TimeWitnessPayload) -> bytes:
    return canonical_bytes(
        {
            "domain": "research.authority-time-witness",
            "algorithm": "ed25519-jcs-v1",
            "key_id": key_id,
            "payload": payload.model_dump(mode="json"),
        }
    )


def decode_time_witness(data: bytes) -> SignedTimeWitness:
    if not isinstance(data, bytes) or len(data) > MAX_WITNESS_BYTES:
        raise ValueError("Witness response exceeds its wire boundary")
    return SignedTimeWitness.model_validate(strict_json_loads(data))


def witness_epoch(scope: WitnessScope, source: WitnessSource, source_epoch: str) -> str:
    """First 128 SHA-256 bits, with UUID version 8 and RFC variant bits set.

    This stable identity does not check retirement or qualify a provider generation.
    """
    scope, source = WitnessScope.model_validate(scope), WitnessSource.model_validate(source)
    TypeAdapter(ID).validate_python(source_epoch)
    digest = record_digest(
        "research.authority-time-epoch.v1",
        {
            "scope": scope.model_dump(mode="json"),
            "source_id": source.id,
            "source_key_id": source.key_id,
            "source_epoch": source_epoch,
            "native_profile": source.native_profile,
            "policy_digest": source.policy.digest,
        },
    )
    encoded = bytearray.fromhex(digest.removeprefix("sha256:")[:32])
    encoded[6] = (encoded[6] & 0x0F) | 0x80
    encoded[8] = (encoded[8] & 0x3F) | 0x80
    return str(UUID(bytes=bytes(encoded)))


class WitnessSigner:
    """Trusted envelope primitive; a source service must obtain the fresh observation."""

    def __init__(
        self,
        source: WitnessSource,
        private_key: bytes,
        *,
        crypto_provider: Ed25519Provider | None = None,
    ) -> None:
        self._source = WitnessSource.model_validate(source)
        self.crypto_provider = resolve_crypto(crypto_provider)
        if public_key(self.crypto_provider, private_key).hex() != self.source.public_key:
            raise ValueError("Witness signing key differs from its source")
        self._private = private_key

    @property
    def source(self) -> WitnessSource:
        return self._source

    def sign(self, payload: TimeWitnessPayload) -> SignedTimeWitness:
        payload = TimeWitnessPayload.model_validate(payload)
        _require_source(self.source, payload)
        return SignedTimeWitness(
            key_id=self.source.key_id,
            payload=payload,
            signature=sign(
                self.crypto_provider,
                self._private,
                witness_signing_bytes(self.source.key_id, payload),
            ).hex(),
        )


def _require_source(source: WitnessSource, payload: TimeWitnessPayload) -> None:
    if (
        payload.source_id != source.id
        or payload.native_profile != source.native_profile
        or payload.policy != source.policy
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Witness source configuration differs")


@dataclass(frozen=True)
class VerifiedTimeWitness:
    """Internal authentication product, not a clock-provider or retirement decision."""

    witness: SignedTimeWitness
    source: WitnessSource

    @property
    def epoch(self) -> str:
        payload = self.witness.payload
        return witness_epoch(payload.request.scope, self.source, payload.source_epoch)


@dataclass(frozen=True)
class AuthenticatedSourceMismatch:
    """An exact-request signed response with changed source configuration.

    Retirement still requires the current durable provider binding and revision.
    This is never a usable time observation or a transport-deserialized product.
    """

    witness: SignedTimeWitness
    expected_source: WitnessSource


class WitnessSourceChanged(ResearchError):
    def __init__(self, evidence: AuthenticatedSourceMismatch) -> None:
        super().__init__(
            ErrorCode.STALE_FENCE, "Authenticated witness source configuration changed"
        )
        self._evidence = evidence

    @property
    def evidence(self) -> AuthenticatedSourceMismatch:
        return self._evidence


class PendingTimeWitness:
    """A fresh request accepts at most one completion, including failed verification."""

    def __init__(
        self,
        source: WitnessSource,
        scope: WitnessScope,
        operation_id: str,
        request_digest: str,
        *,
        crypto_provider: Ed25519Provider | None = None,
    ) -> None:
        self._source = WitnessSource.model_validate(source)
        self.crypto_provider = resolve_crypto(crypto_provider)
        self._request = TimeWitnessRequest(
            scope=scope, operation_id=operation_id, request_digest=request_digest, nonce=new_id()
        )
        self._used = False
        self._lock = threading.Lock()

    @property
    def source(self) -> WitnessSource:
        return self._source

    @property
    def request(self) -> TimeWitnessRequest:
        return self._request

    def complete(self, data: bytes) -> VerifiedTimeWitness:
        with self._lock:
            if self._used:
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Witness request was consumed")
            self._used = True
        witness = decode_time_witness(data)
        try:
            if witness.key_id != self.source.key_id:
                raise ValueError("Witness signing key differs")
            verify(
                self.crypto_provider,
                bytes.fromhex(self.source.public_key),
                witness_signing_bytes(witness.key_id, witness.payload),
                bytes.fromhex(witness.signature),
            )
        except ValueError as error:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Witness authentication failed"
            ) from error
        if witness.payload.request != self._request:
            raise ResearchError(ErrorCode.STALE_FENCE, "Witness operation binding differs")
        try:
            _require_source(self.source, witness.payload)
        except ResearchError:
            raise WitnessSourceChanged(AuthenticatedSourceMismatch(witness, self.source)) from None
        return VerifiedTimeWitness(witness, self.source)
