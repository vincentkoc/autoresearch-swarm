"""Verify recovery envelopes without native files, transports or private keys."""

from research_swarm.crypto import Ed25519Provider, resolve_crypto, verify
from research_swarm.domain.backup import (
    RecoveryAdmission,
    RestoreClaim,
    RestoreClaimRequest,
    RestoreReceipt,
    SignedRestoreClaim,
    SignedRestoreRequest,
)
from research_swarm.domain.errors import ErrorCode, ResearchError


def verify_request(
    request: SignedRestoreRequest, *, crypto_provider: Ed25519Provider | None = None
) -> RestoreClaimRequest:
    request = SignedRestoreRequest.model_validate(request)
    try:
        verify(
            resolve_crypto(crypto_provider),
            bytes.fromhex(request.request.target_public_key),
            request.request.digest.encode("ascii"),
            bytes.fromhex(request.signature),
        )
    except ValueError as error:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Restore target proof differs"
        ) from error
    return request.request


def verify_claim(
    claim: SignedRestoreClaim,
    *,
    source_public_key: str,
    crypto_provider: Ed25519Provider | None = None,
) -> RestoreClaim:
    claim = SignedRestoreClaim.model_validate(claim)
    if claim.claim.source_public_key != source_public_key:
        raise ResearchError(ErrorCode.STALE_FENCE, "Restore claim source key differs")
    try:
        verify(
            resolve_crypto(crypto_provider),
            bytes.fromhex(source_public_key),
            claim.claim.digest.encode("ascii"),
            bytes.fromhex(claim.signature),
        )
    except ValueError as error:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Restore source signature differs"
        ) from error
    return claim.claim


def validate_claim_binding(
    receipt: RestoreReceipt,
    signed: SignedRestoreClaim,
    *,
    crypto_provider: Ed25519Provider | None = None,
) -> None:
    if receipt.source_public_key is None:
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Restore has no source key binding")
    claim = verify_claim(
        signed, source_public_key=receipt.source_public_key, crypto_provider=crypto_provider
    )
    if claim.request != receipt.claim_request():
        raise ResearchError(ErrorCode.STALE_FENCE, "Source claim elected another restoration")


def verify_admission(
    admission: RecoveryAdmission,
    receipt: RestoreReceipt,
    *,
    crypto_provider: Ed25519Provider | None = None,
) -> None:
    validate_claim_binding(receipt, admission.signed_claim, crypto_provider=crypto_provider)
    if admission.restoration_digest != receipt.digest:
        raise ResearchError(ErrorCode.STALE_FENCE, "Recovery admission restoration differs")
    try:
        verify(
            resolve_crypto(crypto_provider),
            bytes.fromhex(receipt.target_public_key),
            admission.proof_digest.encode("ascii"),
            bytes.fromhex(admission.target_signature),
        )
    except ValueError as error:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Recovery target proof differs"
        ) from error
