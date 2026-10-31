"""Verify hosted election proofs against trusted pins; no storage or election mutation."""

from research_swarm.control.recovery_proofs import verify_claim, verify_request
from research_swarm.crypto import Ed25519Provider, resolve_crypto, verify
from research_swarm.domain.backup import BackupCustodyPin
from research_swarm.domain.checkpoint_elections import (
    LogicalElection,
    LogicalElectionRequest,
    SignedLogicalElection,
    SignedLogicalElectionRequest,
)
from research_swarm.domain.checkpoint_images import LogicalImportIntent
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope


def verify_election_request(
    signed: SignedLogicalElectionRequest,
    *,
    expected_intent: LogicalImportIntent,
    expected_backup_id: str,
    expected_source: WitnessAuthorityScope,
    expected_deployment_digest: str,
    expected_custody: BackupCustodyPin,
    crypto: Ed25519Provider | None = None,
) -> LogicalElectionRequest:
    """Authenticate the target and all pinned identities; this does not elect it."""
    signed = SignedLogicalElectionRequest.model_validate(signed)
    payload = signed.request
    request = payload.signed_request.request
    if (
        payload.intent != expected_intent
        or request.backup_id != expected_backup_id
        or payload.source_authority != expected_source
        or payload.source_deployment_digest != expected_deployment_digest
        or (
            request.source_custody_id,
            request.workspace_id,
            request.controller_id,
            request.source_control_epoch,
        )
        != (
            expected_custody.custody_id,
            expected_custody.workspace_id,
            expected_custody.controller_id,
            expected_custody.control_epoch,
        )
        or request.target_public_key == expected_custody.public_key
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Logical election request identity differs")
    provider = resolve_crypto(crypto)
    verify_request(payload.signed_request, crypto_provider=provider)
    _verify(provider, request.target_public_key, payload.digest, signed.signature)
    return payload


def verify_election(
    signed: SignedLogicalElection,
    *,
    expected_intent: LogicalImportIntent,
    expected_backup_id: str,
    expected_source: WitnessAuthorityScope,
    expected_deployment_digest: str,
    expected_custody: BackupCustodyPin,
    crypto: Ed25519Provider | None = None,
) -> LogicalElection:
    """Verify both nested native signatures and both hosted binding signatures."""
    signed = SignedLogicalElection.model_validate(signed)
    election = signed.election
    provider = resolve_crypto(crypto)
    verify_election_request(
        election.signed_request,
        expected_intent=expected_intent,
        expected_backup_id=expected_backup_id,
        expected_source=expected_source,
        expected_deployment_digest=expected_deployment_digest,
        expected_custody=expected_custody,
        crypto=provider,
    )
    verify_claim(
        election.signed_claim,
        source_public_key=expected_custody.public_key,
        crypto_provider=provider,
    )
    _verify(provider, expected_custody.public_key, election.digest, signed.signature)
    return election


def _verify(provider: Ed25519Provider, key: str, digest: str, signature: str) -> None:
    try:
        verify(provider, bytes.fromhex(key), digest.encode("ascii"), bytes.fromhex(signature))
    except ValueError as error:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Logical election signature differs"
        ) from error
