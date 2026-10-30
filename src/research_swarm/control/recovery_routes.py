"""Public route verification starts with an independently trusted source pin."""

from research_swarm.control.recovery_proofs import verify_admission
from research_swarm.crypto import Ed25519Provider, resolve_crypto, verify
from research_swarm.domain.backup import BackupCustodyPin
from research_swarm.domain.endpoints import controller_url
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.recovery_routes import SignedWorkerRecoveryRoute


def verify_recovery_route(
    signed: SignedWorkerRecoveryRoute,
    trusted_source: BackupCustodyPin,
    *,
    crypto_provider: Ed25519Provider | None = None,
) -> None:
    route = SignedWorkerRecoveryRoute.model_validate(signed).route
    pin = BackupCustodyPin.model_validate(trusted_source)
    receipt = route.restoration
    if (pin.workspace_id, pin.controller_id, pin.custody_id, pin.control_epoch, pin.public_key) != (
        receipt.workspace_id,
        receipt.controller_id,
        receipt.source_custody_id,
        receipt.source_control_epoch,
        receipt.source_public_key,
    ):
        raise ResearchError(
            ErrorCode.STALE_FENCE, "Recovery route does not match trusted source custody"
        )
    if (
        route.binding.workspace_id != receipt.workspace_id
        or route.binding.controller_id != receipt.controller_id
        or route.binding.control_epoch > receipt.source_control_epoch
        or route.attempt_id not in receipt.unresolved_attempt_ids
        or route.binding.worker_id not in receipt.worker_ids
        or route.source_controller_url != controller_url(route.source_controller_url)
        or route.destination_url != controller_url(route.destination_url)
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Recovery route identity differs")
    verify_admission(route.admission, receipt, crypto_provider=crypto_provider)
    try:
        verify(
            resolve_crypto(crypto_provider),
            bytes.fromhex(receipt.target_public_key),
            route.digest.encode("ascii"),
            bytes.fromhex(signed.target_signature),
        )
    except ValueError as error:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Recovery route signature differs"
        ) from error
