"""One irreversible source-custody election; it grants no target execution by itself."""

from __future__ import annotations

from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from filelock import FileLock

from research_swarm.checkpoints.backup import source_custody, verify_backup
from research_swarm.checkpoints.preservation import verify_preservation
from research_swarm.checkpoints.restore import RestoreCustody, RestoreIntent, _verify_completed
from research_swarm.control.recovery_proofs import verify_claim as verify_claim
from research_swarm.control.recovery_proofs import verify_request as verify_request
from research_swarm.domain.backup import (
    BackupManifest,
    RestoreClaim,
    RestoreClaimRequest,
    SignedRestoreClaim,
    SignedRestoreRequest,
    WorkspaceSeal,
)
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id, utc_now
from research_swarm.runtime import LocalRuntime
from research_swarm.state.private_files import read_private, write_private
from research_swarm.state.sql import next_counter


def _manifest_binding(manifest: BackupManifest, request: RestoreClaimRequest) -> None:
    if (
        request.backup_id != manifest.seal.backup_id
        or request.manifest_digest != manifest.digest
        or request.workspace_id != manifest.seal.workspace_id
        or request.controller_id != manifest.seal.controller_id
        or request.source_custody_id != manifest.source_custody_id
        or request.source_control_epoch != manifest.seal.control_epoch
        or request.target_control_epoch != next_counter(manifest.seal.control_epoch)
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Restore claim does not bind this checkpoint")


def request_restore_claim(
    bundle: Path, destination: Path, *, restore_id: str, expected_digest: str | None = None
) -> SignedRestoreRequest:
    backup = verify_backup(bundle, expected_digest=expected_digest)
    if backup.custody_pin is None:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE,
            "Legacy checkpoint has no custody pin; re-export and reimport",
        )
    destination = destination.expanduser().resolve()
    intent = RestoreIntent(
        restore_id=restore_id, manifest_digest=backup.manifest.digest, destination=str(destination)
    )
    # A request can never create an installation or turn a partial import into a
    # completed one. Its public signature proves possession of this target key.
    if (destination / "recovery.pending").exists() or not (destination / "restore.json").is_file():
        raise ResearchError(ErrorCode.STALE_FENCE, "Restore target is incomplete")
    receipt = _verify_completed(backup, destination, intent)
    if receipt.source_public_key is None:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE,
            "Legacy inactive receipt has no source key binding; import into a new target",
        )
    custody = RestoreCustody.model_validate(
        strict_json_loads(read_private(destination, "restore-custody.json", limit=16_384))
    )
    request = receipt.claim_request()
    _manifest_binding(backup.manifest, request)
    key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(custody.private_key))
    return SignedRestoreRequest(
        request=request, signature=key.sign(request.digest.encode("ascii")).hex()
    )


def claim_restore(
    runtime: LocalRuntime,
    context: CommandContext,
    bundle: Path,
    signed_request: SignedRestoreRequest,
) -> SignedRestoreClaim:
    context.principal.require("*")
    if context.principal.workspace_id != runtime.identity.workspace_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Restore claim workspace differs")
    request = verify_request(signed_request)
    with FileLock(runtime.root / "backup.lock"):
        custody = source_custody(runtime)
        public = custody.key.public_key().public_bytes_raw().hex()
        if custody.claim is not None:
            previous = verify_claim(custody.claim, source_public_key=public)
            if previous.request != request:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Checkpoint already elected another target"
                )
            signed = custody.claim
        else:
            backup = verify_backup(bundle, expected_digest=request.manifest_digest)
            if backup.custody_pin is None:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE,
                    "Legacy checkpoint has no custody pin; re-export and reimport",
                )
            _manifest_binding(backup.manifest, request)
            if (custody.id, public) != (
                backup.manifest.source_custody_id,
                backup.manifest.source_public_key,
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Original source custody differs")
            row = runtime.database.read_record(context, "workspace_gate", request.workspace_id)
            if (
                row["state"] != "sealed"
                or WorkspaceSeal.model_validate(row["data"]) != backup.manifest.seal
            ):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Original source is not sealed at this cut"
                )
            verify_preservation(backup, runtime.database, request.restore_id)
            claim = RestoreClaim(
                claim_id=new_id(), request=request, source_public_key=public, claimed_at=utc_now()
            )
            signed = SignedRestoreClaim(
                claim=claim, signature=custody.key.sign(claim.digest.encode("ascii")).hex()
            )
            custody = custody.model_copy(update={"claim": signed})
        # Key and election share one pinned file: losing a separate decision file
        # cannot recreate an apparently unused authority. An exact retry finishes
        # the same durability barrier before returning the original signed claim.
        write_private(
            runtime.root / "backup-custody.json", canonical_bytes(custody.model_dump(mode="json"))
        )
        return signed
