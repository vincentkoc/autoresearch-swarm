"""Capture one sealed hosted image without writes, artifact I/O or activation authority."""

import hmac
from dataclasses import dataclass, field

from research_swarm.checkpoints.capture import CaptureLimits, capture_rows, overflow, preflight_rows
from research_swarm.checkpoints.cut import inspect_sealed_cut
from research_swarm.checkpoints.image_format import LAYOUT_DIGEST
from research_swarm.control.auth import CredentialStore
from research_swarm.domain.backup import BackupCustodyPin
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.checkpoint_images import LogicalCheckpointManifest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.state.database import Transaction
from research_swarm.state.time_providers import TimeProviders

if __package__:
    from .checkpoint_schema import require_checkpoint_schema
else:
    from checkpoint_schema import require_checkpoint_schema


@dataclass(frozen=True)
class CapturedLogicalImage:
    manifest: LogicalCheckpointManifest
    bodies: tuple[bytes, ...] = field(repr=False)


def require_source_operator_in(sql, manifest, token):
    expected = next(
        item for item in manifest.credentials if item.principal.id == manifest.provisioning_actor_id
    )
    if (
        not isinstance(token, str)
        or not token.startswith("rsw_")
        or len(token) > 256
        or not hmac.compare_digest(CredentialStore._digest(token), expected.token_digest)
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Checkpoint operator is unavailable")
    row = sql.execute(
        "SELECT principal FROM credentials WHERE id=? AND token_digest=? AND workspace_id=? "
        "AND expires_at IS NULL AND revoked=0 AND typeof(revoked)='integer' "
        "AND typeof(principal)='text' AND length(CAST(principal AS BLOB))<=65536",
        (expected.id, expected.token_digest, expected.principal.workspace_id),
    ).fetchone()
    if row is None or Principal.model_validate_json(row["principal"]) != expected.principal:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Checkpoint operator is unavailable")
    expected.principal.require("*")
    return expected.principal


def capture_checkpoint(
    store, manifest, *, token, backup_id, expected_custody: BackupCustodyPin, limits=None
):
    """Internal source port; the caller owns custody and any later publication/election."""
    manifest.require_store(store.identity)
    limits = CaptureLimits() if limits is None else CaptureLimits.model_validate(limits)
    expected_custody = BackupCustodyPin.model_validate(expected_custody)

    def capture(sql):
        manifest.require_store(store.identity)
        principal = require_source_operator_in(sql, manifest, token)
        require_checkpoint_schema(sql, manifest)
        inventory = preflight_rows(sql, limits)
        config = manifest.configuration
        cut = inspect_sealed_cut(
            sql,
            workspace_id=config.authority.workspace_id,
            controller_id=config.authority.controller_id,
            storage_limit_bytes=config.storage_limit_bytes,
            expected_backup_id=backup_id,
            require_custody=True,
            require_seal_history=True,
        )
        if cut.custody_pin != expected_custody:
            raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint custody differs")
        context = CommandContext(principal=principal, idempotency_key="checkpoint:" + backup_id)
        provider = TimeProviders(store.identity).load(
            Transaction(sql, context, observe_waits=False), config.authority
        )
        if provider.data.source != config.source:
            raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint witness source differs")
        captured = capture_rows(sql, backup_id=backup_id, inventory=inventory, limits=limits)
        image = LogicalCheckpointManifest(
            table_layout_digest=LAYOUT_DIGEST,
            source_authority=config.authority,
            source_deployment_digest=manifest.digest,
            source_configuration_digest=config.digest,
            source_storage_digest=config.storage.digest,
            source_custody=expected_custody,
            operator_id=principal.id,
            seal=cut.seal,
            seal_revision=cut.seal_revision,
            seal_digest=cut.seal_digest,
            snapshot_evidence_through=cut.evidence_through,
            tables=captured.tables,
            chunks=captured.descriptors,
            artifacts=cut.artifacts,
        )
        if len(canonical_bytes(image.model_dump(mode="json"))) > limits.manifest_bytes:
            raise overflow()
        return CapturedLogicalImage(image, captured.bodies)

    return store.read(capture)
