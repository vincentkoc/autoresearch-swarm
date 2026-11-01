"""Restore exact imported generations through bounded R2 I/O without ledger mutation."""

from research_swarm.domain.artifacts import ArtifactRecord, StoredBlob, UploadSpec
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.state.database import encode

if __package__:
    from ..runtime.exclusive import exclusive
    from .checkpoint_admission import HostedRecoveryVerifier
    from .checkpoint_capture import capture_checkpoint
    from .checkpoint_import import verify_target_configuration
    from .checkpoint_source import load_source, parse_checkpoint_request, source_operator
    from .checkpoint_target import load_target
else:
    from checkpoint_admission import HostedRecoveryVerifier
    from checkpoint_capture import capture_checkpoint
    from checkpoint_import import verify_target_configuration
    from checkpoint_source import load_source, parse_checkpoint_request, source_operator
    from checkpoint_target import load_target
    from exclusive import exclusive


class ArtifactGenerationRequest(VersionedRecord):
    artifact_id: ID
    generation: ID


class ExportArtifact(ArtifactGenerationRequest):
    backup_id: ID
    manifest_digest: Digest


def imported_artifact(sql, workspace, request):
    row = sql.execute(
        "SELECT revision,payload FROM records WHERE workspace_id=? AND kind='artifact' "
        "AND id=? AND state='verified' AND typeof(payload)='text' "
        "AND length(CAST(payload AS BLOB))<=16384",
        (workspace, request.artifact_id),
    ).fetchone()
    if row is None:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Recovery artifact is unavailable")
    artifact = ArtifactRecord.model_validate_json(row["payload"])
    upload = UploadSpec(
        id=artifact.storage_generation,
        workspace_id=workspace,
        actor_id=artifact.published_by,
        digest=artifact.reference.digest,
        size_bytes=artifact.reference.size_bytes,
        media_type=artifact.reference.media_type,
    )
    if (
        artifact.state != "verified"
        or artifact.workspace_id != workspace
        or artifact.reference.id != request.artifact_id
        or upload.id != request.generation
        or upload.reference() != artifact.reference
    ):
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Recovery artifact identity differs")
    return upload, (row["revision"], row["payload"])


async def restore_target_artifact(
    state,
    open_store,
    manifest,
    secrets,
    *,
    identity,
    credential,
    target_json,
    request_json,
    content_hex,
    crypto,
    objects,
):
    custody = load_target(target_json, manifest, secrets, credential=credential)
    binding = custody.binding
    issuer = verify_target_configuration(
        manifest,
        secrets,
        binding.intent,
        identity=identity,
        credential=credential,
        target_seed=custody.seed,
        expected_source=binding.expected_source,
        expected_custody=binding.expected_custody,
        crypto=crypto,
    )
    request = parse_checkpoint_request(request_json, ArtifactGenerationRequest)
    storage = manifest.configuration.storage
    if objects.config != storage or objects.transport.configuration_digest != storage.digest:
        raise ResearchError(ErrorCode.STALE_FENCE, "Recovery artifact destination differs")
    if type(content_hex) is not str or len(content_hex) > 2 * storage.max_object_bytes:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Recovery artifact exceeds byte bound")
    try:
        content = bytes.fromhex(content_hex)
        if content.hex() != content_hex:
            raise ValueError("Noncanonical artifact bytes")
    except ValueError:
        raise ResearchError(
            ErrorCode.ARTIFACT_UNVERIFIED, "Invalid recovery artifact bytes"
        ) from None
    context = CommandContext(principal=issuer, idempotency_key="logical-artifact-inspection")
    verifier = HostedRecoveryVerifier(manifest, binding)

    async def restore():
        store = open_store()
        manifest.require_store(store.identity)

        def inspect(sql):
            verifier.read_admission(sql, authority=store.identity, context=context, crypto=crypto)
            return imported_artifact(sql, issuer.workspace_id, request)

        upload, original = store.read(inspect)
        # publish uses conditional creation, then reads and verifies complete bytes and metadata.
        blob = await objects.publish(upload, content)
        current_upload, current = store.read(inspect)
        if current_upload != upload or current != original:
            raise ResearchError(ErrorCode.STALE_FENCE, "Recovery artifact changed during transfer")
        if objects.config != storage or objects.transport.configuration_digest != storage.digest:
            raise ResearchError(ErrorCode.STALE_FENCE, "Recovery artifact destination changed")
        if blob != StoredBlob(reference=upload.reference(), generation=upload.id):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Restored artifact differs")
        return encode(
            {
                "schema_version": "1.0",
                "restore_id": binding.intent.restore_id,
                "storage_digest": storage.digest,
                "artifact": blob.model_dump(mode="json"),
            }
        )

    return await exclusive(state, restore)


async def read_source_artifact(
    state,
    open_store,
    manifest,
    secrets,
    *,
    identity,
    credential,
    source_json,
    request_json,
    crypto,
    objects,
    recovery_binding=None,
):
    custody = load_source(source_json, manifest, secrets, credential=credential, crypto=crypto)
    manifest.require_store(identity)
    request = parse_checkpoint_request(request_json, ExportArtifact)
    storage = manifest.configuration.storage
    if objects.config != storage or objects.transport.configuration_digest != storage.digest:
        raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint artifact source differs")

    async def read():
        store = open_store()
        _, token = source_operator(manifest, secrets)

        def selected():
            image = capture_checkpoint(
                store,
                manifest,
                token=token,
                backup_id=request.backup_id,
                expected_custody=custody.binding.pin,
                crypto=crypto,
                recovery_binding=recovery_binding,
            ).manifest
            if image.digest != request.manifest_digest:
                raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint artifact image differs")
            blob = next(
                (
                    item
                    for item in image.artifacts
                    if (item.reference.id, item.generation)
                    == (request.artifact_id, request.generation)
                ),
                None,
            )
            if blob is None:
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Artifact is outside checkpoint inventory"
                )
            return blob

        blob = selected()
        content = await objects.read(manifest.configuration.authority.workspace_id, blob)
        if selected() != blob:
            raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint artifact source changed")
        if objects.config != storage or objects.transport.configuration_digest != storage.digest:
            raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint artifact source changed")
        return encode(
            {
                "schema_version": "1.0",
                "manifest_digest": request.manifest_digest,
                "artifact": blob.model_dump(mode="json"),
                "content_hex": content.hex(),
            }
        )

    return await exclusive(state, read)
