"""Durable same-workspace transfer evidence, separate from expiring read snapshots."""

from typing import Any

from pydantic import Field, model_validator

from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.orchestration import DriverRoute, OrchestrationBinding
from research_swarm.domain.types import ID, Digest, Name, Revision, Timestamp, VersionedRecord

MAX_CHUNK_ENTRIES = 128
MAX_CHUNK_BYTES = 256 * 1024
MAX_MANIFEST_CHUNKS = 1024
MAX_MANIFEST_ENTRIES = MAX_CHUNK_ENTRIES * MAX_MANIFEST_CHUNKS
MAX_MANIFEST_BYTES = 64 * 1024 * 1024


class TransferManifestChunk(VersionedRecord):
    checkpoint_id: ID
    ordinal: int = Field(strict=True, ge=1)
    entries: tuple[dict[str, Any], ...] = Field(min_length=1, max_length=MAX_CHUNK_ENTRIES)

    @model_validator(mode="after")
    def bounded(self) -> "TransferManifestChunk":
        if len(canonical_bytes(self.model_dump(mode="json"))) > MAX_CHUNK_BYTES:
            raise ValueError("Transfer manifest chunk exceeds 256 KiB")
        return self


class TransferCheckpoint(VersionedRecord):
    workspace_id: ID
    campaign_id: ID
    controller_id: ID
    control_epoch: int = Field(strict=True, ge=1)
    watermark: Revision
    campaign_revision: Revision
    campaign_contract_digest: Digest
    binding_before: OrchestrationBinding
    manifest_digest: Digest
    manifest_chunk_ids: tuple[ID, ...] = Field(max_length=MAX_MANIFEST_CHUNKS)
    entry_count: Revision
    artifact_manifest_digest: Digest
    artifact_count: Revision
    captured_by: ID
    captured_at: Timestamp


class OrchestrationTransfer(VersionedRecord):
    campaign_id: ID
    checkpoint_id: ID
    checkpoint_digest: Digest
    previous_epoch: int = Field(strict=True, ge=1)
    successor: OrchestrationBinding
    start_ids: dict[DriverRoute, ID]
    pending_turn_outcome_id: ID | None = None
    pending_turn_outcome_kind: Name | None = None
    request_digest: Digest
    reason: str = Field(min_length=1, max_length=2048)
    transferred_by: ID
    transferred_at: Timestamp


class DriverStart(VersionedRecord):
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    adapter: Name
    actor_id: ID
    route: DriverRoute
    configuration_digest: Digest
    checkpoint_id: ID
    checkpoint_digest: Digest
    watermark: Revision


class DriverStartAcknowledgment(VersionedRecord):
    start_id: ID
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    observed_watermark: Revision
    acknowledged_by: ID
    acknowledged_at: Timestamp
