"""Content identity is separate from access, retention, and scientific acceptance."""

from __future__ import annotations

from typing import Literal
from uuid import UUID, uuid5

from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import (
    ID,
    ByteCount,
    Digest,
    MediaType,
    Name,
    Timestamp,
    VersionedRecord,
)


class UploadSpec(VersionedRecord):
    id: ID
    workspace_id: ID
    actor_id: ID
    digest: Digest
    size_bytes: ByteCount
    media_type: MediaType = "application/octet-stream"

    def reference(self) -> ArtifactRef:
        return ArtifactRef(
            id=str(uuid5(UUID(self.workspace_id), self.digest)),
            digest=self.digest,
            size_bytes=self.size_bytes,
            media_type=self.media_type,
        )


class StoredBlob(VersionedRecord):
    reference: ArtifactRef
    generation: ID


class ArtifactRecord(VersionedRecord):
    reference: ArtifactRef
    storage_generation: ID
    workspace_id: ID
    published_by: ID
    verified_at: Timestamp
    state: Literal["verified", "deleting", "deleted"] = "verified"
    deletion_reason: str | None = None
    deletion_id: ID | None = None


class ArtifactRetention(VersionedRecord):
    artifact_id: ID
    owner_kind: Name
    owner_id: ID
    purpose: Name
    required: bool = True
