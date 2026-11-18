"""Immutable artifact content references."""

from __future__ import annotations

from pydantic import Field

from research_swarm.domain.types import (
    ID,
    Digest,
    MediaType,
    VersionedRecord,
)


class ArtifactRef(VersionedRecord):
    id: ID
    digest: Digest
    size_bytes: int = Field(strict=True, ge=0)
    media_type: MediaType = "application/octet-stream"
