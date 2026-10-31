"""Bounded async R2 byte operations; the controller owns quota and lifecycle.

No method here publishes an artifact, authorizes a reader, or releases occupancy.
The caller must retain that authority across I/O and revalidate it before commit.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Annotated, Protocol

from pydantic import Field, StringConstraints

from research_swarm.domain.artifacts import StoredBlob, UploadSpec
from research_swarm.domain.canonical import content_digest, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Name, VersionedRecord
from research_swarm.storage.r2_objects import generation_key, generation_metadata


class BoundedR2Configuration(VersionedRecord):
    workspace_id: ID
    bucket_identity: Name
    prefix: Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]{1,128}$")]
    max_object_bytes: int = Field(default=1024**2, strict=True, ge=1, le=1024**2)

    @property
    def digest(self) -> str:
        return record_digest("research.bounded-r2-configuration", self.model_dump(mode="json"))


@dataclass(frozen=True)
class ObjectBytes:
    content: bytes
    metadata: Mapping[str, str]

    def __post_init__(self) -> None:
        if not isinstance(self.content, bytes):
            raise TypeError("R2 content must be immutable bytes")
        metadata = dict(self.metadata)
        if any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in metadata.items()
        ):
            raise TypeError("R2 metadata must contain string keys and values")
        object.__setattr__(self, "metadata", MappingProxyType(metadata))


class AsyncR2Transport(Protocol):
    @property
    def configuration_digest(self) -> str: ...

    async def get(self, key: str, *, max_bytes: int) -> ObjectBytes | None:
        """Bound allocation and consumption; an absent object is not write-closure proof."""
        ...

    async def put(
        self, key: str, content: bytes, metadata: Mapping[str, str], *, if_absent: bool
    ) -> bool:
        """Return false only for an atomic failed precondition, never an uncertain write."""
        ...


class BoundedR2Objects:
    def __init__(self, config: BoundedR2Configuration, transport: AsyncR2Transport) -> None:
        config = BoundedR2Configuration.model_validate(config)
        if transport.configuration_digest != config.digest:
            raise ResearchError(ErrorCode.STALE_FENCE, "R2 destination configuration differs")
        self.config, self.transport = config, transport

    def _key(self, workspace_id: str, blob: StoredBlob) -> str:
        if workspace_id != self.config.workspace_id:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "R2 workspace differs")
        if self.transport.configuration_digest != self.config.digest:
            raise ResearchError(ErrorCode.STALE_FENCE, "R2 destination configuration changed")
        if blob.reference.size_bytes > self.config.max_object_bytes:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "R2 object exceeds the bounded profile"
            )
        return generation_key(self.config.prefix, workspace_id, blob)

    async def publish(self, upload: UploadSpec, content: bytes) -> StoredBlob:
        upload = UploadSpec.model_validate(upload)
        blob = StoredBlob(reference=upload.reference(), generation=upload.id)
        key = self._key(upload.workspace_id, blob)
        if (
            not isinstance(content, bytes)
            or len(content) != upload.size_bytes
            or content_digest(content) != upload.digest
        ):
            raise ResearchError(
                ErrorCode.ARTIFACT_UNVERIFIED, "Upload bytes differ from their contract"
            )
        await self.transport.put(key, content, generation_metadata(blob), if_absent=True)
        # A successful PUT or conditional conflict alone never proves stored content.
        await self.read(upload.workspace_id, blob)
        return blob

    async def read(self, workspace_id: str, blob: StoredBlob) -> bytes:
        result = await self.read_if_present(workspace_id, blob)
        if result is None:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "R2 object is absent")
        return result

    async def read_if_present(self, workspace_id: str, blob: StoredBlob) -> bytes | None:
        """Observe transfer progress; absence never proves that a writer has closed."""
        blob = StoredBlob.model_validate(blob)
        key = self._key(workspace_id, blob)
        result = await self.transport.get(key, max_bytes=self.config.max_object_bytes)
        self._key(workspace_id, blob)  # Destination identity must survive suspension.
        if result is None:
            return None
        if result.metadata == generation_metadata(blob, retired=True) and not result.content:
            raise ResearchError(
                ErrorCode.INVALID_TRANSITION, "R2 generation is permanently retired"
            )
        if (
            result.metadata != generation_metadata(blob)
            or len(result.content) != blob.reference.size_bytes
            or content_digest(result.content) != blob.reference.digest
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "R2 stored content differs")
        return result.content

    async def retire(self, workspace_id: str, blob: StoredBlob) -> None:
        blob = StoredBlob.model_validate(blob)
        key = self._key(workspace_id, blob)
        metadata = generation_metadata(blob, retired=True)
        # Keep the same key permanently occupied: delayed conditional payload PUTs
        # cannot resurrect a retired generation, including after a lost response.
        if not await self.transport.put(key, b"", metadata, if_absent=False):
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "R2 retirement was not confirmed")
        self._key(workspace_id, blob)
        result = await self.transport.get(key, max_bytes=0)
        self._key(workspace_id, blob)
        if result is None or result.content or result.metadata != metadata:
            raise ResearchError(
                ErrorCode.ARTIFACT_UNVERIFIED, "R2 retirement fence was not confirmed"
            )
