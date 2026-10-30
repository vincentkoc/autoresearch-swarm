"""Bounded frozen read models and retention pins shared by all transports."""

from __future__ import annotations

from pydantic import Field

from research_swarm.domain.types import ID, Digest, Revision, Timestamp, VersionedRecord


class CreateRecoverySnapshot(VersionedRecord):
    snapshot_id: ID
    lifetime_seconds: int = Field(default=300, strict=True, ge=30, le=3600)
    max_items: int = Field(default=100_000, strict=True, ge=1, le=100_000)
    max_bytes: int = Field(default=256 * 1024 * 1024, strict=True, ge=1, le=256 * 1024 * 1024)


class RecoverySnapshotTarget(VersionedRecord):
    snapshot_id: ID


class CollectExpiredSnapshots(VersionedRecord):
    limit: int = Field(default=8, strict=True, ge=1, le=64)


class AdvanceEventFloor(VersionedRecord):
    event_floor: int = Field(strict=True, ge=1)
    previous_floor: int = Field(strict=True, ge=1)


class RecoverySnapshot(VersionedRecord):
    id: ID
    workspace_id: ID
    actor_id: ID
    controller_id: ID
    control_epoch: int = Field(strict=True, ge=1)
    watermark: Revision
    event_floor: int = Field(strict=True, ge=1)
    scope_digest: Digest
    request_digest: Digest
    projection_version: str = "public-read-v1"
    item_count: Revision
    size_bytes: Revision
    created_at: Timestamp
    expires_at: Timestamp


class RecoveryPage(RecoverySnapshotTarget):
    limit: int = Field(default=50, strict=True, ge=1, le=100)
    cursor: str | None = Field(default=None, max_length=2048)


class RecoveryCursor(VersionedRecord):
    snapshot_id: ID
    scope_digest: Digest
    watermark: Revision
    after: Revision


class RecoveryItem(RecoverySnapshotTarget):
    ordinal: int = Field(strict=True, ge=1, le=100_000)
    offset: int = Field(default=0, strict=True, ge=0, le=256 * 1024 * 1024)
    limit: int = Field(default=64 * 1024, strict=True, ge=1, le=64 * 1024)
