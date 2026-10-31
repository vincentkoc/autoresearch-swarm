"""Bounded logical checkpoint wire records; integrity is not restoration authority."""

import base64
from typing import Annotated, Literal, get_args

from pydantic import Field, StrictInt, StrictStr, field_validator, model_validator

from research_swarm.domain.artifacts import StoredBlob
from research_swarm.domain.assignments import PublicKeyHex, SignatureHex
from research_swarm.domain.backup import BackupCustodyPin, WorkspaceSeal
from research_swarm.domain.canonical import MAX_SAFE_INTEGER, canonical_bytes, record_digest
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import ID, Digest, Name, Record, VersionedRecord

MAX_CHUNK_BYTES = 1024 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_IMAGE_BYTES = 64 * 1024 * 1024
MAX_IMAGE_ROWS = 100_000
MAX_IMAGE_CHUNKS = 4096

LogicalTable = Literal[
    "workspaces",
    "records",
    "commands",
    "events",
    "outbox",
    "budgets",
    "budget_reservations",
    "budget_settlements",
    "budget_entries",
    "recovery_snapshots",
    "recovery_items",
    "credentials",
]
TABLE_ORDER = get_args(LogicalTable)
Counter = Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]


class BlobCell(Record):
    blob_base64: StrictStr = Field(max_length=MAX_CHUNK_BYTES)

    @field_validator("blob_base64")
    @classmethod
    def canonical_encoding(cls, value: str) -> str:
        raw = base64.b64decode(value, validate=True)
        if base64.b64encode(raw).decode("ascii") != value:
            raise ValueError("BLOB encoding is not canonical padded base64")
        return value


Cell = (
    StrictStr
    | Annotated[StrictInt, Field(ge=-MAX_SAFE_INTEGER, le=MAX_SAFE_INTEGER)]
    | BlobCell
    | None
)
ImageRow = Annotated[tuple[Cell, ...], Field(min_length=1, max_length=10)]


class LogicalChunk(VersionedRecord):
    profile: Literal["research-state-chunk-v1"] = "research-state-chunk-v1"
    backup_id: ID
    ordinal: Counter
    table: LogicalTable
    first_row: Counter
    rows: tuple[ImageRow, ...] = Field(min_length=1, max_length=MAX_IMAGE_ROWS)

    @model_validator(mode="after")
    def bounded(self) -> "LogicalChunk":
        if len(canonical_bytes(self.model_dump(mode="json"))) > MAX_CHUNK_BYTES:
            raise ValueError("Logical chunk exceeds its encoded byte bound")
        return self


class TableInventory(VersionedRecord):
    table: LogicalTable
    row_count: int = Field(strict=True, ge=0, le=MAX_IMAGE_ROWS)


class ChunkDescriptor(VersionedRecord):
    ordinal: int = Field(strict=True, ge=0, lt=MAX_IMAGE_CHUNKS)
    table: LogicalTable
    first_row: int = Field(strict=True, ge=0, lt=MAX_IMAGE_ROWS)
    row_count: int = Field(strict=True, ge=1, le=MAX_IMAGE_ROWS)
    size_bytes: int = Field(strict=True, ge=1, le=MAX_CHUNK_BYTES)
    digest: Digest


class LogicalCheckpointManifest(VersionedRecord):
    profile: Literal["sealed-do-logical-v1"] = "sealed-do-logical-v1"
    database_schema: Literal[3] = 3
    table_layout_digest: Digest
    source_authority: WitnessAuthorityScope
    source_deployment_digest: Digest
    source_configuration_digest: Digest
    source_storage_digest: Digest
    source_custody: BackupCustodyPin
    operator_id: ID
    seal: WorkspaceSeal
    seal_revision: int = Field(strict=True, ge=1, le=MAX_SAFE_INTEGER)
    seal_digest: Digest
    snapshot_evidence_through: Counter
    tables: tuple[TableInventory, ...] = Field(min_length=12, max_length=12)
    chunks: tuple[ChunkDescriptor, ...] = Field(min_length=1, max_length=MAX_IMAGE_CHUNKS)
    artifacts: tuple[StoredBlob, ...] = Field(default=(), max_length=10_000)
    activation_requires_source_custody: Literal[True] = True

    @property
    def digest(self) -> str:
        return record_digest("research.logical-checkpoint.v1", self.model_dump(mode="json"))

    @model_validator(mode="after")
    def coherent(self) -> "LogicalCheckpointManifest":
        scope, pin, seal = self.source_authority, self.source_custody, self.seal
        identity = (scope.workspace_id, scope.controller_id, scope.control_epoch)
        if (
            identity != (seal.workspace_id, seal.controller_id, seal.control_epoch)
            or identity != (pin.workspace_id, pin.controller_id, pin.control_epoch)
            or scope.control_epoch < 1
            or self.snapshot_evidence_through < seal.evidence_through
            or self.seal_digest
            != record_digest("research.workspace-seal.v1", seal.model_dump(mode="json"))
        ):
            raise ValueError("Logical checkpoint source cut differs")
        if (
            tuple(item.table for item in self.tables) != TABLE_ORDER
            or self.tables[0].row_count != 1
        ):
            raise ValueError("Logical checkpoint requires the complete ordered table inventory")
        if sum(item.row_count for item in self.tables) > MAX_IMAGE_ROWS:
            raise ValueError("Logical checkpoint row inventory exceeds its bound")
        positions = {name: 0 for name in TABLE_ORDER}
        previous_table, total_bytes = 0, 0
        for ordinal, chunk in enumerate(self.chunks):
            table_index = TABLE_ORDER.index(chunk.table)
            if (
                chunk.ordinal != ordinal
                or table_index < previous_table
                or chunk.first_row != positions[chunk.table]
            ):
                raise ValueError("Logical checkpoint chunk coverage is not contiguous")
            positions[chunk.table] += chunk.row_count
            previous_table, total_bytes = table_index, total_bytes + chunk.size_bytes
        if (
            positions != {item.table: item.row_count for item in self.tables}
            or total_bytes > MAX_IMAGE_BYTES
        ):
            raise ValueError("Logical checkpoint chunk inventory differs or exceeds its bound")
        ids = [item.reference.id for item in self.artifacts]
        generations = [item.generation for item in self.artifacts]
        if ids != sorted(set(ids)) or len(set(generations)) != len(generations):
            raise ValueError("Logical checkpoint artifact inventory is not unique and ordered")
        if len(canonical_bytes(self.model_dump(mode="json"))) > MAX_MANIFEST_BYTES:
            raise ValueError("Logical checkpoint manifest exceeds its encoded byte bound")
        return self


class SignedLogicalCheckpoint(VersionedRecord):
    manifest: LogicalCheckpointManifest
    signature: SignatureHex


class LogicalImportIntent(VersionedRecord):
    restore_id: ID
    manifest_digest: Digest
    target_backend: Literal["cloudflare-do"] = "cloudflare-do"
    target_location: Name
    target_deployment_digest: Digest
    target_public_key: PublicKeyHex
    operator_id: ID

    @property
    def digest(self) -> str:
        return record_digest("research.logical-import-intent.v1", self.model_dump(mode="json"))
