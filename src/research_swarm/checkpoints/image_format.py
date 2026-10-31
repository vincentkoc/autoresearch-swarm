"""Fixed SQL layout and authenticated logical-image bytes, without import authority."""

import base64
from collections.abc import Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from pydantic import TypeAdapter

from research_swarm.crypto import Ed25519Provider, public_key, sign, verify
from research_swarm.domain.backup import BackupCustodyPin
from research_swarm.domain.canonical import (
    MAX_SAFE_INTEGER,
    canonical_bytes,
    content_digest,
    record_digest,
    strict_json_loads,
)
from research_swarm.domain.checkpoint_images import (
    MAX_CHUNK_BYTES,
    MAX_IMAGE_BYTES,
    MAX_IMAGE_CHUNKS,
    MAX_MANIFEST_BYTES,
    TABLE_ORDER,
    BlobCell,
    Cell,
    ChunkDescriptor,
    ImageRow,
    LogicalCheckpointManifest,
    LogicalChunk,
    LogicalTable,
    SignedLogicalCheckpoint,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.state.sql import SqlValue


@dataclass(frozen=True)
class Column:
    name: str
    kind: Literal["text", "integer", "blob"] = "text"
    nullable: bool = False


@dataclass(frozen=True)
class TableLayout:
    name: LogicalTable
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]


def _layout(name: LogicalTable, fields: str, primary_key: str) -> TableLayout:
    columns = []
    for field in fields.split():
        raw = field.rstrip("?")
        kind: Literal["text", "integer", "blob"] = (
            "integer" if raw.endswith(":i") else "blob" if raw.endswith(":b") else "text"
        )
        columns.append(Column(raw.split(":")[0], kind, field.endswith("?")))
    return TableLayout(name, tuple(columns), tuple(primary_key.split()))


LAYOUT = (
    _layout("workspaces", "id controller_id control_epoch:i next_sequence:i event_floor:i", "id"),
    _layout(
        "records",
        "workspace_id kind id revision:i state campaign_id? parent_id? "
        "payload created_at updated_at",
        "workspace_id kind id",
    ),
    _layout(
        "commands",
        "workspace_id actor_id idempotency_key request_digest command_id response created_at",
        "workspace_id actor_id idempotency_key",
    ),
    _layout("events", "workspace_id sequence:i id payload", "workspace_id sequence"),
    _layout(
        "outbox", "event_id workspace_id sequence:i delivered:i delivery_attempts:i", "event_id"
    ),
    _layout(
        "budgets",
        "workspace_id campaign_id dimension payload",
        "workspace_id campaign_id dimension",
    ),
    _layout(
        "budget_reservations",
        "workspace_id id campaign_id owner_id state payload",
        "workspace_id id",
    ),
    _layout(
        "budget_settlements",
        "workspace_id settlement_id request_digest response",
        "workspace_id settlement_id",
    ),
    _layout(
        "budget_entries",
        "workspace_id id campaign_id dimension settlement_id payload",
        "workspace_id id",
    ),
    _layout(
        "recovery_snapshots",
        "workspace_id id actor_id watermark:i expires_at released_at? expired_at? payload",
        "workspace_id id",
    ),
    _layout(
        "recovery_items",
        "workspace_id snapshot_id ordinal:i content_digest size_bytes:i content:b",
        "workspace_id snapshot_id ordinal",
    ),
    _layout(
        "credentials",
        "id token_digest workspace_id principal created_at expires_at? revoked:i",
        "id",
    ),
)
TABLES = MappingProxyType({table.name: table for table in LAYOUT})
ROW = TypeAdapter(ImageRow)
LAYOUT_DIGEST = record_digest(
    "research.logical-checkpoint-layout.v1",
    {
        "tables": [
            {
                "table": table.name,
                "columns": [
                    {"name": column.name, "kind": column.kind, "nullable": column.nullable}
                    for column in table.columns
                ],
                "primary_key": table.primary_key,
                "text_order": "utf8-binary",
            }
            for table in LAYOUT
        ]
    },
)


def encode_row(table: LogicalTable, values: Sequence[SqlValue]) -> tuple[Cell, ...]:
    cells = tuple(
        BlobCell(blob_base64=base64.b64encode(value).decode("ascii"))
        if isinstance(value, bytes)
        else value
        for value in values
    )
    row = ROW.validate_python(cells)
    row_values(table, row)
    return row


def row_values(table: LogicalTable, row: tuple[Cell, ...]) -> tuple[SqlValue, ...]:
    columns = TABLES[table].columns
    if len(row) != len(columns):
        raise ValueError("Logical row column count differs")
    result: list[SqlValue] = []
    for column, cell in zip(columns, row, strict=True):
        if cell is None and column.nullable:
            result.append(None)
        elif column.kind == "text" and type(cell) is str:
            cell.encode("utf-8", errors="strict")
            result.append(cell)
        elif column.kind == "integer" and type(cell) is int and abs(cell) <= MAX_SAFE_INTEGER:
            result.append(cell)
        elif column.kind == "blob" and isinstance(cell, BlobCell):
            result.append(base64.b64decode(cell.blob_base64, validate=True))
        else:
            raise ValueError("Logical row cell type differs")
    return tuple(result)


def primary_key(table: LogicalTable, values: tuple[SqlValue, ...]) -> tuple[bytes | int, ...]:
    columns = {column.name: index for index, column in enumerate(TABLES[table].columns)}
    result: list[bytes | int] = []
    for field in TABLES[table].primary_key:
        value = values[columns[field]]
        if type(value) is str:
            result.append(value.encode("utf-8", errors="strict"))
        elif type(value) is int:
            result.append(value)
        else:
            raise ValueError("Logical row key is not an exact string or integer")
    return tuple(result)


def describe_chunk(chunk: LogicalChunk) -> tuple[ChunkDescriptor, bytes]:
    body = canonical_bytes(chunk.model_dump(mode="json"))
    return ChunkDescriptor(
        ordinal=chunk.ordinal,
        table=chunk.table,
        first_row=chunk.first_row,
        row_count=len(chunk.rows),
        size_bytes=len(body),
        digest=content_digest(body),
    ), body


def sign_manifest(
    manifest: LogicalCheckpointManifest, seed: bytes, *, crypto: Ed25519Provider
) -> bytes:
    if public_key(crypto, seed).hex() != manifest.source_custody.public_key:
        raise ValueError("Logical checkpoint signing key differs")
    signed = SignedLogicalCheckpoint(
        manifest=manifest, signature=sign(crypto, seed, manifest.digest.encode("ascii")).hex()
    )
    return canonical_bytes(signed.model_dump(mode="json"))


@dataclass(frozen=True)
class VerifiedLogicalImage:
    """Authenticated wire contents only; no artifact, relational or execution verdict."""

    manifest: LogicalCheckpointManifest
    chunks: tuple[LogicalChunk, ...]


def verify_image(
    envelope: bytes,
    bodies: Sequence[bytes],
    *,
    expected_digest: str,
    expected_source: WitnessAuthorityScope,
    expected_custody: BackupCustodyPin,
    crypto: Ed25519Provider,
) -> VerifiedLogicalImage:
    try:
        # The fixed signed envelope adds only a signature and schema metadata.
        if not isinstance(envelope, bytes) or len(envelope) > MAX_MANIFEST_BYTES + 256:
            raise ValueError("Logical checkpoint envelope exceeds its bound")
        if (
            len(bodies) > MAX_IMAGE_CHUNKS
            or any(not isinstance(body, bytes) or len(body) > MAX_CHUNK_BYTES for body in bodies)
            or sum(map(len, bodies)) > MAX_IMAGE_BYTES
        ):
            raise ValueError("Logical checkpoint chunks exceed their bounds")
        signed = SignedLogicalCheckpoint.model_validate(strict_json_loads(envelope))
        manifest = signed.manifest
        if (
            canonical_bytes(signed.model_dump(mode="json")) != envelope
            or manifest.digest != expected_digest
            or manifest.source_authority != expected_source
            or manifest.source_custody != expected_custody
            or manifest.table_layout_digest != LAYOUT_DIGEST
            or tuple(item.table for item in manifest.tables) != TABLE_ORDER
            or len(bodies) != len(manifest.chunks)
        ):
            raise ValueError("Logical checkpoint identity or layout differs")
        verify(
            crypto,
            bytes.fromhex(expected_custody.public_key),
            manifest.digest.encode("ascii"),
            bytes.fromhex(signed.signature),
        )
        chunks = []
        keys: dict[str, tuple[bytes | int, ...]] = {}
        for descriptor, body in zip(manifest.chunks, bodies, strict=True):
            if len(body) != descriptor.size_bytes or content_digest(body) != descriptor.digest:
                raise ValueError("Logical checkpoint chunk bytes differ")
            chunk = LogicalChunk.model_validate(strict_json_loads(body))
            actual, encoded = describe_chunk(chunk)
            if (
                encoded != body
                or actual != descriptor
                or chunk.backup_id != manifest.seal.backup_id
            ):
                raise ValueError("Logical checkpoint chunk reference differs")
            for row in chunk.rows:
                key = primary_key(chunk.table, row_values(chunk.table, row))
                if chunk.table in keys and key <= keys[chunk.table]:
                    raise ValueError("Logical checkpoint row keys are duplicated or unordered")
                keys[chunk.table] = key
            chunks.append(chunk)
        return VerifiedLogicalImage(manifest, tuple(chunks))
    except (ValueError, TypeError, KeyError, RecursionError):
        raise ResearchError(
            ErrorCode.ARTIFACT_UNVERIFIED, "Logical checkpoint integrity differs"
        ) from None
