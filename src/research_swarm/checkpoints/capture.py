"""Bounded deterministic row capture inside an already admitted source snapshot."""

from collections.abc import Iterator
from dataclasses import dataclass

from pydantic import Field

from research_swarm.checkpoints.image_format import (
    LAYOUT,
    TableLayout,
    describe_chunk,
    encode_row,
    primary_key,
)
from research_swarm.domain.canonical import MAX_SAFE_INTEGER, canonical_bytes
from research_swarm.domain.checkpoint_images import (
    MAX_CHUNK_BYTES,
    MAX_IMAGE_BYTES,
    MAX_IMAGE_CHUNKS,
    MAX_IMAGE_ROWS,
    MAX_MANIFEST_BYTES,
    BlobCell,
    Cell,
    ChunkDescriptor,
    LogicalChunk,
    TableInventory,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import Record
from research_swarm.state.schema_contracts import quoted
from research_swarm.state.sql import SqlSession, SqlValue


class CaptureLimits(Record):
    """Source allocation limits may only narrow the wire format's limits."""

    image_bytes: int = Field(default=4 * 1024**2, strict=True, ge=1, le=MAX_IMAGE_BYTES)
    chunk_bytes: int = Field(default=128 * 1024, strict=True, ge=512, le=MAX_CHUNK_BYTES)
    row_bytes: int = Field(default=64 * 1024, strict=True, ge=1, le=MAX_CHUNK_BYTES)
    manifest_bytes: int = Field(default=128 * 1024, strict=True, ge=1, le=MAX_MANIFEST_BYTES)
    rows: int = Field(default=4096, strict=True, ge=1, le=MAX_IMAGE_ROWS)
    chunks: int = Field(default=1024, strict=True, ge=1, le=MAX_IMAGE_CHUNKS)
    page_rows: int = Field(default=8, strict=True, ge=1, le=8)


def overflow() -> ResearchError:
    return ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Logical capture exceeds its profile")


def preflight_rows(sql: SqlSession, limits: CaptureLimits) -> tuple[TableInventory, ...]:
    """Read scalar lengths/types/counts only, before any portable payload is allocated."""
    inventory, total_rows, total_bytes = [], 0, 0
    for table in LAYOUT:
        name = quoted(table.name)
        count = sql.execute(
            f"SELECT COUNT(*) FROM (SELECT 1 FROM {name} LIMIT ?)",
            (limits.rows - total_rows + 1,),
        ).one()[0]
        total_rows += count
        if total_rows > limits.rows:
            raise overflow()
        predicates, sizes = [], []
        for column in table.columns:
            field = quoted(column.name)
            allowed = f"typeof({field})='{column.kind}'"
            if column.kind == "integer":
                allowed += f" AND {field} BETWEEN {-MAX_SAFE_INTEGER} AND {MAX_SAFE_INTEGER}"
            if column.nullable:
                allowed = f"({allowed}) OR {field} IS NULL"
            predicates.append(f"NOT ({allowed})")
            size = (
                f"6*length(CAST({field} AS BLOB))+2"
                if column.kind == "text"
                else f"4*((length({field})+2)/3)+18"
                if column.kind == "blob"
                else "20"
            )
            sizes.append(f"CASE WHEN {field} IS NULL THEN 4 ELSE {size} END")
        if sql.execute(f"SELECT 1 FROM {name} WHERE {' OR '.join(predicates)} LIMIT 1").fetchone():
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Logical SQL cell type differs")
        # Worst-case JSON escaping is conservative, including embedded NUL in TEXT.
        size_expression = "+".join([str(len(table.columns) + 1), *sizes])
        sizes_row = sql.execute(
            f"SELECT COALESCE(SUM(n),0),COALESCE(MAX(n),0) "
            f"FROM (SELECT ({size_expression}) n FROM {name})"
        ).one()
        total_bytes += sizes_row[0]
        if sizes_row[1] > min(limits.row_bytes, limits.chunk_bytes - 512):
            raise overflow()
        if total_bytes > limits.image_bytes:
            raise overflow()
        inventory.append(TableInventory(table=table.name, row_count=count))
    return tuple(inventory)


@dataclass(frozen=True)
class CapturedRows:
    tables: tuple[TableInventory, ...]
    descriptors: tuple[ChunkDescriptor, ...]
    bodies: tuple[bytes, ...]


def _key_columns(table: TableLayout) -> tuple[str, ...]:
    columns = {column.name: column for column in table.columns}
    return tuple(
        quoted(name) + (" COLLATE BINARY" if columns[name].kind == "text" else "")
        for name in table.primary_key
    )


def capture_rows(
    sql: SqlSession,
    *,
    backup_id: str,
    inventory: tuple[TableInventory, ...],
    limits: CaptureLimits,
) -> CapturedRows:
    """Consume the preflight inventory in the same synchronous read transaction."""
    descriptors: list[ChunkDescriptor] = []
    bodies: list[bytes] = []
    total_bytes = 0
    if tuple(item.table for item in inventory) != tuple(table.name for table in LAYOUT):
        raise ValueError("Logical capture requires the complete table inventory")
    for table, counted in zip(LAYOUT, inventory, strict=True):
        total_bytes = _capture_table(
            sql, table, counted, backup_id, limits, descriptors, bodies, total_bytes
        )
    return CapturedRows(inventory, tuple(descriptors), tuple(bodies))


def _capture_table(
    sql: SqlSession,
    table: TableLayout,
    counted: TableInventory,
    backup_id: str,
    limits: CaptureLimits,
    descriptors: list[ChunkDescriptor],
    bodies: list[bytes],
    total_bytes: int,
) -> int:
    first_row, packed_size = 0, 0
    packed: list[tuple[Cell, ...]] = []

    def frame() -> dict[str, object]:
        return {
            "schema_version": "1.0",
            "profile": "research-state-chunk-v1",
            "backup_id": backup_id,
            "ordinal": len(bodies),
            "table": table.name,
            "first_row": first_row,
            "rows": [],
        }

    def flush() -> None:
        nonlocal first_row, packed_size, total_bytes
        if not packed:
            return
        if len(bodies) >= limits.chunks:
            raise overflow()
        chunk = LogicalChunk.model_validate({**frame(), "rows": packed})
        descriptor, body = describe_chunk(chunk)
        total_bytes += len(body)
        if len(body) > limits.chunk_bytes or total_bytes > limits.image_bytes:
            raise overflow()
        descriptors.append(descriptor)
        bodies.append(body)
        first_row += len(packed)
        packed.clear()
        packed_size = 0

    for cells, body in iter_rows(sql, table, counted, limits):
        size = len(body)
        envelope_size = len(canonical_bytes(frame()))
        if packed and envelope_size + packed_size + size + len(packed) > limits.chunk_bytes:
            flush()
            envelope_size = len(canonical_bytes(frame()))
        if envelope_size + size > limits.chunk_bytes:
            raise overflow()
        packed.append(cells)
        packed_size += size
    flush()
    return total_bytes


def row_bytes(cells: tuple[Cell, ...]) -> bytes:
    return canonical_bytes(
        [cell.model_dump(mode="json") if isinstance(cell, BlobCell) else cell for cell in cells]
    )


def iter_rows(
    sql: SqlSession, table: TableLayout, counted: TableInventory, limits: CaptureLimits
) -> Iterator[tuple[tuple[Cell, ...], bytes]]:
    """Consume fully inside the owning synchronous SQL callback after size preflight."""
    name = quoted(table.name)
    fields = ",".join(quoted(column.name) for column in table.columns)
    order = ",".join(_key_columns(table))
    positions = {column.name: ordinal for ordinal, column in enumerate(table.columns)}
    previous: tuple[bytes | int, ...] | None = None
    last_values: tuple[SqlValue, ...] | None = None
    consumed = 0
    while True:
        where = ""
        parameters: tuple[SqlValue, ...] = ()
        if last_values is not None:
            placeholders = ",".join("?" for _ in last_values)
            where = f"WHERE ({order})>({placeholders})"
            parameters = last_values
        page = sql.execute(
            f"SELECT {fields} FROM {name} {where} ORDER BY {order} LIMIT ?",
            (*parameters, limits.page_rows),
        ).fetchall()
        if not page:
            break
        for row in page:
            values = tuple(row[column.name] for column in table.columns)
            cells = encode_row(table.name, values)
            key = primary_key(table.name, values)
            if previous is not None and key <= previous:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Logical row order differs")
            previous = key
            consumed += 1
            if consumed > counted.row_count:
                raise ResearchError(ErrorCode.STALE_FENCE, "Logical row inventory changed")
            body = row_bytes(cells)
            if len(body) > limits.row_bytes:
                raise overflow()
            last_values = tuple(values[positions[field]] for field in table.primary_key)
            yield cells, body
        del page
    if consumed != counted.row_count:
        raise ResearchError(ErrorCode.STALE_FENCE, "Logical row inventory changed")
