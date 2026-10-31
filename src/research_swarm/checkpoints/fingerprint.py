"""Stream a complete portable-state fingerprint within an admitted SQL snapshot."""

import hashlib

from research_swarm.checkpoints.capture import CaptureLimits, iter_rows, preflight_rows
from research_swarm.checkpoints.image_format import LAYOUT, LAYOUT_DIGEST
from research_swarm.checkpoints.image_schema import require_portable_image_schema
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.state.schema import SCHEMA_VERSION
from research_swarm.state.sql import SqlSession

# Inactive restoration adds worker archives, fences, events and outbox rows.
# These limits bound scalar preflight and page reads, not an in-memory image.
INACTIVE_LIMITS = CaptureLimits(image_bytes=32 * 1024**2, rows=32768)


def fingerprint_state(sql: SqlSession, *, limits: CaptureLimits = INACTIVE_LIMITS) -> str:
    """No authority verdict: the owner separately checks metadata, intent and phase."""
    require_portable_image_schema(sql)
    inventory = preflight_rows(sql, limits)
    digest = hashlib.sha256(b"research.logical-inactive-state.v1\0")

    def append(value: bytes) -> None:
        digest.update(str(len(value)).encode("ascii") + b":" + value)

    append(
        canonical_bytes(
            {
                "database_schema": SCHEMA_VERSION,
                "table_layout_digest": LAYOUT_DIGEST,
                "tables": [item.model_dump(mode="json") for item in inventory],
            }
        )
    )
    for table, counted in zip(LAYOUT, inventory, strict=True):
        append(table.name.encode("ascii"))
        for _, body in iter_rows(sql, table, counted, limits):
            append(body)
    return "sha256:" + digest.hexdigest()
