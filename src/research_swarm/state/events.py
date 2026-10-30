"""Logical stream retention is independent from the permanent event ledger."""

from __future__ import annotations

import sqlite3
from typing import Any

from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError


def event_page(
    connection: sqlite3.Connection, workspace_id: str, *, after: int, limit: int
) -> dict[str, Any]:
    if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 1000:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid event cursor or page size")
    row = connection.execute(
        "SELECT next_sequence-1 AS watermark,event_floor FROM workspaces WHERE id=?",
        (workspace_id,),
    ).fetchone()
    if row is None:
        raise ResearchError(ErrorCode.NOT_FOUND, "Workspace not found")
    if after < row["event_floor"] - 1:
        raise ResearchError(
            ErrorCode.EVENT_CURSOR_EXPIRED,
            "Event history no longer covers this cursor; recover a frozen snapshot",
            details={
                "event_floor": row["event_floor"],
                "watermark": row["watermark"],
                "recovery_command": "stream.create_snapshot",
            },
        )
    if after > row["watermark"]:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Event cursor is in the future")
    rows = connection.execute(
        "SELECT payload FROM events WHERE workspace_id=? AND sequence>? ORDER BY sequence LIMIT ?",
        (workspace_id, after, limit),
    )
    events = [strict_json_loads(item["payload"]) for item in rows]
    return {
        "events": events,
        "cursor": events[-1]["sequence"] if events else after,
        "event_floor": row["event_floor"],
        "watermark": row["watermark"],
    }
