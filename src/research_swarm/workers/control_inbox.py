"""Cancellation receipts share the worker journal's transaction boundary."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.controls import CancellationDirective, ControlAcknowledgment
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import utc_now
from research_swarm.domain.workers import WorkerBinding

if TYPE_CHECKING:
    from research_swarm.workers.journal_records import JournalEntry


SCHEMA = """
CREATE TABLE IF NOT EXISTS control_inbox (
    attempt_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    received_at TEXT NOT NULL,
    acknowledged_at TEXT
);
CREATE INDEX IF NOT EXISTS control_inbox_pending ON control_inbox(acknowledged_at)
WHERE acknowledged_at IS NULL;
"""


def read_control(connection: sqlite3.Connection, attempt_id: str) -> CancellationDirective | None:
    row = connection.execute(
        "SELECT payload FROM control_inbox WHERE attempt_id=?", (attempt_id,)
    ).fetchone()
    return CancellationDirective.model_validate_json(row["payload"]) if row is not None else None


def store_control(
    connection: sqlite3.Connection,
    binding: WorkerBinding,
    directive: CancellationDirective,
) -> None:
    if directive.binding != binding:
        raise ResearchError(ErrorCode.STALE_FENCE, "Cancellation belongs to another enrollment")
    existing = read_control(connection, directive.attempt_id)
    if existing is not None:
        if existing != directive:
            raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Cancellation identity changed")
        return
    connection.execute(
        "INSERT INTO control_inbox(attempt_id,payload,received_at) VALUES (?,?,?)",
        (directive.attempt_id, directive.model_dump_json(), utc_now()),
    )


def merge_control(connection: sqlite3.Connection, entry: JournalEntry) -> JournalEntry:
    directive = read_control(connection, entry.dispatch.attempt_id)
    if directive is None:
        return entry
    binding = WorkerBinding.model_validate(
        {key: getattr(entry.dispatch, key) for key in WorkerBinding.model_fields}
    )
    if directive.binding != binding or directive.dispatch_digest != record_digest(
        "research.dispatch", entry.dispatch.model_dump(mode="json")
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Cancellation differs from admitted dispatch")
    # Terminal journal bytes may already be published. Preserve their exact identity.
    if entry.state in {"closed", "not_started"} or entry.cancel_requested:
        return entry
    return entry.model_copy(update={"cancel_requested": True})


def pending_acknowledgments(
    connection: sqlite3.Connection,
    binding: WorkerBinding,
    limit: int,
) -> tuple[ControlAcknowledgment, ...]:
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("Control acknowledgment limit must be between 1 and 100")
    rows = connection.execute(
        "SELECT payload FROM control_inbox WHERE acknowledged_at IS NULL ORDER BY rowid LIMIT ?",
        (limit,),
    ).fetchall()
    controls = tuple(CancellationDirective.model_validate_json(row["payload"]) for row in rows)
    if any(item.binding != binding for item in controls):
        raise ResearchError(ErrorCode.STALE_FENCE, "Control inbox contains another enrollment")
    return tuple(item.acknowledgment() for item in controls)


def acknowledge_controls(
    connection: sqlite3.Connection,
    binding: WorkerBinding,
    acknowledgments: tuple[ControlAcknowledgment, ...],
) -> None:
    if len(acknowledgments) > 100 or len({a.attempt_id for a in acknowledgments}) != len(
        acknowledgments
    ):
        raise ValueError("Control acknowledgment batch must contain at most 100 distinct attempts")
    for acknowledgment in acknowledgments:
        control = read_control(connection, acknowledgment.attempt_id)
        if (
            control is None
            or control.binding != binding
            or control.acknowledgment() != acknowledgment
        ):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Acknowledgment differs from persisted control"
            )
    for acknowledgment in acknowledgments:
        connection.execute(
            "UPDATE control_inbox SET acknowledged_at=COALESCE(acknowledged_at,?) "
            "WHERE attempt_id=?",
            (utc_now(), acknowledgment.attempt_id),
        )
