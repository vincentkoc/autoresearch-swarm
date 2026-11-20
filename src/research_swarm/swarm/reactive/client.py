"""Restricted agent operations for one explicitly granted trusted-local session."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Annotated, Any

from pydantic import Field, ValidationError

from research_swarm.swarm.canonical import MAX_SAFE_INTEGER, canonical_bytes
from research_swarm.swarm.errors import ErrorCode, ResearchError
from research_swarm.swarm.reactive.contracts import (
    DecisionProposal,
    DecisionRecord,
    JournalEvent,
    SessionState,
)
from research_swarm.swarm.reactive.journal import DecisionJournal, JournalConflict
from research_swarm.swarm.records import Record, VersionedRecord

MAX_REPLY_BYTES = 256 * 1024
MAX_RESULT_BYTES = MAX_REPLY_BYTES - 1024
Cursor = Annotated[int, Field(strict=True, ge=-1, le=MAX_SAFE_INTEGER)]


class EmptyRequest(Record):
    pass


class RespondRequest(Record):
    proposal: DecisionProposal


class ExportRequest(Record):
    after: Cursor = -1
    limit: int = Field(default=20, strict=True, ge=1, le=100)
    max_bytes: int = Field(default=65536, strict=True, ge=1024, le=MAX_RESULT_BYTES)


class EventPage(VersionedRecord):
    events: tuple[JournalEvent, ...]
    next_after: Cursor
    has_more: bool


class DecisionClient:
    def __init__(self, session_path: Path) -> None:
        try:
            self._journal = DecisionJournal.open(session_path)
        except (OSError, sqlite3.Error, ValueError):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Local reactive session is unavailable"
            ) from None

    def dispatch(self, operation: str, arguments: dict[str, Any]) -> Any:
        """One validation and dispatch path for Python, JSON CLI and stdio MCP."""
        result: dict[str, Any] | None
        try:
            if operation == "inspect":
                EmptyRequest.model_validate(arguments)
                result = self._journal.inspect().model_dump(mode="json")
            elif operation == "pending":
                EmptyRequest.model_validate(arguments)
                pending = self._journal.pending()
                result = pending.model_dump(mode="json") if pending is not None else None
            elif operation == "respond":
                request = RespondRequest.model_validate(arguments)
                result = self._journal.respond(request.proposal).model_dump(mode="json")
            elif operation == "export":
                result = self._export(ExportRequest.model_validate(arguments)).model_dump(
                    mode="json"
                )
            else:
                raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Unknown reactive operation")
            if len(canonical_bytes(result)) > MAX_RESULT_BYTES:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Reactive response exceeds the byte limit"
                )
            return result
        except JournalConflict as error:
            raise ResearchError(ErrorCode.INVALID_TRANSITION, str(error)) from None
        except (ValidationError, ValueError):
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Reactive request does not match its contract"
            ) from None
        except (OSError, sqlite3.Error):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Local reactive session is unavailable"
            ) from None

    def _export(self, request: ExportRequest) -> EventPage:
        rows = self._journal.events(after=request.after, limit=request.limit + 1)
        count = payload_bytes = 0
        for row in rows[: request.limit]:
            # The empty envelope accounts for the cursor, flag and array brackets.
            # Appending a row after count existing events requires count commas in all.
            envelope = EventPage(events=(), next_after=row.sequence, has_more=count + 1 < len(rows))
            event_size = len(canonical_bytes(row.model_dump(mode="json")))
            envelope_size = len(canonical_bytes(envelope.model_dump(mode="json")))
            if envelope_size + payload_bytes + event_size + count > request.max_bytes:
                if count == 0:
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE,
                        "One event exceeds max_bytes; request a larger bounded page",
                    )
                break
            payload_bytes += event_size
            count += 1
        return EventPage(
            events=rows[:count],
            next_after=rows[count - 1].sequence if count else request.after,
            has_more=count < len(rows),
        )

    def inspect(self) -> SessionState:
        return SessionState.model_validate(self.dispatch("inspect", {}))

    def pending(self) -> DecisionRecord | None:
        value = self.dispatch("pending", {})
        return DecisionRecord.model_validate(value) if value is not None else None

    def respond(self, proposal: DecisionProposal) -> DecisionRecord:
        return DecisionRecord.model_validate(self.dispatch("respond", {"proposal": proposal}))

    def export(self, *, after: int = -1, limit: int = 20, max_bytes: int = 65536) -> EventPage:
        return EventPage.model_validate(
            self.dispatch("export", {"after": after, "limit": limit, "max_bytes": max_bytes})
        )
