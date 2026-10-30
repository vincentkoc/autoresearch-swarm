"""Exact reader custody checks; this module cannot establish native closure."""

from collections.abc import Mapping
from typing import Any

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.state.database import JsonObject, Transaction
from research_swarm.state.recovery_guard import require_recovery_authority
from research_swarm.state.sql import SqlSession, StoreIdentity


def reader_references(transaction: Transaction, session_id: str) -> tuple[JsonObject, ...]:
    rows = transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='artifact_reference' "
        "AND state='retained' AND json_extract(payload,'$.reader_session_id')=? ORDER BY id",
        (transaction.workspace_id, session_id),
    ).fetchall()
    references = tuple(transaction.get("artifact_reference", row["id"]) for row in rows)
    for reference in references:
        retention = reference["data"]["retention"]
        if retention["owner_kind"] != "artifact_read" or retention["purpose"] != "read":
            raise ResearchError(
                ErrorCode.ARTIFACT_UNVERIFIED, "Reader session has a non-reader reference"
            )
    return references


def references_digest(references: tuple[JsonObject, ...]) -> str:
    return record_digest("research.recovery-reader-references.v1", {"references": list(references)})


def reader_digest(session: JsonObject) -> str:
    return record_digest("research.recovery-reader-session.v1", session)


def require_recovery_reader(
    connection: SqlSession,
    context: CommandContext,
    authority: StoreIdentity,
    operation: str,
    payload: Mapping[str, Any],
    request_digest: str,
) -> None:
    guard = context._recovery_reader
    if (
        guard is None
        or operation != "artifact.reconcile_reader_session"
        or guard.request_digest != request_digest
        or payload != {"session_id": guard.reader_session_id}
        or context._recovery_operation is not None
    ):
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Recovery reader closure proof is required"
        )
    require_recovery_authority(connection, context, authority, guard)
    transaction = Transaction(connection, context)
    session = transaction.get("artifact_read_session", guard.reader_session_id)
    refs = reader_references(transaction, guard.reader_session_id)
    if (
        reader_digest(session) != guard.reader_digest
        or references_digest(refs) != guard.references_digest
    ):
        raise ResearchError(
            ErrorCode.STALE_FENCE, "Recovery reader custody changed after closure proof"
        )
