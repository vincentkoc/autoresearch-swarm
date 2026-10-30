"""Mint one reader-maintenance capability after the owner closure boundary succeeds."""

from dataclasses import asdict

from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.crypto import Ed25519Provider
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, RecoveryReaderGuard
from research_swarm.state.database import Database, JsonObject, Transaction, _request_digest
from research_swarm.state.recovery_readers import (
    reader_digest,
    reader_references,
    references_digest,
    require_recovery_reader,
)
from research_swarm.state.sql import SqlSession


def recovery_reader_context(
    database: Database,
    context: CommandContext,
    session: JsonObject,
    *,
    crypto_provider: Ed25519Provider | None = None,
) -> CommandContext:
    authority = recovery_authority(database, context, crypto_provider=crypto_provider)
    if authority is None:
        return context
    operation, payload = "artifact.reconcile_reader_session", {"session_id": session["id"]}
    digest = _request_digest(context, operation, payload)

    def read(connection: SqlSession) -> CommandContext:
        transaction = Transaction(connection, context)
        if transaction.get("artifact_read_session", session["id"]) != session:
            raise ResearchError(ErrorCode.STALE_FENCE, "Reader changed after native closure proof")
        guarded = context.model_copy()
        guarded._recovery_reader = RecoveryReaderGuard(
            **asdict(authority),
            reader_session_id=session["id"],
            reader_digest=reader_digest(session),
            references_digest=references_digest(reader_references(transaction, session["id"])),
            request_digest=digest,
        )
        require_recovery_reader(
            connection, guarded, database.authority_identity, operation, payload, digest
        )
        return guarded

    return database.store.read(read)
