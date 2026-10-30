"""Admit original saved-turn reconciliation without executing its saved action."""

from dataclasses import asdict

from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.crypto import Ed25519Provider
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, RecoveryTurnGuard
from research_swarm.state.database import Database, JsonObject, Transaction, _request_digest
from research_swarm.state.recovery_turns import (
    OPERATIONS,
    require_recovery_turn,
    scope_digest,
    turn_scope,
)
from research_swarm.state.sql import SqlSession


def recovery_turn_context(
    database: Database,
    context: CommandContext,
    operation: str,
    payload: JsonObject,
    *,
    crypto_provider: Ed25519Provider | None = None,
) -> CommandContext:
    authority = recovery_authority(database, context, crypto_provider=crypto_provider)
    if authority is None:
        return context
    context.principal.require("orchestration.manage")
    context.principal.require("read")
    if operation not in OPERATIONS:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery cannot execute saved actions")
    digest = _request_digest(context, operation, payload)

    def read(connection: SqlSession) -> CommandContext:
        scope = turn_scope(
            Transaction(connection, context), authority.restore_id, str(payload["turn_id"])
        )
        guarded = context.model_copy()
        guarded._recovery_turn = RecoveryTurnGuard(
            **asdict(authority),
            turn_id=str(payload["turn_id"]),
            scope_digest=scope_digest(scope),
            operation=operation,
            request_digest=digest,
        )
        require_recovery_turn(
            connection, guarded, database.authority_identity, operation, payload, digest
        )
        return guarded

    return database.store.read(read)
