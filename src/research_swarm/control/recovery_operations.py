"""Mint evidence-only capabilities from an elected, admitted restoration."""

from dataclasses import asdict

from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.control.worker_records import AttemptData, WorkerData
from research_swarm.crypto import Ed25519Provider
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, RecoveryOperationGuard
from research_swarm.state.database import Database, JsonObject, Transaction, _request_digest
from research_swarm.state.recovery_guard import ROOT_OPERATIONS, require_recovery_operation
from research_swarm.state.sql import SqlSession
from research_swarm.state.workspace_gate import require_workspace_mutation

MAX_JOURNAL_BYTES = 1024 * 1024


def recovery_context(
    database: Database,
    context: CommandContext,
    operation: str,
    payload: JsonObject,
    *,
    attempt_id: str,
    upload_id: str | None = None,
    artifact_id: str | None = None,
    crypto_provider: Ed25519Provider | None = None,
) -> CommandContext:
    """Called only by named control handlers; callers cannot supply this proof over the wire."""
    authority = recovery_authority(database, context, crypto_provider=crypto_provider)
    if authority is None:
        return context
    if operation not in ROOT_OPERATIONS:
        raise ResearchError(
            ErrorCode.STALE_FENCE, "Workspace does not admit this recovery operation"
        )

    def read(connection: SqlSession) -> CommandContext:
        transaction = Transaction(connection, context)
        attempt = AttemptData.model_validate(transaction.get("attempt", attempt_id)["data"])
        worker = WorkerData.model_validate(
            transaction.get("worker_session", attempt.dispatch.worker_session_id)["data"]
        )
        guarded = context.model_copy()
        guarded._recovery_operation = RecoveryOperationGuard(
            **asdict(authority),
            attempt_id=attempt_id,
            worker_id=worker.binding.worker_id,
            session_id=worker.binding.worker_session_id,
            worker_principal_id=worker.principal_id,
            root_operation=operation,
            root_request_digest=_request_digest(context, operation, payload),
            upload_id=upload_id,
            artifact_id=artifact_id,
        )
        require_recovery_operation(
            connection,
            guarded,
            database.authority_identity,
            operation,
            payload,
            _request_digest(context, operation, payload),
        )
        return guarded

    return database.store.read(read)


def require_raw_operation(
    database: Database,
    context: CommandContext,
    operation: str,
    payload: JsonObject,
) -> None:
    """Raw status/transfer callbacks cannot bypass the inactive workspace gate."""
    database.store.read(
        lambda connection: require_workspace_mutation(
            connection,
            context.principal.workspace_id,
            operation,
            context=context,
            authority=database.authority_identity,
            payload=payload,
            request_digest=_request_digest(context, operation, payload),
        )
    )


def require_journal_upload(context: CommandContext, role: str, name: str | None, size: int) -> None:
    if context._recovery_operation is not None and (
        role != "journal" or name is not None or size > MAX_JOURNAL_BYTES
    ):
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Recovery accepts bounded journals only"
        )
