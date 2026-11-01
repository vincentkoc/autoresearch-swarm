"""Quarantine and explicitly reconcile original evidence under active authority."""

import asyncio
from typing import Literal

from research_swarm.control.dispatch import validate_command_input
from research_swarm.control.recovery_operations import require_raw_operation
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction

if __package__:
    from ..delivery.transfers import TransferQuery, TransferRead, TransferRecordKind
    from .research_router import supports_reconciliation, validated
    from .result_publication import decode_evidence, read_evidence, select_evidence
else:
    from research_router import supports_reconciliation, validated
    from result_publication import decode_evidence, read_evidence, select_evidence
    from transfers import TransferQuery, TransferRead, TransferRecordKind

COMMANDS = frozenset({"worker.submit_evidence", "attempt.reconcile"})
ReconciliationRecordKind = Literal[
    TransferRecordKind, "attempt_submission", "attempt_reconciliation"
]


class ReconciliationQuery(TransferQuery):
    kind: ReconciliationRecordKind


class ReconciliationRead(TransferRead):
    kind: ReconciliationRecordKind


class HostedReconciliation:
    commands = COMMANDS

    def __init__(self, router):
        self.router = router
        self.database, self.service = router.database, router.service.reconciliation

    def _identity(self, token, context, scope):
        if not supports_reconciliation(self.router.configuration):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Active reconciliation is unavailable"
            )
        current = self.router._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Evidence credential changed")
        context.principal.require(scope)
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Evidence owner was cancelled")

    def query(self, token, context, operation, payload):
        self._identity(token, context, "read")
        if operation == "get":
            request = validated(ReconciliationRead, payload)
            return self.router.records.get(context, request.kind, request.id)
        return self.router.records.list(context, validated(ReconciliationQuery, payload))

    def _select_in(self, transaction, operation, request, scope):
        if operation == "worker.submit_evidence":
            worker, attempt = self.service._owner(transaction, request)
            publication = request
        else:
            submission, worker, attempt = self.service._load(transaction, request)
            publication = submission.publication
        records = select_evidence(
            self.router.service.results,
            transaction,
            publication,
            attempt,
            worker,
            scope=scope,
        )
        return publication, worker, attempt, records

    def _select(self, context, operation, request, scope):
        require_raw_operation(self.database, context, operation, request.model_dump(mode="json"))
        return self.database.store.read(
            lambda sql: self._select_in(Transaction(sql, context), operation, request, scope)
        )

    async def command(self, token, context, operation, payload):
        if operation not in COMMANDS:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Reconciliation is unavailable")
        definition, request = validate_command_input(context, operation, payload)
        scope, normalized = definition.scope, request.model_dump(mode="json")
        self._identity(token, context, scope)
        previous = self.database.replay(context, operation, normalized, scope)
        if previous is not None:
            return previous
        selected = self._select(context, operation, request, scope)
        publication, worker, _, records = selected
        submitting = operation == "worker.submit_evidence"
        bodies = await read_evidence(
            self.router,
            publication,
            records,
            lambda: self._identity(token, context, scope),
            observations=not submitting,
        )
        self._identity(token, context, scope)
        if self._select(context, operation, request, scope) != selected:
            raise ResearchError(ErrorCode.STALE_FENCE, "Original evidence changed during I/O")
        journal, report = (None, None) if submitting else decode_evidence(publication, bodies)

        def accept(transaction):
            if self._select_in(transaction, operation, request, scope) != selected:
                raise ResearchError(ErrorCode.STALE_FENCE, "Original evidence changed")
            if submitting:
                return self.service._submit_verified(transaction, request, worker, None)
            return self.service._reconcile(transaction, request, journal, report)

        def commit():
            self._identity(token, context, scope)
            return self.database.execute(context, operation, normalized, scope, accept)

        return await self.router._witnessed(context, operation, normalized, commit)
