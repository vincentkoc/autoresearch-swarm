"""Buffer verified execution evidence before one witnessed acceptance transaction."""

import asyncio

from research_swarm.control.observations import PublishAttempt, evidence_roles
from research_swarm.control.recovery_operations import require_raw_operation
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.observation_bytes import decode_observation_bytes
from research_swarm.projects.workload import WorkloadReport
from research_swarm.state.database import Transaction
from research_swarm.workers.journal_records import JournalEntry

if __package__:
    from .research_router import supports_publication, validated
else:
    from research_router import supports_publication, validated


COMMANDS = frozenset({"worker.publish_attempt"})
MAX_EVIDENCE_ARTIFACTS = 32
MAX_EVIDENCE_BYTES = 8 * 1024**2


def select_evidence(service, transaction, request, attempt, worker, *, scope):
    if len(evidence_roles(request)) > MAX_EVIDENCE_ARTIFACTS:
        raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Publication has too many artifacts")
    records = service._evidence_records_in(transaction, request, attempt, worker, scope=scope)
    if sum(record.reference.size_bytes for record in records.values()) > MAX_EVIDENCE_BYTES:
        raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Publication exceeds its byte limit")
    return records


async def read_evidence(router, request, records, check_identity, *, observations=True):
    """Verify every authorized generation, retaining only bounded observation bytes."""
    selected = {request.journal_artifact.id}
    if request.report_artifact is not None:
        selected.add(request.report_artifact.id)
    bodies = {}
    for identifier, record in records.items():
        body = await router.objects.read(
            record.workspace_id, router.service.artifacts._blob(record)
        )
        check_identity()
        if observations and identifier in selected:
            bodies[identifier] = body
    return bodies


def decode_evidence(request, bodies):
    try:
        journal = JournalEntry.model_validate(
            decode_observation_bytes(request.journal_artifact, bodies[request.journal_artifact.id])
        )
        report = (
            WorkloadReport.model_validate(
                decode_observation_bytes(
                    request.report_artifact, bodies[request.report_artifact.id]
                )
            )
            if request.report_artifact is not None
            else None
        )
    except ValueError as error:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid execution observation"
        ) from error
    return journal, report


class HostedResultPublication:
    commands = COMMANDS

    def __init__(self, router):
        self.router = router
        self.database = router.database
        self.service = router.service.results

    def _identity(self, token, context):
        if not supports_publication(self.router.configuration):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Execution result publication is unavailable"
            )
        current = self.router._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Publication credential changed")
        context.principal.require("worker.publish")
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Publication owner was cancelled")

    def _select_in(self, transaction, request):
        attempt, worker = self.service._publication_owner_in(transaction, request)
        records = select_evidence(
            self.service, transaction, request, attempt, worker, scope="worker.publish"
        )
        return attempt, worker, records

    def _select(self, context, request):
        require_raw_operation(
            self.database, context, "worker.publish_attempt", request.model_dump(mode="json")
        )
        return self.database.store.read(
            lambda sql: self._select_in(Transaction(sql, context), request)
        )

    async def command(self, token, context, operation, payload):
        self._identity(token, context)
        if operation not in COMMANDS:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Publication is unavailable")
        request = validated(PublishAttempt, payload)
        normalized = request.model_dump(mode="json")
        previous = self.database.replay(context, operation, normalized, "worker.publish")
        if previous is not None:
            return previous
        selected = self._select(context, request)
        bodies = await read_evidence(
            self.router, request, selected[2], lambda: self._identity(token, context)
        )
        self._identity(token, context)
        if self._select(context, request) != selected:
            raise ResearchError(ErrorCode.STALE_FENCE, "Execution evidence changed during I/O")
        journal, report = decode_evidence(request, bodies)

        def accept(transaction):
            if self._select_in(transaction, request) != selected:
                raise ResearchError(ErrorCode.STALE_FENCE, "Execution evidence changed")
            return self.service._accept(transaction, request, journal, report)

        def commit():
            self._identity(token, context)
            return self.database.execute(
                context,
                operation,
                normalized,
                "worker.publish",
                accept,
                on_rejection=lambda transaction, error: self.service._fence_rejection(
                    transaction, request, error
                ),
            )

        return await self.router._witnessed(context, operation, normalized, commit)
