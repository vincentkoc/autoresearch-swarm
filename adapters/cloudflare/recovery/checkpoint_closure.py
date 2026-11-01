"""Private operational settlement from original journal bytes; no scientific acceptance."""

import asyncio

from research_swarm.control.observations import PublishAttempt
from research_swarm.control.reconciliation import AttemptSubmission, ReconcileAttempt
from research_swarm.control.recovery_operations import MAX_JOURNAL_BYTES, recovery_context
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, Revision, VersionedRecord
from research_swarm.state.database import Transaction, encode

if __package__:
    from ..runtime.exclusive import exclusive
    from .checkpoint_source import parse_checkpoint_request
    from .recovery_router import ExistingRecoveryRouter
else:
    from checkpoint_source import parse_checkpoint_request
    from exclusive import exclusive
    from recovery_router import ExistingRecoveryRouter


class CheckpointClosureCommand(VersionedRecord):
    request_id: ID
    expected_revision: Revision
    payload: ReconcileAttempt


async def reconcile_target_attempt(
    state,
    open_store,
    manifest,
    secrets,
    *,
    identity,
    credential,
    target_json,
    request_json,
    crypto,
    objects,
):
    secrets.require_bootstrap(credential)
    manifest.require_store(identity)
    request = parse_checkpoint_request(request_json, CheckpointClosureCommand)
    router = await ExistingRecoveryRouter.open(
        state,
        open_store,
        manifest,
        secrets,
        target_json=target_json,
        crypto=crypto,
        objects=objects,
    )
    database, service = router.database, router.service
    intent = request.payload
    payload = intent.model_dump(mode="json")
    operation = "attempt.reconcile_closure"

    async def reconcile():
        context = CommandContext(
            principal=router.operator,
            idempotency_key="logical-closure:" + request.request_id,
            expected_revision=request.expected_revision,
        )
        router._verify(context)

        def submitted(sql):
            row = Transaction(sql, context).get("attempt_submission", intent.submission_id)
            submission = AttemptSubmission.model_validate(row["data"])
            if (
                row["state"] != "quarantined"
                or submission.publication.attempt_id != intent.attempt_id
            ):
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Closure submission differs")
            if submission.publication.journal_artifact.size_bytes > MAX_JOURNAL_BYTES:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Closure journal exceeds its bound"
                )
            return submission.publication.journal_artifact

        reference = database.store.read(submitted)
        context = recovery_context(
            database,
            context,
            operation,
            payload,
            attempt_id=intent.attempt_id,
            artifact_id=reference.id,
            crypto_provider=crypto,
        )
        previous = database.replay(context, operation, payload, "attempt.reconcile")
        if previous is not None:
            return encode(previous)

        def select(sql):
            tx = Transaction(sql, context)
            submission, worker, attempt = service.reconciliation._load(tx, intent)
            evidence = PublishAttempt(attempt_id=intent.attempt_id, journal_artifact=reference)
            if submission.publication.journal_artifact != reference:
                raise ResearchError(ErrorCode.STALE_FENCE, "Closure journal reference changed")
            records = service.results._evidence_records_in(
                tx, evidence, attempt, worker, scope="attempt.reconcile"
            )
            return submission, worker, attempt, records

        selected = database.store.read(select)
        record = selected[3][reference.id]
        body = await objects.read(record.workspace_id, service.artifacts._blob(record))
        journal = service.worker_outputs._parse_journal(body)
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Recovery closure owner was cancelled")
        router._verify(context)

        def commit(tx):
            router._verify(context, tx.connection)
            if select(tx.connection) != selected:
                raise ResearchError(ErrorCode.STALE_FENCE, "Closure evidence changed during I/O")
            return service.closures._close(tx, intent, journal)

        return encode(database.execute(context, operation, payload, "attempt.reconcile", commit))

    return await exclusive(state, reconcile)
