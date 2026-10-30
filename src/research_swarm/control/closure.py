"""Operational closure can settle custody without accepting scientific evidence."""

from __future__ import annotations

from research_swarm.control.campaigns import settle_pause
from research_swarm.control.confirmation_runtime import settle_exposure
from research_swarm.control.observations import (
    PublishAttempt,
    output_intent,
    validate_native_observation,
)
from research_swarm.control.reconciliation import ReconcileAttempt, ReconciliationControl
from research_swarm.control.worker_outputs import verify_evidence_receipt
from research_swarm.control.worker_state import settle_cancellation
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import (
    ID,
    Amount,
    Digest,
    Dimension,
    Revision,
    Timestamp,
    VersionedRecord,
    new_id,
    utc_now,
)
from research_swarm.domain.workers import WorkerBinding
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.ledger import BudgetLedger
from research_swarm.workers.contracts import NativeOutcome
from research_swarm.workers.journal_records import JournalEntry


class AttemptClosure(VersionedRecord):
    attempt_id: ID
    submission_id: ID
    publication_digest: Digest
    original_binding: WorkerBinding
    journal_artifact: ArtifactRef
    native_outcome: NativeOutcome | None
    not_started: bool
    consumed: dict[Dimension, Amount]
    controller_id: ID
    control_epoch: Revision
    operator_id: ID
    recorded_at: Timestamp
    reason: str


class ClosureControl:
    def __init__(self, database: Database, reconciliation: ReconciliationControl) -> None:
        self.database, self.reconciliation = database, reconciliation

    def reconcile(self, context: CommandContext, intent: ReconcileAttempt) -> JsonObject:
        return self.database.with_preflight(
            context,
            "attempt.reconcile_closure",
            intent.model_dump(mode="json"),
            "attempt.reconcile",
            lambda: self._preflight(context, intent),
        )

    def _preflight(self, context: CommandContext, intent: ReconcileAttempt) -> JsonObject:
        submission, worker, attempt = self.database.store.read(
            lambda connection: self.reconciliation._load(Transaction(connection, context), intent)
        )
        # Scientific artifacts may be damaged. Only original native evidence is
        # needed for this irreversible operational disposition.
        request = PublishAttempt(
            attempt_id=intent.attempt_id, journal_artifact=submission.publication.journal_artifact
        )
        results = self.reconciliation.results
        with results.pin_evidence(
            context, request, attempt, worker, scope="attempt.reconcile"
        ) as records:
            journal = JournalEntry.model_validate(
                results._read(records[request.journal_artifact.id])
            )
            return self.database.execute(
                context,
                "attempt.reconcile_closure",
                intent.model_dump(mode="json"),
                "attempt.reconcile",
                lambda transaction: self._close(transaction, intent, journal),
            )

    def _close(
        self, transaction: Transaction, intent: ReconcileAttempt, journal: JournalEntry
    ) -> JsonObject:
        submission, worker, attempt = self.reconciliation._load(transaction, intent)
        row = transaction.get("attempt", intent.attempt_id)
        if attempt.publication_status == "accepted":
            raise ResearchError(
                ErrorCode.INVALID_TRANSITION, "Accepted evidence requires correction"
            )
        if attempt.publication_status == "closed_unaccepted":
            assert attempt.closure_receipt_id is not None
            recorded = transaction.get("attempt_closure", attempt.closure_receipt_id)
            if recorded["data"]["publication_digest"] != submission.publication_digest:
                raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Closure evidence differs")
            return {"disposition": "closed_unaccepted", "attempt": row, "closure": recorded}
        request = submission.publication
        verified = validate_native_observation(
            transaction,
            request,
            journal,
            worker,
            crypto_provider=self.reconciliation.results.crypto_provider,
        )
        if verified.unknown:
            raise ResearchError(ErrorCode.EXECUTION_UNKNOWN, "Operational closure is not proven")
        if not verified.not_started and (
            journal.outcome is None or journal.outcome.exit_code is None
        ):
            raise ResearchError(ErrorCode.EXECUTION_UNKNOWN, "Native exit is unavailable")
        identifier, now = new_id(), utc_now()
        reference = request.journal_artifact
        receipt = (
            verify_evidence_receipt(
                transaction, output_intent(attempt, "journal", None), reference, worker, attempt
            )
            if worker.project_ids is not None
            else None
        )
        ArtifactCatalog.available(transaction, reference)
        ArtifactCatalog.retain(
            transaction,
            ArtifactRetention(
                artifact_id=reference.id,
                owner_kind="attempt_closure",
                owner_id=identifier,
                purpose="native_closure",
            ),
        )
        if receipt is not None:
            assert receipt.retention_id is not None
            ArtifactCatalog.release(
                transaction,
                receipt.retention_id,
                owner_release=("worker_upload", receipt.upload_id),
                owner_scope="attempt.reconcile",
            )
        for reservation_id in attempt.dispatch.reservation_ids:
            BudgetLedger.settle(transaction, reservation_id, intent.attempt_id, verified.consumed)
        settle_exposure(
            transaction, verified.job, intent.attempt_id, not_started=verified.not_started
        )
        closure = AttemptClosure(
            attempt_id=intent.attempt_id,
            submission_id=intent.submission_id,
            publication_digest=submission.publication_digest,
            original_binding=worker.binding,
            journal_artifact=reference,
            native_outcome=journal.outcome,
            not_started=verified.not_started,
            consumed=verified.consumed,
            controller_id=intent.expected_controller_id,
            control_epoch=intent.expected_control_epoch,
            operator_id=transaction.context.principal.id,
            recorded_at=now,
            reason=intent.reason,
        )
        recorded = transaction.insert(
            "attempt_closure",
            identifier,
            closure.model_dump(mode="json"),
            state="immutable",
            campaign_id=attempt.dispatch.campaign_id,
            parent_id=intent.attempt_id,
        )
        updated = transaction.update(
            "attempt",
            intent.attempt_id,
            expected_revision=row["revision"],
            state="closed_unaccepted",
            data=attempt.model_copy(
                update={
                    "publication_status": "closed_unaccepted",
                    "closure_receipt_id": identifier,
                    "outcome_artifact": reference,
                    "result_digest": None,
                    "finding": "Operational closure without scientific acceptance: "
                    + intent.reason,
                }
            ).model_dump(mode="json"),
        )
        self.reconciliation.results.acceptance.settle(
            transaction, intent.attempt_id, "closed_unaccepted"
        )
        native_resolution = (
            "not_started"
            if verified.not_started
            else "cancelled"
            if journal.outcome is not None and journal.outcome.reason == "cancelled"
            else "completed"
        )
        settle_cancellation(
            transaction,
            intent.attempt_id,
            "closed_unaccepted",
            now,
            native_resolution=native_resolution,
        )
        job = transaction.update(
            "job",
            verified.job_row["id"],
            expected_revision=verified.job_row["revision"],
            state="closed_unaccepted",
            data=verified.job.model_copy(
                update={"outputs": {}, "stopping_reason": "trial_closed_unaccepted"}
            ).model_dump(mode="json"),
        )
        trial = verified.trial.model_copy(
            update={"execution_outcome": "closed_unaccepted", "evaluation_status": "incomplete"}
        )
        self.reconciliation.results._advance_trial(transaction, verified.trial_row, trial)
        settle_pause(transaction, attempt.dispatch.campaign_id)
        transaction.event("job.closed_unaccepted", job, {"attempt_id": intent.attempt_id})
        transaction.event("attempt.closure_reconciled", recorded, {"attempt_id": intent.attempt_id})
        return {"disposition": "closed_unaccepted", "attempt": updated, "closure": recorded}
