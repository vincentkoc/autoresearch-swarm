"""Quarantine original evidence, then reconcile it under explicit current authority."""

from __future__ import annotations

from uuid import UUID, uuid5

from pydantic import Field

from research_swarm.control.obligations import admitted_before_completion
from research_swarm.control.observations import PublishAttempt, evidence_roles, output_intent
from research_swarm.control.results import ResultControl
from research_swarm.control.worker_outputs import verify_evidence_receipt
from research_swarm.control.worker_records import AttemptData, WorkerData
from research_swarm.control.worker_sessions import archive_session, historical_attempt_owner
from research_swarm.control.workers import authority
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.lifecycle import TERMINAL_CAMPAIGNS
from research_swarm.domain.types import (
    ID,
    Digest,
    Revision,
    Timestamp,
    VersionedRecord,
    new_id,
    utc_now,
)
from research_swarm.domain.workers import WorkerBinding
from research_swarm.projects.workload import WorkloadReport
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.workers.journal import JournalEntry, JournalState


class AttemptSubmission(VersionedRecord):
    publication: PublishAttempt
    publication_digest: Digest
    original_binding: WorkerBinding
    submitted_by: ID
    submitted_at: Timestamp


class ReconcileAttempt(VersionedRecord):
    attempt_id: ID
    submission_id: ID
    expected_controller_id: ID
    expected_control_epoch: Revision
    reason: str = Field(min_length=1, max_length=2048)


class AttemptReconciliation(VersionedRecord):
    attempt_id: ID
    submission_id: ID
    publication_digest: Digest
    run_result_digest: Digest | None
    original_binding: WorkerBinding
    controller_id: ID
    control_epoch: Revision
    operator_id: ID
    reconciled_at: Timestamp
    reason: str


def publication_digest(request: PublishAttempt) -> str:
    return record_digest("research.attempt-publication", request.model_dump(mode="json"))


class ReconciliationControl:
    def __init__(self, database: Database, results: ResultControl) -> None:
        self.database, self.results = database, results

    @staticmethod
    def _owner(transaction: Transaction, request: PublishAttempt) -> tuple[WorkerData, AttemptData]:
        attempt = AttemptData.model_validate(transaction.get("attempt", request.attempt_id)["data"])
        return historical_attempt_owner(
            transaction,
            attempt.dispatch.worker_id,
            attempt.dispatch.worker_session_id,
            request.attempt_id,
            require_signed=False,
        )

    def submit(self, context: CommandContext, request: PublishAttempt) -> JsonObject:
        return self.database.with_preflight(
            context,
            "worker.submit_evidence",
            request.model_dump(mode="json"),
            "worker.publish",
            lambda: self._submit_preflight(context, request),
        )

    def _submit_preflight(self, context: CommandContext, request: PublishAttempt) -> JsonObject:
        payload = request.model_dump(mode="json")
        with self.database.connect() as connection:
            connection.execute("BEGIN")
            worker, attempt = self._owner(Transaction(connection, context), request)
        with self.results.pin_evidence(context, request, attempt, worker, scope="worker.publish"):
            return self.database.execute(
                context,
                "worker.submit_evidence",
                payload,
                "worker.publish",
                lambda transaction: self._submit(transaction, request),
            )

    def _submit(self, transaction: Transaction, request: PublishAttempt) -> JsonObject:
        worker, attempt = self._owner(transaction, request)
        archive_session(transaction, worker)
        digest = publication_digest(request)
        identifier = str(uuid5(UUID(transaction.workspace_id), "attempt-submission:" + digest))
        try:
            existing = transaction.get("attempt_submission", identifier)
        except ResearchError as error:
            if error.code != ErrorCode.NOT_FOUND:
                raise
        else:
            data = AttemptSubmission.model_validate(existing["data"])
            if (
                data.publication != request
                or data.publication_digest != digest
                or data.original_binding != worker.binding
                or data.submitted_by != transaction.context.principal.id
            ):
                raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Original evidence changed")
            return existing
        for role, name, reference in evidence_roles(request):
            if worker.project_ids is not None:
                verify_evidence_receipt(
                    transaction, output_intent(attempt, role, name), reference, worker, attempt
                )
            ArtifactCatalog.available(transaction, reference)
            ArtifactCatalog.retain(
                transaction,
                ArtifactRetention(
                    artifact_id=reference.id,
                    owner_kind="attempt_submission",
                    owner_id=identifier,
                    purpose="quarantined_evidence",
                ),
            )
        data = AttemptSubmission(
            publication=request,
            publication_digest=digest,
            original_binding=worker.binding,
            submitted_by=transaction.context.principal.id,
            submitted_at=utc_now(),
        )
        row = transaction.insert(
            "attempt_submission",
            identifier,
            data.model_dump(mode="json"),
            state="quarantined",
            campaign_id=attempt.dispatch.campaign_id,
            parent_id=request.attempt_id,
        )
        transaction.event("attempt.evidence_submitted", row, {"publication_digest": digest})
        return row

    @staticmethod
    def _load(
        transaction: Transaction, intent: ReconcileAttempt
    ) -> tuple[AttemptSubmission, WorkerData, AttemptData]:
        controller_id, epoch = authority(transaction)
        if controller_id != intent.expected_controller_id or epoch != intent.expected_control_epoch:
            raise ResearchError(ErrorCode.STALE_FENCE, "Reconciliation authority changed")
        row = transaction.get("attempt", intent.attempt_id)
        if transaction.context.expected_revision != row["revision"]:
            raise ResearchError(ErrorCode.REVISION_CONFLICT, "Attempt revision changed")
        submission_row = transaction.get("attempt_submission", intent.submission_id)
        submission = AttemptSubmission.model_validate(submission_row["data"])
        if (
            submission_row["state"] != "quarantined"
            or submission.publication.attempt_id != intent.attempt_id
            or publication_digest(submission.publication) != submission.publication_digest
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Submission identity differs")
        archived = transaction.get("worker_session", submission.original_binding.worker_session_id)
        worker = WorkerData.model_validate(archived["data"])
        attempt = AttemptData.model_validate(row["data"])
        if (
            archived["state"] != "immutable"
            or worker.binding != submission.original_binding
            or worker.principal_id != submission.submitted_by
            or worker.binding.workspace_id != transaction.workspace_id
            or any(
                getattr(attempt.dispatch, name) != getattr(worker.binding, name)
                for name in WorkerBinding.model_fields
            )
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Original session authority differs")
        return submission, worker, attempt

    def reconcile(self, context: CommandContext, intent: ReconcileAttempt) -> JsonObject:
        return self.database.with_preflight(
            context,
            "attempt.reconcile",
            intent.model_dump(mode="json"),
            "attempt.reconcile",
            lambda: self._reconcile_preflight(context, intent),
        )

    def _reconcile_preflight(self, context: CommandContext, intent: ReconcileAttempt) -> JsonObject:
        payload = intent.model_dump(mode="json")
        with self.database.connect() as connection:
            connection.execute("BEGIN")
            submission, worker, attempt = self._load(Transaction(connection, context), intent)
        request = submission.publication
        with self.results.pin_evidence(
            context, request, attempt, worker, scope="attempt.reconcile"
        ) as records:
            journal = JournalEntry.model_validate(
                self.results._read(records[request.journal_artifact.id])
            )
            report = (
                WorkloadReport.model_validate(
                    self.results._read(records[request.report_artifact.id])
                )
                if request.report_artifact is not None
                else None
            )
            return self.database.execute(
                context,
                "attempt.reconcile",
                payload,
                "attempt.reconcile",
                lambda transaction: self._reconcile(transaction, intent, journal, report),
            )

    def _reconcile(
        self,
        transaction: Transaction,
        intent: ReconcileAttempt,
        journal: JournalEntry,
        report: WorkloadReport | None,
    ) -> JsonObject:
        submission, worker, attempt = self._load(transaction, intent)
        if attempt.publication_status == "closed_unaccepted":
            raise ResearchError(
                ErrorCode.INVALID_TRANSITION, "Attempt was closed without acceptance"
            )
        if attempt.publication_status == "accepted":
            if attempt.result_digest != submission.publication_digest:
                raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Accepted observation differs")
            return {
                "disposition": "already_accepted",
                "attempt": transaction.get("attempt", intent.attempt_id),
                "reconciliation": None,
            }
        campaign = transaction.get("campaign", attempt.dispatch.campaign_id)
        if campaign["state"] in TERMINAL_CAMPAIGNS and not admitted_before_completion(
            transaction, campaign["id"], intent.attempt_id
        ):
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Terminal research cannot be resumed")
        if journal.state not in {JournalState.CLOSED, JournalState.NOT_STARTED}:
            raise ResearchError(
                ErrorCode.EXECUTION_UNKNOWN, "Reconciliation requires native closure"
            )
        accepted = self.results._accept_observation(
            transaction, submission.publication, journal, report, worker
        )
        result_digest = (
            transaction.get("run_result", intent.attempt_id)["data"]["result_digest"]
            if accepted["state"] != "not_started"
            else None
        )
        receipt = AttemptReconciliation(
            attempt_id=intent.attempt_id,
            submission_id=intent.submission_id,
            publication_digest=submission.publication_digest,
            run_result_digest=result_digest,
            original_binding=worker.binding,
            controller_id=intent.expected_controller_id,
            control_epoch=intent.expected_control_epoch,
            operator_id=transaction.context.principal.id,
            reconciled_at=utc_now(),
            reason=intent.reason,
        )
        recorded = transaction.insert(
            "attempt_reconciliation",
            new_id(),
            receipt.model_dump(mode="json"),
            state="immutable",
            campaign_id=attempt.dispatch.campaign_id,
            parent_id=intent.attempt_id,
        )
        transaction.event("attempt.reconciled", recorded, {"attempt_id": intent.attempt_id})
        return {"disposition": "reconciled", "attempt": accepted, "reconciliation": recorded}
