"""Authenticate native observations, settle their cost, and advance stage dependencies."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager

from research_swarm.control.campaigns import settle_pause
from research_swarm.control.confirmation_runtime import release_unused_exposure, settle_exposure
from research_swarm.control.models import JobData, TrialData
from research_swarm.control.observations import (
    PublishAttempt as PublishAttempt,
)
from research_swarm.control.observations import (
    evidence_roles,
    output_intent,
    validate_observation,
)
from research_swarm.control.worker_outputs import (
    WorkerOutputIntent,
    WorkerUploadTarget,
    verify_evidence_receipt,
    verify_upload,
)
from research_swarm.control.worker_records import WorkerData
from research_swarm.control.worker_state import settle_cancellation
from research_swarm.control.workers import AttemptData, worker_authority
from research_swarm.domain.artifacts import ArtifactRecord, ArtifactRetention, StoredBlob
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef, RunResult
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.lifecycle import TERMINAL_CAMPAIGNS
from research_swarm.domain.types import utc_now
from research_swarm.projects.workload import WorkloadReport
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.ledger import BudgetLedger
from research_swarm.workers.journal import JournalEntry


class ResultControl:
    def __init__(self, database: Database, artifacts: ArtifactService) -> None:
        self.database, self.artifacts = database, artifacts

    @staticmethod
    def _settle_uploads(transaction: Transaction, attempt: AttemptData, worker: WorkerData) -> None:
        """Preserve every finalized observation under the closed attempt's lifecycle."""
        rows = transaction.connection.execute(
            "SELECT id FROM records WHERE workspace_id=? AND kind='worker_upload' "
            "AND parent_id=? AND state='finalized'",
            (transaction.workspace_id, attempt.dispatch.attempt_id),
        )
        for row in rows:
            target = WorkerUploadTarget(
                worker_id=attempt.dispatch.worker_id,
                session_id=attempt.dispatch.worker_session_id,
                attempt_id=attempt.dispatch.attempt_id,
                upload_id=row["id"],
            )
            _, receipt, _, _, _ = verify_upload(transaction, target, worker, attempt)
            assert receipt.artifact is not None and receipt.retention_id is not None
            ArtifactCatalog.retain(
                transaction,
                ArtifactRetention(
                    artifact_id=receipt.artifact.id,
                    owner_kind="attempt",
                    owner_id=attempt.dispatch.attempt_id,
                    purpose="evidence",
                ),
            )
            ArtifactCatalog.release(
                transaction,
                receipt.retention_id,
                owner_release=("worker_upload", receipt.upload_id),
                owner_scope="attempt.reconcile"
                if transaction.context.principal.id != worker.principal_id
                else None,
            )

    def publish(self, context: CommandContext, request: PublishAttempt) -> JsonObject:
        return self.database.with_preflight(
            context,
            "worker.publish_attempt",
            request.model_dump(mode="json"),
            "worker.publish",
            lambda: self._publish_preflight(context, request),
        )

    def _publish_preflight(self, context: CommandContext, request: PublishAttempt) -> JsonObject:
        payload = request.model_dump(mode="json")
        with self.database.connect() as connection:
            connection.execute("BEGIN")
            transaction = Transaction(connection, context)
            admitted = AttemptData.model_validate(
                transaction.get("attempt", request.attempt_id)["data"]
            )
            worker = worker_authority(
                transaction,
                admitted.dispatch.worker_id,
                admitted.dispatch.worker_session_id,
            )
        with self.pin_evidence(
            context, request, admitted, worker, scope="worker.publish"
        ) as records:
            journal = JournalEntry.model_validate(self._read(records[request.journal_artifact.id]))
            report = (
                WorkloadReport.model_validate(self._read(records[request.report_artifact.id]))
                if request.report_artifact is not None
                else None
            )
            return self.database.execute(
                context,
                "worker.publish_attempt",
                payload,
                "worker.publish",
                lambda transaction: self._accept(transaction, request, journal, report),
            )

    @contextmanager
    def pin_evidence(
        self,
        context: CommandContext,
        request: PublishAttempt,
        attempt: AttemptData,
        worker: WorkerData,
        *,
        scope: str,
    ) -> Iterator[dict[str, ArtifactRecord]]:
        """Entry points authenticate their caller before opening original evidence."""
        with ExitStack() as pins:
            records: dict[str, ArtifactRecord] = {}
            for role, name, reference in evidence_roles(request):
                intent = output_intent(attempt, role, name)

                def authorize(
                    transaction: Transaction,
                    reference: ArtifactRef,
                    intent: WorkerOutputIntent = intent,
                ) -> None:
                    if worker.project_ids is not None:
                        verify_evidence_receipt(transaction, intent, reference, worker, attempt)

                manager = (
                    self.artifacts.pinned_authorized(
                        context, reference, scope=scope, authorize=authorize
                    )
                    if worker.project_ids is not None or scope == "attempt.reconcile"
                    else self.artifacts.pinned(context, reference)
                )
                records[reference.id] = pins.enter_context(manager)
            yield records

    def _read(self, record: ArtifactRecord) -> JsonObject:
        reference = record.reference
        if reference.size_bytes > 1024 * 1024:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Observation exceeds its size bound")
        with self.artifacts.store.reader(
            record.workspace_id,
            StoredBlob(reference=reference, generation=record.storage_generation),
        ) as stream:
            value = strict_json_loads(stream.read(1024 * 1024 + 1))
        if not isinstance(value, dict):
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Observation must be an object")
        return dict(value)

    def _accept(
        self,
        transaction: Transaction,
        request: PublishAttempt,
        journal: JournalEntry,
        report: WorkloadReport | None,
    ) -> JsonObject:
        row = transaction.get("attempt", request.attempt_id)
        attempt = AttemptData.model_validate(row["data"])
        worker = worker_authority(
            transaction, attempt.dispatch.worker_id, attempt.dispatch.worker_session_id
        )
        return self._accept_observation(transaction, request, journal, report, worker)

    def _accept_observation(
        self,
        transaction: Transaction,
        request: PublishAttempt,
        journal: JournalEntry,
        report: WorkloadReport | None,
        worker: WorkerData,
    ) -> JsonObject:
        """Caller supplies independently authorized original enrollment; no authority is issued."""
        row = transaction.get("attempt", request.attempt_id)
        attempt = AttemptData.model_validate(row["data"])
        digest = record_digest("research.attempt-publication", request.model_dump(mode="json"))
        if attempt.publication_status == "closed_unaccepted":
            raise ResearchError(
                ErrorCode.INVALID_TRANSITION, "Attempt was closed without acceptance"
            )
        if attempt.publication_status == "accepted":
            if attempt.result_digest != digest:
                raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Accepted observation changed")
            return row
        observation = validate_observation(transaction, request, journal, report, worker)
        job_row, trial_row = observation.job_row, observation.trial_row
        job, trial = observation.job, observation.trial
        not_started, unknown = observation.not_started, observation.unknown
        success, state, consumed = observation.success, observation.state, observation.consumed
        outcome = journal.outcome
        for role, output_name, reference in evidence_roles(request):
            receipt = (
                verify_evidence_receipt(
                    transaction,
                    output_intent(attempt, role, output_name),
                    reference,
                    worker,
                    attempt,
                )
                if worker.project_ids is not None
                else None
            )
            ArtifactCatalog.available(transaction, reference)
            ArtifactCatalog.retain(
                transaction,
                ArtifactRetention(
                    artifact_id=reference.id,
                    owner_kind="attempt",
                    owner_id=row["id"],
                    purpose="evidence",
                ),
            )
            if receipt is not None:
                assert receipt.retention_id is not None
                ArtifactCatalog.release(
                    transaction,
                    receipt.retention_id,
                    owner_release=("worker_upload", receipt.upload_id),
                    owner_scope="attempt.reconcile"
                    if transaction.context.principal.id != worker.principal_id
                    else None,
                )
        if unknown:
            for reservation_id in attempt.dispatch.reservation_ids:
                BudgetLedger.mark_uncertain(transaction, reservation_id)
            updated = transaction.update(
                "attempt",
                row["id"],
                expected_revision=row["revision"],
                state="execution_unknown",
                data=attempt.model_copy(
                    update={
                        "outcome_artifact": request.journal_artifact,
                        "finding": request.finding,
                    }
                ).model_dump(mode="json"),
            )
            transaction.update(
                "job",
                job_row["id"],
                expected_revision=job_row["revision"],
                state="execution_unknown",
                data=job.model_dump(mode="json"),
            )
            transaction.update(
                "trial",
                trial_row["id"],
                expected_revision=trial_row["revision"],
                state="execution_unknown",
                data=trial.model_copy(
                    update={
                        "resource_status": "unknown",
                        "execution_outcome": "closed_unaccepted"
                        if trial.execution_outcome == "closed_unaccepted"
                        else "unknown",
                    }
                ).model_dump(mode="json"),
            )
            transaction.event("attempt.execution_unknown", updated, {})
            return updated
        if worker.project_ids is not None:
            self._settle_uploads(transaction, attempt, worker)
        for reservation_id in attempt.dispatch.reservation_ids:
            BudgetLedger.settle(transaction, reservation_id, row["id"], consumed)
        settle_exposure(transaction, job, row["id"], not_started=not_started)
        result = None
        if outcome is not None and not not_started:
            assert outcome.exit_code is not None
            result = RunResult(
                campaign_id=attempt.dispatch.campaign_id,
                trial_id=job.trial_id,
                job_id=job_row["id"],
                attempt_id=row["id"],
                worker_id=attempt.dispatch.worker_id,
                worker_session_id=attempt.dispatch.worker_session_id,
                spec_digest=attempt.dispatch.spec_digest,
                control_epoch=attempt.dispatch.control_epoch,
                fencing_generation=attempt.dispatch.fencing_generation,
                status="succeeded"
                if success
                else "cancelled"
                if state == "cancelled"
                else "failed",
                exit_code=outcome.exit_code,
                started_at=outcome.released_at or outcome.prepared_at,
                finished_at=outcome.finished_at,
                elapsed_ms=outcome.elapsed_ms,
                observed_seed=report.observed_seed if report else None,
                process_closure_ref=request.journal_artifact,
                artifacts=tuple(request.outputs.values()),
                measurements=report.measurements if report else (),
                consumed=consumed,
                phase_timings_ms=report.phase_timings_ms if report else {},
                execution_fingerprint=attempt.profile.execution_fingerprint,
            ).sealed()
        updated = transaction.update(
            "attempt",
            row["id"],
            expected_revision=row["revision"],
            state=state,
            data=attempt.model_copy(
                update={
                    "outcome_artifact": request.journal_artifact,
                    "report_artifact": request.report_artifact,
                    "result_digest": digest,
                    "publication_status": "accepted",
                    "finding": request.finding,
                }
            ).model_dump(mode="json"),
        )
        if result is not None:
            transaction.insert(
                "run_result",
                row["id"],
                result.model_dump(mode="json"),
                state="immutable",
                campaign_id=row["campaign_id"],
                parent_id=row["id"],
            )
        settle_cancellation(transaction, row["id"], state, utc_now())
        updated_job = transaction.update(
            "job",
            job_row["id"],
            expected_revision=job_row["revision"],
            state=state,
            data=job.model_copy(update={"outputs": request.outputs if success else {}}).model_dump(
                mode="json"
            ),
        )
        self._advance_trial(transaction, trial_row, trial)
        settle_pause(transaction, row["campaign_id"])
        transaction.event("job.completed", updated_job, {"attempt_id": row["id"]})
        transaction.event("attempt.observation_accepted", updated, {"publication_digest": digest})
        return updated

    @staticmethod
    def _advance_trial(transaction: Transaction, row: JsonObject, trial: TrialData) -> None:
        jobs = {identifier: transaction.get("job", identifier) for identifier in trial.job_ids}
        withheld = trial.execution_outcome == "closed_unaccepted"
        terminal = transaction.get("campaign", row["campaign_id"])["state"] in TERMINAL_CAMPAIGNS
        stopping_reason = "trial_closed_unaccepted" if withheld else "campaign_completed"
        if withheld or terminal:
            for identifier, job_row in jobs.items():
                data = JobData.model_validate(job_row["data"])
                if (
                    job_row["state"] not in {"ready", "blocked"}
                    or data.current_attempt_id is not None
                ):
                    continue
                changed = transaction.update(
                    "job",
                    identifier,
                    expected_revision=job_row["revision"],
                    state="not_started",
                    data=data.model_copy(update={"stopping_reason": stopping_reason}).model_dump(
                        mode="json"
                    ),
                )
                jobs[identifier] = changed
                release_unused_exposure(transaction, data)
                transaction.event("job.admission_stopped", changed, {"reason": stopping_reason})
        pending = (
            set()
            if withheld or terminal
            else {identifier for identifier, job in jobs.items() if job["state"] == "blocked"}
        )
        while pending:
            resolved = set()
            for identifier in sorted(pending):
                job = jobs[identifier]
                data = JobData.model_validate(job["data"])
                states = {jobs[parent]["state"] for parent in data.predecessor_ids}
                state = (
                    "failed"
                    if states & {"failed", "cancelled", "not_started"}
                    else "ready"
                    if states <= {"succeeded"}
                    else "blocked"
                )
                if state != "blocked":
                    jobs[identifier] = transaction.update(
                        "job",
                        identifier,
                        expected_revision=job["revision"],
                        state=state,
                        data=job["data"],
                    )
                    transaction.event("job.dependency_resolved", jobs[identifier], {})
                    if state == "failed":
                        release_unused_exposure(transaction, data)
                    resolved.add(identifier)
            if not resolved:
                break
            pending -= resolved
        states = {job["state"] for job in jobs.values()}
        complete = states <= {
            "succeeded",
            "failed",
            "cancelled",
            "not_started",
            "closed_unaccepted",
        }
        unknown = "execution_unknown" in states
        execution = (
            "closed_unaccepted"
            if withheld
            else "succeeded"
            if states == {"succeeded"}
            else "cancelled"
            if terminal and complete and "not_started" in states and "failed" not in states
            else "failed"
            if complete
            else "unknown"
            if unknown
            else "pending"
        )
        state = "execution_unknown" if unknown else execution if complete else "running"
        transaction.update(
            "trial",
            row["id"],
            expected_revision=row["revision"],
            state=state,
            data=trial.model_copy(
                update={
                    "execution_outcome": execution,
                    "resource_status": "closed" if complete else "unknown" if unknown else "active",
                }
            ).model_dump(mode="json"),
        )
