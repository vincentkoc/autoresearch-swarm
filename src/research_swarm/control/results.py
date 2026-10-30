"""Authenticate native observations, settle their cost, and advance stage dependencies."""

from __future__ import annotations

from contextlib import ExitStack
from decimal import Decimal

from pydantic import Field

from research_swarm.control.confirmation_runtime import release_unused_exposure, settle_exposure
from research_swarm.control.models import JobData, TrialData
from research_swarm.control.workers import AttemptData, worker_authority
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef, RunResult
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, Name, VersionedRecord
from research_swarm.projects.manifest import TimingContract
from research_swarm.projects.workload import WorkloadReport
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.ledger import BudgetLedger
from research_swarm.workers.journal import JournalEntry, JournalState


class PublishAttempt(VersionedRecord):
    attempt_id: ID
    journal_artifact: ArtifactRef
    report_artifact: ArtifactRef | None = None
    outputs: dict[Name, ArtifactRef] = Field(default_factory=dict)
    logs: dict[Name, ArtifactRef] = Field(default_factory=dict)
    finding: str = Field(default="", max_length=4096)


class ResultControl:
    def __init__(self, database: Database, artifacts: ArtifactService) -> None:
        self.database, self.artifacts = database, artifacts

    def publish(self, context: CommandContext, request: PublishAttempt) -> JsonObject:
        payload = request.model_dump(mode="json")
        previous = self.database.replay(
            context, "worker.publish_attempt", payload, "worker.publish"
        )
        if previous is not None:
            return previous
        initial = self.database.read_record(context, "attempt", request.attempt_id)
        admitted = AttemptData.model_validate(initial["data"])
        # Reject another worker before opening any submitted evidence.
        with self.database.connect() as connection:
            worker_authority(
                Transaction(connection, context),
                admitted.dispatch.worker_id,
                admitted.dispatch.worker_session_id,
            )
        with ExitStack() as pins:
            references = [
                request.journal_artifact,
                *request.outputs.values(),
                *request.logs.values(),
            ]
            if request.report_artifact is not None:
                references.append(request.report_artifact)
            for reference in references:
                pins.enter_context(self.artifacts.pinned(context, reference))
            journal = JournalEntry.model_validate(self._read(context, request.journal_artifact))
            report = (
                WorkloadReport.model_validate(self._read(context, request.report_artifact))
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

    def _read(self, context: CommandContext, reference: ArtifactRef) -> JsonObject:
        if reference.size_bytes > 1024 * 1024:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Observation exceeds its size bound")
        with self.artifacts.reader(context, reference) as stream:
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
        worker_authority(
            transaction, attempt.dispatch.worker_id, attempt.dispatch.worker_session_id
        )
        digest = record_digest("research.attempt-publication", request.model_dump(mode="json"))
        if attempt.publication_status == "accepted":
            if attempt.result_digest != digest:
                raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Accepted observation changed")
            return row
        job_row = transaction.get("job", attempt.dispatch.job_id)
        job = JobData.model_validate(job_row["data"])
        trial_row = transaction.get("trial", job.trial_id)
        trial = TrialData.model_validate(trial_row["data"])
        if (
            journal.dispatch != attempt.dispatch
            or journal.spec != trial.spec
            or job.current_attempt_id != request.attempt_id
            or journal.command.argv != job.stage.argv
            or journal.command.timeout_ms > job.stage.timeout_ms
        ):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Observation differs from its original grant"
            )
        if journal.state not in {
            JournalState.CLOSED,
            JournalState.NOT_STARTED,
            JournalState.UNKNOWN,
        }:
            raise ResearchError(
                ErrorCode.INVALID_TRANSITION, "Observation is not a completion or uncertainty"
            )
        outcome = journal.outcome
        if outcome is not None and outcome.closure.identity != journal.native:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Native closure identity differs")
        not_started = (
            journal.state == JournalState.NOT_STARTED and journal.native is None and outcome is None
        )
        closed = (
            journal.state in {JournalState.CLOSED, JournalState.NOT_STARTED}
            and outcome is not None
            and outcome.closure.scope_closed
        )
        not_started = not_started or (
            closed
            and journal.state == JournalState.NOT_STARTED
            and outcome is not None
            and outcome.reason == "not_started"
            and outcome.released_at is None
        )
        unknown = journal.state == JournalState.UNKNOWN
        if not (not_started or closed or unknown):
            raise ResearchError(ErrorCode.EXECUTION_UNKNOWN, "Observation lacks native closure")
        if journal.prepared is not None:
            journal.prepared.verify(attempt.dispatch, trial.spec)
            if journal.prepared.profile_digest != attempt.profile.execution_fingerprint:
                raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Execution profile differs")
            prepared_context = journal.prepared.context
            timing = TimingContract.model_validate(trial.spec.extensions["timing"])
            if prepared_context.time_budget_ms != timing.budget_ms or {
                name: (value.digest, value.size_bytes)
                for name, value in prepared_context.inputs.items()
            } != {name: (value.digest, value.size_bytes) for name, value in attempt.inputs.items()}:
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE, "Workload inputs or budget differ"
                )
        elif not not_started:
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Workload was not prepared")
        if report is not None and (
            journal.prepared is None
            or report.context_digest != journal.prepared.context.context_digest
            or report.observed_seed != trial.spec.seed
        ):
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Report context or seed differs")
        if set(request.logs) - {"stdout", "stderr"}:
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Unknown native log stream")
        if closed and set(request.logs) != {"stdout", "stderr"}:
            raise ResearchError(
                ErrorCode.ARTIFACT_UNVERIFIED, "Closed execution requires both log streams"
            )
        if outcome is not None:
            for name, reference in request.logs.items():
                observed = getattr(outcome, name)
                if (
                    reference.digest != observed.digest
                    or reference.size_bytes != observed.retained_bytes
                ):
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "Native log identity differs"
                    )
        for reference in [
            request.journal_artifact,
            *request.outputs.values(),
            *request.logs.values(),
            *([request.report_artifact] if request.report_artifact else []),
        ]:
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
                        "execution_outcome": "unknown",
                    }
                ).model_dump(mode="json"),
            )
            transaction.event("attempt.execution_unknown", updated, {})
            return updated
        success = (
            closed
            and outcome is not None
            and outcome.reason == "exited"
            and outcome.exit_code == 0
            and report is not None
            and not request.finding
        )
        if success and (
            set(request.outputs) != set(job.stage.output_names)
            or report is None
            or {output.name for output in report.outputs} != set(job.stage.output_names)
        ):
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Successful stage output contract differs"
            )
        state = (
            "not_started"
            if not_started
            else (
                "succeeded"
                if success
                else "cancelled"
                if outcome is not None and outcome.reason == "cancelled"
                else "failed"
            )
        )
        consumed = {
            "attempts": Decimal(0 if not_started else 1),
            "execution_ms": Decimal(0 if outcome is None else outcome.elapsed_ms),
        }
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
        transaction.event("job.completed", updated_job, {"attempt_id": row["id"]})
        transaction.event("attempt.observation_accepted", updated, {"publication_digest": digest})
        return updated

    @staticmethod
    def _advance_trial(transaction: Transaction, row: JsonObject, trial: TrialData) -> None:
        jobs = {identifier: transaction.get("job", identifier) for identifier in trial.job_ids}
        pending = {identifier for identifier, job in jobs.items() if job["state"] == "blocked"}
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
        complete = states <= {"succeeded", "failed", "cancelled", "not_started"}
        unknown = "execution_unknown" in states
        execution = (
            "succeeded"
            if states == {"succeeded"}
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
