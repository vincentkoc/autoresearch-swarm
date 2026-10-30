"""One observation validator shared by current publication and explicit reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from pydantic import Field

from research_swarm.control.models import JobData, TrialData
from research_swarm.control.worker_outputs import OutputRole, WorkerOutputIntent
from research_swarm.control.worker_records import AttemptData, WorkerData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import ID, Name, VersionedRecord
from research_swarm.projects.manifest import TimingContract
from research_swarm.projects.workload import WorkloadReport
from research_swarm.state.database import JsonObject, Transaction
from research_swarm.workers.journal import JournalEntry, JournalState
from research_swarm.workers.timed_launch import verify_timing


class PublishAttempt(VersionedRecord):
    attempt_id: ID
    journal_artifact: ArtifactRef
    report_artifact: ArtifactRef | None = None
    outputs: dict[Name, ArtifactRef] = Field(default_factory=dict)
    logs: dict[Name, ArtifactRef] = Field(default_factory=dict)
    finding: str = Field(default="", max_length=4096)


def evidence_roles(request: PublishAttempt) -> list[tuple[OutputRole, str | None, ArtifactRef]]:
    if set(request.logs) - {"stdout", "stderr"}:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Unknown native log stream")
    result: list[tuple[OutputRole, str | None, ArtifactRef]] = [
        ("journal", None, request.journal_artifact)
    ]
    if request.report_artifact is not None:
        result.append(("report", None, request.report_artifact))
    result.extend(("output", name, ref) for name, ref in request.outputs.items())
    for name in ("stdout", "stderr"):
        if name in request.logs:
            result.append(("stdout" if name == "stdout" else "stderr", None, request.logs[name]))
    return result


def output_intent(attempt: AttemptData, role: OutputRole, name: str | None) -> WorkerOutputIntent:
    return WorkerOutputIntent(
        worker_id=attempt.dispatch.worker_id,
        session_id=attempt.dispatch.worker_session_id,
        attempt_id=attempt.dispatch.attempt_id,
        role=role,
        output_name=name,
    )


@dataclass(frozen=True)
class ValidatedObservation:
    job_row: JsonObject
    trial_row: JsonObject
    job: JobData
    trial: TrialData
    not_started: bool
    unknown: bool
    success: bool
    state: str
    consumed: dict[str, Decimal]


@dataclass(frozen=True)
class NativeObservation:
    job_row: JsonObject
    trial_row: JsonObject
    job: JobData
    trial: TrialData
    not_started: bool
    closed: bool
    unknown: bool
    consumed: dict[str, Decimal]


def validate_native_observation(
    transaction: Transaction,
    request: PublishAttempt,
    journal: JournalEntry,
    worker: WorkerData,
) -> NativeObservation:
    attempt = AttemptData.model_validate(transaction.get("attempt", request.attempt_id)["data"])
    job_row = transaction.get("job", attempt.dispatch.job_id)
    job = JobData.model_validate(job_row["data"])
    trial_row = transaction.get("trial", job.trial_id)
    trial = TrialData.model_validate(trial_row["data"])
    if (
        journal.dispatch != attempt.dispatch
        or journal.spec != trial.spec
        or journal.signed_assignment != attempt.signed_assignment
        or journal.assignment_time != attempt.assignment_time
        or journal.clock_mode != worker.clock_mode
        or job.current_attempt_id != request.attempt_id
        or journal.command.argv != job.stage.argv
        or journal.command.timeout_ms > job.stage.timeout_ms
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Observation differs from its original grant")
    if journal.timing is not None:
        assert worker.assignment_authority is not None
        assert journal.assignment_time is not None and journal.signed_assignment is not None
        verify_timing(
            journal.timing,
            worker.assignment_authority,
            journal.assignment_time,
            journal.signed_assignment,
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
    consumed = {
        "attempts": Decimal(0 if not_started else 1),
        "execution_ms": Decimal(0 if outcome is None else outcome.elapsed_ms),
    }
    return NativeObservation(job_row, trial_row, job, trial, not_started, closed, unknown, consumed)


def validate_observation(
    transaction: Transaction,
    request: PublishAttempt,
    journal: JournalEntry,
    report: WorkloadReport | None,
    worker: WorkerData,
) -> ValidatedObservation:
    native = validate_native_observation(transaction, request, journal, worker)
    job_row, trial_row, job, trial = native.job_row, native.trial_row, native.job, native.trial
    not_started, closed, unknown = native.not_started, native.closed, native.unknown
    attempt = AttemptData.model_validate(transaction.get("attempt", request.attempt_id)["data"])
    outcome = journal.outcome
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
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Workload inputs or budget differ")
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
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Native log identity differs")
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
    return ValidatedObservation(
        job_row, trial_row, job, trial, not_started, unknown, success, state, native.consumed
    )
