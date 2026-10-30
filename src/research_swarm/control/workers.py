"""Worker enrollment and atomic job admission; native launch remains worker-owned."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from research_swarm.control.confirmation_runtime import claim_confirmation
from research_swarm.control.models import CampaignData, JobData, ProjectRevisionData, TrialData
from research_swarm.domain.assignments import (
    AssignmentAuthority,
    AssignmentPayload,
    SignedAssignment,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import DispatchEnvelope
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import (
    ID,
    Digest,
    Name,
    Timestamp,
    VersionedRecord,
    new_id,
    utc_now,
)
from research_swarm.domain.workers import EngineCapabilities as EngineCapabilities
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.ledger import BudgetLedger
from research_swarm.workers.authority import WorkerBinding, authorize_dispatch
from research_swarm.workers.signatures import AssignmentSigner

NATIVE_CLEANUP_MS = 5000


class EnrollWorker(VersionedRecord):
    worker_id: ID
    session_id: ID
    principal_id: ID
    name: Name
    profiles: tuple[EngineCapabilities, ...] = Field(min_length=1, max_length=32)
    max_concurrency: int = Field(default=1, strict=True, ge=1, le=1024)

    @model_validator(mode="after")
    def distinct_profiles(self) -> EnrollWorker:
        if len({profile.name for profile in self.profiles}) != len(self.profiles):
            raise ValueError("Worker profiles must be unique")
        return self


class WorkerData(VersionedRecord):
    binding: WorkerBinding
    principal_id: ID
    name: Name
    profiles: tuple[EngineCapabilities, ...]
    max_concurrency: int
    assignment_authority: AssignmentAuthority | None = None
    enrolled_at: Timestamp = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def matching_authority(self) -> WorkerData:
        if (
            self.assignment_authority is not None
            and self.assignment_authority.binding != self.binding
        ):
            raise ValueError("Worker signing authority differs from enrollment")
        return self


class ClaimJob(VersionedRecord):
    worker_id: ID
    session_id: ID
    job_id: ID


class AttemptData(VersionedRecord):
    dispatch: DispatchEnvelope
    profile: EngineCapabilities
    inputs: dict[Name, ArtifactRef]
    source_artifact: ArtifactRef
    source_tree_digest: Digest
    evaluation_request_id: ID | None = None
    signed_assignment: SignedAssignment | None = None
    outcome_artifact: ArtifactRef | None = None
    report_artifact: ArtifactRef | None = None
    result_digest: Digest | None = None
    publication_status: Literal["pending", "accepted"] = "pending"
    finding: str = ""


def authority(transaction: Transaction) -> tuple[str, int]:
    row = transaction.connection.execute(
        "SELECT controller_id,control_epoch FROM workspaces WHERE id=?", (transaction.workspace_id,)
    ).fetchone()
    if row is None:
        raise ResearchError(ErrorCode.NOT_FOUND, "Workspace not found")
    return row["controller_id"], row["control_epoch"]


def worker_authority(transaction: Transaction, worker_id: str, session_id: str) -> WorkerData:
    row = transaction.get("worker", worker_id)
    worker = WorkerData.model_validate(row["data"])
    if transaction.context.principal.id != worker.principal_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Principal does not own this worker")
    controller_id, epoch = authority(transaction)
    if (
        row["state"] != "active"
        or worker.binding.worker_session_id != session_id
        or worker.binding.control_epoch != epoch
        or worker.binding.controller_id != controller_id
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Worker enrollment is no longer current")
    return worker


class WorkerControl:
    def __init__(
        self, database: Database, dispatch_key: bytes, assignment_key: bytes | None = None
    ) -> None:
        if len(dispatch_key) < 32:
            raise ValueError("Worker control requires at least 256 bits of dispatch authority")
        self.database = database
        self.dispatch_key = dispatch_key
        if assignment_key is not None and len(assignment_key) != 32:
            raise ValueError("Assignment signing requires a 32-byte Ed25519 key")
        self._assignment_key = assignment_key

    def enroll(self, context: CommandContext, request: EnrollWorker) -> JsonObject:
        def enroll(transaction: Transaction) -> JsonObject:
            existing = transaction.connection.execute(
                "SELECT id FROM records WHERE workspace_id=? AND kind='worker' AND id=?",
                (transaction.workspace_id, request.worker_id),
            ).fetchone()
            if existing is not None:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION,
                    "Worker already enrolled. Resume its session; "
                    "replacement requires explicit fencing",
                )
            controller_id, epoch = authority(transaction)
            binding = WorkerBinding(
                workspace_id=transaction.workspace_id,
                controller_id=controller_id,
                control_epoch=epoch,
                worker_id=request.worker_id,
                worker_session_id=request.session_id,
            )
            worker = transaction.insert(
                "worker",
                request.worker_id,
                WorkerData(
                    binding=binding,
                    principal_id=request.principal_id,
                    name=request.name,
                    profiles=request.profiles,
                    max_concurrency=request.max_concurrency,
                    assignment_authority=(
                        AssignmentSigner(binding, self._assignment_key).authority
                        if self._assignment_key is not None
                        else None
                    ),
                ).model_dump(mode="json"),
                state="active",
            )
            transaction.event("worker.enrolled", worker, {"session_id": request.session_id})
            return worker

        return self.database.execute(
            context, "worker.enroll", request.model_dump(mode="json"), "worker.manage", enroll
        )

    def claim(self, context: CommandContext, request: ClaimJob) -> JsonObject:
        def claim(transaction: Transaction) -> JsonObject:
            worker = worker_authority(transaction, request.worker_id, request.session_id)
            row = transaction.get("job", request.job_id)
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Job revision changed")
            if row["state"] != "ready":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Job is not ready for admission")
            job = JobData.model_validate(row["data"])
            campaign_row = transaction.get("campaign", row["campaign_id"])
            campaign = CampaignData.model_validate(campaign_row["data"])
            trial_row = transaction.get("trial", job.trial_id)
            trial = TrialData.model_validate(trial_row["data"])
            if campaign_row["state"] != "running" and not (
                campaign_row["state"] == "pausing" and trial_row["state"] == "running"
            ):
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Campaign admission is closed")
            if trial.execution_outcome not in {"pending"}:
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Trial is not executable")
            project = ProjectRevisionData.model_validate(
                transaction.get("project_revision", campaign.contract.project_revision_id)["data"]
            )
            profile = next(
                (
                    profile
                    for profile in worker.profiles
                    if profile.name == job.stage.resources.engine_profile
                ),
                None,
            )
            if profile is None:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Worker lacks the requested engine"
                )
            profile.require(
                job.stage.resources, project.resolved.manifest.environment.network_policy
            )
            if project.resolved.manifest.evaluation.measurement_trust == "protected":
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE,
                    "Protected evaluator admission is not qualified",
                )
            for predecessor_id in job.predecessor_ids:
                if transaction.get("job", predecessor_id)["state"] != "succeeded":
                    raise ResearchError(
                        ErrorCode.INVALID_TRANSITION, "Stage dependency is incomplete"
                    )
            inputs = dict(job.inputs)
            for name, predecessor_id in job.input_sources.items():
                parent = JobData.model_validate(transaction.get("job", predecessor_id)["data"])
                if name not in parent.outputs:
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "Predecessor output is unavailable"
                    )
                inputs[name] = parent.outputs[name]
            for reference in inputs.values():
                ArtifactCatalog.available(transaction, reference)
            active = transaction.connection.execute(
                "SELECT payload FROM records WHERE workspace_id=? AND kind='attempt' "
                "AND state NOT IN ('succeeded','failed','cancelled','not_started')",
                (transaction.workspace_id,),
            ).fetchall()
            active_attempts = [AttemptData.model_validate_json(item["payload"]) for item in active]
            if (
                sum(item.dispatch.worker_id == request.worker_id for item in active_attempts)
                >= worker.max_concurrency
            ):
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Worker concurrency is occupied"
                )
            active_trials = {
                item["id"]
                for item in transaction.connection.execute(
                    "SELECT id FROM records WHERE workspace_id=? AND kind='trial' "
                    "AND campaign_id=? AND state IN ('running','execution_unknown')",
                    (transaction.workspace_id, row["campaign_id"]),
                ).fetchall()
            }
            if (
                job.trial_id not in active_trials
                and len(active_trials) >= campaign.contract.search.max_parallel_trials
            ):
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Campaign trial concurrency is occupied"
                )
            attempt_id = new_id()
            claim_confirmation(transaction, trial, job, profile.execution_fingerprint, attempt_id)
            reservation = BudgetLedger.reserve(
                transaction,
                row["campaign_id"],
                attempt_id,
                {
                    "attempts": Decimal(1),
                    "execution_ms": Decimal(job.stage.timeout_ms + NATIVE_CLEANUP_MS),
                },
            )
            now = datetime.now(UTC)
            expiry = (
                (now + timedelta(minutes=2))
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
            deadline = (
                (now + timedelta(minutes=2, milliseconds=job.stage.timeout_ms + NATIVE_CLEANUP_MS))
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
            assert trial.spec.spec_digest is not None
            envelope = authorize_dispatch(
                DispatchEnvelope(
                    **worker.binding.model_dump(exclude={"schema_version"}),
                    campaign_id=row["campaign_id"],
                    trial_id=job.trial_id,
                    job_id=request.job_id,
                    attempt_id=attempt_id,
                    spec_digest=trial.spec.spec_digest,
                    fencing_generation=row["revision"],
                    reservation_ids=(reservation.id,),
                    stage_id=job.stage.id,
                    expires_at=expiry,
                    hard_deadline_at=deadline,
                    launch_permit_digest=trial.spec.spec_digest,
                ),
                self.dispatch_key,
            )
            candidate = transaction.get("candidate", trial.candidate_id)["data"]
            source = ArtifactRef.model_validate(candidate["source_artifact"])
            ArtifactCatalog.available(transaction, source)
            signed_assignment = None
            if worker.assignment_authority is not None:
                if self._assignment_key is None:
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE, "Assignment signing authority is absent"
                    )
                signer = AssignmentSigner(worker.binding, self._assignment_key)
                if signer.authority != worker.assignment_authority:
                    raise ResearchError(
                        ErrorCode.STALE_FENCE, "Assignment signing authority changed"
                    )
                signed_assignment = signer.sign(
                    AssignmentPayload(
                        dispatch=envelope,
                        spec=trial.spec,
                        profile=profile,
                        inputs=inputs,
                        source_artifact=source,
                        source_tree_digest=trial.spec.source.tree_digest,
                        evaluation_request_id=job.evaluation_request_id,
                        network_policy=project.resolved.manifest.environment.network_policy,
                        issued_at=now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                    )
                )
            attempt = transaction.insert(
                "attempt",
                attempt_id,
                AttemptData(
                    dispatch=envelope,
                    profile=profile,
                    inputs=inputs,
                    source_artifact=source,
                    source_tree_digest=trial.spec.source.tree_digest,
                    evaluation_request_id=job.evaluation_request_id,
                    signed_assignment=signed_assignment,
                ).model_dump(mode="json"),
                state="reserved",
                campaign_id=row["campaign_id"],
                parent_id=request.job_id,
            )
            updated_job = transaction.update(
                "job",
                row["id"],
                expected_revision=row["revision"],
                state="reserved",
                data=job.model_copy(
                    update={"current_attempt_id": attempt_id, "inputs": inputs}
                ).model_dump(mode="json"),
            )
            transaction.update(
                "trial",
                trial_row["id"],
                expected_revision=trial_row["revision"],
                state="running",
                data=trial.model_copy(update={"resource_status": "active"}).model_dump(mode="json"),
            )
            transaction.event("job.reserved", updated_job, {"attempt_id": attempt_id})
            transaction.event("attempt.dispatch_requested", attempt, {"dispatch_id": envelope.id})
            return attempt

        return self.database.execute(
            context, "worker.claim_job", request.model_dump(mode="json"), "worker.claim", claim
        )
