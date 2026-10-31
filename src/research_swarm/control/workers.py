"""Worker enrollment and atomic job admission; native launch remains worker-owned."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from pydantic import Field, model_validator

from research_swarm.control.acceptance import AcceptanceControl
from research_swarm.control.confirmation_runtime import claim_confirmation
from research_swarm.control.job_inputs import resolved_job_inputs
from research_swarm.control.models import CampaignData, JobData, ProjectRevisionData, TrialData
from research_swarm.control.time_authority import (
    AuthorityTime,
    AuthorityTimeProvider,
    ClockAuthorityScope,
)
from research_swarm.control.worker_records import (
    AttemptData as AttemptData,
)
from research_swarm.control.worker_records import (
    WorkerData as WorkerData,
)
from research_swarm.control.worker_sessions import archive_session
from research_swarm.control.worker_state import admission_open
from research_swarm.crypto import Ed25519Provider, resolve_crypto
from research_swarm.domain.assignments import (
    AssignmentPayload,
)
from research_swarm.domain.clocks import (
    AssignmentTimePayload,
    ClockChallenge,
    ClockMode,
    ClockReplyPayload,
    SignedClockReply,
    timed_assignment_digest,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import DispatchEnvelope
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import (
    ID,
    Digest,
    Name,
    VersionedRecord,
    new_id,
    utc_now,
)
from research_swarm.domain.workers import EngineCapabilities as EngineCapabilities
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.ledger import BudgetLedger
from research_swarm.state.sql import SqlSession
from research_swarm.workers.authority import WorkerBinding, authorize_dispatch
from research_swarm.workers.clock_signatures import ClockSigner, ClockVerifier
from research_swarm.workers.signatures import AssignmentSigner

NATIVE_CLEANUP_MS = 5000


class EnrollWorker(VersionedRecord):
    worker_id: ID
    session_id: ID
    principal_id: ID
    name: Name
    profiles: tuple[EngineCapabilities, ...] = Field(min_length=1, max_length=32)
    max_concurrency: int = Field(default=1, strict=True, ge=1, le=1024)
    project_ids: tuple[ID, ...] | None = Field(default=None, min_length=1, max_length=128)
    clock_mode: ClockMode = "local-utc-v1"
    environment_contracts: tuple[Digest, ...] | None = Field(
        default=None, min_length=1, max_length=128, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def distinct_profiles(self) -> EnrollWorker:
        if len({profile.name for profile in self.profiles}) != len(self.profiles):
            raise ValueError("Worker profiles must be unique")
        if self.project_ids is not None and len(set(self.project_ids)) != len(self.project_ids):
            raise ValueError("Worker projects must be unique")
        if (
            self.environment_contracts is not None
            and tuple(sorted(set(self.environment_contracts))) != self.environment_contracts
        ):
            raise ValueError("Worker environment contracts must be sorted and unique")
        return self


class ClaimJob(VersionedRecord):
    worker_id: ID
    session_id: ID
    job_id: ID


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
        self,
        database: Database,
        dispatch_key: bytes,
        assignment_key: bytes | None = None,
        *,
        authority_time_provider: AuthorityTimeProvider | None = None,
        crypto_provider: Ed25519Provider | None = None,
    ) -> None:
        if len(dispatch_key) < 32:
            raise ValueError("Worker control requires at least 256 bits of dispatch authority")
        self.database = database
        self.crypto_provider = resolve_crypto(crypto_provider)
        self.dispatch_key = dispatch_key
        if assignment_key is not None and len(assignment_key) != 32:
            raise ValueError("Assignment signing requires a 32-byte Ed25519 key")
        self._assignment_key = assignment_key
        if (
            authority_time_provider is not None
            and authority_time_provider.authority_identity != database.authority_identity
        ):
            raise ValueError("Clock provider must share the controller state authority")
        self._authority_time_provider = authority_time_provider
        self.acceptance = AcceptanceControl(database, self._time)

    def _clock_signer(self, worker: WorkerData) -> ClockSigner:
        if self._assignment_key is None or worker.assignment_authority is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Clock signing authority is absent"
            )
        signer = ClockSigner(
            worker.binding, self._assignment_key, crypto_provider=self.crypto_provider
        )
        if signer.authority != worker.assignment_authority:
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock signing authority changed")
        return signer

    def _time(self, worker: WorkerData) -> AuthorityTime:
        signer = self._clock_signer(worker)
        binding = worker.binding
        if self._authority_time_provider is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Authority time provider is absent"
            )
        return self._authority_time_provider.observe(
            ClockAuthorityScope(
                binding.workspace_id,
                binding.controller_id,
                binding.control_epoch,
                signer.authority.key_id,
            )
        )

    def observation_time(self, worker: WorkerData) -> AuthorityTime:
        """Qualified time for operational observations, without extending a lease."""
        return self._time(worker)

    def clock(self, context: CommandContext, request: ClockChallenge) -> SignedClockReply:
        context.principal.require("worker.query")
        request = ClockChallenge.model_validate(request)

        def read(connection: SqlSession) -> SignedClockReply:
            worker = worker_authority(
                Transaction(connection, context), request.worker_id, request.session_id
            )
            if worker.clock_mode != "bounded-continuous-v1":
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE,
                    "Worker is enrolled for local UTC, not calibrated authority time",
                )
            observed = self._time(worker)
            return self._clock_signer(worker).reply(
                ClockReplyPayload(
                    binding=worker.binding,
                    clock_epoch=observed.epoch,
                    nonce=request.nonce,
                    authority_time_ms=observed.milliseconds,
                    clock_profile=observed.profile,
                    policy=observed.policy,
                )
            )

        return self.database.store.read(read)

    def input_time(self, worker: WorkerData, attempt: AttemptData) -> str:
        if worker.clock_mode == "local-utc-v1":
            if attempt.assignment_time is not None:
                raise ResearchError(ErrorCode.STALE_FENCE, "Assignment clock mode differs")
            return utc_now()
        if attempt.assignment_time is None or attempt.signed_assignment is None:
            raise ResearchError(ErrorCode.STALE_FENCE, "Assignment time binding is absent")
        signer = self._clock_signer(worker)
        ClockVerifier(signer.authority, crypto_provider=self.crypto_provider).binding(
            attempt.assignment_time, attempt.signed_assignment
        )
        observed = self._time(worker)
        grant = attempt.assignment_time.payload
        if grant.clock_epoch != observed.epoch or grant.policy_digest != observed.policy.digest:
            raise ResearchError(ErrorCode.STALE_FENCE, "Issuing clock epoch is unavailable")
        return observed.datetime.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def enroll(self, context: CommandContext, request: EnrollWorker) -> JsonObject:
        def enroll(transaction: Transaction) -> JsonObject:
            if request.clock_mode == "bounded-continuous-v1" and (
                self._assignment_key is None or self._authority_time_provider is None
            ):
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE,
                    "Calibrated workers require signing and qualified time authority",
                )
            if request.project_ids is not None:
                if self._assignment_key is None:
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE,
                        "Scoped workers require signed assignment authority",
                    )
                for identifier in request.project_ids:
                    transaction.get("project", identifier)
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
                    project_ids=request.project_ids,
                    clock_mode=request.clock_mode,
                    environment_contracts=request.environment_contracts,
                    assignment_authority=(
                        AssignmentSigner(
                            binding, self._assignment_key, crypto_provider=self.crypto_provider
                        ).authority
                        if self._assignment_key is not None
                        else None
                    ),
                ).model_dump(mode="json"),
                state="active",
            )
            archive_session(transaction, WorkerData.model_validate(worker["data"]))
            transaction.event("worker.enrolled", worker, {"session_id": request.session_id})
            return worker

        payload = request.model_dump(mode="json")
        # Local enrollment acknowledgments predate the opt-in clock contract.
        if request.clock_mode == "local-utc-v1":
            payload.pop("clock_mode")
        return self.database.execute(context, "worker.enroll", payload, "worker.manage", enroll)

    def claim(self, context: CommandContext, request: ClaimJob) -> JsonObject:
        def claim(transaction: Transaction) -> JsonObject:
            worker = worker_authority(transaction, request.worker_id, request.session_id)
            if not admission_open(transaction, worker.binding):
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Worker admission is draining")
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
            if (
                worker.environment_contracts is not None
                and trial.spec.environment_digest not in worker.environment_contracts
            ):
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE,
                    "Environment contract is outside worker admission",
                )
            if worker.project_ids is not None and trial.spec.project_id not in worker.project_ids:
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Project is outside worker scope"
                )
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
            inputs = resolved_job_inputs(transaction, job)
            active = transaction.connection.execute(
                "SELECT payload FROM records WHERE workspace_id=? AND kind='attempt' "
                "AND state NOT IN "
                "('succeeded','failed','cancelled','not_started','closed_unaccepted')",
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
            clock_time = (
                self._time(worker) if worker.clock_mode == "bounded-continuous-v1" else None
            )
            now = clock_time.datetime if clock_time is not None else datetime.now(UTC)
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
            assignment_time = None
            if worker.assignment_authority is not None:
                if self._assignment_key is None:
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE, "Assignment signing authority is absent"
                    )
                signer = AssignmentSigner(
                    worker.binding, self._assignment_key, crypto_provider=self.crypto_provider
                )
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
                if clock_time is not None:
                    assignment_time = self._clock_signer(worker).bind(
                        AssignmentTimePayload(
                            binding=worker.binding,
                            clock_epoch=clock_time.epoch,
                            assignment_digest=timed_assignment_digest(signed_assignment),
                            policy_digest=clock_time.policy.digest,
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
                    assignment_time=assignment_time,
                ).model_dump(mode="json"),
                state="reserved",
                campaign_id=row["campaign_id"],
                parent_id=request.job_id,
            )
            self.acceptance.issue(
                transaction,
                attempt,
                worker,
                now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                clock_time,
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
