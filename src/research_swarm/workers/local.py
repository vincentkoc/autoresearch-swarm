"""An explicit local worker executes admitted jobs without owning research decisions."""

from __future__ import annotations

import hashlib
import subprocess
import sys
import time
from contextlib import ExitStack, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from filelock import FileLock

from research_swarm.client import ResearchClient
from research_swarm.control.models import TrialData
from research_swarm.control.results import PublishAttempt
from research_swarm.control.worker_artifacts import WorkerArtifactRead
from research_swarm.control.worker_outputs import OutputRole, WorkerOutputIntent
from research_swarm.control.worker_queries import WorkerRead
from research_swarm.control.workers import AttemptData, ClaimJob, WorkerData
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import new_id, utc_now
from research_swarm.projects.manifest import TimingContract
from research_swarm.projects.native_files import SourceReader
from research_swarm.projects.source import extract_bundle
from research_swarm.projects.workload import (
    WorkloadContext,
    WorkloadInput,
    read_report,
    verify_outputs,
)
from research_swarm.runtime import private_state_directory, read_private, write_private
from research_swarm.workers.authority import DispatchVerifier, VerifiedDispatch
from research_swarm.workers.clock_estimates import ClockSampler
from research_swarm.workers.clock_signatures import ClockVerifier, VerifiedAssignmentTime
from research_swarm.workers.clocks import continuous_time
from research_swarm.workers.contracts import NativeCommand
from research_swarm.workers.journal import JournalEntry, JournalState, WorkerJournal
from research_swarm.workers.prepared import PreparedExecution
from research_swarm.workers.resources import MachineResources, ResourceDemand
from research_swarm.workers.signatures import AssignmentVerifier, VerifiedAssignment
from research_swarm.workers.supervisor import reconcile_outcome, spawn_supervisor
from research_swarm.workers.timed_launch import LiveTimingOrigin, capture_timing, verify_timing
from research_swarm.workers.timed_supervision import start_timed_supervisor


class LocalWorker:
    def __init__(
        self,
        client: ResearchClient,
        worker: WorkerData,
        *,
        key: bytes | None = None,
        root: Path,
        machine_registry: Path,
        executable: Path | None = None,
        maximum_output_bytes: int = 256 * 1024 * 1024,
    ) -> None:
        self.client, self.worker = client, WorkerData.model_validate(worker)
        self.scoped = worker.project_ids is not None
        self.assignment_verifier = (
            AssignmentVerifier(worker.assignment_authority)
            if worker.assignment_authority is not None
            else None
        )
        self.verifier = DispatchVerifier(worker.binding, key) if key is not None else None
        self.clock_sampler: ClockSampler | None = None
        if worker.clock_mode == "bounded-continuous-v1":
            assert worker.assignment_authority is not None
            self.clock_sampler = ClockSampler(worker.assignment_authority)
        if self.assignment_verifier is None and self.verifier is None:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Worker verification key is absent")
        self.root = root.absolute()
        self.executable = (executable or Path(sys.executable)).absolute()
        self.maximum_output_bytes = maximum_output_bytes
        self.profile = next(
            (profile for profile in worker.profiles if profile.name == "python-cpu"), None
        )
        if self.profile is None or self.profile.backend != "python":
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Local worker requires a Python CPU profile"
            )
        with private_state_directory(self.root):
            self.journal = WorkerJournal(self.root / "journal.sqlite")
            (self.root / "locks").mkdir(mode=0o700, exist_ok=True)
        machine_registry = machine_registry.absolute()
        with private_state_directory(machine_registry.parent):
            self.resources = MachineResources(machine_registry)
        capacities = {"cpu": self.profile.cpu_slots}
        if self.profile.memory_bytes:
            capacities["memory"] = self.profile.memory_bytes
        if self.profile.scratch_bytes:
            capacities["scratch"] = self.profile.scratch_bytes
        self.resources.enroll(worker.binding, ResourceDemand(amounts=capacities))

    def claim(self, job: dict[str, Any]) -> dict[str, Any]:
        return dict(
            self.client.command(
                "worker.claim_job",
                ClaimJob(
                    worker_id=self.worker.binding.worker_id,
                    session_id=self.worker.binding.worker_session_id,
                    job_id=job["id"],
                ),
                idempotency_key="claim:" + self.worker.binding.worker_session_id + ":" + job["id"],
                expected_revision=job["revision"],
            )["result"]
        )

    def _get(self, kind: Literal["attempt", "trial"], identifier: str) -> dict[str, Any]:
        if not self.scoped:
            return self.client.get(kind, identifier)
        return self.client.worker_get(
            WorkerRead(
                worker_id=self.worker.binding.worker_id,
                session_id=self.worker.binding.worker_session_id,
                kind=kind,
                id=identifier,
            )
        )

    def _download(self, attempt_id: str, reference: ArtifactRef, path: Path) -> None:
        if path.exists():
            digest, size = hashlib.sha256(), 0
            with SourceReader(path.parent) as reader, reader.open(path.name) as stream:
                while chunk := stream.read(1024 * 1024):
                    size += len(chunk)
                    if size > reference.size_bytes:
                        break
                    digest.update(chunk)
            if size != reference.size_bytes or "sha256:" + digest.hexdigest() != reference.digest:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Cached worker input differs")
            return
        observed = (
            self.client.worker_download(
                WorkerArtifactRead(
                    worker_id=self.worker.binding.worker_id,
                    session_id=self.worker.binding.worker_session_id,
                    attempt_id=attempt_id,
                    artifact_id=reference.id,
                ),
                path,
            )
            if self.scoped
            else self.client.download(reference.id, path)
        )
        if observed != reference:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Downloaded worker input differs")

    def _verify(
        self, attempt: AttemptData, trial: TrialData
    ) -> VerifiedDispatch | VerifiedAssignment | VerifiedAssignmentTime:
        timed = None
        if self.worker.clock_mode == "bounded-continuous-v1":
            if (
                attempt.assignment_time is None
                or attempt.signed_assignment is None
                or self.worker.assignment_authority is None
            ):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Signed timing authority is required"
                )
            timed = ClockVerifier(self.worker.assignment_authority).binding(
                attempt.assignment_time, attempt.signed_assignment
            )
        elif attempt.assignment_time is not None:
            raise ResearchError(ErrorCode.STALE_FENCE, "Assignment clock mode differs")
        if (
            self.worker.project_ids is not None
            and trial.spec.project_id not in self.worker.project_ids
        ):
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Project is outside worker scope")
        if self.assignment_verifier is not None:
            if attempt.signed_assignment is None:
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Signed assignment is required")
            verified = (
                timed.assignment
                if timed is not None
                else self.assignment_verifier.verify(
                    attempt.signed_assignment,
                    authority_now=utc_now(),
                    allow_expired_observation=True,
                )
            )
            payload = verified.assignment.payload
            if (
                payload.dispatch != attempt.dispatch
                or payload.spec != trial.spec
                or payload.profile != attempt.profile
                or payload.inputs != attempt.inputs
                or payload.source_artifact != attempt.source_artifact
                or payload.source_tree_digest != attempt.source_tree_digest
                or payload.evaluation_request_id != attempt.evaluation_request_id
            ):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Assignment presentation differs"
                )
            if payload.network_policy != "inherit":
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Network isolation is unavailable"
                )
            return timed if timed is not None else verified
        if attempt.signed_assignment is not None:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Assignment public key is not pinned"
            )
        assert self.verifier is not None
        return self.verifier.verify(attempt.dispatch, trial.spec, allow_expired_observation=True)

    def _prepare(self, attempt: AttemptData, trial: TrialData) -> JournalEntry:
        # Expired authenticated grants can be recorded as not started. The journal's
        # native release gate still refuses expiry, including a race after verification.
        verified = self._verify(attempt, trial)
        stage = next(stage for stage in trial.spec.stages if stage.id == attempt.dispatch.stage_id)
        root = self.root / "work" / attempt.dispatch.attempt_id
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            received = self.journal.get(attempt.dispatch.attempt_id)
        except ResearchError as error:
            if error.code != ErrorCode.NOT_FOUND:
                raise
            received = self.journal.receive(
                verified,
                NativeCommand(
                    argv=stage.argv,
                    cwd=str(root / ("source-" + new_id())),
                    timeout_ms=stage.timeout_ms,
                ),
                require_preparation=True,
            )
        received = self.journal.settle_unstarted(attempt.dispatch.attempt_id)
        if received.state == JournalState.NOT_STARTED:
            return received
        if self.profile != attempt.profile or stage.argv[0] not in {
            "python",
            "python3",
            str(self.executable),
        }:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Local executable profile differs"
            )
        execution = Path(received.command.cwd)
        source = root / "source.zip"
        self._download(attempt.dispatch.attempt_id, attempt.source_artifact, source)
        marker = root / "prepared.json"
        if marker.exists():
            prepared = PreparedExecution.model_validate(
                strict_json_loads(read_private(root, marker.name, limit=1024 * 1024))
            )
            prepared.verify(attempt.dispatch, trial.spec)
            prepared.verify_context_file()
            if prepared.working_directory is None:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Prepared directory is absent"
                )
            if execution != Path(prepared.working_directory):
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Prepared directory changed")
            self.resources.verify_held(
                JournalEntry(
                    dispatch=attempt.dispatch,
                    spec=trial.spec,
                    command=NativeCommand(
                        argv=stage.argv, cwd=str(execution), timeout_ms=stage.timeout_ms
                    ),
                ),
                prepared.machine_claim_id,
            )
        else:
            amounts = {"cpu": stage.resources.cpu_slots}
            if stage.resources.memory_bytes:
                amounts["memory"] = stage.resources.memory_bytes
            if stage.resources.scratch_bytes:
                amounts["scratch"] = stage.resources.scratch_bytes
            plain = (
                verified.assignment if isinstance(verified, VerifiedAssignmentTime) else verified
            )
            dispatch = plain.dispatch if isinstance(plain, VerifiedAssignment) else plain
            claim = self.resources.reserve(dispatch, ResourceDemand(amounts=amounts))
            if execution.exists():
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "Interrupted materialization requires a new attempt",
                )
            extract_bundle(source, execution, expected_tree_digest=attempt.source_tree_digest)
            inputs = root / "inputs"
            inputs.mkdir(mode=0o700, exist_ok=True)
            materialized = {}
            for index, (name, reference) in enumerate(sorted(attempt.inputs.items())):
                path = inputs / str(index)
                self._download(attempt.dispatch.attempt_id, reference, path)
                materialized[name] = WorkloadInput(
                    path=str(path), digest=reference.digest, size_bytes=reference.size_bytes
                )
            outputs = root / "outputs"
            outputs.mkdir(mode=0o700, exist_ok=True)
            timing = TimingContract.model_validate(trial.spec.extensions["timing"])
            context = WorkloadContext(
                trial_id=attempt.dispatch.trial_id,
                attempt_id=attempt.dispatch.attempt_id,
                stage_id=stage.id,
                spec_digest=attempt.dispatch.spec_digest,
                seed=trial.spec.seed,
                time_budget_ms=timing.budget_ms,
                inputs=materialized,
                output_root=str(outputs),
                report_path=str(root / "report.json"),
            ).sealed()
            write_private(root / "context.json", context.model_dump_json().encode("utf-8"))
            prepared = PreparedExecution(
                context=context,
                context_path=str(root / "context.json"),
                executable=str(self.executable),
                profile_digest=attempt.profile.execution_fingerprint,
                resource_registry=str(self.resources.path),
                machine_claim_id=claim.id,
                working_directory=str(execution),
            )
            write_private(marker, prepared.model_dump_json().encode("utf-8"))
        return self.journal.bind_prepared(attempt.dispatch.attempt_id, prepared)

    def _timed_origin(self, entry: JournalEntry) -> LiveTimingOrigin:
        sampler = self.clock_sampler
        if sampler is None or entry.signed_assignment is None or entry.assignment_time is None:
            raise ResearchError(ErrorCode.STALE_FENCE, "Live sampling origin is absent")
        timed = sampler.verifier.binding(entry.assignment_time, entry.signed_assignment)
        if entry.timing is None:
            pending = sampler.begin()
            estimate = pending.complete(self.client.worker_clock(pending.challenge))
            verified = capture_timing(timed, estimate, sampler.read())
            self.journal.bind_timing(entry.dispatch.attempt_id, verified)
        else:
            if entry.timing.caps.origin_session_id != sampler.origin_session_id:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Original sampling origin is unavailable"
                )
            verified = verify_timing(
                entry.timing,
                sampler.verifier.authority,
                entry.assignment_time,
                entry.signed_assignment,
            )
        return LiveTimingOrigin(sampler, verified)

    def execute(self, attempt_id: str) -> dict[str, Any]:
        row = self._get("attempt", attempt_id)
        attempt = AttemptData.model_validate(row["data"])
        if attempt.dispatch.worker_id != self.worker.binding.worker_id:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Attempt belongs to another worker")
        with (
            FileLock(self.root / "locks" / (attempt.dispatch.attempt_id + ".lock")),
            ExitStack() as lifecycle,
        ):
            row = self._get("attempt", attempt_id)
            attempt = AttemptData.model_validate(row["data"])
            if attempt.publication_status == "accepted":
                return row
            trial = TrialData.model_validate(self._get("trial", attempt.dispatch.trial_id)["data"])
            self._verify(attempt, trial)
            try:
                entry = self.journal.get(attempt_id)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
                entry = None
            if entry is None or (entry.state == JournalState.RECEIVED and entry.prepared is None):
                try:
                    entry = self._prepare(attempt, trial)
                except (OSError, ValueError, ResearchError) as error:
                    if (
                        isinstance(error, ResearchError)
                        and error.code == ErrorCode.RESOURCE_UNAVAILABLE
                    ):
                        raise
                    entry = self.journal.get(attempt_id)
                    if entry.state != JournalState.RECEIVED:
                        raise
                    self.journal.request_cancel(attempt_id, entry.last_controller_sequence + 1)
                    entry = self.journal.settle_unstarted(attempt_id)
                    entry = self.journal.note_reconciliation(
                        attempt_id, "Preparation failed: " + type(error).__name__
                    )
            if (
                entry.dispatch != attempt.dispatch
                or entry.spec != trial.spec
                or entry.signed_assignment != attempt.signed_assignment
                or entry.assignment_time != attempt.assignment_time
                or entry.clock_mode != self.worker.clock_mode
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Worker journal differs from admission")
            process = None
            supervised = None
            if entry.state == JournalState.RECEIVED:
                if entry.clock_mode == "bounded-continuous-v1":
                    try:
                        origin = self._timed_origin(entry)
                        supervised = start_timed_supervisor(self.journal, attempt_id, origin)
                    except (OSError, ValueError, RuntimeError, ResearchError) as error:
                        entry = self.journal.get(attempt_id)
                        self.journal.request_cancel(attempt_id, entry.last_controller_sequence + 1)
                        entry = self.journal.settle_unstarted(attempt_id)
                        entry = self.journal.note_reconciliation(
                            attempt_id, "Clock admission unavailable: " + type(error).__name__
                        )
                    else:
                        lifecycle.callback(supervised.close)
                        process = supervised.process
                        entry = self.journal.get(attempt_id)
                else:
                    process = spawn_supervisor(self.journal, attempt_id)
            if entry.clock_mode == "bounded-continuous-v1":
                observation_clock = continuous_time
                # A recovered origin cannot turn old counter values into new authority.
                until = (
                    int(entry.timing.caps.hard_ns) / 1_000_000_000 + 10
                    if process is not None and entry.timing is not None
                    else continuous_time() + 10
                )
            else:
                observation_clock = time.monotonic
                expiry = datetime.fromisoformat(
                    entry.dispatch.hard_deadline_at.replace("Z", "+00:00")
                )
                wait_seconds = max(0, (expiry - datetime.now(UTC)).total_seconds()) + 10
                until = time.monotonic() + wait_seconds
            while entry.state not in {
                JournalState.CLOSED,
                JournalState.NOT_STARTED,
                JournalState.UNKNOWN,
            }:
                if process is not None and process.poll() is not None:
                    entry = reconcile_outcome(self.journal, attempt_id)
                    if entry.state not in {JournalState.CLOSED, JournalState.NOT_STARTED}:
                        if entry.supervisor_id is None:
                            self.journal.request_cancel(
                                attempt_id, entry.last_controller_sequence + 1
                            )
                            entry = self.journal.settle_unstarted(attempt_id)
                        else:
                            entry = self.journal.unknown(
                                attempt_id, entry.supervisor_id, "Supervisor exited without closure"
                            )
                    break
                if observation_clock() >= until:
                    entry = reconcile_outcome(self.journal, attempt_id)
                    if (
                        entry.state not in {JournalState.CLOSED, JournalState.NOT_STARTED}
                        and entry.supervisor_id is not None
                    ):
                        entry = self.journal.unknown(
                            attempt_id, entry.supervisor_id, "Native outcome is overdue"
                        )
                    break
                time.sleep(0.025)
                entry = self.journal.get(attempt_id)
            if process is not None:
                # Reap only the dedicated supervisor started by this worker invocation.
                with suppress(subprocess.TimeoutExpired):
                    process.wait(timeout=2)
            lifecycle.pop_all().close()
            entry = reconcile_outcome(self.journal, attempt_id)
            if supervised is not None and supervised.errors:
                entry = self.journal.note_reconciliation(
                    attempt_id,
                    (entry.finding + "; " if entry.finding else "")
                    + "Clock admission channel: "
                    + ", ".join(supervised.errors),
                )
            if entry.state in {JournalState.CLOSED, JournalState.NOT_STARTED}:
                try:
                    self.resources.release(entry)
                except ResearchError as error:
                    if error.code != ErrorCode.NOT_FOUND or entry.native is not None:
                        raise
            else:
                assert entry.prepared is not None
                claim = self.resources.observe_claim(entry, entry.prepared.machine_claim_id)
                self.resources.quarantine(claim, "Execution lacks native closure")
            return self._publish(entry)

    def _upload(
        self,
        attempt_id: str,
        role: OutputRole,
        path: Path,
        *,
        idempotency_key: str,
        output_name: str | None = None,
    ) -> ArtifactRef:
        if not self.scoped:
            return self.client.upload_file(path, idempotency_key=idempotency_key)
        return self.client.worker_upload_file(
            WorkerOutputIntent(
                worker_id=self.worker.binding.worker_id,
                session_id=self.worker.binding.worker_session_id,
                attempt_id=attempt_id,
                role=role,
                output_name=output_name,
            ),
            path,
            idempotency_key=idempotency_key,
        )

    def _publish(self, entry: JournalEntry) -> dict[str, Any]:
        attempt_id = entry.dispatch.attempt_id
        root = self.root / "work" / attempt_id
        journal_path = root / "observation.json"
        write_private(journal_path, entry.model_dump_json().encode("utf-8"))
        journal_digest = record_digest("research.worker-observation", entry.model_dump(mode="json"))
        journal_artifact = self._upload(
            attempt_id, "journal", journal_path, idempotency_key="journal:" + journal_digest
        )
        outputs: dict[str, ArtifactRef] = {}
        logs: dict[str, ArtifactRef] = {}
        report_artifact = None
        finding = (
            entry.finding if entry.state in {JournalState.UNKNOWN, JournalState.NOT_STARTED} else ""
        )
        if entry.outcome is not None:
            for name in ("stdout", "stderr"):
                logs[name] = self._upload(
                    attempt_id,
                    "stdout" if name == "stdout" else "stderr",
                    self.root / "executions" / attempt_id / "output" / name,
                    idempotency_key=attempt_id + ":" + name,
                )
            if (
                entry.outcome.reason == "exited"
                and entry.outcome.exit_code == 0
                and entry.outcome.closure.scope_closed
            ):
                assert entry.prepared is not None
                stage = next(
                    stage for stage in entry.spec.stages if stage.id == entry.dispatch.stage_id
                )
                try:
                    report = read_report(entry.prepared.context, stage)
                    verified = verify_outputs(
                        entry.prepared.context, report, maximum_bytes=self.maximum_output_bytes
                    )
                except (ValueError, OSError) as error:
                    finding = "Invalid workload report: " + type(error).__name__
                else:
                    report_artifact = self._upload(
                        attempt_id,
                        "report",
                        Path(entry.prepared.context.report_path),
                        idempotency_key=attempt_id + ":report",
                    )
                    for output in verified:
                        reference = self._upload(
                            attempt_id,
                            "output",
                            Path(entry.prepared.context.output_root) / output.output.path,
                            idempotency_key=attempt_id + ":output:" + output.output.name,
                            output_name=output.output.name,
                        )
                        if (reference.digest, reference.size_bytes) != (
                            output.digest,
                            output.size_bytes,
                        ):
                            raise ResearchError(
                                ErrorCode.ARTIFACT_UNVERIFIED, "Output changed during publication"
                            )
                        outputs[output.output.name] = reference
        publication = PublishAttempt(
            attempt_id=attempt_id,
            journal_artifact=journal_artifact,
            report_artifact=report_artifact,
            outputs=outputs,
            logs=logs,
            finding=finding,
        )
        digest = record_digest("research.attempt-publication", publication.model_dump(mode="json"))
        return dict(
            self.client.command(
                "worker.publish_attempt", publication, idempotency_key="publish:" + digest
            )["result"]
        )
