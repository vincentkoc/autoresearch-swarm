"""Original-session evidence delivery without constructing an execution worker."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import httpx
from filelock import FileLock, Timeout
from pydantic import Field, TypeAdapter

from research_swarm.client import ResearchClient
from research_swarm.control.observations import PublishAttempt, output_intent
from research_swarm.control.reconciliation import AttemptSubmission, publication_digest
from research_swarm.control.worker_queries import WorkerRead
from research_swarm.control.worker_records import AttemptData, WorkerData
from research_swarm.domain.canonical import (
    canonical_bytes,
    content_digest,
    record_digest,
    strict_json_loads,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Digest, VersionedRecord, new_id
from research_swarm.runtime import private_state_directory, read_private, write_private
from research_swarm.workers.clock_signatures import ClockVerifier
from research_swarm.workers.journal import JournalEntry, JournalState, WorkerJournal
from research_swarm.workers.remote_enrollment import RemoteWorkerBundle, _installation
from research_swarm.workers.resources import MachineResources
from research_swarm.workers.supervisor import reconcile_outcome
from research_swarm.workers.timed_launch import verify_timing

MAX_JOURNAL_BYTES = 1024 * 1024
MAX_INTENT_BYTES = 2 * MAX_JOURNAL_BYTES


class RecoveryIntent(VersionedRecord):
    identity_digest: Digest
    journal: JournalEntry
    upload_key: ID = Field(default_factory=new_id)
    submission_key: ID = Field(default_factory=new_id)
    publication: PublishAttempt | None = None
    submission_id: ID | None = None


class RecoveryReport(VersionedRecord):
    attempt_id: ID
    native_state: JournalState
    custody_state: Literal["released", "retained"]
    delivery_state: Literal["quarantined", "pending", "blocked", "controller_terminal"]
    submitted_native_state: JournalState
    submission_id: ID | None = None
    controller_disposition: Literal["accepted", "closed_unaccepted"] | None = None
    error: ErrorCode | None = None
    rejected_command_id: ID | None = None
    next_action: Literal[
        "operator_reconcile",
        "observe_native",
        "retry_delivery",
        "repair_authority",
        "review_rejection",
        "none",
    ]


def _authenticate(entry: JournalEntry, bundle: RemoteWorkerBundle) -> None:
    authority = bundle.worker.assignment_authority
    if (
        authority is None
        or entry.signed_assignment is None
        or entry.assignment_time is None
        or entry.clock_mode != bundle.worker.clock_mode
        or entry.spec.project_id not in (bundle.worker.project_ids or ())
        or entry.signed_assignment.payload.profile not in bundle.worker.profiles
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Original signed authority is unavailable")
    # Historical verification is deliberately discarded; no launch token is retained.
    ClockVerifier(authority).binding(entry.assignment_time, entry.signed_assignment)
    if entry.timing is not None:
        verify_timing(entry.timing, authority, entry.assignment_time, entry.signed_assignment)


def _historical(
    client: ResearchClient, bundle: RemoteWorkerBundle, entry: JournalEntry
) -> AttemptData:
    binding = bundle.worker.binding
    session = client.worker_get(
        WorkerRead(
            worker_id=binding.worker_id,
            session_id=binding.worker_session_id,
            kind="worker_session",
            id=binding.worker_session_id,
        )
    )
    if WorkerData.model_validate(session["data"]) != bundle.worker:
        raise ResearchError(ErrorCode.STALE_FENCE, "Original worker session differs")
    row = client.worker_get(
        WorkerRead(
            worker_id=binding.worker_id,
            session_id=binding.worker_session_id,
            kind="historical_attempt",
            id=entry.dispatch.attempt_id,
        )
    )
    attempt = AttemptData.model_validate(row["data"])
    if (
        attempt.dispatch != entry.dispatch
        or attempt.signed_assignment != entry.signed_assignment
        or attempt.assignment_time != entry.assignment_time
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Original attempt authority differs")
    return attempt


def _save(root: Path, intent: RecoveryIntent) -> None:
    content = canonical_bytes(intent.model_dump(mode="json"))
    if len(content) > MAX_INTENT_BYTES:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Recovery intent exceeds its limit")
    write_private(root / "intent.json", content)


def _intent(
    root: Path, entry: JournalEntry, bundle: RemoteWorkerBundle, identity: str
) -> RecoveryIntent:
    path = root / "intent.json"
    if path.exists():
        previous = RecoveryIntent.model_validate(
            strict_json_loads(read_private(root, path.name, limit=MAX_INTENT_BYTES))
        )
        if previous.identity_digest != identity or previous.journal.dispatch != entry.dispatch:
            raise ResearchError(ErrorCode.STALE_FENCE, "Recovery intent authority differs")
        _authenticate(previous.journal, bundle)
        frozen = canonical_bytes(previous.journal.model_dump(mode="json"))
        if previous.publication is not None and (
            previous.publication.journal_artifact.digest != content_digest(frozen)
            or previous.publication.journal_artifact.size_bytes != len(frozen)
            or previous.publication.attempt_id != entry.dispatch.attempt_id
            or previous.publication.report_artifact is not None
            or previous.publication.outputs
            or previous.publication.logs
        ):
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Recovery payload differs")
        # An uncertain delivery retains its original bytes even if native state advances.
        if previous.submission_id is None or previous.journal == entry:
            return previous
    if len(canonical_bytes(entry.model_dump(mode="json"))) > MAX_JOURNAL_BYTES:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Recovery journal exceeds its limit"
        )
    intent = RecoveryIntent(identity_digest=identity, journal=entry)
    _save(root, intent)
    return intent


def _deliver(
    root: Path, intent: RecoveryIntent, bundle: RemoteWorkerBundle, client: ResearchClient
) -> RecoveryIntent:
    attempt = _historical(client, bundle, intent.journal)
    content = canonical_bytes(intent.journal.model_dump(mode="json"))
    if len(content) > MAX_JOURNAL_BYTES:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Recovery journal exceeds its limit"
        )
    if intent.publication is None:
        if attempt.publication_status != "pending":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Original attempt is already settled")
        # This derived file is rebuilt from the persisted intent, never from a changing journal.
        path = root / "journal.json"
        write_private(path, content)
        reference = client.worker_upload_file(
            output_intent(attempt, "journal", None),
            path,
            idempotency_key=intent.upload_key,
            media_type="application/json",
        )
        intent = intent.model_copy(
            update={
                "publication": PublishAttempt(
                    attempt_id=attempt.dispatch.attempt_id,
                    journal_artifact=reference,
                    finding="Original native journal for explicit operator reconciliation",
                )
            }
        )
        _save(root, intent)
    assert intent.publication is not None
    row = client.command(
        "worker.submit_evidence", intent.publication, idempotency_key=intent.submission_key
    )["result"]
    submitted = AttemptSubmission.model_validate(row["data"])
    if (
        row["state"] != "quarantined"
        or submitted.publication != intent.publication
        or submitted.publication_digest != publication_digest(intent.publication)
        or submitted.original_binding != bundle.worker.binding
        or submitted.submitted_by != bundle.worker.principal_id
        or (intent.submission_id is not None and intent.submission_id != row["id"])
    ):
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Evidence acknowledgment differs")
    intent = intent.model_copy(update={"submission_id": TypeAdapter(ID).validate_python(row["id"])})
    _save(root, intent)
    return intent


def _recover_locked(root: Path, attempt_id: str) -> RecoveryReport:
    state = _installation(root)
    if not state.initialized or state.bundle is None:
        raise ResearchError(
            ErrorCode.EXECUTION_UNKNOWN, "Original worker installation is incomplete"
        )
    assert state.journal_identity is not None and state.registry_identity is not None
    bundle = state.bundle
    journal = WorkerJournal.open_existing(root / "execution/journal.sqlite", state.journal_identity)
    resources = MachineResources.open_existing(
        Path(state.machine_registry), state.registry_identity
    )
    driver = root / "execution/driver"
    identity = {
        "controller_url": bundle.controller_url,
        "credential_id": bundle.credential_id,
        "worker": bundle.worker.model_dump(mode="json"),
        "journal_identity": journal.identity,
        "registry_identity": resources.identity,
    }
    if (driver / "identity.json").exists():
        if strict_json_loads(read_private(driver, "identity.json", limit=128 * 1024)) != identity:
            raise ResearchError(ErrorCode.STALE_FENCE, "Original scoped driver authority differs")
    elif (driver / "commands").exists():
        raise ResearchError(ErrorCode.STALE_FENCE, "Original scoped driver identity is missing")
    entry = journal.get(attempt_id)
    _authenticate(entry, bundle)
    entry = reconcile_outcome(journal, attempt_id)
    _authenticate(entry, bundle)
    closed = entry.state in {JournalState.CLOSED, JournalState.NOT_STARTED}
    if closed:
        try:
            resources.release(entry)
        except ResearchError as failure:
            if (
                failure.code != ErrorCode.NOT_FOUND
                or entry.state != JournalState.NOT_STARTED
                or entry.native is not None
                or entry.outcome is not None
            ):
                raise
    recovery = root / "execution/recovery" / attempt_id
    with private_state_directory(recovery):
        intent = _intent(recovery, entry, bundle, record_digest("research.recovery", identity))
        error = None
        terminal = None
        rejected_command_id = None
        try:
            with ResearchClient.connect(bundle.controller_url, bundle.token) as client:
                intent = _deliver(recovery, intent, bundle, client)
        except ResearchError as failure:
            error = failure.code
            rejected_command_id = failure.command_id
            if error == ErrorCode.INVALID_TRANSITION:
                try:
                    with ResearchClient.connect(bundle.controller_url, bundle.token) as client:
                        status = _historical(client, bundle, intent.journal).publication_status
                    if status in {"accepted", "closed_unaccepted"}:
                        terminal, error, rejected_command_id = status, None, None
                except ResearchError as reread:
                    error, rejected_command_id = reread.code, reread.command_id
                except httpx.RequestError:
                    error, rejected_command_id = ErrorCode.TRANSPORT_UNAVAILABLE, None
        except httpx.RequestError:
            error = ErrorCode.TRANSPORT_UNAVAILABLE
        pending = rejected_command_id is None and error in {
            ErrorCode.TRANSPORT_UNAVAILABLE,
            ErrorCode.DELIVERY_UNCERTAIN,
            ErrorCode.RESOURCE_UNAVAILABLE,
        }
        delivery = (
            "controller_terminal"
            if terminal
            else "pending"
            if pending
            else "blocked"
            if error
            else "quarantined"
        )
        return RecoveryReport(
            attempt_id=attempt_id,
            native_state=entry.state,
            custody_state="released" if closed else "retained",
            delivery_state=delivery,
            submitted_native_state=intent.journal.state,
            submission_id=intent.submission_id,
            error=error,
            controller_disposition=terminal,
            rejected_command_id=rejected_command_id,
            next_action="none"
            if terminal and closed
            else "observe_native"
            if terminal
            else "retry_delivery"
            if pending
            else "review_rejection"
            if rejected_command_id
            else "repair_authority"
            if error
            else ("operator_reconcile" if closed and intent.journal == entry else "observe_native"),
        )


def recover_worker_attempt(root: Path, attempt_id: str) -> RecoveryReport:
    """Observe one original attempt; never enroll, claim, cancel, spawn, or accept results."""
    attempt_id = TypeAdapter(ID).validate_python(attempt_id)
    root = root.expanduser().absolute()
    # Check the original installation before creating any lock or recovery directory.
    _installation(root)
    try:
        with FileLock(root / "enrollment.lock", timeout=0):
            driver = root / "execution/driver"
            locks = root / "execution/locks"
            with (
                private_state_directory(driver),
                private_state_directory(locks),
                FileLock(driver / "driver.lock", timeout=0),
                FileLock(locks / (attempt_id + ".lock"), timeout=0),
            ):
                return _recover_locked(root, attempt_id)
    except Timeout:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE, "Original worker is owned by another driver"
        ) from None
