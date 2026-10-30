"""Attempt-bound artifact authority without workspace-wide read permissions."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import BinaryIO

from research_swarm.control.workers import AttemptData, WorkerControl, WorkerData, worker_authority
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, VersionedRecord, utc_now
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService
from research_swarm.state.database import Database, Transaction
from research_swarm.state.sql import SqlSession


class WorkerAttemptTarget(VersionedRecord):
    worker_id: ID
    session_id: ID
    attempt_id: ID


class WorkerArtifactRead(WorkerAttemptTarget):
    artifact_id: ID


def assigned_attempt(
    transaction: Transaction, worker_id: str, session_id: str, attempt_id: str
) -> tuple[WorkerData, AttemptData]:
    worker = worker_authority(transaction, worker_id, session_id)
    attempt = AttemptData.model_validate(transaction.get("attempt", attempt_id)["data"])
    dispatch = attempt.dispatch
    if (
        dispatch.worker_id != worker_id
        or dispatch.worker_session_id != session_id
        or dispatch.controller_id != worker.binding.controller_id
        or dispatch.control_epoch != worker.binding.control_epoch
        or dispatch.workspace_id != worker.binding.workspace_id
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Attempt belongs to another authority")
    if attempt.signed_assignment is None:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Scoped transfers require a signed assignment"
        )
    project_id = attempt.signed_assignment.payload.spec.project_id
    if worker.project_ids is not None and project_id not in worker.project_ids:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Project is outside worker scope")
    return worker, attempt


def input_reference(
    transaction: Transaction, request: WorkerArtifactRead, workers: WorkerControl | None = None
) -> ArtifactRef:
    worker, attempt = assigned_attempt(
        transaction, request.worker_id, request.session_id, request.attempt_id
    )
    if attempt.publication_status != "pending":
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Input grant is already settled")
    if workers is None:
        if worker.clock_mode != "local-utc-v1" or attempt.assignment_time is not None:
            raise ResearchError(ErrorCode.STALE_FENCE, "Issuing clock authority is unavailable")
        now = utc_now()
    else:
        now = workers.input_time(worker, attempt)
    if min(attempt.dispatch.expires_at, attempt.dispatch.hard_deadline_at) <= now:
        raise ResearchError(ErrorCode.STALE_FENCE, "Input acquisition grant has expired")
    references = [attempt.source_artifact, *attempt.inputs.values()]
    reference = next((ref for ref in references if ref.id == request.artifact_id), None)
    if reference is None:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Artifact is not an assigned input")
    return reference


class WorkerArtifacts:
    def __init__(
        self, database: Database, artifacts: ArtifactService, workers: WorkerControl | None = None
    ) -> None:
        if database.authority_identity != artifacts.database.authority_identity:
            raise ValueError("Worker artifacts must share the research transaction authority")
        if (
            workers is not None
            and database.authority_identity != workers.database.authority_identity
        ):
            raise ValueError("Worker input clocks must share the research transaction authority")
        self.database, self.artifacts = database, artifacts
        self.workers = workers

    def input_metadata(self, context: CommandContext, request: WorkerArtifactRead) -> ArtifactRef:
        context.principal.require("worker.inputs")
        request = WorkerArtifactRead.model_validate(request)

        def read(connection: SqlSession) -> ArtifactRef:
            transaction = Transaction(connection, context)
            reference = input_reference(transaction, request, self.workers)
            return ArtifactCatalog.available(transaction, reference).reference

        return self.database.store.read(read)

    @contextmanager
    def input_reader(
        self, context: CommandContext, request: WorkerArtifactRead
    ) -> Iterator[BinaryIO]:
        request = WorkerArtifactRead.model_validate(request)
        reference = self.input_metadata(context, request)

        def authorize(transaction: Transaction, observed: ArtifactRef) -> None:
            if input_reference(transaction, request, self.workers) != observed:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Assigned input changed")

        with self.artifacts.reader_authorized(
            context, reference, scope="worker.inputs", authorize=authorize
        ) as stream:
            yield stream
