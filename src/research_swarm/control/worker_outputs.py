"""Attempt-bound upload receipts preserve provenance across content deduplication."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

from pydantic import model_validator

from research_swarm.control.recovery_operations import (
    recovery_context,
    require_journal_upload,
    require_raw_operation,
)
from research_swarm.control.worker_artifacts import WorkerAttemptTarget
from research_swarm.control.worker_records import WorkerData
from research_swarm.control.worker_sessions import historical_attempt_owner
from research_swarm.control.workers import AttemptData
from research_swarm.crypto import Ed25519Provider
from research_swarm.domain.artifacts import (
    ArtifactRecord,
    ArtifactRetention,
    StoredBlob,
    UploadSpec,
)
from research_swarm.domain.canonical import content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext, artifact_context
from research_swarm.domain.types import ID, Name, VersionedRecord
from research_swarm.domain.workers import WorkerBinding
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService, BeginUpload
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.sql import SqlSession

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from research_swarm.workers.journal_records import JournalEntry

OutputRole = Literal["journal", "report", "stdout", "stderr", "output"]


class WorkerOutputIntent(WorkerAttemptTarget):
    role: OutputRole
    output_name: Name | None = None

    @model_validator(mode="after")
    def named_output(self) -> WorkerOutputIntent:
        if (self.role == "output") != (self.output_name is not None):
            raise ValueError("Only declared outputs require an output name")
        return self


class BeginWorkerUpload(WorkerOutputIntent):
    content: BeginUpload


class WorkerUploadTarget(WorkerAttemptTarget):
    upload_id: ID


class WorkerUploadData(VersionedRecord):
    binding: WorkerBinding
    attempt_id: ID
    upload_id: ID
    role: OutputRole
    output_name: Name | None = None
    artifact: ArtifactRef | None = None
    retention_id: ID | None = None


def require_pending(attempt: AttemptData) -> None:
    if attempt.publication_status != "pending":
        raise ResearchError(ErrorCode.INVALID_TRANSITION, "Attempt publication is already accepted")


def owned_upload(
    transaction: Transaction, request: WorkerUploadTarget
) -> tuple[JsonObject, WorkerUploadData, JsonObject, UploadSpec, AttemptData]:
    worker, attempt = historical_attempt_owner(
        transaction, request.worker_id, request.session_id, request.attempt_id
    )
    return verify_upload(transaction, request, worker, attempt)


def verify_upload(
    transaction: Transaction,
    request: WorkerUploadTarget,
    worker: WorkerData,
    attempt: AttemptData,
) -> tuple[JsonObject, WorkerUploadData, JsonObject, UploadSpec, AttemptData]:
    """Verify persisted provenance after the caller authenticates its own authority."""
    receipt = transaction.get("worker_upload", request.upload_id)
    data = WorkerUploadData.model_validate(receipt["data"])
    row = transaction.get("upload", request.upload_id)
    upload = UploadSpec.model_validate(row["data"])
    if (
        data.binding != worker.binding
        or worker.binding.workspace_id != transaction.workspace_id
        or request.worker_id != worker.binding.worker_id
        or request.session_id != worker.binding.worker_session_id
        or request.attempt_id != attempt.dispatch.attempt_id
        or any(
            getattr(attempt.dispatch, name) != getattr(worker.binding, name)
            for name in WorkerBinding.model_fields
        )
        or data.attempt_id != request.attempt_id
        or data.upload_id != request.upload_id
        or upload.actor_id != worker.principal_id
    ):
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Upload belongs to another attempt authority"
        )
    return receipt, data, row, upload, attempt


def evidence_receipt(
    transaction: Transaction,
    intent: WorkerOutputIntent,
    reference: ArtifactRef,
) -> WorkerUploadData:
    """Require a completed transfer for this exact attempt, role, and canonical reference."""
    worker, attempt = historical_attempt_owner(
        transaction, intent.worker_id, intent.session_id, intent.attempt_id
    )
    return verify_evidence_receipt(transaction, intent, reference, worker, attempt)


def verify_evidence_receipt(
    transaction: Transaction,
    intent: WorkerOutputIntent,
    reference: ArtifactRef,
    worker: WorkerData,
    attempt: AttemptData,
) -> WorkerUploadData:
    """Original transfer ownership is independent of the reconciling operator."""
    row = transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='worker_upload' "
        "AND parent_id=? AND state='finalized' "
        "AND json_extract(payload,'$.role')=? "
        "AND json_extract(payload,'$.output_name') IS ? "
        "AND json_extract(payload,'$.artifact.id')=? LIMIT 1",
        (
            transaction.workspace_id,
            intent.attempt_id,
            intent.role,
            intent.output_name,
            reference.id,
        ),
    ).fetchone()
    if row is None:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Attempt upload receipt is absent")
    target = WorkerUploadTarget(
        worker_id=intent.worker_id,
        session_id=intent.session_id,
        attempt_id=intent.attempt_id,
        upload_id=row["id"],
    )
    _, data, _, _, _ = verify_upload(transaction, target, worker, attempt)
    if data.artifact != reference or data.retention_id is None:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Attempt upload receipt differs")
    return data


class WorkerOutputs:
    def __init__(
        self,
        database: Database,
        artifacts: ArtifactService,
        *,
        crypto_provider: Ed25519Provider | None = None,
    ) -> None:
        if database.authority_identity != artifacts.database.authority_identity:
            raise ValueError("Worker outputs must share artifact transaction authority")
        self.database, self.artifacts = database, artifacts
        self.crypto_provider = crypto_provider

    def _recovery_context(
        self,
        context: CommandContext,
        operation: str,
        request: WorkerUploadTarget,
        payload: JsonObject | None = None,
    ) -> CommandContext:
        context = recovery_context(
            self.database,
            context,
            operation,
            request.model_dump(mode="json") if payload is None else payload,
            attempt_id=request.attempt_id,
            upload_id=request.upload_id,
            crypto_provider=self.crypto_provider,
        )

        def read(connection: SqlSession) -> None:
            _, data, _, upload, _ = owned_upload(Transaction(connection, context), request)
            require_journal_upload(context, data.role, data.output_name, upload.size_bytes)

        self.database.store.read(read)
        return context

    def _validate_journal(
        self, context: CommandContext, request: WorkerUploadTarget, blob: StoredBlob
    ) -> JournalEntry | None:
        if context._recovery_operation is None:
            return None
        from research_swarm.workers.journal_records import JournalEntry

        with self.artifacts.store.reader(context.principal.workspace_id, blob) as stream:
            raw = stream.read(1024 * 1024 + 1)
        try:
            journal = JournalEntry.model_validate(strict_json_loads(raw))
        except ValueError as error:
            raise ResearchError(
                ErrorCode.ARTIFACT_UNVERIFIED, "Recovery journal is invalid"
            ) from error

        self.database.store.read(
            lambda connection: self._verify_journal_in(
                Transaction(connection, context), request, blob, journal
            )
        )
        return journal

    def _verify_journal_in(
        self,
        transaction: Transaction,
        request: WorkerUploadTarget,
        blob: StoredBlob,
        journal: JournalEntry,
    ) -> None:
        from research_swarm.control.observations import PublishAttempt, validate_native_grant

        worker, _ = historical_attempt_owner(
            transaction, request.worker_id, request.session_id, request.attempt_id
        )
        validate_native_grant(
            transaction,
            PublishAttempt(attempt_id=request.attempt_id, journal_artifact=blob.reference),
            journal,
            worker,
            crypto_provider=self.crypto_provider,
        )

    def _check_owner(self, context: CommandContext, request: WorkerUploadTarget) -> None:
        # Check immutable ownership before creating a lock for a supplied identifier.
        # Current enrollment and mutable state are checked again inside the guard.
        def read(connection: SqlSession) -> None:
            transaction = Transaction(connection, context)
            data = WorkerUploadData.model_validate(
                transaction.get("worker_upload", request.upload_id)["data"]
            )
            upload = UploadSpec.model_validate(transaction.get("upload", request.upload_id)["data"])
            if (
                upload.actor_id != context.principal.id
                or data.binding.worker_id != request.worker_id
                or data.binding.worker_session_id != request.session_id
                or data.attempt_id != request.attempt_id
            ):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Upload belongs to another authority"
                )

        self.database.store.read(read)

    def begin(self, context: CommandContext, request: BeginWorkerUpload) -> JsonObject:
        request = BeginWorkerUpload.model_validate(request)
        return self.database.with_preflight(
            context,
            "worker.begin_upload",
            request.model_dump(mode="json"),
            "worker.outputs",
            lambda: self._begin(context, request),
        )

    def _begin(self, context: CommandContext, request: BeginWorkerUpload) -> JsonObject:
        context = recovery_context(
            self.database,
            context,
            "worker.begin_upload",
            request.model_dump(mode="json"),
            attempt_id=request.attempt_id,
            crypto_provider=self.crypto_provider,
        )
        require_journal_upload(
            context, request.role, request.output_name, request.content.size_bytes
        )

        def begin(transaction: Transaction) -> JsonObject:
            worker, attempt = historical_attempt_owner(
                transaction, request.worker_id, request.session_id, request.attempt_id
            )
            require_pending(attempt)
            assert attempt.signed_assignment is not None
            stage = next(
                stage
                for stage in attempt.signed_assignment.payload.spec.stages
                if stage.id == attempt.dispatch.stage_id
            )
            if request.role == "output" and request.output_name not in stage.output_names:
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Output name is not declared")
            upload = self.artifacts.create_upload_in(transaction, request.content)
            receipt = transaction.insert(
                "worker_upload",
                upload["id"],
                WorkerUploadData(
                    binding=worker.binding,
                    attempt_id=request.attempt_id,
                    upload_id=upload["id"],
                    role=request.role,
                    output_name=request.output_name,
                ).model_dump(mode="json"),
                state="pending",
                campaign_id=attempt.dispatch.campaign_id,
                parent_id=request.attempt_id,
            )
            transaction.event("worker.upload_started", receipt, {"upload_id": upload["id"]})
            return upload

        return self.database.execute(
            context, "worker.begin_upload", request.model_dump(mode="json"), "worker.outputs", begin
        )

    def status(self, context: CommandContext, request: WorkerUploadTarget) -> JsonObject:
        context.principal.require("worker.outputs")
        request = WorkerUploadTarget.model_validate(request)
        self._check_owner(context, request)
        with self.artifacts.store.upload_guard(context.principal.workspace_id, request.upload_id):
            return self._status(context, request)

    def _status(self, context: CommandContext, request: WorkerUploadTarget) -> JsonObject:

        def read(connection: SqlSession) -> tuple[WorkerUploadData, JsonObject, UploadSpec]:
            _, data, row, upload, _ = owned_upload(Transaction(connection, context), request)
            return data, row, upload

        data, row, upload = self.database.store.read(read)
        if row["state"] == "pending":
            context = self._recovery_context(context, "worker.output", request)
            require_raw_operation(
                self.database, context, "worker.output", request.model_dump(mode="json")
            )
        progress = self.artifacts.store.progress(upload) if row["state"] == "pending" else None
        return {
            **row,
            "transfer": progress.model_dump(mode="json") if progress else None,
            "artifact": data.artifact.model_dump(mode="json") if data.artifact else None,
        }

    def append(
        self, context: CommandContext, request: WorkerUploadTarget, offset: int, content: bytes
    ) -> int:
        context.principal.require("worker.outputs")
        request = WorkerUploadTarget.model_validate(request)
        self._check_owner(context, request)
        with self.artifacts.store.upload_guard(context.principal.workspace_id, request.upload_id):
            payload = {
                **request.model_dump(mode="json"),
                "offset": offset,
                "content_digest": content_digest(content),
            }
            context = self._recovery_context(context, "worker.upload_part", request, payload)
            require_raw_operation(self.database, context, "worker.upload_part", payload)
            return self._append(context, request, offset, content)

    def _append(
        self, context: CommandContext, request: WorkerUploadTarget, offset: int, content: bytes
    ) -> int:
        context.principal.require("worker.outputs")
        request = WorkerUploadTarget.model_validate(request)

        def read(connection: SqlSession) -> UploadSpec:
            _, _, row, upload, attempt = owned_upload(Transaction(connection, context), request)
            require_pending(attempt)
            if row["state"] != "pending":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload is not pending")
            return upload

        upload = self.database.store.read(read)
        self.artifacts.store.begin(upload)
        return self.artifacts.store.append(upload, offset, content)

    def finalize(self, context: CommandContext, request: WorkerUploadTarget) -> JsonObject:
        request = WorkerUploadTarget.model_validate(request)
        previous = self.database.replay(
            context, "worker.finalize_upload", request.model_dump(mode="json"), "worker.outputs"
        )
        if previous is not None:
            return previous
        self._check_owner(context, request)
        with self.artifacts.store.upload_guard(context.principal.workspace_id, request.upload_id):
            previous = self.database.replay(
                context, "worker.finalize_upload", request.model_dump(mode="json"), "worker.outputs"
            )
            if previous is not None:
                return previous
            context = self._recovery_context(context, "worker.finalize_upload", request)
            return self._finalize(context, request)

    def _finalize(self, context: CommandContext, request: WorkerUploadTarget) -> JsonObject:
        context.principal.require("worker.outputs")
        request = WorkerUploadTarget.model_validate(request)
        payload = request.model_dump(mode="json")

        def read(connection: SqlSession) -> tuple[JsonObject, UploadSpec]:
            _, data, row, upload, attempt = owned_upload(Transaction(connection, context), request)
            if data.artifact is None:
                require_pending(attempt)
            if row["state"] not in {"pending", "finalized", "cleanup"}:
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload cannot be finalized")
            return row, upload

        row, upload = self.database.store.read(read)
        require_raw_operation(self.database, context, "worker.finalize_upload", payload)
        if row["state"] == "pending":
            self.artifacts.store.begin(upload)
            blob = self.artifacts.store.finalize(upload)
        else:
            blob = StoredBlob(reference=upload.reference(), generation=upload.id)
        journal = (
            self._validate_journal(context, request, blob) if row["state"] == "pending" else None
        )

        def finalize(transaction: Transaction) -> JsonObject:
            if journal is not None:
                self._verify_journal_in(transaction, request, blob, journal)
            receipt, data, _, _, attempt = owned_upload(transaction, request)
            if data.artifact is None:
                require_pending(attempt)
            result = self.artifacts.publish_upload_in(transaction, upload.id, blob)
            artifact = ArtifactRecord.model_validate(result["data"])
            if data.artifact is not None and data.artifact != artifact.reference:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Upload provenance changed")
            if data.artifact is None:
                retained = ArtifactCatalog.retain(
                    transaction,
                    ArtifactRetention(
                        artifact_id=artifact.reference.id,
                        owner_kind="worker_upload",
                        owner_id=upload.id,
                        purpose="unpublished_evidence",
                    ),
                )
                receipt = transaction.update(
                    "worker_upload",
                    upload.id,
                    expected_revision=receipt["revision"],
                    state="finalized",
                    data=data.model_copy(
                        update={
                            "artifact": artifact.reference,
                            "retention_id": retained["id"],
                        }
                    ).model_dump(mode="json"),
                )
                transaction.event(
                    "worker.upload_finalized", receipt, {"artifact_id": artifact.reference.id}
                )
            return {"upload_id": upload.id, "artifact": artifact.reference.model_dump(mode="json")}

        response = self.database.execute(
            context, "worker.finalize_upload", payload, "worker.outputs", finalize
        )
        # Publication is already committed. Cleanup remains quota-accounted and
        # explicitly recoverable; its failure cannot replace the accepted acknowledgment.
        try:
            self._cleanup(context, request, upload, blob)
        except Exception:
            logger.warning("Upload cleanup remains pending: %s", request.upload_id, exc_info=True)
        return response

    def cleanup(self, context: CommandContext, request: WorkerUploadTarget) -> JsonObject:
        request = WorkerUploadTarget.model_validate(request)
        payload = request.model_dump(mode="json")
        previous = self.database.replay(context, "worker.cleanup_upload", payload, "worker.outputs")
        if previous is not None:
            return previous
        self._check_owner(context, request)
        with self.artifacts.store.upload_guard(context.principal.workspace_id, request.upload_id):
            previous = self.database.replay(
                context, "worker.cleanup_upload", payload, "worker.outputs"
            )
            if previous is not None:
                return previous

            context = self._recovery_context(context, "worker.cleanup_upload", request)
            require_raw_operation(self.database, context, "worker.cleanup_upload", payload)

            def read(connection: SqlSession) -> tuple[JsonObject, UploadSpec]:
                _, _, row, upload, _ = owned_upload(Transaction(connection, context), request)
                return row, upload

            row, upload = self.database.store.read(read)
            if row["state"] not in {"cleanup", "finalized"}:
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload is not published")
            self._cleanup(
                context,
                request,
                upload,
                StoredBlob(reference=upload.reference(), generation=upload.id),
            )

            def acknowledge(transaction: Transaction) -> JsonObject:
                _, _, row, _, _ = owned_upload(transaction, request)
                if row["state"] != "finalized":
                    raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload cleanup changed")
                return {"upload_id": upload.id, "state": "finalized"}

            return self.database.execute(
                context, "worker.cleanup_upload", payload, "worker.outputs", acknowledge
            )

    def abort(self, context: CommandContext, request: WorkerUploadTarget) -> JsonObject:
        context.principal.require("worker.outputs")
        request = WorkerUploadTarget.model_validate(request)
        self._check_owner(context, request)

        def authorize(transaction: Transaction, row: JsonObject) -> UploadSpec:
            receipt, data, current, upload, _ = owned_upload(transaction, request)
            if (
                current != row
                or data.artifact is not None
                or data.retention_id is not None
                or receipt["state"] not in {"pending", "aborted"}
                or current["state"] not in {"pending", "aborting", "aborted"}
            ):
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Published worker output cannot be aborted"
                )
            return upload

        def finished(transaction: Transaction) -> None:
            receipt, _, _, _, _ = owned_upload(transaction, request)
            if receipt["state"] == "aborted":
                return
            updated = transaction.update(
                "worker_upload",
                request.upload_id,
                expected_revision=receipt["revision"],
                state="aborted",
                data=receipt["data"],
            )
            transaction.event("worker.upload_aborted", updated, {"upload_id": request.upload_id})

        with self.artifacts.store.upload_guard(context.principal.workspace_id, request.upload_id):
            previous = self.database.replay(
                context, "worker.abort_upload", request.model_dump(mode="json"), "worker.outputs"
            )
            if previous is not None:

                def completed(connection: SqlSession) -> bool:
                    transaction = Transaction(connection, context)
                    return bool(
                        transaction.get("upload", request.upload_id)["state"] == "aborted"
                        and transaction.get("worker_upload", request.upload_id)["state"]
                        == "aborted"
                    )

                if self.database.store.read(completed):
                    return previous
            context = self._recovery_context(context, "worker.abort_upload", request)
            require_raw_operation(
                self.database, context, "worker.abort_upload", request.model_dump(mode="json")
            )
            return self.artifacts.abort_authorized(
                context,
                request.upload_id,
                scope="worker.outputs",
                operation="worker.abort_upload",
                payload=request.model_dump(mode="json"),
                authorize=authorize,
                completion_operation="worker.finish_upload_abort",
                completion_key="worker-upload-aborted:" + request.upload_id,
                on_finish=finished,
            )

    def _cleanup(
        self,
        context: CommandContext,
        request: WorkerUploadTarget,
        upload: UploadSpec,
        blob: StoredBlob,
    ) -> None:
        def read(connection: SqlSession) -> tuple[WorkerUploadData, JsonObject]:
            _, data, row, _, _ = owned_upload(Transaction(connection, context), request)
            return data, row

        data, row = self.database.store.read(read)
        if row["state"] != "cleanup":
            return
        assert data.artifact is not None

        def authorize(transaction: Transaction, reference: ArtifactRef) -> None:
            _, data, _, _, _ = owned_upload(transaction, request)
            if data.artifact != reference:
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Canonical upload reference differs"
                )

        with self.artifacts.pinned_authorized(
            context, data.artifact, scope="worker.outputs", authorize=authorize
        ):
            self.artifacts.store.delete(upload.workspace_id, blob)

            def finished(transaction: Transaction) -> JsonObject:
                _, _, row, _, _ = owned_upload(transaction, request)
                if row["state"] == "finalized":
                    return row
                if row["state"] != "cleanup":
                    raise ResearchError(
                        ErrorCode.INVALID_TRANSITION, "Upload cleanup state changed"
                    )
                result = transaction.update(
                    "upload",
                    upload.id,
                    expected_revision=row["revision"],
                    data=row["data"],
                    state="finalized",
                )
                transaction.event("artifact.upload_finalized", result, {})
                return result

            self.database.execute(
                artifact_context(
                    context,
                    "worker-cleanup:" + upload.id,
                    "worker.finish_upload_cleanup",
                    {"upload_id": upload.id},
                ),
                "worker.finish_upload_cleanup",
                {"upload_id": upload.id},
                "worker.outputs",
                finished,
            )
