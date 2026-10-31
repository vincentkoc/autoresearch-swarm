"""Scoped buffered outputs with explicit retirement and shared provenance transactions."""

import asyncio

from research_swarm.control.recovery_operations import require_raw_operation
from research_swarm.control.worker_outputs import (
    BeginWorkerUpload,
    WorkerUploadTarget,
    owned_upload,
    require_pending,
)
from research_swarm.control.worker_sessions import historical_attempt_owner
from research_swarm.domain.artifacts import StoredBlob, UploadProgress
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction
from research_swarm.state.drive_guard import require_storage_authority

if __package__:
    from .research_router import supports_outputs, validated
else:
    from research_router import supports_outputs, validated


COMMANDS = frozenset(
    {
        "worker.begin_upload",
        "worker.finalize_upload",
        "worker.abort_upload",
        "worker.cleanup_upload",
    }
)


class HostedWorkerOutputs:
    commands = COMMANDS

    def __init__(self, router):
        self.router = router
        self.database = router.database
        self.service = router.service.worker_outputs

    def _identity(self, token, context):
        if not supports_outputs(self.router.configuration):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Scoped output transfer is unavailable"
            )
        current = self.router._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Output credential changed")
        context.principal.require("worker.outputs")
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Output transfer owner was cancelled")

    def _select(self, context, request, operation, payload):
        # Hosted restoration has no admitted native recovery proof. This gate
        # refuses inactive workspaces before any storage access, including reads.
        require_raw_operation(self.database, context, operation, payload)

        def select(sql):
            transaction = Transaction(sql, context)
            selected = owned_upload(transaction, request)
            require_storage_authority(transaction, request.upload_id)
            return selected

        return self.database.store.read(select)

    def _unchanged(self, token, context, request, operation, payload, selected, catalog):
        self._identity(token, context)
        if self._select(context, request, operation, payload) != selected:
            raise ResearchError(ErrorCode.STALE_FENCE, "Output authority changed during I/O")
        if self.router.artifacts._catalog(context, selected[3]) != catalog:
            raise ResearchError(ErrorCode.STALE_FENCE, "Canonical output changed during I/O")

    async def command(self, token, context, operation, payload):
        self._identity(token, context)
        if operation not in COMMANDS:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Output command is unavailable")
        request = validated(
            BeginWorkerUpload if operation == "worker.begin_upload" else WorkerUploadTarget,
            payload,
        )
        normalized = request.model_dump(mode="json")
        previous = self.database.replay(context, operation, normalized, "worker.outputs")
        if previous is not None:
            return previous
        require_raw_operation(self.database, context, operation, normalized)
        if operation == "worker.begin_upload":

            def preflight(sql):
                _, attempt = historical_attempt_owner(
                    Transaction(sql, context),
                    request.worker_id,
                    request.session_id,
                    request.attempt_id,
                )
                require_pending(attempt)

            self.database.store.read(preflight)

            def callback(transaction):
                return self.service._begin_in(transaction, request)
        else:
            selected = self._select(context, request, operation, normalized)
            _, data, row, upload, attempt = selected
            own = StoredBlob(reference=upload.reference(), generation=upload.id)
            catalog = self.router.artifacts._catalog(context, upload)
            if operation == "worker.finalize_upload":
                if data.artifact is None:
                    require_pending(attempt)
                if row["state"] == "pending":
                    if upload.size_bytes == 0:
                        await self.router.objects.publish(upload, b"")
                    else:
                        await self.router.objects.read(upload.workspace_id, own)
                    if catalog is not None and catalog["state"] == "verified":
                        _, canonical = self.router.artifacts._selected(context, upload)
                        if canonical.generation != own.generation:
                            await self.router.objects.read(upload.workspace_id, canonical)
                elif row["state"] in {"cleanup", "finalized"}:
                    _, canonical = self.router.artifacts._selected(context, upload)
                    await self.router.objects.read(upload.workspace_id, canonical)
                else:
                    raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload was aborted")
            elif operation == "worker.abort_upload":
                self.database.store.read(
                    lambda sql: self.service._authorize_abort_in(
                        Transaction(sql, context), request, row
                    )
                )
            elif row["state"] == "cleanup":
                _, canonical = self.router.artifacts._selected(context, upload)
                if data.artifact != canonical.reference or canonical.generation == own.generation:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Canonical output differs")
                await self.router.objects.read(upload.workspace_id, canonical)
                self._unchanged(token, context, request, operation, normalized, selected, catalog)
                await self.router.objects.retire(upload.workspace_id, own)
            elif row["state"] in {"aborting", "aborted"}:
                self.database.store.read(
                    lambda sql: self.service._authorize_abort_in(
                        Transaction(sql, context), request, row
                    )
                )
                if catalog is not None and catalog["data"]["storage_generation"] == own.generation:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Cannot retire canonical bytes")
                if row["state"] == "aborting":
                    await self.router.objects.retire(upload.workspace_id, own)
            elif row["state"] != "finalized":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload has no cleanup intent")

            self._unchanged(token, context, request, operation, normalized, selected, catalog)

            def callback(transaction):
                if owned_upload(transaction, request) != selected:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Output authority changed")
                require_storage_authority(transaction, upload.id)
                if self.router.artifacts._catalog_in(transaction, upload) != catalog:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Canonical output changed")
                if operation == "worker.finalize_upload":
                    return self.service._finalize_in(transaction, request, own)
                if operation == "worker.abort_upload":
                    self.service._authorize_abort_in(transaction, request, row)
                    return self.router.service.artifacts.abort_upload_in(transaction, upload.id)
                if row["state"] == "cleanup":
                    self.router.service.artifacts.finish_upload_in(
                        transaction, upload.id, "cleanup", "finalized"
                    )
                elif row["state"] in {"aborting", "aborted"}:
                    self.service._finish_abort_in(transaction, request)
                state = "aborted" if row["state"] in {"aborting", "aborted"} else "finalized"
                return {"upload_id": upload.id, "state": state}

        def commit():
            self._identity(token, context)
            return self.database.execute(context, operation, normalized, "worker.outputs", callback)

        return await self.router._witnessed(context, operation, normalized, commit)

    async def upload_part(self, token, context, payload, offset, content):
        self._identity(token, context)
        request = validated(WorkerUploadTarget, payload)
        if type(offset) is not int or offset != 0:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Bounded outputs require offset zero"
            )
        normalized = {
            **request.model_dump(mode="json"),
            "offset": offset,
            "content_digest": content_digest(content),
        }
        operation = "worker.upload_part"
        selected = self._select(context, request, operation, normalized)
        _, _, row, upload, attempt = selected
        require_pending(attempt)
        if row["state"] != "pending":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload is not pending")
        catalog = self.router.artifacts._catalog(context, upload)
        await self.router.objects.publish(upload, content)
        self._unchanged(token, context, request, operation, normalized, selected, catalog)
        return len(content)

    async def status(self, token, context, payload):
        self._identity(token, context)
        request = validated(WorkerUploadTarget, payload)
        normalized = request.model_dump(mode="json")
        selected = self._select(context, request, "worker.output", normalized)
        _, data, row, upload, _ = selected
        catalog = self.router.artifacts._catalog(context, upload)
        progress = None
        if row["state"] == "pending":
            body = await self.router.objects.read_if_present(
                upload.workspace_id, StoredBlob(reference=upload.reference(), generation=upload.id)
            )
            progress = UploadProgress(
                received_bytes=0 if body is None else len(body), published=body is not None
            )
        self._unchanged(token, context, request, "worker.output", normalized, selected, catalog)
        return {
            **row,
            "transfer": progress.model_dump(mode="json") if progress else None,
            "artifact": data.artifact.model_dump(mode="json") if data.artifact else None,
        }
