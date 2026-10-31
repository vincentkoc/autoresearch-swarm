"""Bounded upload lifecycle; every entry runs inside the authority's exclusive gate.

R2 I/O finishes before a fresh witness and synchronous ledger commit. Cleanup is
an explicit command so replay never depends on bytes or consumes a second witness.
"""

import asyncio

from pydantic import ValidationError

from research_swarm.domain.artifacts import ArtifactRecord, StoredBlob, UploadProgress
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.state.artifacts import BeginUpload
from research_swarm.state.database import Transaction
from research_swarm.state.drive_guard import require_storage_authority
from research_swarm.state.workspace_gate import require_workspace_mutation

if __package__:
    from .research_router import supports_artifacts
else:
    from research_router import supports_artifacts


class UploadTarget(VersionedRecord):
    upload_id: ID


class ArtifactTarget(VersionedRecord):
    id: ID


COMMANDS = frozenset(
    {
        "artifact.begin_upload",
        "artifact.finalize_upload",
        "artifact.abort_upload",
        "artifact.cleanup_upload",
    }
)


def validated(model, payload):
    try:
        return model.model_validate(payload)
    except ValidationError as error:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid artifact request"
        ) from error


class HostedArtifacts:
    def __init__(self, router):
        self.router = router
        self.database, self.service = router.database, router.service.artifacts

    def _identity(self, token, context):
        if not supports_artifacts(self.router.configuration):
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Artifact profile is unavailable")
        current = self.router._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Artifact credential changed")
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Artifact operation owner was cancelled")

    def _admit(self, token, context):
        self._identity(token, context)
        context.principal.require("artifact.upload")
        context.principal.require("read")

    def metadata(self, token, context, payload):
        self._identity(token, context)
        context.principal.require("read")
        request = validated(ArtifactTarget, payload)
        return self.service.get(context, request.id).model_dump(mode="json")

    def _readable(self, token, context, artifact_id):
        self._identity(token, context)
        context.principal.require("read")

        def read(sql):
            require_workspace_mutation(sql, context.principal.workspace_id, "artifact.read")
            row = Transaction(sql, context).get("artifact", artifact_id)
            record = ArtifactRecord.model_validate(row["data"])
            if (
                row["state"] != "verified"
                or record.state != "verified"
                or record.workspace_id != context.principal.workspace_id
                or record.reference.id != artifact_id
            ):
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Artifact is unavailable")
            return row, record

        return self.database.store.read(read)

    async def content(self, token, context, payload):
        request = validated(ArtifactTarget, payload)
        before, record = self._readable(token, context, request.id)
        body = await self.router.objects.read(record.workspace_id, self.service._blob(record))
        after, _ = self._readable(token, context, request.id)
        if before != after:
            raise ResearchError(ErrorCode.STALE_FENCE, "Artifact changed during download")
        # All storage I/O has finished while holding the gate. Bytes are immutable;
        # no reader or generation pin escapes into HTTP response delivery.
        return record.reference, body

    def _upload_in(self, transaction, upload_id, *, expected=None):
        row = transaction.get("upload", upload_id)
        upload = self.service._upload_owner(transaction.context, row)
        require_workspace_mutation(
            transaction.connection, transaction.workspace_id, "artifact.storage"
        )
        require_storage_authority(transaction, upload_id)
        if expected is not None and row != expected:
            raise ResearchError(ErrorCode.STALE_FENCE, "Upload changed during storage I/O")
        return row, upload

    def _upload(self, context, upload_id, *, expected=None):
        return self.database.store.read(
            lambda sql: self._upload_in(Transaction(sql, context), upload_id, expected=expected)
        )

    @staticmethod
    def _catalog_in(transaction, upload):
        found = transaction.connection.execute(
            "SELECT id FROM records WHERE workspace_id=? AND kind='artifact' AND id=?",
            (upload.workspace_id, upload.reference().id),
        ).fetchone()
        return transaction.get("artifact", found["id"]) if found else None

    def _catalog(self, context, upload):
        return self.database.store.read(
            lambda sql: self._catalog_in(Transaction(sql, context), upload)
        )

    def _selected(self, context, upload):
        row = self._catalog(context, upload)
        if row is None or row["state"] != "verified":
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Published artifact is unavailable")
        record = ArtifactRecord.model_validate(row["data"])
        if (
            record.state != "verified"
            or record.workspace_id != upload.workspace_id
            or record.reference.id != upload.reference().id
            or record.reference.digest != upload.digest
            or record.reference.size_bytes != upload.size_bytes
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Published artifact differs")
        return row, self.service._blob(record)

    def _commit(self, token, context, operation, payload, callback):
        self._admit(token, context)
        return self.database.execute(context, operation, payload, "artifact.upload", callback)

    def _unchanged(self, token, context, row, upload, catalog, *, track_catalog):
        self._admit(token, context)
        self._upload(context, upload.id, expected=row)
        if track_catalog and self._catalog(context, upload) != catalog:
            raise ResearchError(ErrorCode.STALE_FENCE, "Canonical artifact changed")

    async def command(self, token, context, operation, payload):
        self._admit(token, context)
        if operation not in COMMANDS:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Artifact command is unavailable")
        request = validated(
            BeginUpload if operation == "artifact.begin_upload" else UploadTarget, payload
        )
        normalized = (
            request.model_dump(mode="json")
            if operation == "artifact.begin_upload"
            else {"upload_id": request.upload_id}
        )
        previous = self.database.replay(context, operation, normalized, "artifact.upload")
        if previous is not None:
            return previous
        if operation == "artifact.begin_upload":

            def callback(transaction):
                return self.service.create_upload_in(transaction, request)
        else:
            row, upload = self._upload(context, request.upload_id)
            own = StoredBlob(reference=upload.reference(), generation=upload.id)
            catalog = None
            track_catalog = operation in {"artifact.finalize_upload", "artifact.cleanup_upload"}
            if operation == "artifact.finalize_upload":
                catalog = self._catalog(context, upload)
                if row["state"] == "pending":
                    if upload.size_bytes == 0:
                        # The ordinary client skips parts for an empty file.
                        await self.router.objects.publish(upload, b"")
                    else:
                        await self.router.objects.read(upload.workspace_id, own)
                    if catalog is not None and catalog["state"] == "verified":
                        selected_row, selected = self._selected(context, upload)
                        if selected_row != catalog:
                            raise ResearchError(ErrorCode.STALE_FENCE, "Canonical artifact changed")
                        if selected.generation != own.generation:
                            await self.router.objects.read(upload.workspace_id, selected)
                elif row["state"] in {"cleanup", "finalized"}:
                    catalog, selected = self._selected(context, upload)
                    await self.router.objects.read(upload.workspace_id, selected)
                else:
                    raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload was aborted")
            elif operation == "artifact.cleanup_upload":
                catalog = self._catalog(context, upload)
                if row["state"] == "cleanup":
                    catalog, selected = self._selected(context, upload)
                    if selected.generation == own.generation:
                        raise ResearchError(ErrorCode.STALE_FENCE, "Cannot retire canonical bytes")
                    await self.router.objects.read(upload.workspace_id, selected)
                    self._unchanged(token, context, row, upload, catalog, track_catalog=True)
                if row["state"] in {"cleanup", "aborting"}:
                    if (
                        catalog is not None
                        and catalog["data"]["storage_generation"] == own.generation
                    ):
                        raise ResearchError(ErrorCode.STALE_FENCE, "Cannot retire canonical bytes")
                    await self.router.objects.retire(upload.workspace_id, own)
                elif row["state"] not in {"finalized", "aborted"}:
                    raise ResearchError(
                        ErrorCode.INVALID_TRANSITION, "Upload has no cleanup intent"
                    )

            self._unchanged(token, context, row, upload, catalog, track_catalog=track_catalog)

            def callback(transaction):
                self._upload_in(transaction, upload.id, expected=row)
                if track_catalog and self._catalog_in(transaction, upload) != catalog:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Canonical artifact changed")
                if operation == "artifact.finalize_upload":
                    return self.service.publish_upload_in(transaction, upload.id, own)
                if operation == "artifact.abort_upload":
                    return self.service.abort_upload_in(transaction, upload.id)
                after = {"cleanup": "finalized", "aborting": "aborted"}.get(row["state"])
                if after is None:
                    return transaction.get("upload", upload.id)
                return self.service.finish_upload_in(transaction, upload.id, row["state"], after)

        return await self.router._witnessed(
            context,
            operation,
            normalized,
            lambda: self._commit(token, context, operation, normalized, callback),
        )

    async def upload_part(self, token, context, upload_id, offset, content):
        self._admit(token, context)
        target = validated(UploadTarget, {"upload_id": upload_id})
        if type(offset) is not int or offset != 0:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Bounded uploads require offset zero"
            )
        row, upload = self._upload(context, target.upload_id)
        if row["state"] != "pending":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload is not pending")
        await self.router.objects.publish(upload, content)
        self._admit(token, context)
        self._upload(context, upload.id, expected=row)
        return len(content)

    async def status(self, token, context, payload):
        self._admit(token, context)
        target = validated(UploadTarget, payload)
        row, upload = self._upload(context, target.upload_id)
        progress = None
        if row["state"] == "pending":
            blob = StoredBlob(reference=upload.reference(), generation=upload.id)
            body = await self.router.objects.read_if_present(upload.workspace_id, blob)
            progress = UploadProgress(
                received_bytes=0 if body is None else len(body), published=body is not None
            )
            self._admit(token, context)
            self._upload(context, upload.id, expected=row)
        return {**row, "transfer": progress.model_dump(mode="json") if progress else None}
