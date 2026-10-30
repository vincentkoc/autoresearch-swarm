"""Workspace artifact authority and crash-recoverable storage lifecycle.

Byte transfers and hashing never run inside a SQLite write transaction. Immutable
storage generations fence stale cleanup without conflating a content hash with
the lifetime of a physical object.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from pydantic import Field

from research_swarm.domain.artifacts import (
    ArtifactRecord,
    ArtifactRetention,
    StoredBlob,
    UploadSpec,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ByteCount, Digest, VersionedRecord, new_id, utc_now
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.process_identity import LocalProcessIdentity
from research_swarm.storage.base import BlobStore


class BeginUpload(VersionedRecord):
    digest: Digest
    size_bytes: ByteCount
    media_type: str = Field(default="application/octet-stream", min_length=1, max_length=128)


class ArtifactCatalog:
    """Use these operations in the same transaction that accepts scientific evidence."""

    @staticmethod
    def available(transaction: Transaction, reference: ArtifactRef) -> ArtifactRecord:
        reference = ArtifactRef.model_validate(reference)
        row = transaction.get("artifact", reference.id)
        record = ArtifactRecord.model_validate(row["data"])
        if record.state != "verified" or record.reference != reference:
            raise ResearchError(
                ErrorCode.ARTIFACT_UNVERIFIED, "Artifact is unavailable or mismatched"
            )
        return record

    @classmethod
    def retain(
        cls,
        transaction: Transaction,
        retention: ArtifactRetention,
        *,
        reader_session_id: str | None = None,
    ) -> JsonObject:
        retention = ArtifactRetention.model_validate(retention)
        row = transaction.get("artifact", retention.artifact_id)
        reference = ArtifactRecord.model_validate(row["data"]).reference
        cls.available(transaction, reference)
        existing = transaction.connection.execute(
            "SELECT id FROM records WHERE workspace_id=? AND kind='artifact_reference' "
            "AND parent_id=? AND state='retained'",
            (transaction.workspace_id, retention.artifact_id),
        ).fetchall()
        for item in existing:
            previous = transaction.get("artifact_reference", item["id"])
            data = previous["data"]
            if data["retention"] == retention.model_dump(mode="json"):
                return previous
        reference_row = transaction.insert(
            "artifact_reference",
            new_id(),
            {
                "retention": retention.model_dump(mode="json"),
                "actor_id": transaction.context.principal.id,
                "reader_session_id": reader_session_id,
            },
            state="retained",
            parent_id=retention.artifact_id,
        )
        transaction.event(
            "artifact.retained", reference_row, {"artifact_id": retention.artifact_id}
        )
        return reference_row

    @staticmethod
    def release(
        transaction: Transaction,
        reference_id: str,
        *,
        owner_release: tuple[str, str] | None = None,
    ) -> JsonObject:
        row = transaction.get("artifact_reference", reference_id)
        principal = transaction.context.principal
        if row["data"]["actor_id"] != principal.id:
            principal.require("artifact.admin")
        if row["state"] == "released":
            return row
        retention = ArtifactRetention.model_validate(row["data"]["retention"])
        if retention.required and owner_release != (retention.owner_kind, retention.owner_id):
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                "Required evidence must be released by its owner's retention lifecycle",
            )
        updated = transaction.update(
            "artifact_reference",
            reference_id,
            expected_revision=row["revision"],
            data=row["data"],
            state="released",
        )
        transaction.event("artifact.released", updated, {})
        return updated


class ArtifactService:
    def __init__(
        self,
        database: Database,
        store: BlobStore,
        *,
        storage_limit_bytes: int = 1024**4,
    ) -> None:
        if type(storage_limit_bytes) is not int or storage_limit_bytes < 0:
            raise ValueError("Storage limit must be a nonnegative integer")
        self.database = database
        self.store = store
        self.storage_limit_bytes = storage_limit_bytes
        self.reader_session_id = new_id()
        self.process_identity = LocalProcessIdentity.current()

    @staticmethod
    def _upload_owner(context: CommandContext, row: JsonObject) -> UploadSpec:
        upload = UploadSpec.model_validate(row["data"])
        if upload.actor_id != context.principal.id:
            context.principal.require("artifact.admin")
        return upload

    @staticmethod
    def _internal(context: CommandContext, key: str) -> CommandContext:
        return CommandContext(principal=context.principal, idempotency_key=key)

    @staticmethod
    def _blob(record: ArtifactRecord) -> StoredBlob:
        return StoredBlob(reference=record.reference, generation=record.storage_generation)

    def begin_upload(self, context: CommandContext, request: BeginUpload) -> JsonObject:
        request = BeginUpload.model_validate(request)

        def create(transaction: Transaction) -> JsonObject:
            rows = transaction.connection.execute(
                "SELECT kind,id FROM records WHERE workspace_id=? AND "
                "((kind='upload' AND state IN ('pending','cleanup','aborting')) OR "
                "(kind='artifact' AND state IN ('verified','deleting')))",
                (transaction.workspace_id,),
            ).fetchall()
            occupied = 0
            for item in rows:
                data = transaction.get(item["kind"], item["id"])["data"]
                occupied += (
                    data["size_bytes"]
                    if item["kind"] == "upload"
                    else data["reference"]["size_bytes"]
                )
            if occupied + request.size_bytes > self.storage_limit_bytes:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    "Artifact storage capacity is exhausted",
                    details={"allocated_bytes": occupied, "limit_bytes": self.storage_limit_bytes},
                )
            upload = UploadSpec(
                id=new_id(),
                workspace_id=transaction.workspace_id,
                actor_id=context.principal.id,
                **request.model_dump(exclude={"schema_version"}),
            )
            row = transaction.insert(
                "upload", upload.id, upload.model_dump(mode="json"), state="pending"
            )
            transaction.event("artifact.upload_started", row, {"size_bytes": upload.size_bytes})
            return row

        response = self.database.execute(
            context,
            "artifact.begin_upload",
            request.model_dump(mode="json"),
            "artifact.upload",
            create,
        )
        row = self.database.read_record(context, "upload", response["result"]["id"])
        if row["state"] == "pending":
            self.store.begin(self._upload_owner(context, row))
        return response

    def upload_part(
        self, context: CommandContext, upload_id: str, offset: int, content: bytes
    ) -> int:
        context.principal.require("artifact.upload")
        row = self.database.read_record(context, "upload", upload_id)
        upload = self._upload_owner(context, row)
        if row["state"] != "pending":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload is not pending")
        return self.store.append(upload, offset, content)

    def finalize_upload(self, context: CommandContext, upload_id: str) -> JsonObject:
        context.principal.require("artifact.upload")
        row = self.database.read_record(context, "upload", upload_id)
        upload = self._upload_owner(context, row)
        if row["state"] in {"aborted", "aborting"}:
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload was aborted")
        if row["state"] == "pending":
            blob = self.store.finalize(upload)
        else:
            blob = StoredBlob(reference=upload.reference(), generation=upload.id)

        def publish(transaction: Transaction) -> JsonObject:
            current = transaction.get("upload", upload_id)
            if current["state"] not in {"pending", "cleanup", "finalized"}:
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload cannot be published")
            existing = transaction.connection.execute(
                "SELECT id FROM records WHERE workspace_id=? AND kind='artifact' AND id=?",
                (transaction.workspace_id, blob.reference.id),
            ).fetchone()
            if existing is not None:
                artifact_row = transaction.get("artifact", blob.reference.id)
                artifact = ArtifactRecord.model_validate(artifact_row["data"])
                if artifact.state == "deleting":
                    raise ResearchError(
                        ErrorCode.RESOURCE_UNAVAILABLE, "Artifact deletion is incomplete"
                    )
                if artifact.state == "deleted":
                    if current["state"] != "pending":
                        raise ResearchError(
                            ErrorCode.ARTIFACT_UNVERIFIED, "Published artifact was deleted"
                        )
                    artifact = ArtifactRecord(
                        reference=blob.reference,
                        storage_generation=blob.generation,
                        workspace_id=transaction.workspace_id,
                        published_by=context.principal.id,
                        verified_at=utc_now(),
                    )
                    artifact_row = transaction.update(
                        "artifact",
                        artifact.reference.id,
                        expected_revision=artifact_row["revision"],
                        data=artifact.model_dump(mode="json"),
                        state="verified",
                    )
            else:
                artifact = ArtifactRecord(
                    reference=blob.reference,
                    storage_generation=blob.generation,
                    workspace_id=transaction.workspace_id,
                    published_by=context.principal.id,
                    verified_at=utc_now(),
                )
                artifact_row = transaction.insert(
                    "artifact",
                    artifact.reference.id,
                    artifact.model_dump(mode="json"),
                    state="verified",
                )
            if current["state"] == "pending":
                state = "finalized" if artifact.storage_generation == blob.generation else "cleanup"
                current = transaction.update(
                    "upload",
                    upload_id,
                    expected_revision=current["revision"],
                    data=current["data"],
                    state=state,
                )
                transaction.event("artifact.verified", artifact_row, {"upload_id": upload_id})
            return artifact_row

        response = self.database.execute(
            context,
            "artifact.finalize_upload",
            {"upload_id": upload_id},
            "artifact.upload",
            publish,
        )
        current = self.database.read_record(context, "upload", upload_id)
        if current["state"] == "cleanup":
            selected = ArtifactRecord.model_validate(response["result"]["data"])
            with self.pinned(context, selected.reference):
                self.store.delete(upload.workspace_id, blob)
                self._finish_upload(context, upload_id, "cleanup", "finalized")
        return response

    def _finish_upload(
        self, context: CommandContext, upload_id: str, before: str, after: str
    ) -> None:
        def finish(transaction: Transaction) -> JsonObject:
            row = transaction.get("upload", upload_id)
            if row["state"] != before:
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload lifecycle changed")
            updated = transaction.update(
                "upload",
                upload_id,
                expected_revision=row["revision"],
                data=row["data"],
                state=after,
            )
            transaction.event("artifact.upload_" + after, updated, {})
            return updated

        self.database.execute(
            self._internal(context, f"upload-{after}:{upload_id}"),
            "artifact.finish_upload",
            {"upload_id": upload_id, "state": after},
            "artifact.upload",
            finish,
        )

    def abort_upload(self, context: CommandContext, upload_id: str) -> JsonObject:
        def abort(transaction: Transaction) -> JsonObject:
            row = transaction.get("upload", upload_id)
            self._upload_owner(context, row)
            if row["state"] not in {"pending", "cleanup", "aborting", "aborted"}:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Published upload cannot be aborted"
                )
            if row["state"] in {"pending", "cleanup"}:
                row = transaction.update(
                    "upload",
                    upload_id,
                    expected_revision=row["revision"],
                    data=row["data"],
                    state="aborting",
                )
                transaction.event("artifact.upload_aborting", row, {})
            return row

        response = self.database.execute(
            context,
            "artifact.abort_upload",
            {"upload_id": upload_id},
            "artifact.upload",
            abort,
        )
        row = self.database.read_record(context, "upload", upload_id)
        if row["state"] == "aborting":
            upload = self._upload_owner(context, row)
            self.store.discard_upload(upload)
            self.store.delete(
                upload.workspace_id, StoredBlob(reference=upload.reference(), generation=upload.id)
            )
            self._finish_upload(context, upload_id, "aborting", "aborted")
        return response

    def get(self, context: CommandContext, artifact_id: str) -> ArtifactRecord:
        row = self.database.read_record(context, "artifact", artifact_id)
        return ArtifactRecord.model_validate(row["data"])

    def retain(self, context: CommandContext, retention: ArtifactRetention) -> JsonObject:
        if retention.owner_kind.startswith("artifact_"):
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Internal artifact owners are reserved"
            )
        return self.database.execute(
            context,
            "artifact.retain",
            retention.model_dump(mode="json"),
            "artifact.retain",
            lambda transaction: ArtifactCatalog.retain(transaction, retention),
        )

    def release(self, context: CommandContext, reference_id: str) -> JsonObject:
        return self.database.execute(
            context,
            "artifact.release",
            {"reference_id": reference_id},
            "artifact.release",
            lambda transaction: ArtifactCatalog.release(transaction, reference_id),
        )

    @contextmanager
    def pinned(self, context: CommandContext, reference: ArtifactRef) -> Iterator[ArtifactRecord]:
        """Hold bytes through verification and a subsequent evidence-acceptance transaction."""
        context.principal.require("read")
        pin_id = new_id()

        def pin(transaction: Transaction) -> JsonObject:
            ArtifactCatalog.available(transaction, reference)
            session = transaction.connection.execute(
                "SELECT state FROM records WHERE workspace_id=? "
                "AND kind='artifact_read_session' AND id=?",
                (transaction.workspace_id, self.reader_session_id),
            ).fetchone()
            if session is None:
                transaction.insert(
                    "artifact_read_session",
                    self.reader_session_id,
                    self.process_identity.model_dump(mode="json"),
                    state="active",
                )
            elif session["state"] != "active":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Reader session is closed")
            return ArtifactCatalog.retain(
                transaction,
                ArtifactRetention(
                    artifact_id=reference.id,
                    owner_kind="artifact_read",
                    owner_id=pin_id,
                    purpose="read",
                ),
                reader_session_id=self.reader_session_id,
            )

        response = self.database.execute(
            self._internal(context, "pin:" + pin_id),
            "artifact.pin",
            {"reference": reference.model_dump(mode="json")},
            "read",
            pin,
        )
        try:
            record = self.get(context, reference.id)
            self.store.verify(context.principal.workspace_id, self._blob(record))
            yield record
        finally:
            self.database.execute(
                self._internal(context, "unpin:" + pin_id),
                "artifact.unpin",
                {"reference_id": response["result"]["id"]},
                "read",
                lambda transaction: ArtifactCatalog.release(
                    transaction,
                    response["result"]["id"],
                    owner_release=("artifact_read", pin_id),
                ),
            )

    def reconcile_reader_session(self, context: CommandContext, session_id: str) -> JsonObject:
        context.principal.require("artifact.admin")
        session = self.database.read_record(context, "artifact_read_session", session_id)
        LocalProcessIdentity.model_validate(session["data"]).require_closed()

        def reconcile(transaction: Transaction) -> JsonObject:
            row = transaction.get("artifact_read_session", session_id)
            if row["state"] == "closed":
                return row
            refs = transaction.connection.execute(
                "SELECT id FROM records WHERE workspace_id=? "
                "AND kind='artifact_reference' AND state='retained'",
                (transaction.workspace_id,),
            ).fetchall()
            for item in refs:
                reference = transaction.get("artifact_reference", item["id"])
                if reference["data"].get("reader_session_id") == session_id:
                    retention = ArtifactRetention.model_validate(reference["data"]["retention"])
                    ArtifactCatalog.release(
                        transaction,
                        item["id"],
                        owner_release=(retention.owner_kind, retention.owner_id),
                    )
            updated = transaction.update(
                "artifact_read_session",
                session_id,
                expected_revision=row["revision"],
                data=row["data"],
                state="closed",
            )
            transaction.event("artifact.reader_reconciled", updated, {})
            return updated

        return self.database.execute(
            context,
            "artifact.reconcile_reader_session",
            {"session_id": session_id},
            "artifact.admin",
            reconcile,
        )

    def fetch(self, context: CommandContext, reference: ArtifactRef, destination: Path) -> None:
        with self.pinned(context, reference) as record:
            self.store.fetch(context.principal.workspace_id, self._blob(record), destination)

    def delete(self, context: CommandContext, artifact_id: str, reason: str) -> JsonObject:
        if context.expected_revision is None or not reason.strip():
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Deletion needs a revision and reason"
            )

        def mark(transaction: Transaction) -> JsonObject:
            row = transaction.get("artifact", artifact_id)
            if row["revision"] != context.expected_revision:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Artifact revision changed")
            record = ArtifactRecord.model_validate(row["data"])
            if record.state != "verified":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Artifact is not verified")
            retained = transaction.connection.execute(
                "SELECT 1 FROM records WHERE workspace_id=? AND kind='artifact_reference' "
                "AND parent_id=? AND state='retained' LIMIT 1",
                (transaction.workspace_id, artifact_id),
            ).fetchone()
            if retained is not None:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Artifact still has retained references"
                )
            record = record.model_copy(
                update={
                    "state": "deleting",
                    "deletion_reason": reason,
                    "deletion_id": new_id(),
                }
            )
            updated = transaction.update(
                "artifact",
                artifact_id,
                expected_revision=row["revision"],
                data=record.model_dump(mode="json"),
                state="deleting",
            )
            transaction.event("artifact.deleting", updated, {"reason": reason})
            return updated

        response = self.database.execute(
            context,
            "artifact.delete",
            {"artifact_id": artifact_id, "reason": reason},
            "artifact.delete",
            mark,
        )
        self._finish_deletion(context, ArtifactRecord.model_validate(response["result"]["data"]))
        return response

    def _finish_deletion(self, context: CommandContext, record: ArtifactRecord) -> None:
        self.store.delete(record.workspace_id, self._blob(record))

        def finish(transaction: Transaction) -> JsonObject:
            row = transaction.get("artifact", record.reference.id)
            current = ArtifactRecord.model_validate(row["data"])
            if current != record:
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT, "Artifact deletion generation changed"
                )
            deleted = current.model_copy(update={"state": "deleted"})
            updated = transaction.update(
                "artifact",
                record.reference.id,
                expected_revision=row["revision"],
                data=deleted.model_dump(mode="json"),
                state="deleted",
            )
            transaction.event("artifact.deleted", updated, {"reason": record.deletion_reason})
            return updated

        self.database.execute(
            self._internal(context, "finish-delete:" + str(record.deletion_id)),
            "artifact.finish_deletion",
            {"record": record.model_dump(mode="json")},
            "artifact.delete",
            finish,
        )

    def reconcile_deletion(self, context: CommandContext, artifact_id: str) -> None:
        context.principal.require("artifact.delete")
        record = self.get(context, artifact_id)
        if record.state == "deleting":
            self._finish_deletion(context, record)

    def occupancy(self, context: CommandContext) -> dict[str, Any]:
        snapshot = self.database.snapshot(context)
        retained = staging = 0
        for row in snapshot["records"]:
            if row["kind"] == "artifact" and row["state"] in {"verified", "deleting"}:
                retained += row["payload"]["reference"]["size_bytes"]
            if row["kind"] == "upload" and row["state"] in {"pending", "cleanup", "aborting"}:
                staging += row["payload"]["size_bytes"]
        return {
            "retained_bytes": retained,
            "staging_reserved_bytes": staging,
            "limit_bytes": self.storage_limit_bytes,
        }
