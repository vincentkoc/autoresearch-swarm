"""Workspace artifact authority and crash-recoverable storage lifecycle.

Byte transfers and hashing never run inside a SQLite write transaction. Immutable
storage generations fence stale cleanup without conflating a content hash with
the lifetime of a physical object.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO

from research_swarm.crypto import Ed25519Provider
from research_swarm.domain.artifacts import (
    ArtifactRecord,
    ArtifactRetention,
    StoredBlob,
    UploadSpec,
)
from research_swarm.domain.canonical import content_digest, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext, artifact_context
from research_swarm.domain.types import (
    ByteCount,
    Digest,
    MediaType,
    VersionedRecord,
    new_id,
    utc_now,
)
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.reader_sessions import (
    NativeReaderOwnership,
    ReaderOwnership,
    capture_owner,
    decode_owner,
    require_owner_closed,
)
from research_swarm.state.sql import SqlSession
from research_swarm.storage.base import BlobStore


class BeginUpload(VersionedRecord):
    digest: Digest
    size_bytes: ByteCount
    media_type: MediaType = "application/octet-stream"


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
            if (
                retention.owner_kind == "manual"
                and not retention.required
                and data["actor_id"] != transaction.context.principal.id
            ):
                continue
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
        owner_scope: str | None = None,
    ) -> JsonObject:
        """Internal owner lifecycles can name their already verified command authority."""
        row = transaction.get("artifact_reference", reference_id)
        principal = transaction.context.principal
        retention = ArtifactRetention.model_validate(row["data"]["retention"])
        if owner_scope is not None:
            if owner_release != (retention.owner_kind, retention.owner_id):
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Retention owner differs")
            principal.require(owner_scope)
        elif row["data"]["actor_id"] != principal.id:
            principal.require("artifact.admin")
        if row["state"] == "released":
            return row
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
        max_object_bytes: int | None = None,
        reader_ownership: ReaderOwnership | None = None,
    ) -> None:
        if type(storage_limit_bytes) is not int or storage_limit_bytes < 0:
            raise ValueError("Storage limit must be a nonnegative integer")
        if max_object_bytes is not None and (
            type(max_object_bytes) is not int or max_object_bytes < 0
        ):
            raise ValueError("Object limit must be a nonnegative integer")
        self.database = database
        self.store = store
        self.storage_limit_bytes = storage_limit_bytes
        self.max_object_bytes = max_object_bytes
        self.reader_session_id = new_id()
        self.reader_ownership = (
            reader_ownership if reader_ownership is not None else NativeReaderOwnership()
        )
        self.reader_owner = capture_owner(self.reader_ownership)

    @staticmethod
    def _upload_owner(context: CommandContext, row: JsonObject) -> UploadSpec:
        upload = UploadSpec.model_validate(row["data"])
        if upload.actor_id != context.principal.id:
            context.principal.require("artifact.admin")
        return upload

    @staticmethod
    def _internal(
        context: CommandContext,
        key: str,
        operation: str,
        payload: JsonObject,
        *,
        cleanup_pin_id: str | None = None,
    ) -> CommandContext:
        return artifact_context(context, key, operation, payload, cleanup_pin_id=cleanup_pin_id)

    @staticmethod
    def _blob(record: ArtifactRecord) -> StoredBlob:
        return StoredBlob(reference=record.reference, generation=record.storage_generation)

    def _storage_authority(self, context: CommandContext, upload_id: str) -> None:
        from research_swarm.state.drive_guard import require_storage_authority
        from research_swarm.state.workspace_gate import require_workspace_mutation

        def read(connection: SqlSession) -> None:
            require_workspace_mutation(
                connection, context.principal.workspace_id, "artifact.storage"
            )
            require_storage_authority(Transaction(connection, context), upload_id)

        self.database.store.read(read)

    def create_upload_in(self, transaction: Transaction, request: BeginUpload) -> JsonObject:
        """Create quota-accounted upload state inside the caller's authorized transaction."""
        if self.max_object_bytes is not None and request.size_bytes > self.max_object_bytes:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                "Artifact exceeds the storage adapter's object limit",
                details={"max_object_bytes": self.max_object_bytes},
            )
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
                data["size_bytes"] if item["kind"] == "upload" else data["reference"]["size_bytes"]
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
            actor_id=transaction.context.principal.id,
            **request.model_dump(exclude={"schema_version"}),
        )
        row = transaction.insert(
            "upload", upload.id, upload.model_dump(mode="json"), state="pending"
        )
        transaction.event("artifact.upload_started", row, {"size_bytes": upload.size_bytes})
        return row

    def begin_upload(self, context: CommandContext, request: BeginUpload) -> JsonObject:
        request = BeginUpload.model_validate(request)

        response = self.database.execute(
            context,
            "artifact.begin_upload",
            request.model_dump(mode="json"),
            "artifact.upload",
            lambda transaction: self.create_upload_in(transaction, request),
        )
        with self.store.upload_guard(context.principal.workspace_id, response["result"]["id"]):
            row = self.database.read_record(context, "upload", response["result"]["id"])
            if row["state"] == "pending":
                self._storage_authority(context, response["result"]["id"])
                self.store.begin(self._upload_owner(context, row))
        return response

    def upload_part(
        self, context: CommandContext, upload_id: str, offset: int, content: bytes
    ) -> int:
        context.principal.require("artifact.upload")
        self._upload_owner(context, self.database.read_record(context, "upload", upload_id))
        with self.store.upload_guard(context.principal.workspace_id, upload_id):
            return self._upload_part(context, upload_id, offset, content)

    def _upload_part(
        self, context: CommandContext, upload_id: str, offset: int, content: bytes
    ) -> int:
        context.principal.require("artifact.upload")
        row = self.database.read_record(context, "upload", upload_id)
        upload = self._upload_owner(context, row)
        if row["state"] != "pending":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload is not pending")
        self._storage_authority(context, upload_id)
        return self.store.append(upload, offset, content)

    def upload_status(self, context: CommandContext, upload_id: str) -> JsonObject:
        context.principal.require("artifact.upload")
        self._upload_owner(context, self.database.read_record(context, "upload", upload_id))
        with self.store.upload_guard(context.principal.workspace_id, upload_id):
            row = self.database.read_record(context, "upload", upload_id)
            upload = self._upload_owner(context, row)
            if row["state"] == "pending":
                self._storage_authority(context, upload_id)
            progress = self.store.progress(upload) if row["state"] == "pending" else None
            return {**row, "transfer": progress.model_dump(mode="json") if progress else None}

    def publish_upload_in(
        self, transaction: Transaction, upload_id: str, blob: StoredBlob
    ) -> JsonObject:
        """Publish a verified generation inside the caller's authorized transaction."""
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
                    published_by=transaction.context.principal.id,
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
                published_by=transaction.context.principal.id,
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

    def finalize_upload(self, context: CommandContext, upload_id: str) -> JsonObject:
        context.principal.require("artifact.upload")
        self._upload_owner(context, self.database.read_record(context, "upload", upload_id))
        with self.store.upload_guard(context.principal.workspace_id, upload_id):
            return self._finalize_upload(context, upload_id)

    def _finalize_upload(self, context: CommandContext, upload_id: str) -> JsonObject:
        context.principal.require("artifact.upload")
        row = self.database.read_record(context, "upload", upload_id)
        upload = self._upload_owner(context, row)
        if row["state"] in {"aborted", "aborting"}:
            previous = self.database.replay(
                context, "artifact.finalize_upload", {"upload_id": upload_id}, "artifact.upload"
            )
            if previous is not None:
                return previous
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload was aborted")
        if row["state"] == "pending":
            self._storage_authority(context, upload_id)
            blob = self.store.finalize(upload)
        else:
            blob = StoredBlob(reference=upload.reference(), generation=upload.id)

        response = self.database.execute(
            context,
            "artifact.finalize_upload",
            {"upload_id": upload_id},
            "artifact.upload",
            lambda transaction: self.publish_upload_in(transaction, upload_id, blob),
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
            self._internal(
                context,
                f"upload-{after}:{upload_id}",
                "artifact.finish_upload",
                {"upload_id": upload_id, "state": after},
            ),
            "artifact.finish_upload",
            {"upload_id": upload_id, "state": after},
            "artifact.upload",
            finish,
        )

    def abort_upload(self, context: CommandContext, upload_id: str) -> JsonObject:
        context.principal.require("artifact.upload")
        self._upload_owner(context, self.database.read_record(context, "upload", upload_id))
        with self.store.upload_guard(context.principal.workspace_id, upload_id):
            return self._abort_upload(context, upload_id)

    def _abort_upload(self, context: CommandContext, upload_id: str) -> JsonObject:
        return self.abort_authorized(
            context,
            upload_id,
            scope="artifact.upload",
            operation="artifact.abort_upload",
            payload={"upload_id": upload_id},
            authorize=lambda transaction, row: self._upload_owner(context, row),
            completion_operation="artifact.finish_upload",
            completion_key="upload-aborted:" + upload_id,
        )

    def abort_authorized(
        self,
        context: CommandContext,
        upload_id: str,
        *,
        scope: str,
        operation: str,
        payload: JsonObject,
        authorize: Callable[[Transaction, JsonObject], UploadSpec],
        completion_operation: str,
        completion_key: str,
        on_finish: Callable[[Transaction], None] | None = None,
    ) -> JsonObject:
        """Internal hook: caller holds the upload guard; authorization spans both commits."""
        context.principal.require(scope)

        def abort(transaction: Transaction) -> JsonObject:
            row = transaction.get("upload", upload_id)
            authorize(transaction, row)
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
            operation,
            payload,
            scope,
            abort,
        )

        def current(connection: SqlSession) -> tuple[JsonObject, UploadSpec]:
            transaction = Transaction(connection, context)
            row = transaction.get("upload", upload_id)
            return row, authorize(transaction, row)

        row, upload = self.database.store.read(current)
        if row["state"] == "aborting":
            self.store.discard_upload(upload)
            self.store.delete(
                upload.workspace_id, StoredBlob(reference=upload.reference(), generation=upload.id)
            )
        if row["state"] == "aborting" or row["state"] == "aborted" and on_finish is not None:

            def finish(transaction: Transaction) -> JsonObject:
                current = transaction.get("upload", upload_id)
                authorize(transaction, current)
                if current["state"] == "aborting":
                    current = transaction.update(
                        "upload",
                        upload_id,
                        expected_revision=current["revision"],
                        data=current["data"],
                        state="aborted",
                    )
                    transaction.event("artifact.upload_aborted", current, {})
                elif current["state"] != "aborted":
                    raise ResearchError(ErrorCode.INVALID_TRANSITION, "Upload lifecycle changed")
                if on_finish is not None:
                    on_finish(transaction)
                return current

            finished = {"upload_id": upload_id, "state": "aborted"}
            self.database.execute(
                self._internal(context, completion_key, completion_operation, finished),
                completion_operation,
                finished,
                scope,
                finish,
            )
        return response

    def get(self, context: CommandContext, artifact_id: str) -> ArtifactRecord:
        row = self.database.read_record(context, "artifact", artifact_id)
        return ArtifactRecord.model_validate(row["data"])

    def publish_bytes(
        self,
        context: CommandContext,
        content: bytes,
        *,
        purpose: str,
        media_type: str = "application/json",
        max_bytes: int = 1024 * 1024,
    ) -> ArtifactRef:
        """Publish bounded controller-generated metadata through the ordinary upload lifecycle."""
        if type(max_bytes) is not int or not 1 <= max_bytes <= 8 * 1024 * 1024:
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid inline artifact bound")
        if len(content) > max_bytes:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Inline artifact exceeds its bound"
            )
        key = record_digest(
            "research.inline-transfer",
            {
                "parent_key": context.idempotency_key,
                "purpose": purpose,
            },
        ).removeprefix("sha256:")
        request = BeginUpload(
            digest=content_digest(content), size_bytes=len(content), media_type=media_type
        )
        begin = self._internal(
            context, "inline-begin:" + key, "artifact.begin_upload", request.model_dump(mode="json")
        )
        upload = self.begin_upload(
            begin,
            request,
        )["result"]
        status = self.upload_status(context, upload["id"])
        if status["state"] == "pending" and not status["transfer"]["published"]:
            offset = status["transfer"]["received_bytes"]
            if offset < len(content):
                try:
                    self.upload_part(context, upload["id"], offset, content[offset:])
                except ResearchError as error:
                    if error.code not in {ErrorCode.NOT_FOUND, ErrorCode.INVALID_TRANSITION}:
                        raise
                    current = self.upload_status(context, upload["id"])
                    if current["state"] not in {"finalized", "cleanup"} and not (
                        current["state"] == "pending" and current["transfer"]["published"]
                    ):
                        raise
        finish = self._internal(
            context, "inline-finish:" + key, "artifact.finalize_upload", {"upload_id": upload["id"]}
        )
        row = self.finalize_upload(finish, upload["id"])["result"]
        return ArtifactRecord.model_validate(row["data"]).reference

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
        with self.pinned_authorized(
            context, reference, scope="read", authorize=lambda t, r: None
        ) as record:
            yield record

    @contextmanager
    def pinned_authorized(
        self,
        context: CommandContext,
        reference: ArtifactRef,
        *,
        scope: str,
        authorize: Callable[[Transaction, ArtifactRef], None],
    ) -> Iterator[ArtifactRecord]:
        """Internal control-plane hook: authorize exact bytes in the pin transaction."""
        context.principal.require(scope)
        pin_id = new_id()

        def pin(transaction: Transaction) -> JsonObject:
            authorize(transaction, reference)
            record = ArtifactCatalog.available(transaction, reference)
            session = transaction.connection.execute(
                "SELECT state,payload FROM records WHERE workspace_id=? "
                "AND kind='artifact_read_session' AND id=?",
                (transaction.workspace_id, self.reader_session_id),
            ).fetchone()
            if session is None:
                transaction.insert(
                    "artifact_read_session",
                    self.reader_session_id,
                    self.reader_owner.model_dump(mode="json"),
                    state="active",
                )
            elif session["state"] != "active":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Reader session is closed")
            elif decode_owner(strict_json_loads(session["payload"])) != self.reader_owner:
                raise ResearchError(ErrorCode.STALE_FENCE, "Reader session ownership differs")
            retained = ArtifactCatalog.retain(
                transaction,
                ArtifactRetention(
                    artifact_id=reference.id,
                    owner_kind="artifact_read",
                    owner_id=pin_id,
                    purpose="read",
                ),
                reader_session_id=self.reader_session_id,
            )
            return {"retention": retained, "artifact": record.model_dump(mode="json")}

        response = self.database.execute(
            self._internal(
                context,
                "pin:" + pin_id,
                "artifact.pin",
                {"reference": reference.model_dump(mode="json")},
            ),
            "artifact.pin",
            {"reference": reference.model_dump(mode="json")},
            scope,
            pin,
        )
        try:
            record = ArtifactRecord.model_validate(response["result"]["artifact"])
            self.store.verify(context.principal.workspace_id, self._blob(record))
            yield record
        finally:
            self.database.execute(
                self._internal(
                    context,
                    "unpin:" + pin_id,
                    "artifact.unpin",
                    {"reference_id": response["result"]["retention"]["id"]},
                    cleanup_pin_id=pin_id,
                ),
                "artifact.unpin",
                {"reference_id": response["result"]["retention"]["id"]},
                scope,
                lambda transaction: ArtifactCatalog.release(
                    transaction,
                    response["result"]["retention"]["id"],
                    owner_release=("artifact_read", pin_id),
                ),
            )

    def reconcile_reader_session(
        self,
        context: CommandContext,
        session_id: str,
        *,
        crypto_provider: Ed25519Provider | None = None,
    ) -> JsonObject:
        return self.database.with_preflight(
            context,
            "artifact.reconcile_reader_session",
            {"session_id": session_id},
            "artifact.admin",
            lambda: self._reconcile_reader_session(
                context, session_id, crypto_provider=crypto_provider
            ),
        )

    def _reconcile_reader_session(
        self,
        context: CommandContext,
        session_id: str,
        *,
        crypto_provider: Ed25519Provider | None = None,
    ) -> JsonObject:
        from research_swarm.control.recovery_readers import recovery_reader_context
        from research_swarm.state.recovery_readers import reader_references

        context.principal.require("artifact.admin")
        payload = {"session_id": session_id}
        session = self.database.read_record(context, "artifact_read_session", session_id)
        owner = decode_owner(session["data"])
        if session["state"] != "closed":
            require_owner_closed(self.reader_ownership, owner)
        context = recovery_reader_context(
            self.database, context, session, crypto_provider=crypto_provider
        )

        def reconcile(transaction: Transaction) -> JsonObject:
            row = transaction.get("artifact_read_session", session_id)
            if decode_owner(row["data"]) != owner:
                raise ResearchError(ErrorCode.STALE_FENCE, "Reader session ownership changed")
            if row["state"] == "closed":
                return row
            if row["revision"] != session["revision"] or session["state"] != "active":
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Reader session changed")
            for reference in reader_references(transaction, session_id):
                retention = ArtifactRetention.model_validate(reference["data"]["retention"])
                ArtifactCatalog.release(
                    transaction,
                    reference["id"],
                    owner_release=("artifact_read", retention.owner_id),
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
            payload,
            "artifact.admin",
            reconcile,
        )

    def fetch(self, context: CommandContext, reference: ArtifactRef, destination: Path) -> None:
        with self.pinned(context, reference) as record:
            self.store.fetch(context.principal.workspace_id, self._blob(record), destination)

    @contextmanager
    def reader(self, context: CommandContext, reference: ArtifactRef) -> Iterator[BinaryIO]:
        with (
            self.pinned(context, reference) as record,
            self.store.reader(context.principal.workspace_id, self._blob(record)) as source,
        ):
            yield source

    @contextmanager
    def reader_authorized(
        self,
        context: CommandContext,
        reference: ArtifactRef,
        *,
        scope: str,
        authorize: Callable[[Transaction, ArtifactRef], None],
    ) -> Iterator[BinaryIO]:
        with (
            self.pinned_authorized(context, reference, scope=scope, authorize=authorize) as record,
            self.store.reader(context.principal.workspace_id, self._blob(record)) as source,
        ):
            yield source

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
        # Reuse the generation's lifecycle guard across the byte operation and its
        # terminal commit. Duplicate delivery must observe that commit before it
        # can touch storage; the workspace seal refuses every unfinished deletion.
        with self.store.upload_guard(record.workspace_id, record.storage_generation):
            current = self.database.store.read(
                lambda connection: ArtifactRecord.model_validate(
                    Transaction(connection, context).get("artifact", record.reference.id)["data"]
                )
            )
            if current.storage_generation != record.storage_generation or (
                current.state == "deleted" and current.deletion_id == record.deletion_id
            ):
                return
            if current != record:
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT, "Artifact deletion generation changed"
                )
            self._delete_generation(context, record)

    def _delete_generation(self, context: CommandContext, record: ArtifactRecord) -> None:
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
            self._internal(
                context,
                "finish-delete:" + str(record.deletion_id),
                "artifact.finish_deletion",
                {"record": record.model_dump(mode="json")},
            ),
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
