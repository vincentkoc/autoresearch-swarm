"""Generation-scoped R2 blobs with durable native staging and late-write fences."""

from __future__ import annotations

import hashlib
import io
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryFile
from typing import BinaryIO

from filelock import FileLock

from research_swarm.domain.artifacts import StoredBlob, UploadProgress, UploadSpec
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.state.private_files import private_state_directory, read_private, write_private
from research_swarm.storage.filesystem import CHUNK_BYTES, FilesystemBlobStore
from research_swarm.storage.r2_config import R2Config
from research_swarm.storage.r2_transport import ObjectAbsent, R2Transport, unavailable


class R2StagingBinding(VersionedRecord):
    workspace_id: ID
    controller_id: ID
    staging_id: ID
    configuration_digest: Digest


class Generation(VersionedRecord):
    blob: StoredBlob
    upload: UploadSpec | None
    retired: bool = False


class R2BlobStore:
    def __init__(
        self, root: Path, binding: R2StagingBinding, config: R2Config, transport: R2Transport
    ) -> None:
        if not root.is_dir() or root.is_symlink():
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "R2 staging requires explicit initialization or recovery",
            )
        with private_state_directory(root):
            saved = R2StagingBinding.model_validate(
                strict_json_loads(read_private(root, "binding.json", limit=4096))
            )
            if (
                saved != binding
                or binding.configuration_digest != config.digest
                or transport.configuration_digest != config.digest
            ):
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "R2 staging or destination differs"
                )
            for name in ("bytes", "generations", "locks", "readers"):
                path = root / name
                if not path.is_dir() or path.is_symlink():
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE, "R2 staging is incomplete"
                    )
        self.root, self.binding, self.config, self.transport = (
            root.resolve(),
            binding,
            config,
            transport,
        )
        self.local = FilesystemBlobStore(self.root / "bytes")

    @classmethod
    def initialize(
        cls, root: Path, binding: R2StagingBinding, config: R2Config, transport: R2Transport
    ) -> R2BlobStore:
        if (
            binding.configuration_digest != config.digest
            or transport.configuration_digest != config.digest
        ):
            raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "R2 destination differs")
        if root.exists() or root.is_symlink():
            raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "R2 staging already exists")
        with private_state_directory(root):
            for name in ("bytes", "generations", "locks", "readers"):
                (root / name).mkdir(mode=0o700)
            write_private(root / "binding.json", canonical_bytes(binding.model_dump(mode="json")))
        return cls(root, binding, config, transport)

    def _workspace(self, value: str) -> None:
        if value != self.binding.workspace_id:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "R2 staging belongs to another workspace"
            )

    def _key(self, blob: StoredBlob) -> str:
        return "/".join(
            (
                self.config.prefix,
                self.binding.workspace_id,
                blob.reference.digest[7:],
                blob.generation,
            )
        )

    def _metadata(self, blob: StoredBlob, *, retired: bool = False) -> dict[str, str]:
        return {
            "research-state": "retired" if retired else "payload",
            "research-generation": blob.generation,
            "research-digest": blob.reference.digest[7:],
            "research-size": str(blob.reference.size_bytes),
        }

    def _path(self, directory: str, name: str) -> Path:
        parent = self.root / directory
        if not parent.is_dir() or parent.is_symlink():
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "R2 staging directory changed")
        path = parent / name
        if path.is_symlink():
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "R2 staging entry was substituted")
        return path

    @contextmanager
    def _operation(self, workspace_id: str, blob: StoredBlob) -> Iterator[None]:
        self._workspace(workspace_id)
        StoredBlob.model_validate(blob)
        with FileLock(self._path("locks", blob.generation + ".lock"), timeout=30):
            yield

    def _save(self, record: Generation) -> None:
        write_private(
            self._path("generations", record.blob.generation + ".json"),
            canonical_bytes(record.model_dump(mode="json")),
        )

    def _record(
        self, blob: StoredBlob, upload: UploadSpec | None = None, *, create: bool = False
    ) -> Generation:
        path = self._path("generations", blob.generation + ".json")
        if not path.exists():
            if not create:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "R2 generation metadata requires recovery"
                )
            record = Generation(blob=blob, upload=upload)
            self._save(record)
        else:
            record = Generation.model_validate(
                strict_json_loads(read_private(self.root, "generations/" + path.name, limit=8192))
            )
        if record.blob != blob or (upload is not None and record.upload != upload):
            raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "R2 generation identity differs")
        return record

    @staticmethod
    def _active(record: Generation) -> None:
        if record.retired:
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "R2 generation is retired")

    def _upload(self, upload: UploadSpec) -> StoredBlob:
        upload = UploadSpec.model_validate(upload)
        self._workspace(upload.workspace_id)
        if upload.size_bytes > self.config.max_object_bytes:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "R2 object exceeds its single-PUT limit"
            )
        return StoredBlob(reference=upload.reference(), generation=upload.id)

    @contextmanager
    def upload_guard(self, workspace_id: str, upload_id: str) -> Iterator[None]:
        self._workspace(workspace_id)
        with self.local.upload_guard(workspace_id, upload_id):
            yield

    def begin(self, upload: UploadSpec) -> int:
        blob = self._upload(upload)
        with self._operation(upload.workspace_id, blob):
            self._active(self._record(blob, upload, create=True))
            return self.local.begin(upload)

    def append(self, upload: UploadSpec, offset: int, content: bytes) -> int:
        blob = self._upload(upload)
        with self._operation(upload.workspace_id, blob):
            self._active(self._record(blob, upload))
            return self.local.append(upload, offset, content)

    def _copy_remote(self, blob: StoredBlob, output: BinaryIO | None = None) -> None:
        try:
            with self.transport.get(self._key(blob)) as remote:
                if (
                    remote.metadata != self._metadata(blob)
                    or remote.size_bytes != blob.reference.size_bytes
                ):
                    raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "R2 object identity differs")
                digest, size = hashlib.sha256(), 0
                while block := remote.body.read(CHUNK_BYTES):
                    size += len(block)
                    if size > blob.reference.size_bytes:
                        raise ResearchError(
                            ErrorCode.ARTIFACT_UNVERIFIED, "R2 object exceeds its declared size"
                        )
                    digest.update(block)
                    if output is not None:
                        output.write(block)
                if (
                    size != blob.reference.size_bytes
                    or "sha256:" + digest.hexdigest() != blob.reference.digest
                ):
                    raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "R2 object content differs")
        except (ResearchError, ObjectAbsent):
            raise
        except Exception:
            raise unavailable() from None

    def progress(self, upload: UploadSpec) -> UploadProgress:
        blob = self._upload(upload)
        with self._operation(upload.workspace_id, blob):
            self._active(self._record(blob, upload))
            try:
                self._copy_remote(blob)
            except ObjectAbsent:
                local = self.local.progress(upload)
                return UploadProgress(received_bytes=local.received_bytes, published=False)
            return UploadProgress(received_bytes=upload.size_bytes, published=True)

    def finalize(self, upload: UploadSpec) -> StoredBlob:
        blob = self._upload(upload)
        with self._operation(upload.workspace_id, blob):
            self._active(self._record(blob, upload))
            try:
                self._copy_remote(blob)
            except ObjectAbsent:
                self.local.finalize(upload)
                with self.local.reader(upload.workspace_id, blob) as source:
                    self.transport.put(
                        self._key(blob),
                        source,
                        upload.size_bytes,
                        self._metadata(blob),
                        if_absent=True,
                    )
                self._copy_remote(blob)
            self.local.discard_upload(upload)
            self.local.delete(upload.workspace_id, blob)
            return blob

    @contextmanager
    def reader(self, workspace_id: str, blob: StoredBlob) -> Iterator[BinaryIO]:
        self._path("readers", "unused")
        with TemporaryFile(dir=self.root / "readers") as stream:
            with self._operation(workspace_id, blob):
                self._active(self._record(blob))
                try:
                    self._copy_remote(blob, stream)
                except ObjectAbsent:
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "R2 object content is absent"
                    ) from None
                stream.seek(0)
            yield stream

    def verify(self, workspace_id: str, blob: StoredBlob) -> None:
        with self._operation(workspace_id, blob):
            self._active(self._record(blob))
            try:
                self._copy_remote(blob)
            except ObjectAbsent:
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "R2 object content is absent"
                ) from None

    def fetch(self, workspace_id: str, blob: StoredBlob, destination: Path) -> None:
        created = False
        try:
            with self.reader(workspace_id, blob) as source, destination.open("xb") as output:
                created = True
                while block := source.read(CHUNK_BYTES):
                    output.write(block)
        except BaseException:
            if created:
                destination.unlink(missing_ok=True)
            raise

    def discard_upload(self, upload: UploadSpec) -> None:
        blob = self._upload(upload)
        with self._operation(upload.workspace_id, blob):
            self._record(blob, upload, create=True)
            self.local.discard_upload(upload)

    def delete(self, workspace_id: str, blob: StoredBlob) -> None:
        with self._operation(workspace_id, blob):
            record = self._record(blob, create=True)
            if not record.retired:
                record = record.model_copy(update={"retired": True})
                self._save(record)
            metadata = self._metadata(blob, retired=True)
            self.transport.put(self._key(blob), io.BytesIO(), 0, metadata, if_absent=False)
            # Positive, permanent same-key evidence fences even a delayed conditional PUT.
            with self.transport.get(self._key(blob)) as remote:
                if remote.metadata != metadata or remote.size_bytes != 0 or remote.body.read(1):
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "R2 deletion fence was not confirmed"
                    )
            self.local.delete(workspace_id, blob)
            if record.upload is not None:
                self.local.discard_upload(record.upload)
