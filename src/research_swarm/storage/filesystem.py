"""Local content-addressed blobs with resumable uploads and immutable publication.

The root belongs to the controller, on a qualified local filesystem. Candidate
processes must not receive write access to it. Locks coordinate trusted processes;
they do not isolate hostile code running under the controller's operating-system user.
"""

from __future__ import annotations

import hashlib
import os
import stat
import sys
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import BinaryIO, cast

from filelock import FileLock
from pydantic import TypeAdapter

from research_swarm.domain.artifacts import StoredBlob, UploadProgress, UploadSpec
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import ID, new_id

CHUNK_BYTES = 1024 * 1024
MAX_UPLOAD_PART_BYTES = 8 * CHUNK_BYTES
_identifier = TypeAdapter(ID)


def _sync_directory(path: Path) -> None:
    if sys.platform != "win32":
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


@contextmanager
def _file(path: Path, mode: str) -> Iterator[BinaryIO]:
    if path.is_symlink():
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Artifact must not be a symbolic link")
    flags = {"rb": os.O_RDONLY, "r+b": os.O_RDWR, "xb": os.O_WRONLY | os.O_CREAT | os.O_EXCL}
    descriptor = os.open(
        path,
        flags[mode]
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NONBLOCK", 0),
        0o600,
    )
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ResearchError(
                ErrorCode.ARTIFACT_UNVERIFIED, "Artifact must be a private regular file"
            )
        with os.fdopen(descriptor, mode) as stream:
            descriptor = -1
            yield cast(BinaryIO, stream)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _check(stream: BinaryIO, reference: ArtifactRef) -> None:
    stream.seek(0)
    digest = hashlib.sha256()
    size = 0
    while block := stream.read(CHUNK_BYTES):
        digest.update(block)
        size += len(block)
    if size != reference.size_bytes or "sha256:" + digest.hexdigest() != reference.digest:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Artifact content or size mismatch")
    stream.seek(0)


class FilesystemBlobStore:
    def __init__(self, root: Path) -> None:
        if os.name not in {"posix", "nt"}:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Native file locks are required")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if root.is_symlink() or not root.is_dir():
            raise ValueError("Artifact root must be a real directory")
        self.root = root.resolve()

    def _directory(self, *parts: str) -> Path:
        directory = self.root
        for part in parts:
            directory /= part
            try:
                directory.mkdir(mode=0o700)
                _sync_directory(directory.parent)
            except FileExistsError:
                pass
            if directory.is_symlink() or not directory.is_dir():
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Invalid artifact storage directory"
                )
        return directory

    def _workspace(self, workspace_id: str) -> str:
        return _identifier.validate_python(workspace_id)

    def _sync_ancestry(self, directory: Path) -> None:
        while directory != directory.parent:
            _sync_directory(directory)
            directory = directory.parent
        _sync_directory(directory)

    def _staged(self, upload: UploadSpec) -> Path:
        return self._directory(upload.workspace_id, "uploads") / upload.id

    def _object(self, workspace_id: str, blob: StoredBlob) -> Path:
        digest = blob.reference.digest.removeprefix("sha256:")
        return self._directory(workspace_id, "objects", digest[:2], digest) / blob.generation

    def _lock(self, workspace_id: str, key: str) -> FileLock:
        path = self._directory(workspace_id, "locks") / (key + ".lock")
        if path.is_symlink():
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Invalid artifact lock")
        return FileLock(path, timeout=30)

    @contextmanager
    def upload_guard(self, workspace_id: str, upload_id: str) -> Iterator[None]:
        """Serialize lifecycle decisions separately from each atomic byte operation."""
        with self._lock(
            self._workspace(workspace_id), "upload-" + _identifier.validate_python(upload_id)
        ):
            yield

    def begin(self, upload: UploadSpec) -> int:
        upload = UploadSpec.model_validate(upload)
        with self._lock(upload.workspace_id, upload.id):
            path = self._staged(upload)
            blob = StoredBlob(reference=upload.reference(), generation=upload.id)
            published = self._object(upload.workspace_id, blob)
            if published.exists() or published.is_symlink():
                with _file(published, "rb") as stream:
                    _check(stream, blob.reference)
                return upload.size_bytes
            try:
                with _file(path, "xb") as stream:
                    stream.flush()
                    os.fsync(stream.fileno())
                _sync_directory(path.parent)
                return 0
            except FileExistsError:
                with _file(path, "rb") as stream:
                    return os.fstat(stream.fileno()).st_size

    def append(self, upload: UploadSpec, offset: int, content: bytes) -> int:
        upload = UploadSpec.model_validate(upload)
        if type(offset) is not int or offset < 0 or not isinstance(content, bytes):
            raise ValueError("Upload requires a nonnegative byte offset and bytes")
        if len(content) > MAX_UPLOAD_PART_BYTES or offset + len(content) > upload.size_bytes:
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Upload exceeds its declared size")
        with self._lock(upload.workspace_id, upload.id):
            path = self._staged(upload)
            try:
                with _file(path, "r+b") as stream:
                    size = os.fstat(stream.fileno()).st_size
                    if offset < size:
                        stream.seek(offset)
                        overlap = min(size - offset, len(content))
                        if stream.read(overlap) != content[:overlap]:
                            raise ResearchError(
                                ErrorCode.IDEMPOTENCY_CONFLICT, "Uploaded bytes changed"
                            )
                        if offset + len(content) <= size:
                            return size
                        content = content[overlap:]
                        offset = size
                    if offset > size:
                        raise ResearchError(
                            ErrorCode.REVISION_CONFLICT,
                            "Upload offset does not match stored bytes",
                            details={"next_offset": size},
                        )
                    stream.seek(offset)
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                    return offset + len(content)
            except FileNotFoundError as error:
                raise ResearchError(ErrorCode.NOT_FOUND, "Staged upload not found") from error

    def progress(self, upload: UploadSpec) -> UploadProgress:
        upload = UploadSpec.model_validate(upload)
        with self._lock(upload.workspace_id, upload.id):
            blob = StoredBlob(reference=upload.reference(), generation=upload.id)
            published = self._object(upload.workspace_id, blob)
            if published.exists() or published.is_symlink():
                with _file(published, "rb") as source:
                    _check(source, blob.reference)
                return UploadProgress(received_bytes=upload.size_bytes, published=True)
            try:
                with _file(self._staged(upload), "rb") as source:
                    received = os.fstat(source.fileno()).st_size
            except FileNotFoundError:
                received = 0
            if received > upload.size_bytes:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Staged upload exceeds its size")
            return UploadProgress(received_bytes=received, published=False)

    def finalize(self, upload: UploadSpec) -> StoredBlob:
        upload = UploadSpec.model_validate(upload)
        reference = upload.reference()
        blob = StoredBlob(reference=reference, generation=upload.id)
        digest = reference.digest.removeprefix("sha256:")
        with self._lock(upload.workspace_id, upload.id), self._lock(upload.workspace_id, digest):
            staged = self._staged(upload)
            target = self._object(upload.workspace_id, blob)
            if target.exists() or target.is_symlink():
                with _file(target, "rb") as stream:
                    _check(stream, reference)
                self._sync_ancestry(target.parent)
                staged.unlink(missing_ok=True)
                _sync_directory(staged.parent)
                return blob
            try:
                with _file(staged, "r+b") as stream:
                    _check(stream, reference)
                    os.fsync(stream.fileno())
                os.replace(staged, target)
                self._sync_ancestry(target.parent)
                _sync_directory(staged.parent)
            except FileNotFoundError as error:
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Upload content is absent"
                ) from error
        return blob

    @contextmanager
    def _verified(self, workspace_id: str, blob: StoredBlob) -> Iterator[BinaryIO]:
        workspace_id = self._workspace(workspace_id)
        blob = StoredBlob.model_validate(blob)
        reference = blob.reference
        with self._lock(workspace_id, reference.digest.removeprefix("sha256:")):
            try:
                with _file(self._object(workspace_id, blob), "rb") as stream:
                    _check(stream, reference)
                    yield stream
            except FileNotFoundError as error:
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Artifact content is absent"
                ) from error

    def verify(self, workspace_id: str, blob: StoredBlob) -> None:
        with self._verified(workspace_id, blob):
            pass

    @contextmanager
    def reader(self, workspace_id: str, blob: StoredBlob) -> Iterator[BinaryIO]:
        """Open a verified generation without copying it or holding a lock over I/O waits.

        The artifact service retains its catalog pin for the reader's lifetime.
        Publication never writes an existing generation's bytes.
        """
        workspace_id = self._workspace(workspace_id)
        blob = StoredBlob.model_validate(blob)
        try:
            with ExitStack() as stack:
                with self._lock(workspace_id, blob.reference.digest.removeprefix("sha256:")):
                    stream = stack.enter_context(_file(self._object(workspace_id, blob), "rb"))
                    _check(stream, blob.reference)
                yield stream
        except FileNotFoundError as error:
            raise ResearchError(
                ErrorCode.ARTIFACT_UNVERIFIED, "Artifact content is absent"
            ) from error

    def fetch(self, workspace_id: str, blob: StoredBlob, destination: Path) -> None:
        # An exclusive destination never overwrites user files or follows a leaf symlink.
        created = False
        try:
            with self._verified(workspace_id, blob) as source, _file(destination, "xb") as target:
                created = True
                while block := source.read(CHUNK_BYTES):
                    target.write(block)
                target.flush()
                os.fsync(target.fileno())
            _sync_directory(destination.parent)
        except BaseException:
            if created:
                destination.unlink(missing_ok=True)
            raise

    def discard_upload(self, upload: UploadSpec) -> None:
        upload = UploadSpec.model_validate(upload)
        with self._lock(upload.workspace_id, upload.id):
            path = self._staged(upload)
            path.unlink(missing_ok=True)
            _sync_directory(path.parent)

    def delete(self, workspace_id: str, blob: StoredBlob) -> None:
        workspace_id = self._workspace(workspace_id)
        blob = StoredBlob.model_validate(blob)
        with self._lock(workspace_id, blob.reference.digest.removeprefix("sha256:")):
            path = self._object(workspace_id, blob)
            if path.is_symlink():
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Refusing a substituted object")
            path.unlink(missing_ok=True)
            _sync_directory(path.parent)

    def put(self, workspace_id: str, actor_id: str, content: bytes) -> StoredBlob:
        """Convenience for trusted adapters; this does not grant catalog access."""
        upload = UploadSpec(
            id=new_id(),
            workspace_id=workspace_id,
            actor_id=actor_id,
            digest="sha256:" + hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
        )
        self.begin(upload)
        for offset in range(0, len(content), MAX_UPLOAD_PART_BYTES):
            self.append(upload, offset, content[offset : offset + MAX_UPLOAD_PART_BYTES])
        return self.finalize(upload)
