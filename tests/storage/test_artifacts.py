from __future__ import annotations

import hashlib
import io
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import pytest

from research_swarm.domain.artifacts import UploadSpec
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.storage.filesystem import FilesystemBlobStore
from research_swarm.swarm.storage.filesystem import _check


@pytest.fixture
def workspace_id() -> str:
    return new_id()


def digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


@pytest.fixture
def store(tmp_path: Path) -> FilesystemBlobStore:
    return FilesystemBlobStore(tmp_path / "artifacts")


def upload_spec(workspace_id: str, content: bytes) -> UploadSpec:
    return UploadSpec(
        id=new_id(),
        workspace_id=workspace_id,
        actor_id=new_id(),
        digest=digest(content),
        size_bytes=len(content),
    )


def test_chunk_replay_partial_crash_and_conflicting_bytes(
    store: FilesystemBlobStore, workspace_id: str
) -> None:
    upload = upload_spec(workspace_id, b"abcdef")
    assert store.begin(upload) == 0
    assert store.append(upload, 0, b"abc") == 3
    # The interrupted full chunk is retried; the durable matching prefix is retained.
    assert store.append(upload, 0, b"abcdef") == 6
    assert store.append(upload, 0, b"abcdef") == 6
    with pytest.raises(ResearchError) as error:
        store.append(upload, 1, b"ZZ")
    assert error.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    blob = store.finalize(upload)
    assert store.finalize(upload) == blob
    store.verify(workspace_id, blob)


def test_wrong_digest_short_upload_and_oversize_are_not_published(
    store: FilesystemBlobStore, workspace_id: str
) -> None:
    upload = upload_spec(workspace_id, b"abc")
    store.begin(upload)
    store.append(upload, 0, b"a")
    with pytest.raises(ResearchError, match="size mismatch"):
        store.finalize(upload)
    with pytest.raises(ResearchError, match="declared size"):
        store.append(upload, 1, b"xxxx")
    store.append(upload, 1, b"zz")
    with pytest.raises(ResearchError, match="size mismatch"):
        store.finalize(upload)


def test_download_never_overwrites_destination(
    store: FilesystemBlobStore, workspace_id: str, tmp_path: Path
) -> None:
    blob = store.put(workspace_id, new_id(), b"original")
    destination = tmp_path / "output"
    store.fetch(workspace_id, blob, destination)
    assert destination.read_bytes() == b"original"
    with pytest.raises(FileExistsError):
        store.fetch(workspace_id, blob, destination)
    assert destination.read_bytes() == b"original"


def test_generation_fences_old_deletion(store: FilesystemBlobStore, workspace_id: str) -> None:
    older = store.put(workspace_id, new_id(), b"content")
    newer = store.put(workspace_id, new_id(), b"content")
    assert older.reference == newer.reference
    assert older.generation != newer.generation
    store.delete(workspace_id, older)
    store.delete(workspace_id, older)
    store.verify(workspace_id, newer)


def _append_in_process(root: str, upload: dict[str, Any]) -> int:
    return FilesystemBlobStore(Path(root)).append(UploadSpec.model_validate(upload), 0, b"abcdef")


def test_native_locks_coordinate_separate_processes(
    store: FilesystemBlobStore, workspace_id: str
) -> None:
    upload = upload_spec(workspace_id, b"abcdef")
    store.begin(upload)
    with ProcessPoolExecutor(max_workers=2, mp_context=get_context("spawn")) as executor:
        futures = [
            executor.submit(_append_in_process, str(store.root), upload.model_dump(mode="json"))
            for _ in range(2)
        ]
        assert [future.result(timeout=30) for future in futures] == [6, 6]
    store.verify(workspace_id, store.finalize(upload))


def test_symlink_storage_directory_is_rejected(
    store: FilesystemBlobStore, workspace_id: str, tmp_path: Path
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    try:
        (store.root / workspace_id).symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("Symlink creation requires platform privileges")
    with pytest.raises(ResearchError, match="storage directory"):
        store.begin(upload_spec(workspace_id, b"abc"))
    assert not list(outside.iterdir())


def test_begin_retry_after_publication_does_not_allocate_another_copy(
    store: FilesystemBlobStore, workspace_id: str
) -> None:
    upload = upload_spec(workspace_id, b"abc")
    store.begin(upload)
    store.append(upload, 0, b"abc")
    blob = store.finalize(upload)
    assert store.begin(upload) == 3
    assert not store._staged(upload).exists()
    store.verify(workspace_id, blob)


def test_corrupt_blob_verification_stops_at_the_first_excess_byte(workspace_id: str) -> None:
    reference = upload_spec(workspace_id, b"abc").reference()
    stream = io.BytesIO(b"abc" + b"x" * (2**21))
    with pytest.raises(ResearchError, match="content or size mismatch") as caught:
        _check(stream, reference)
    assert caught.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert stream.tell() == reference.size_bytes + 1
