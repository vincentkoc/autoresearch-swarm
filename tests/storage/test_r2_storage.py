"""Storage failure contracts against a deterministic conditional-object simulator."""

import io
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Lock

import pytest

from research_swarm.domain.artifacts import StoredBlob
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.storage.r2 import R2BlobStore, R2StagingBinding
from research_swarm.storage.r2_config import R2Config
from research_swarm.storage.r2_transport import ObjectAbsent, RemoteObject, unavailable
from tests.storage.test_artifacts import upload_spec
from tests.storage.test_artifacts import workspace_id as workspace_id


class ObjectSimulator:
    def __init__(self, config):
        self.configuration_digest = config.digest
        self.objects, self.pending, self.calls = {}, [], []
        self.defer_payload = self.lose_payload = self.lose_fence = False
        self.lock = Lock()

    @contextmanager
    def get(self, key):
        with self.lock:
            if key not in self.objects:
                raise ObjectAbsent
            content, metadata = self.objects[key]
        with io.BytesIO(content) as source:
            yield RemoteObject(source, len(content), metadata.copy())

    def put(self, key, body, size_bytes, metadata, *, if_absent):
        content = body.read()
        assert len(content) == size_bytes
        with self.lock:
            self.calls.append((key, if_absent, metadata["research-state"]))
            if if_absent and self.defer_payload:
                self.pending.append((key, content, dict(metadata)))
                raise unavailable()
            if if_absent and key in self.objects:
                return False
            self.objects[key] = (content, dict(metadata))
            if (if_absent and self.lose_payload) or (not if_absent and self.lose_fence):
                raise unavailable()
        return True

    def complete_pending(self):
        with self.lock:
            for key, content, metadata in self.pending:
                if key not in self.objects:
                    self.objects[key] = (content, metadata)
            self.pending.clear()


@pytest.fixture
def r2(tmp_path, workspace_id):
    config = R2Config(
        endpoint="https://fixture.r2.cloudflarestorage.com",
        bucket="fixture-bucket",
        max_object_bytes=64,
    )
    transport = ObjectSimulator(config)
    binding = R2StagingBinding(
        workspace_id=workspace_id,
        controller_id=new_id(),
        staging_id=new_id(),
        configuration_digest=config.digest,
    )
    return R2BlobStore.initialize(tmp_path / "r2", binding, config, transport)


def test_chunk_replay_publish_reopen_and_corruption_before_any_reader_bytes(
    r2, workspace_id, tmp_path
):
    upload = upload_spec(workspace_id, b"abcdef")
    assert r2.begin(upload) == 0
    assert r2.append(upload, 0, b"abc") == 3
    assert r2.append(upload, 0, b"abcdef") == 6
    with pytest.raises(ResearchError) as changed:
        r2.append(upload, 0, b"BADBAD")
    assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    blob = r2.finalize(upload)
    assert r2.progress(upload).published
    assert r2.local.progress(upload).received_bytes == 0
    reopened = R2BlobStore(r2.root, r2.binding, r2.config, r2.transport)
    assert reopened.finalize(upload) == blob
    assert len(r2.transport.calls) == 1
    destination = tmp_path / "download"
    reopened.fetch(workspace_id, blob, destination)
    assert destination.read_bytes() == b"abcdef"
    key = r2.transport.calls[0][0]
    _, metadata = r2.transport.objects[key]
    r2.transport.objects[key] = (b"BADBAD", metadata)
    entered = False
    with pytest.raises(ResearchError) as corrupt, reopened.reader(workspace_id, blob):
        entered = True
    assert corrupt.value.code == ErrorCode.ARTIFACT_UNVERIFIED and not entered
    assert list((r2.root / "readers").iterdir()) == []


def test_lost_publication_reply_recovers_the_same_generation(r2, workspace_id):
    upload = upload_spec(workspace_id, b"payload")
    r2.begin(upload)
    r2.append(upload, 0, b"payload")
    r2.transport.lose_payload = True
    with pytest.raises(ResearchError):
        r2.finalize(upload)
    r2.transport.lose_payload = False
    blob = r2.finalize(upload)
    assert blob.generation == upload.id and r2.finalize(upload) == blob
    r2.verify(workspace_id, blob)
    assert len(r2.transport.calls) == 1


def test_late_conditional_write_cannot_resurrect_discarded_generation(r2, workspace_id):
    upload = upload_spec(workspace_id, b"pending")
    r2.begin(upload)
    r2.append(upload, 0, b"pending")
    r2.transport.defer_payload = True
    with pytest.raises(ResearchError):
        r2.finalize(upload)
    assert not r2.transport.objects and len(r2.transport.pending) == 1
    r2.discard_upload(upload)
    r2.delete(workspace_id, StoredBlob(reference=upload.reference(), generation=upload.id))
    r2.transport.complete_pending()
    content, metadata = next(iter(r2.transport.objects.values()))
    assert content == b"" and metadata["research-state"] == "retired"
    with pytest.raises(ResearchError):
        r2.finalize(upload)


def test_failed_delete_retries_without_changing_another_generation(r2, workspace_id):
    blobs = []
    for _ in range(2):
        upload = upload_spec(workspace_id, b"")
        r2.begin(upload)
        blobs.append(r2.finalize(upload))
    first, second = blobs
    assert first.generation != second.generation
    r2.transport.lose_fence = True
    with pytest.raises(ResearchError):
        r2.delete(workspace_id, first)
    r2.transport.lose_fence = False
    r2.delete(workspace_id, first)
    r2.delete(workspace_id, first)
    with r2.reader(workspace_id, second) as source:
        assert source.read() == b""
    assert sorted(metadata["research-state"] for _, metadata in r2.transport.objects.values()) == [
        "payload",
        "retired",
    ]


def test_object_limit_precedes_staging(r2, workspace_id):
    with pytest.raises(ResearchError):
        r2.begin(upload_spec(workspace_id, b"x" * 65))
    assert r2.transport.calls == [] and list((r2.root / "generations").iterdir()) == []
    upload = upload_spec(workspace_id, b"x" * 64)
    r2.begin(upload)
    r2.append(upload, 0, b"x" * 64)
    assert r2.finalize(upload).reference.size_bytes == 64


def test_storage_reopen_refuses_destination_identity_change_or_missing_staging(r2, tmp_path):
    changed = r2.config.model_copy(update={"bucket": "another-bucket"})
    with pytest.raises(ResearchError) as mismatch:
        R2BlobStore(r2.root, r2.binding, changed, ObjectSimulator(changed))
    assert mismatch.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    with pytest.raises(ResearchError):
        R2BlobStore(tmp_path / "missing", r2.binding, r2.config, r2.transport)
    assert not (tmp_path / "missing").exists()
    with pytest.raises(ResearchError):
        R2BlobStore.initialize(r2.root, r2.binding, r2.config, r2.transport)


def test_independent_store_instances_publish_once_and_preserve_existing_destination(
    r2, workspace_id, tmp_path
):
    upload = upload_spec(workspace_id, b"parallel")
    r2.begin(upload)
    r2.append(upload, 0, b"parallel")
    second = R2BlobStore(r2.root, r2.binding, r2.config, r2.transport)
    barrier = Barrier(2)

    def publish_once(store):
        barrier.wait()
        return store.finalize(upload)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(publish_once, (r2, second)))
    assert results[0] == results[1] and len(r2.transport.calls) == 1
    destination = tmp_path / "existing"
    destination.write_bytes(b"original")
    with pytest.raises(FileExistsError):
        second.fetch(workspace_id, results[0], destination)
    assert destination.read_bytes() == b"original"
    r2.transport.objects.clear()
    with pytest.raises(ResearchError) as missing:
        second.verify(workspace_id, results[0])
    assert missing.value.code == ErrorCode.ARTIFACT_UNVERIFIED
