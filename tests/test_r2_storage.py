"""Storage failure contracts against a deterministic conditional-object simulator."""

import io
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Lock

import pytest

from research_swarm.domain.artifacts import StoredBlob
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.artifacts import ArtifactService, BeginUpload
from research_swarm.storage.r2 import R2BlobStore, R2StagingBinding
from research_swarm.storage.r2_config import R2Config
from research_swarm.storage.r2_transport import ObjectAbsent, RemoteObject, unavailable
from tests.test_artifacts import digest, publish, stage, upload_spec


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
def r2(tmp_path, principal):
    config = R2Config(
        endpoint="https://fixture.r2.cloudflarestorage.com",
        bucket="fixture-bucket",
        max_object_bytes=64,
    )
    transport = ObjectSimulator(config)
    binding = R2StagingBinding(
        workspace_id=principal.workspace_id,
        controller_id=new_id(),
        staging_id=new_id(),
        configuration_digest=config.digest,
    )
    return R2BlobStore.initialize(tmp_path / "r2", binding, config, transport)


@pytest.fixture
def service(database, r2):
    return ArtifactService(
        database, r2, storage_limit_bytes=100, max_object_bytes=r2.config.max_object_bytes
    )


def occupied(service, context):
    state = service.occupancy(context())
    return state["retained_bytes"] + state["staging_reserved_bytes"]


def test_chunk_replay_publish_reopen_and_corruption_before_any_reader_bytes(
    r2, principal, tmp_path
):
    upload = upload_spec(principal, b"abcdef")
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
    reopened.fetch(principal.workspace_id, blob, destination)
    assert destination.read_bytes() == b"abcdef"
    key = r2.transport.calls[0][0]
    _, metadata = r2.transport.objects[key]
    r2.transport.objects[key] = (b"BADBAD", metadata)
    entered = False
    with pytest.raises(ResearchError) as corrupt, reopened.reader(principal.workspace_id, blob):
        entered = True
    assert corrupt.value.code == ErrorCode.ARTIFACT_UNVERIFIED and not entered
    assert list((r2.root / "readers").iterdir()) == []


def test_lost_publication_reply_recovers_same_generation_before_catalog_commit(
    service, r2, context
):
    identifier = stage(service, context, b"payload")
    r2.transport.lose_payload = True
    with pytest.raises(ResearchError):
        service.finalize_upload(context("finalize"), identifier)
    assert occupied(service, context) == 7
    r2.transport.lose_payload = False
    result = service.finalize_upload(context("finalize"), identifier)
    assert result["result"]["data"]["storage_generation"] == identifier
    assert len(r2.transport.calls) == 1
    assert service.finalize_upload(context("finalize"), identifier) == result


def test_late_conditional_write_cannot_resurrect_aborted_generation(service, r2, context):
    identifier = stage(service, context, b"pending")
    r2.transport.defer_payload = True
    with pytest.raises(ResearchError):
        service.finalize_upload(context(), identifier)
    assert not r2.transport.objects and len(r2.transport.pending) == 1
    service.abort_upload(context("abort"), identifier)
    assert occupied(service, context) == 0
    r2.transport.complete_pending()
    assert len(r2.transport.objects) == 1
    content, metadata = next(iter(r2.transport.objects.values()))
    assert content == b"" and metadata["research-state"] == "retired"
    with pytest.raises(ResearchError):
        service.finalize_upload(context(), identifier)


def test_unconfirmed_deletion_retains_quota_and_reconciles_positive_fence(service, r2, context):
    artifact = publish(service, context, b"payload")
    r2.transport.lose_fence = True
    with pytest.raises(ResearchError):
        service.delete(context(revision=1), artifact.reference.id, "fixture deletion")
    assert occupied(service, context) == 7
    assert service.get(context(), artifact.reference.id).state == "deleting"
    r2.transport.lose_fence = False
    service.reconcile_deletion(context(), artifact.reference.id)
    assert occupied(service, context) == 0
    assert service.get(context(), artifact.reference.id).state == "deleted"


def test_deduplication_and_old_generation_deletion_preserve_current_payload(service, r2, context):
    first = publish(service, context, b"same")
    duplicate = publish(service, context, b"same")
    assert duplicate == first
    assert sorted(metadata["research-state"] for _, metadata in r2.transport.objects.values()) == [
        "payload",
        "retired",
    ]
    service.delete(context(revision=1), first.reference.id, "replace generation")
    replacement = publish(service, context, b"same")
    assert replacement.storage_generation != first.storage_generation
    r2.delete(
        first.workspace_id,
        StoredBlob(reference=first.reference, generation=first.storage_generation),
    )
    with service.reader(context(), replacement.reference) as source:
        assert source.read() == b"same"


def test_native_reader_pin_blocks_deletion_and_zero_byte_payload_is_not_a_fence(
    service, r2, context
):
    artifact = publish(service, context, b"")
    with service.reader(context(), artifact.reference) as source:
        assert source.read() == b""
        with pytest.raises(ResearchError):
            service.delete(context(revision=1), artifact.reference.id, "still pinned")
    assert next(iter(r2.transport.objects.values()))[1]["research-state"] == "payload"
    service.delete(context(revision=1), artifact.reference.id, "reader closed")
    assert next(iter(r2.transport.objects.values()))[1]["research-state"] == "retired"


def test_object_limit_precedes_upload_admission_and_is_distinct_from_aggregate(
    service, r2, context, principal
):
    before = service.occupancy(context())
    with pytest.raises(ResearchError) as oversized:
        service.begin_upload(context(), BeginUpload(digest=digest(b"x" * 65), size_bytes=65))
    assert oversized.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert service.occupancy(context()) == before
    assert r2.transport.calls == [] and list((r2.root / "generations").iterdir()) == []
    assert publish(service, context, b"x" * 64).reference.size_bytes == 64
    with pytest.raises(ResearchError):
        r2.begin(upload_spec(principal, b"x" * 65))


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
    r2, principal, tmp_path
):
    upload = upload_spec(principal, b"parallel")
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
        second.fetch(principal.workspace_id, results[0], destination)
    assert destination.read_bytes() == b"original"
    r2.transport.objects.clear()
    with pytest.raises(ResearchError) as missing:
        second.verify(principal.workspace_id, results[0])
    assert missing.value.code == ErrorCode.ARTIFACT_UNVERIFIED
