"""Async generation contracts; the in-memory transport does not qualify R2."""

import asyncio

import pytest

from research_swarm.domain.artifacts import StoredBlob, UploadSpec
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.storage.async_r2 import BoundedR2Configuration, BoundedR2Objects, ObjectBytes
from research_swarm.storage.r2_objects import generation_metadata


class Transport:
    def __init__(self, config):
        self.configuration_digest = config.digest
        self.objects = {}
        self.calls = []
        self.arrived = asyncio.Event()
        self.release = None
        self.lose_put_response = False
        self.hide_reads = False

    async def put(self, key, content, metadata, *, if_absent):
        self.calls.append(("put", key, if_absent))
        if if_absent and self.release is not None:
            self.arrived.set()
            await self.release.wait()
        if if_absent and key in self.objects:
            return False
        self.objects[key] = ObjectBytes(content, metadata)
        if self.lose_put_response:
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture response lost")
        return True

    async def get(self, key, *, max_bytes):
        self.calls.append(("get", key, max_bytes))
        result = self.objects.get(key)
        if self.hide_reads:
            return None
        if result is not None and len(result.content) > max_bytes:
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Fixture bounded read refused")
        return result


@pytest.fixture
def objects():
    config = BoundedR2Configuration(
        workspace_id=new_id(), bucket_identity="synthetic-bucket", prefix="research-test"
    )
    transport = Transport(config)
    body = b"small public research fixture\x00\xff"
    upload = UploadSpec(
        id=new_id(),
        workspace_id=config.workspace_id,
        actor_id=new_id(),
        digest=content_digest(body),
        size_bytes=len(body),
    )
    return config, transport, BoundedR2Objects(config, transport), upload, body


async def test_publication_requires_roundtrip_content_and_survives_lost_put_response(objects):
    config, transport, store, upload, body = objects
    transport.lose_put_response = True
    with pytest.raises(ResearchError) as uncertain:
        await store.publish(upload, body)
    assert uncertain.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    transport.lose_put_response = False
    reopened = BoundedR2Objects(config, transport)
    blob = await reopened.publish(upload, body)
    assert len(transport.objects) == 1 and blob.generation == upload.id
    assert await reopened.read(config.workspace_id, blob) == body
    assert [call[0] for call in transport.calls] == ["put", "put", "get", "get"]


@pytest.mark.parametrize("damage", ["digest", "short", "oversized", "foreign"])
async def test_invalid_upload_never_reaches_transport(objects, damage):
    config, transport, store, upload, body = objects
    if damage == "digest":
        body = b"x" * len(body)
    elif damage == "short":
        body = body[:-1]
    elif damage == "oversized":
        upload = upload.model_copy(update={"size_bytes": config.max_object_bytes + 1})
    else:
        upload = upload.model_copy(update={"workspace_id": new_id()})
    with pytest.raises(ResearchError):
        await store.publish(upload, body)
    assert transport.calls == [] and transport.objects == {}


@pytest.mark.parametrize("damage", ["body", "metadata", "absent"])
async def test_successful_put_does_not_substitute_for_complete_verification(objects, damage):
    config, transport, store, upload, body = objects
    blob = await store.publish(upload, body)
    key = next(iter(transport.objects))
    if damage == "body":
        transport.objects[key] = ObjectBytes(b"x" * len(body), generation_metadata(blob))
    elif damage == "metadata":
        transport.objects[key] = ObjectBytes(body, {"research-state": "payload"})
    else:
        del transport.objects[key]
    with pytest.raises(ResearchError) as refused:
        await store.read(config.workspace_id, blob)
    assert refused.value.code == ErrorCode.ARTIFACT_UNVERIFIED


async def test_permanent_retirement_fences_a_delayed_conditional_payload_put(objects):
    config, transport, store, upload, body = objects
    blob = StoredBlob(reference=upload.reference(), generation=upload.id)
    transport.release = asyncio.Event()
    delayed = asyncio.create_task(store.publish(upload, body))
    try:
        await asyncio.wait_for(transport.arrived.wait(), 2)
        await store.retire(config.workspace_id, blob)
        transport.release.set()
        with pytest.raises(ResearchError) as refused:
            await asyncio.wait_for(delayed, 2)
        assert refused.value.code == ErrorCode.INVALID_TRANSITION
        retired = next(iter(transport.objects.values()))
        assert not retired.content and retired.metadata == generation_metadata(blob, retired=True)
        reopened = BoundedR2Objects(config, transport)
        with pytest.raises(ResearchError) as replayed_put:
            await reopened.publish(upload, body)
        assert replayed_put.value.code == ErrorCode.INVALID_TRANSITION
    finally:
        transport.release.set()
        if not delayed.done():
            delayed.cancel()
        await asyncio.gather(delayed, return_exceptions=True)


async def test_retirement_needs_positive_readback_and_can_resume_after_uncertainty(objects):
    config, transport, store, upload, body = objects
    blob = await store.publish(upload, body)
    transport.lose_put_response = True
    with pytest.raises(ResearchError):
        await store.retire(config.workspace_id, blob)
    transport.lose_put_response = False
    transport.hide_reads = True
    with pytest.raises(ResearchError) as absent:
        await store.retire(config.workspace_id, blob)
    assert absent.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    transport.hide_reads = False
    await BoundedR2Objects(config, transport).retire(config.workspace_id, blob)
    assert len(transport.objects) == 1


async def test_duplicate_content_has_distinct_retirable_generations(objects):
    config, transport, store, upload, body = objects
    first = await store.publish(upload, body)
    second = await store.publish(upload.model_copy(update={"id": new_id()}), body)
    assert first.reference == second.reference and first.generation != second.generation
    await store.retire(config.workspace_id, second)
    assert await store.read(config.workspace_id, first) == body
    assert len(transport.objects) == 2  # Retired keys remain as permanent fences.


async def test_destination_drift_refuses_before_any_io(objects):
    config, transport, store, upload, body = objects
    transport.configuration_digest = "sha256:" + "f" * 64
    with pytest.raises(ResearchError):
        BoundedR2Objects(config, transport)
    with pytest.raises(ResearchError):
        await store.publish(upload, body)
    assert transport.calls == []


@pytest.mark.parametrize("operation", ["read", "retire"])
async def test_destination_drift_during_io_cannot_confirm_content_or_retirement(objects, operation):
    config, transport, store, upload, body = objects
    blob = await store.publish(upload, body)
    original = transport.get

    async def changed(key, *, max_bytes):
        result = await original(key, max_bytes=max_bytes)
        transport.configuration_digest = "sha256:" + "f" * 64
        return result

    transport.get = changed
    with pytest.raises(ResearchError) as refused:
        await getattr(store, operation)(config.workspace_id, blob)
    assert refused.value.code == ErrorCode.STALE_FENCE


def test_verified_byte_snapshot_does_not_retain_mutable_metadata():
    metadata = {"research-state": "payload"}
    snapshot = ObjectBytes(b"fixture", metadata)
    metadata["research-state"] = "retired"
    assert snapshot.metadata["research-state"] == "payload"
    with pytest.raises(TypeError):
        snapshot.metadata["research-state"] = "retired"


@pytest.mark.parametrize("metadata", [{"field": []}, {"field": {}}, {1: "value"}])
def test_byte_snapshot_refuses_nonstring_or_nested_mutable_metadata(metadata):
    with pytest.raises(TypeError):
        ObjectBytes(b"fixture", metadata)
