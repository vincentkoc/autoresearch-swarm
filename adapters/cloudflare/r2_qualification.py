"""Actual local R2 binding probes, separate from authoritative artifact routing."""

import asyncio

from r2_binding import R2BindingTransport
from witness_qualification import identifier

from research_swarm.domain.artifacts import StoredBlob, UploadSpec
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.storage.async_r2 import BoundedR2Configuration, BoundedR2Objects
from research_swarm.storage.r2_objects import generation_key, generation_metadata


async def exercise(bucket, module, case):
    config = BoundedR2Configuration(
        workspace_id=identifier("r2-workspace"),
        bucket_identity="local-fixture",
        prefix="async-binding-qualification",
    )
    transport = R2BindingTransport(bucket, module, config)
    objects = BoundedR2Objects(config, transport)
    body = bytes(range(256)) * (4096 if case == "limit" else 1)
    upload = UploadSpec(
        id=identifier("r2-upload-" + ("late" if case == "retired" else case)),
        workspace_id=config.workspace_id,
        actor_id=identifier("r2-actor"),
        digest=content_digest(body),
        size_bytes=len(body),
    )
    blob = StoredBlob(reference=upload.reference(), generation=upload.id)
    key = generation_key(config.prefix, config.workspace_id, blob)

    async def retired():
        try:
            await objects.read(config.workspace_id, blob)
        except ResearchError as error:
            assert error.code == ErrorCode.INVALID_TRANSITION
        else:
            raise AssertionError("Retired payload remained readable")
        assert not await transport.put(key, body, generation_metadata(blob), if_absent=True)
        saved = await transport.get(key, max_bytes=0)
        assert saved.content == b"" and saved.metadata == generation_metadata(blob, retired=True)

    if case in {"roundtrip", "limit"}:
        assert await objects.publish(upload, body) == blob
        assert await objects.read(config.workspace_id, blob) == body
        return {"verified": True, "size_bytes": len(body), "digest": upload.digest}
    if case == "lost-response":
        assert await transport.put(key, body, generation_metadata(blob), if_absent=True)
        # Model a lost successful response by discarding it and opening a fresh adapter.
        reopened = BoundedR2Objects(config, R2BindingTransport(bucket, module, config))
        assert await reopened.publish(upload, body) == blob
        return {"resumed_same_generation": True}
    if case == "late":
        entered, release = asyncio.Event(), asyncio.Event()

        async def late_put():
            entered.set()
            await release.wait()
            return await transport.put(key, body, generation_metadata(blob), if_absent=True)

        delayed = asyncio.create_task(late_put())
        try:
            await entered.wait()
            await objects.retire(config.workspace_id, blob)
            release.set()
            assert await delayed is False
            await retired()
        finally:
            release.set()
            if not delayed.done():
                delayed.cancel()
            await asyncio.gather(delayed, return_exceptions=True)
        return {"late_put_refused": True, "permanent_fence": True}
    if case == "retired":
        await retired()
        return {"permanent_fence": True}
    if case == "corrupt":
        assert await transport.put(key, b"x" * len(body), generation_metadata(blob), if_absent=True)
        try:
            await objects.publish(upload, body)
        except ResearchError as error:
            assert error.code == ErrorCode.ARTIFACT_UNVERIFIED
        else:
            raise AssertionError("Corrupt existing bytes passed publication")
        return {"corruption_refused": True}
    raise ValueError("Unknown R2 binding qualification case")
