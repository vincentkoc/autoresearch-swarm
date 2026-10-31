"""Imported generation copies verify actual bytes without adding logical storage charges."""

import asyncio
import json

import pytest

from adapters.cloudflare.checkpoint_artifacts import restore_target_artifact
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.storage.async_r2 import BoundedR2Objects, ObjectBytes
from research_swarm.storage.r2_objects import generation_metadata
from tests.test_async_r2 import Transport
from tests.test_checkpoint_admission import admit
from tests.test_checkpoint_admission import elected as elected
from tests.test_checkpoint_admission_research import source as source
from tests.test_checkpoint_capture import sealed as sealed
from tests.test_checkpoint_import import recovery as recovery
from tests.test_checkpoint_import import snapshot
from tests.test_checkpoint_request import imported as imported
from tests.test_checkpoint_target import configured as configured
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.test_hosted_router import Gate
from tests.test_manifest import project as project
from tests.test_production_delivery import production as production


@pytest.fixture
async def artifact(elected):
    e = elected
    await admit(e)
    e.blob = e.image.manifest.artifacts[0]
    e.body = await e.source.router.objects.read(
        e.manifest.configuration.authority.workspace_id, e.blob
    )
    e.objects = BoundedR2Objects(
        e.manifest.configuration.storage, Transport(e.manifest.configuration.storage)
    )
    return e


async def call(e, **changes):
    options = {
        "identity": e.target.identity,
        "credential": e.source.secrets.bootstrap,
        "target_json": json.dumps(e.target_config),
        "crypto": e.source.crypto,
        "request_json": json.dumps(
            {"artifact_id": e.blob.reference.id, "generation": e.blob.generation}
        ),
        "content_hex": e.body.hex(),
        "objects": e.objects,
        **changes,
    }
    return await restore_target_artifact(
        Gate(), lambda: e.target, e.manifest, e.source.secrets, **options
    )


async def test_exact_generation_copy_and_retry_leave_every_sql_row_unchanged(artifact):
    e = artifact
    before, source_before = snapshot(e.target), snapshot(e.source.store)
    calls = len(e.source.witness.requests)
    e.source.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic offline")
    first, repeated = await asyncio.gather(call(e), call(e))
    assert first == repeated
    receipt = strict_json_loads(first)
    assert receipt["artifact"] == e.blob.model_dump(mode="json")
    assert receipt["restore_id"] == e.intent.restore_id
    assert receipt["storage_digest"] == e.objects.config.digest
    assert len(e.objects.transport.objects) == 1
    assert await e.objects.read(e.objects.config.workspace_id, e.blob) == e.body
    assert snapshot(e.target) == before and snapshot(e.source.store) == source_before
    assert len(e.source.witness.requests) == calls


async def test_lost_put_reply_resumes_existing_generation_and_requires_readback(artifact):
    e = artifact
    before = snapshot(e.target)
    e.objects.transport.lose_put_response = True
    with pytest.raises(ResearchError) as uncertain:
        await call(e)
    assert uncertain.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    assert len(e.objects.transport.objects) == 1
    e.objects.transport.lose_put_response = False
    e.objects = BoundedR2Objects(e.objects.config, e.objects.transport)
    result = await call(e)
    assert strict_json_loads(result)["artifact"] == e.blob.model_dump(mode="json")
    assert [op[0] for op in e.objects.transport.calls] == ["put", "put", "get"]
    assert snapshot(e.target) == before


@pytest.mark.parametrize("damage", ["wrong_body", "missing", "metadata", "retired"])
async def test_successful_or_conditional_put_cannot_replace_byte_verification(artifact, damage):
    e = artifact
    await call(e)
    key = next(iter(e.objects.transport.objects))
    if damage == "wrong_body":
        e.objects.transport.objects[key] = ObjectBytes(
            b"x" * len(e.body), generation_metadata(e.blob)
        )
    elif damage == "metadata":
        e.objects.transport.objects[key] = ObjectBytes(e.body, {"wrong": "metadata"})
    elif damage == "retired":
        e.objects.transport.objects[key] = ObjectBytes(
            b"", generation_metadata(e.blob, retired=True)
        )
    else:
        e.objects.transport.hide_reads = True
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await call(e)
    assert snapshot(e.target) == before


@pytest.mark.parametrize(
    "damage",
    [
        "bootstrap",
        "generation",
        "artifact",
        "bytes",
        "uppercase",
        "oversize",
        "destination",
        "proof",
    ],
)
async def test_invalid_inputs_never_touch_r2(artifact, damage):
    e, options = artifact, {}
    if damage == "bootstrap":
        options["credential"] = "wrong"
    elif damage in {"generation", "artifact"}:
        request = {"artifact_id": e.blob.reference.id, "generation": e.blob.generation}
        request["artifact_id" if damage == "artifact" else "generation"] = new_id()
        options["request_json"] = json.dumps(request)
    elif damage == "bytes":
        options["content_hex"] = (b"x" * len(e.body)).hex()
    elif damage == "uppercase":
        options["content_hex"] = "FF"
    elif damage == "oversize":
        options["content_hex"] = "a" * (2 * e.objects.config.max_object_bytes + 1)
    elif damage == "destination":
        changed = e.objects.config.model_copy(update={"bucket_identity": "other-bucket"})
        options["objects"] = BoundedR2Objects(changed, Transport(changed))
    else:
        e.target.write(
            lambda sql: sql.execute("DELETE FROM records WHERE kind='logical_recovery_election'")
        )
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await call(e, **options)
    assert e.objects.transport.calls == []
    if "objects" in options:
        assert options["objects"].transport.calls == []
    assert snapshot(e.target) == before


@pytest.mark.parametrize("damage", ["catalog", "admission", "configuration"])
async def test_awaited_copy_rechecks_catalog_admission_and_destination(artifact, damage):
    e = artifact
    transport = e.objects.transport
    transport.release = asyncio.Event()
    pending = asyncio.create_task(call(e))
    await asyncio.wait_for(transport.arrived.wait(), timeout=5)
    if damage == "catalog":
        e.target.write(
            lambda sql: sql.execute(
                "UPDATE records SET revision=revision+1 WHERE kind='artifact' AND id=?",
                (e.blob.reference.id,),
            )
        )
    elif damage == "admission":
        e.target.write(
            lambda sql: sql.execute("UPDATE records SET state='active' WHERE kind='workspace_gate'")
        )
    else:
        transport.configuration_digest = "sha256:" + "a" * 64
    before = snapshot(e.target)
    transport.release.set()
    with pytest.raises(ResearchError):
        await pending
    assert snapshot(e.target) == before
