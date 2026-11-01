"""Sealed source artifact reads retain exact image custody through awaited byte I/O."""

import asyncio
import json

import pytest

from adapters.cloudflare.checkpoint_artifacts import read_source_artifact
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.types import new_id
from tests.adapters.cloudflare.hosted.test_hosted_router import Gate
from tests.adapters.cloudflare.hosted.test_production_delivery import production as production
from tests.adapters.cloudflare.recovery.test_checkpoint_admission import elected as elected
from tests.adapters.cloudflare.recovery.test_checkpoint_admission_research import source as source
from tests.adapters.cloudflare.recovery.test_checkpoint_artifacts import artifact as artifact
from tests.adapters.cloudflare.recovery.test_checkpoint_artifacts import call as restore
from tests.adapters.cloudflare.recovery.test_checkpoint_capture import sealed as sealed
from tests.adapters.cloudflare.recovery.test_checkpoint_import import recovery as recovery
from tests.adapters.cloudflare.recovery.test_checkpoint_import import snapshot
from tests.adapters.cloudflare.recovery.test_checkpoint_request import imported as imported
from tests.adapters.cloudflare.recovery.test_checkpoint_target import configured as configured
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.projects.test_manifest import project as project
from tests.workers.test_time_witnesses import SEED


def request(e):
    return {
        "artifact_id": e.blob.reference.id,
        "generation": e.blob.generation,
        "backup_id": e.image.manifest.seal.backup_id,
        "manifest_digest": e.image.manifest.digest,
    }


async def read(e, **changes):
    source = e.source
    options = {
        "identity": source.store.identity,
        "credential": source.secrets.bootstrap,
        "source_json": json.dumps(
            {
                "schema_version": "1.0",
                "authority": source.manifest.configuration.authority.model_dump(mode="json"),
                "deployment_digest": source.manifest.digest,
                "pin": source.pin.model_dump(mode="json"),
                "custody_seed_hex": SEED.hex(),
            }
        ),
        "request_json": json.dumps(request(e)),
        "crypto": source.crypto,
        "objects": source.router.objects,
        **changes,
    }
    return await read_source_artifact(
        Gate(), lambda: source.store, source.manifest, source.secrets, **options
    )


async def test_source_export_and_target_restore_preserve_every_sql_row(artifact):
    e = artifact
    before = snapshot(e.source.store), snapshot(e.target)
    first = await read(e)
    assert await read(e) == first
    exported = strict_json_loads(first)
    assert exported["manifest_digest"] == e.image.manifest.digest
    assert exported["artifact"] == e.blob.model_dump(mode="json")
    assert bytes.fromhex(exported["content_hex"]) == e.body
    copied = strict_json_loads(await restore(e, content_hex=exported["content_hex"]))
    assert copied["artifact"] == exported["artifact"]
    assert (snapshot(e.source.store), snapshot(e.target)) == before


@pytest.mark.parametrize(
    "damage", ["bootstrap", "image", "backup", "artifact", "generation", "operator"]
)
async def test_source_rechecks_protected_image_and_current_operator_before_r2(artifact, damage):
    e, options = artifact, {}
    if damage == "bootstrap":
        options["credential"] = "wrong"
    elif damage == "operator":
        e.source.store.write(
            lambda sql: sql.execute(
                "UPDATE credentials SET revoked=1 WHERE id=?",
                (e.source.manifest.credentials[0].id,),
            )
        )
    else:
        payload = request(e)
        field = {
            "image": "manifest_digest",
            "backup": "backup_id",
            "artifact": "artifact_id",
            "generation": "generation",
        }[damage]
        payload[field] = "sha256:" + "a" * 64 if damage == "image" else new_id()
        options["request_json"] = json.dumps(payload)
    before, calls = snapshot(e.source.store), list(e.source.router.objects.transport.calls)
    with pytest.raises(ResearchError):
        await read(e, **options)
    assert e.source.router.objects.transport.calls == calls
    assert snapshot(e.source.store) == before


@pytest.mark.parametrize("damage", ["operator", "evidence", "configuration"])
async def test_read_cannot_return_bytes_after_source_custody_changes(artifact, monkeypatch, damage):
    e = artifact
    transport = e.source.router.objects.transport
    original, arrived, release = transport.get, asyncio.Event(), asyncio.Event()

    async def delayed(*args, **kwargs):
        result = await original(*args, **kwargs)
        arrived.set()
        await release.wait()
        return result

    monkeypatch.setattr(transport, "get", delayed)
    pending = asyncio.create_task(read(e))
    await asyncio.wait_for(arrived.wait(), timeout=5)
    if damage == "operator":
        e.source.store.write(
            lambda sql: sql.execute(
                "UPDATE credentials SET revoked=1 WHERE id=?",
                (e.source.manifest.credentials[0].id,),
            )
        )
    elif damage == "evidence":
        e.source.store.write(
            lambda sql: sql.execute("UPDATE records SET updated_at='changed' WHERE kind='artifact'")
        )
    else:
        transport.configuration_digest = "sha256:" + "a" * 64
    before = snapshot(e.source.store)
    release.set()
    with pytest.raises(ResearchError):
        await pending
    assert snapshot(e.source.store) == before
