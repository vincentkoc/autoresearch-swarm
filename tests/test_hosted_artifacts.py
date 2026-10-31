"""Shared artifact ledger on SQLite with async byte faults; not R2 qualification."""

import asyncio
import io
import zipfile

import pytest

from adapters.cloudflare.research_router import HostedArtifactConfiguration
from research_swarm.domain.artifacts import ArtifactRecord, StoredBlob, UploadSpec
from research_swarm.domain.backup import WorkspaceSeal
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction, encode
from research_swarm.storage.async_r2 import BoundedR2Configuration, BoundedR2Objects, ObjectBytes
from research_swarm.storage.r2_objects import generation_key, generation_metadata
from tests.test_async_r2 import Transport
from tests.test_hosted_router import hosted as hosted


@pytest.fixture
async def artifacts(hosted):
    storage = BoundedR2Configuration(
        workspace_id=hosted.principal.workspace_id,
        bucket_identity="synthetic-bucket",
        prefix="bounded-ledger",
        max_object_bytes=4096,
    )
    config = HostedArtifactConfiguration(
        **hosted.config.model_dump(exclude={"profile"}),
        storage=storage,
        storage_limit_bytes=8192,
    )
    # Explicit fixture provisioning; opening a router cannot install or alter this row.
    hosted.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (config.digest, encode(config.model_dump(mode="json"))),
        )
    )
    transport = Transport(storage)
    objects = BoundedR2Objects(storage, transport)
    hosted.router = await hosted.reopen(configuration=config, objects=objects)
    hosted.config, hosted.transport, hosted.objects = config, transport, objects
    return hosted


async def begin(h, body, *, key=None, media_type="application/octet-stream"):
    return await h.router.command(
        h.operator.token,
        "artifact.begin_upload",
        {
            "digest": content_digest(body),
            "size_bytes": len(body),
            "media_type": media_type,
        },
        idempotency_key=key or new_id(),
    )


async def command(h, operation, upload_id, *, key=None):
    return await h.router.command(
        h.operator.token,
        "artifact." + operation,
        {"upload_id": upload_id},
        idempotency_key=key or new_id(),
    )


async def status(h, upload_id):
    return await h.router.query(
        h.operator.token, "artifact.upload_status", {"upload_id": upload_id}
    )


def current(h, kind, identifier):
    return h.database.read_record(
        CommandContext(principal=h.principal, idempotency_key=new_id()), kind, identifier
    )


async def pending(h, body, *, media_type="application/octet-stream"):
    row = (await begin(h, body, media_type=media_type))["result"]
    await h.router.upload_part(h.operator.token, row["id"], 0, body)
    return row


def blob(row):
    upload = UploadSpec.model_validate(row["data"])
    return StoredBlob(reference=upload.reference(), generation=upload.id)


async def test_source_zip_and_empty_file_follow_ordinary_upload_contract(artifacts):
    h = artifacts
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("train.py", "print('synthetic source')\n")
    for body in (archive.getvalue(), b""):
        row = (await begin(h, body))["result"]
        assert (await status(h, row["id"]))["transfer"] == {
            "schema_version": "1.0",
            "received_bytes": 0,
            "published": False,
        }
        if body:  # The SDK skips parts for zero-byte files.
            assert await h.router.upload_part(h.operator.token, row["id"], 0, body) == len(body)
        result = await command(h, "finalize_upload", row["id"])
        record = ArtifactRecord.model_validate(result["result"]["data"])
        assert record.storage_generation == row["id"]
        assert (
            await h.objects.read(record.workspace_id, h.router.service.artifacts._blob(record))
            == body
        )
        assert (await status(h, row["id"]))["state"] == "finalized"
        assert (await status(h, row["id"]))["transfer"] is None


async def test_uncertain_put_and_witness_failure_reopen_without_premature_publication(artifacts):
    h, body = artifacts, b"bounded replay fixture"
    row = (await begin(h, body))["result"]
    h.transport.lose_put_response = True
    with pytest.raises(ResearchError) as lost:
        await h.router.upload_part(h.operator.token, row["id"], 0, body)
    assert lost.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    h.transport.lose_put_response = False
    assert (await status(h, row["id"]))["transfer"]["published"]
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture source offline")
    with pytest.raises(ResearchError):
        await command(h, "finalize_upload", row["id"], key="final")
    assert current(h, "upload", row["id"])["state"] == "pending"
    with pytest.raises(ResearchError) as absent:
        current(h, "artifact", blob(row).reference.id)
    assert absent.value.code == ErrorCode.NOT_FOUND
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    h.witness.failure = None
    accepted = await command(h, "finalize_upload", row["id"], key="final")
    before = len(h.witness.requests), len(h.transport.calls)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture source offline")
    h.transport.hide_reads = True
    assert await command(h, "finalize_upload", row["id"], key="final") == accepted
    assert (len(h.witness.requests), len(h.transport.calls)) == before


async def test_duplicate_retirement_retains_quota_until_fresh_commit_and_preserves_canonical(
    artifacts,
):
    h, body = artifacts, b"x" * 4096
    first = await pending(h, body, media_type="text/plain")
    canonical = await command(h, "finalize_upload", first["id"])
    second = await pending(h, body, media_type="application/octet-stream")
    duplicate = await command(h, "finalize_upload", second["id"], key="duplicate")
    assert duplicate["result"] == canonical["result"]
    assert (await status(h, second["id"]))["state"] == "cleanup"
    with pytest.raises(ResearchError) as full:
        await begin(h, b"a")
    assert full.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture source offline")
    with pytest.raises(ResearchError):
        await command(h, "cleanup_upload", second["id"], key="cleanup")
    assert current(h, "upload", second["id"])["state"] == "cleanup"
    assert await h.objects.read(h.principal.workspace_id, blob(first)) == body
    with pytest.raises(ResearchError) as retired:
        await h.objects.read(h.principal.workspace_id, blob(second))
    assert retired.value.code == ErrorCode.INVALID_TRANSITION
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    h.witness.failure = None
    with pytest.raises(ResearchError) as still_full:
        await begin(h, b"a")
    assert still_full.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    finished = await command(h, "cleanup_upload", second["id"], key="cleanup")
    assert finished["result"]["state"] == "finalized"
    assert current(h, "artifact", blob(first).reference.id) == canonical["result"]
    await begin(h, b"a")
    before = len(h.witness.requests), len(h.transport.calls)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture source offline")
    h.transport.hide_reads = True
    assert await command(h, "cleanup_upload", second["id"], key="cleanup") == finished
    assert await command(h, "finalize_upload", second["id"], key="duplicate") == duplicate
    assert (len(h.witness.requests), len(h.transport.calls)) == before


async def test_abort_intent_does_not_release_quota_and_late_payload_cannot_resurrect(artifacts):
    h, body = artifacts, b"a" * 4096
    row = (await begin(h, body))["result"]
    await begin(h, b"b" * 4096)
    old = blob(row)
    key = generation_key(h.config.storage.prefix, h.principal.workspace_id, old)
    h.transport.release = asyncio.Event()
    # A prior uncertain transport can complete after the authority call has ended.
    late = asyncio.create_task(h.transport.put(key, body, generation_metadata(old), if_absent=True))
    try:
        await h.transport.arrived.wait()
        intent = await command(h, "abort_upload", row["id"], key="abort")
        assert intent["result"]["state"] == "aborting"
        with pytest.raises(ResearchError) as full:
            await begin(h, b"c")
        assert full.value.code == ErrorCode.RESOURCE_UNAVAILABLE
        finished = await command(h, "cleanup_upload", row["id"], key="retire")
        assert finished["result"]["state"] == "aborted"
        h.transport.release.set()
        assert await late is False
        await begin(h, b"c")
        before = len(h.witness.requests), len(h.transport.calls)
        assert await command(h, "abort_upload", row["id"], key="abort") == intent
        assert (len(h.witness.requests), len(h.transport.calls)) == before
        assert (await status(h, row["id"]))["state"] == "aborted"
    finally:
        h.transport.release.set()
        if not late.done():
            late.cancel()
        await asyncio.gather(late, return_exceptions=True)


@pytest.mark.parametrize("damage", ["corrupt", "retired", "transport"])
async def test_status_never_turns_corruption_or_uncertainty_into_zero_progress(artifacts, damage):
    h, body = artifacts, b"fixture"
    row = await pending(h, body)
    key = next(iter(h.transport.objects))
    if damage == "corrupt":
        h.transport.objects[key] = ObjectBytes(b"wrong", generation_metadata(blob(row)))
    elif damage == "retired":
        await h.objects.retire(h.principal.workspace_id, blob(row))
    else:

        async def unavailable(*args, **kwargs):
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture read failed")

        h.transport.get = unavailable
    with pytest.raises(ResearchError) as refused:
        await status(h, row["id"])
    assert (
        refused.value.code
        == {
            "corrupt": ErrorCode.ARTIFACT_UNVERIFIED,
            "retired": ErrorCode.INVALID_TRANSITION,
            "transport": ErrorCode.TRANSPORT_UNAVAILABLE,
        }[damage]
    )


@pytest.mark.parametrize("actor", ["foreign", "without-read", "driver"])
async def test_raw_storage_checks_owner_read_scope_and_dedicated_driver_before_io(artifacts, actor):
    h, body = artifacts, b"fixture"
    row = (await begin(h, body))["result"]
    principal = h.principal.model_copy(
        update={
            "id": new_id() if actor == "foreign" else h.principal.id,
            "scopes": {"artifact.upload"}
            if actor == "without-read"
            else {"read", "artifact.upload"},
        }
    )
    credential = h.credentials.issue(h.principal, principal)
    if actor == "driver":
        h.store.write(
            lambda sql: sql.execute(
                "INSERT INTO records(workspace_id,kind,id,revision,state,payload,"
                "created_at,updated_at) "
                "VALUES (?,'orchestration_owner',?,0,'active','{}','fixture','fixture')",
                (h.principal.workspace_id, h.principal.id),
            )
        )
    before = len(h.transport.calls), len(h.witness.requests)
    with pytest.raises(ResearchError) as refused:
        await h.router.upload_part(credential.token, row["id"], 0, body)
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert (len(h.transport.calls), len(h.witness.requests)) == before


@pytest.mark.parametrize("change", ["credential", "configuration", "upload"])
async def test_authority_is_rechecked_after_io_and_before_publication(artifacts, change):
    h, body = artifacts, b"fixture"
    row = await pending(h, body)
    original = h.transport.get

    async def changed(*args, **kwargs):
        result = await original(*args, **kwargs)
        if change == "credential":
            h.credentials.revoke(h.principal, h.operator.id)
        elif change == "configuration":
            h.store.write(
                lambda sql: sql.execute("UPDATE hosted_authority_config SET digest='changed'")
            )
        else:
            h.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET state='aborting' WHERE workspace_id=? "
                    "AND kind='upload' AND id=?",
                    (h.principal.workspace_id, row["id"]),
                )
            )
        return result

    h.transport.get = changed
    before = len(h.witness.requests)
    with pytest.raises(ResearchError):
        await command(h, "finalize_upload", row["id"])
    assert len(h.witness.requests) == before
    with pytest.raises(ResearchError) as absent:
        current(h, "artifact", blob(row).reference.id)
    assert absent.value.code == ErrorCode.NOT_FOUND


async def test_exclusive_transfer_finishes_before_competing_abort(artifacts):
    h, body = artifacts, b"fixture"
    row = (await begin(h, body))["result"]
    h.transport.release = asyncio.Event()
    transfer = asyncio.create_task(h.router.upload_part(h.operator.token, row["id"], 0, body))
    abort = None
    try:
        await h.transport.arrived.wait()
        abort = asyncio.create_task(command(h, "abort_upload", row["id"]))
        await h.gate.waiting.wait()
        assert not abort.done()
        h.transport.release.set()
        assert await transfer == len(body)
        assert (await abort)["result"]["state"] == "aborting"
        with pytest.raises(ResearchError) as refused:
            await command(h, "finalize_upload", row["id"])
        assert refused.value.code == ErrorCode.INVALID_TRANSITION
        await command(h, "cleanup_upload", row["id"])
    finally:
        h.transport.release.set()
        tasks = [task for task in (transfer, abort) if task is not None]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_whole_body_bound_and_offset_are_enforced_before_transport(artifacts):
    h, body = artifacts, b"fixture"
    row = (await begin(h, body))["result"]
    for offset, data in ((1, body), (False, body), (0, body[:-1]), (0, b"x" * 4097)):
        with pytest.raises(ResearchError):
            await h.router.upload_part(h.operator.token, row["id"], offset, data)
    assert h.transport.calls == []
    with pytest.raises(ResearchError) as large:
        await begin(h, b"x" * 4097)
    assert large.value.code == ErrorCode.RESOURCE_UNAVAILABLE


async def test_provisioned_artifact_destination_and_quota_cannot_drift(artifacts):
    h = artifacts
    for changed in (
        h.config.model_copy(update={"storage_limit_bytes": 8193}),
        h.config.model_copy(
            update={"storage": h.config.storage.model_copy(update={"prefix": "other"})}
        ),
    ):
        with pytest.raises(ResearchError) as refused:
            await h.reopen(configuration=changed, objects=h.objects)
        assert refused.value.code == ErrorCode.STALE_FENCE
    with pytest.raises(ResearchError):
        await h.reopen(configuration=h.config)
    assert h.transport.calls == []


async def test_sealed_workspace_refuses_storage_before_io(artifacts):
    h, body = artifacts, b"fixture"
    row = (await begin(h, body))["result"]
    seal = WorkspaceSeal(
        backup_id=new_id(),
        workspace_id=h.principal.workspace_id,
        controller_id=h.config.authority.controller_id,
        control_epoch=h.config.authority.control_epoch,
        operator_id=h.principal.id,
        reason="synthetic custody",
        sealed_at="2026-10-31T00:00:00.000Z",
        evidence_through=0,
        unresolved_attempt_ids=(),
        worker_ids=(),
    )
    context = CommandContext(principal=h.principal, idempotency_key=new_id())
    h.store.write(
        lambda sql: Transaction(sql, context).insert(
            "workspace_gate",
            h.principal.workspace_id,
            seal.model_dump(mode="json"),
            state="sealed",
        )
    )
    before = len(h.witness.requests), len(h.transport.calls)
    with pytest.raises(ResearchError) as refused:
        await h.router.upload_part(h.operator.token, row["id"], 0, body)
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert (len(h.witness.requests), len(h.transport.calls)) == before


async def test_duplicate_cleanup_reauthenticates_before_retirement(artifacts):
    h, body = artifacts, b"fixture"
    first = await pending(h, body)
    await command(h, "finalize_upload", first["id"])
    second = await pending(h, body)
    await command(h, "finalize_upload", second["id"])
    original = h.transport.get

    async def revoked(*args, **kwargs):
        value = await original(*args, **kwargs)
        h.credentials.revoke(h.principal, h.operator.id)
        return value

    h.transport.get = revoked
    before = len([call for call in h.transport.calls if call[0] == "put"])
    with pytest.raises(ResearchError) as refused:
        await command(h, "cleanup_upload", second["id"])
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert len([call for call in h.transport.calls if call[0] == "put"]) == before
    assert current(h, "upload", second["id"])["state"] == "cleanup"


@pytest.mark.parametrize("intent", ["cleanup", "aborting"])
async def test_every_retirement_path_refuses_the_canonical_generation(artifacts, intent):
    h, body = artifacts, b"fixture"
    row = await pending(h, body)
    await command(h, "finalize_upload", row["id"])
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE records SET state=? WHERE workspace_id=? AND kind='upload' AND id=?",
            (intent, h.principal.workspace_id, row["id"]),
        )
    )
    before = len(h.transport.calls), len(h.witness.requests)
    with pytest.raises(ResearchError) as refused:
        await command(h, "cleanup_upload", row["id"])
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert (len(h.transport.calls), len(h.witness.requests)) == before


async def test_final_transaction_rechecks_upload_changed_during_witness(artifacts):
    h, body = artifacts, b"fixture"
    row = await pending(h, body)
    original = h.router.acquire

    async def changed(request):
        value = await original(request)
        h.store.write(
            lambda sql: sql.execute(
                "UPDATE records SET state='aborting' WHERE workspace_id=? "
                "AND kind='upload' AND id=?",
                (h.principal.workspace_id, row["id"]),
            )
        )
        return value

    h.router.acquire = changed
    with pytest.raises(ResearchError) as refused:
        await command(h, "finalize_upload", row["id"], key="raced")
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert current(h, "upload", row["id"])["state"] == "aborting"
    with pytest.raises(ResearchError) as absent:
        current(h, "artifact", blob(row).reference.id)
    assert absent.value.code == ErrorCode.NOT_FOUND


async def test_storage_cannot_suppress_cancellation_and_publish_a_record(artifacts):
    h, body = artifacts, b"fixture"
    row = await pending(h, body)
    original = h.transport.get
    entered = asyncio.Event()

    async def suppress_cancel(*args, **kwargs):
        value = await original(*args, **kwargs)
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return value

    h.transport.get = suppress_cancel
    before = len(h.witness.requests)
    task = asyncio.create_task(command(h, "finalize_upload", row["id"]))
    try:
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(h.witness.requests) == before
        assert current(h, "upload", row["id"])["state"] == "pending"
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
