"""Attempt-scoped R2 publication faults; provider qualification is separate."""

import asyncio
from urllib.parse import urlencode

import pytest

from adapters.cloudflare.research_router import HostedOutputConfiguration
from research_swarm.control.worker_outputs import WorkerOutputIntent, WorkerUploadTarget
from research_swarm.control.worker_records import WorkerData
from research_swarm.domain.artifacts import StoredBlob, UploadSpec
from research_swarm.domain.backup import WorkspaceSeal
from research_swarm.domain.canonical import content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction, encode
from research_swarm.storage.async_r2 import ObjectBytes
from research_swarm.storage.r2_objects import generation_key
from tests.adapters.cloudflare.hosted.test_hosted_artifacts import current
from tests.adapters.cloudflare.hosted.test_hosted_http import send
from tests.adapters.cloudflare.hosted.test_hosted_research import research as research
from tests.adapters.cloudflare.hosted.test_hosted_research import sdk
from tests.adapters.cloudflare.hosted.test_hosted_router import counts
from tests.adapters.cloudflare.hosted.test_hosted_router import hosted as hosted
from tests.adapters.cloudflare.hosted.test_hosted_worker_inputs import inputs as inputs
from tests.projects.test_manifest import project as project


@pytest.fixture
async def outputs(inputs):
    case, h = inputs, inputs.h
    h.config = HostedOutputConfiguration(**h.config.model_dump(exclude={"profile"}))
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    case.credential = h.credentials.issue(
        h.principal, case.credential.principal.model_copy(update={"scopes": {"worker.outputs"}})
    )
    case.intent = WorkerOutputIntent(
        **case.target.model_dump(exclude={"schema_version", "artifact_id"}), role="journal"
    )
    return case


async def begin(case, body, *, key=None, **changes):
    request = {
        **case.intent.model_dump(mode="json"),
        **changes,
        "content": {"digest": content_digest(body), "size_bytes": len(body)},
    }
    result = await case.h.router.command(
        case.credential.token, "worker.begin_upload", request, idempotency_key=key or new_id()
    )
    return WorkerUploadTarget(
        **case.intent.model_dump(exclude={"schema_version", "role", "output_name"}),
        upload_id=result["result"]["id"],
    )


async def command(case, operation, target, *, key=None):
    return await case.h.router.command(
        case.credential.token,
        "worker." + operation,
        target.model_dump(mode="json"),
        idempotency_key=key or new_id(),
    )


async def pending(case, body):
    target = await begin(case, body)
    if body:
        await case.h.router.worker_upload_part(
            case.credential.token, target.model_dump(mode="json"), 0, body
        )
    return target


def blob(case, target):
    spec = UploadSpec.model_validate(current(case.h, "upload", target.upload_id)["data"])
    return StoredBlob(reference=spec.reference(), generation=spec.id)


def staging(case):
    context = CommandContext(principal=case.h.principal, idempotency_key=new_id())
    return case.h.router.service.artifacts.occupancy(context)["staging_reserved_bytes"]


async def test_ordinary_sdk_uploads_binary_empty_and_duplicate_with_only_output_scope(
    outputs, tmp_path
):
    case, h, loop = outputs, outputs.h, asyncio.get_running_loop()
    before_attempt = current(h, "attempt", case.attempt["id"])
    body = b"scoped research output\x00\xff"

    def exercise():
        path = tmp_path / "output.bin"
        path.write_bytes(body)
        with sdk(h, loop) as operator, sdk(h, loop, token=case.credential.token) as worker:
            canonical = operator.upload_file(path, idempotency_key="operator-copy")
            observed = worker.worker_upload_file(case.intent, path, idempotency_key="journal")
            assert observed == canonical
            # Identical bytes retain distinct role-specific provenance.
            assert (
                worker.worker_upload_file(
                    case.intent.model_copy(update={"role": "report"}),
                    path,
                    idempotency_key="report",
                )
                == canonical
            )
            path.write_bytes(b"")
            assert (
                worker.worker_upload_file(case.intent, path, idempotency_key="empty").size_bytes
                == 0
            )
            with pytest.raises(ResearchError) as denied:
                worker.upload_file(path, idempotency_key="generic")
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED

    await asyncio.to_thread(exercise)
    rows = h.store.read(
        lambda sql: sql.execute(
            "SELECT id,payload FROM records WHERE kind='worker_upload'"
        ).fetchall()
    )
    assert len(rows) == 3 and staging(case) == 2 * len(body)
    for row in rows:
        data = strict_json_loads(row["payload"])
        assert data["retention_id"] is not None and data["attempt_id"] == case.attempt["id"]
        target = WorkerUploadTarget(
            **case.intent.model_dump(exclude={"role", "output_name"}), upload_id=row["id"]
        )
        result = await command(case, "cleanup_upload", target)
        assert result["result"]["state"] == "finalized"
    assert staging(case) == 0
    assert current(h, "attempt", case.attempt["id"]) == before_attempt
    assert (
        h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM budget_settlements").one()[0])
        == 0
    )


async def test_finalize_and_cleanup_replay_without_storage_or_witness_after_reopen(outputs):
    case, h = outputs, outputs.h
    target = await pending(case, b"replay")
    accepted = await command(case, "finalize_upload", target, key="finalize")
    cleaned = await command(case, "cleanup_upload", target, key="cleanup")
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Source unavailable")
    h.transport.hide_reads = True
    before = len(h.transport.calls), len(h.witness.requests)
    assert await command(case, "finalize_upload", target, key="finalize") == accepted
    assert await command(case, "cleanup_upload", target, key="cleanup") == cleaned
    assert (len(h.transport.calls), len(h.witness.requests)) == before
    h.credentials.revoke(h.principal, case.credential.id)
    with pytest.raises(ResearchError) as denied:
        await command(case, "finalize_upload", target, key="finalize")
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED


@pytest.mark.parametrize("materialized", [False, True])
async def test_abort_cleanup_keeps_quota_through_uncertain_retirement(outputs, materialized):
    case, h = outputs, outputs.h
    target = await (pending(case, b"unpublished") if materialized else begin(case, b"unpublished"))
    accepted = await command(case, "abort_upload", target, key="abort")
    assert accepted["result"]["state"] == "aborting"
    assert staging(case) == len(b"unpublished")
    h.transport.lose_put_response = True
    with pytest.raises(ResearchError):
        await command(case, "cleanup_upload", target, key="cleanup")
    assert current(h, "upload", target.upload_id)["state"] == "aborting"
    assert current(h, "worker_upload", target.upload_id)["state"] == "pending"
    assert staging(case) == len(b"unpublished")
    h.transport.lose_put_response = False
    cleaned = await command(case, "cleanup_upload", target, key="cleanup")
    assert cleaned["result"]["state"] == "aborted"
    assert current(h, "worker_upload", target.upload_id)["state"] == "aborted"
    assert staging(case) == 0
    assert await command(case, "abort_upload", target, key="abort") == accepted
    with pytest.raises(ResearchError):
        await h.objects.read(h.principal.workspace_id, blob(case, target))


async def test_duplicate_cleanup_preserves_healthy_copy_if_canonical_bytes_are_corrupt(outputs):
    case, h = outputs, outputs.h
    first = await pending(case, b"duplicate")
    await command(case, "finalize_upload", first)
    second = await pending(case, b"duplicate")
    await command(case, "finalize_upload", second)
    canonical = blob(case, first)
    key = generation_key(h.config.storage.prefix, h.principal.workspace_id, canonical)
    saved = h.transport.objects[key]
    h.transport.objects[key] = ObjectBytes(b"corrupted", saved.metadata)
    with pytest.raises(ResearchError):
        await command(case, "cleanup_upload", second)
    assert current(h, "upload", second.upload_id)["state"] == "cleanup"
    assert await h.objects.read(h.principal.workspace_id, blob(case, second)) == b"duplicate"
    assert staging(case) == len(b"duplicate")
    h.transport.objects[key] = saved
    await command(case, "cleanup_upload", second)
    assert await h.objects.read(h.principal.workspace_id, canonical) == b"duplicate"


@pytest.mark.parametrize("fault", ["principal", "worker_id", "session_id", "attempt_id"])
async def test_wrong_owner_refuses_all_output_routes_before_bytes(outputs, fault):
    case, h = outputs, outputs.h
    target = await begin(case, b"owned")
    token = case.credential.token
    if fault == "principal":
        token = h.operator.token
    else:
        target = target.model_copy(update={fault: new_id()})
    payload = target.model_dump(mode="json")
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    for operation in ("finalize_upload", "abort_upload", "cleanup_upload"):
        with pytest.raises(ResearchError):
            await h.router.command(token, "worker." + operation, payload, idempotency_key=new_id())
    with pytest.raises(ResearchError):
        await h.router.worker_upload_part(token, payload, 0, b"owned")
    with pytest.raises(ResearchError):
        await h.router.query(token, "worker.output", payload)
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before


@pytest.mark.parametrize("fault", ["credential", "worker", "catalog", "cancel"])
async def test_publication_rechecks_after_fresh_witness(outputs, monkeypatch, fault):
    case, h = outputs, outputs.h
    target = await pending(case, b"uncommitted")
    original = h.router.acquire

    async def acquire(request):
        result = await original(request)
        if fault == "credential":
            h.credentials.revoke(h.principal, case.credential.id)
        elif fault == "worker":
            h.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET state='revoked' WHERE kind='worker' AND id=?",
                    (target.worker_id,),
                )
            )
        elif fault == "catalog":
            # A captured absence must also remain absent at publication.
            other = current(h, "artifact", case.data.id)
            h.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET id=? WHERE kind='artifact' AND id=?",
                    (blob(case, target).reference.id, other["id"]),
                )
            )
        else:
            asyncio.current_task().cancel()
        return result

    monkeypatch.setattr(h.router, "acquire", acquire)
    task = asyncio.create_task(command(case, "finalize_upload", target))
    with pytest.raises((ResearchError, asyncio.CancelledError)):
        await task
    assert current(h, "worker_upload", target.upload_id)["state"] == "pending"
    assert current(h, "upload", target.upload_id)["state"] == "pending"


@pytest.mark.parametrize("state", ["fenced", "replacement", "revoked"])
async def test_historical_upload_owner_is_distinct_from_current_execution_authority(outputs, state):
    case, h = outputs, outputs.h
    row = current(h, "worker", case.intent.worker_id)
    if state == "replacement":
        row["data"]["binding"]["worker_session_id"] = new_id()
        row["data"]["assignment_authority"]["binding"] = dict(row["data"]["binding"])
        WorkerData.model_validate(row["data"])
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE records SET state=?,payload=? WHERE kind='worker' AND id=?",
            (
                "active" if state == "replacement" else state,
                encode(row["data"]),
                row["id"],
            ),
        )
    )
    if state == "revoked":
        with pytest.raises(ResearchError) as denied:
            await begin(case, b"historical")
        assert denied.value.code == ErrorCode.STALE_FENCE
    else:
        target = await pending(case, b"historical")
        accepted = await command(case, "finalize_upload", target)
        assert accepted["result"]["upload_id"] == target.upload_id
        assert current(h, "attempt", case.attempt["id"])["data"]["publication_status"] == "pending"


async def test_admitted_append_orders_before_abort_and_retirement(outputs, monkeypatch):
    case, h = outputs, outputs.h
    target = await begin(case, b"ordered")
    reached, release = asyncio.Event(), asyncio.Event()
    original = h.objects.publish

    async def held(*args):
        reached.set()
        await release.wait()
        return await original(*args)

    monkeypatch.setattr(h.objects, "publish", held)
    append = asyncio.create_task(
        h.router.worker_upload_part(
            case.credential.token, target.model_dump(mode="json"), 0, b"ordered"
        )
    )
    abort = None
    try:
        await asyncio.wait_for(reached.wait(), 2)
        h.gate.waiting.clear()
        abort = asyncio.create_task(command(case, "abort_upload", target))
        await asyncio.wait_for(h.gate.waiting.wait(), 2)
        assert not abort.done()
        release.set()
        assert await append == len(b"ordered")
        assert (await abort)["result"]["state"] == "aborting"
        await command(case, "cleanup_upload", target)
        assert staging(case) == 0
    finally:
        release.set()
        for task in (append, abort):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (append, abort) if task), return_exceptions=True)


async def test_input_only_profile_refuses_output_routes_before_body(inputs):
    case, h = inputs, inputs.h
    consume = []
    target = {
        **case.target.model_dump(exclude={"artifact_id", "schema_version"}),
        "upload_id": new_id(),
    }
    for method, path in (
        ("POST", "/v1/worker/output"),
        ("POST", "/v1/commands/worker.begin_upload"),
        ("PUT", "/v1/worker/output/parts?" + urlencode(target)),
    ):
        response = await send(h, method, path, body=b"unused", consume=consume)
        assert response.status == 501
        assert strict_json_loads(response.body)["error"]["code"] == "CAPABILITY_UNAVAILABLE"
    assert consume == []


async def test_pending_cleanup_undeclared_output_and_incomplete_part_are_refused(outputs):
    case, h = outputs, outputs.h
    before = counts(h)
    with pytest.raises(ResearchError) as denied:
        await begin(case, b"invalid", role="output", output_name="undeclared")
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    # The shared command service persists the rejected outcome for exact replay.
    assert counts(h) == (before[0] + 1, *before[1:])
    target = await begin(case, b"complete")
    with pytest.raises(ResearchError):
        await command(case, "cleanup_upload", target)
    before_calls = len(h.transport.calls)
    for offset, body in ((0, b"short"), (1, b"complete")):
        with pytest.raises(ResearchError):
            await h.router.worker_upload_part(
                case.credential.token, target.model_dump(mode="json"), offset, body
            )
    assert len(h.transport.calls) == before_calls
    assert current(h, "upload", target.upload_id)["state"] == "pending"


@pytest.mark.parametrize("phase", ["sealed", "recovering"])
async def test_inactive_workspace_refuses_every_raw_output_route_before_io(outputs, phase):
    case, h = outputs, outputs.h
    target = await pending(case, b"guarded")
    seal = WorkspaceSeal(
        backup_id=new_id(),
        workspace_id=h.principal.workspace_id,
        controller_id=h.config.authority.controller_id,
        control_epoch=h.config.authority.control_epoch,
        operator_id=h.principal.id,
        reason="Synthetic custody",
        sealed_at="2026-10-31T00:00:00.000Z",
        evidence_through=0,
        unresolved_attempt_ids=(case.attempt["id"],),
        worker_ids=(case.intent.worker_id,),
    )
    context = CommandContext(principal=h.principal, idempotency_key=new_id())
    h.store.write(
        lambda sql: Transaction(sql, context).insert(
            "workspace_gate", h.principal.workspace_id, seal.model_dump(mode="json"), state=phase
        )
    )
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    with pytest.raises(ResearchError):
        await begin(case, b"new")
    for operation in ("finalize_upload", "abort_upload", "cleanup_upload"):
        with pytest.raises(ResearchError):
            await command(case, operation, target)
    with pytest.raises(ResearchError):
        await h.router.worker_upload_part(
            case.credential.token, target.model_dump(mode="json"), 0, b"guarded"
        )
    with pytest.raises(ResearchError):
        await h.router.query(case.credential.token, "worker.output", target.model_dump(mode="json"))
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before


@pytest.mark.parametrize("fault", ["corrupt", "missing", "retired"])
async def test_unverified_output_generation_cannot_publish_provenance(outputs, fault):
    case, h = outputs, outputs.h
    target = await pending(case, b"verified")
    own = blob(case, target)
    key = generation_key(h.config.storage.prefix, h.principal.workspace_id, own)
    if fault == "retired":
        await h.objects.retire(h.principal.workspace_id, own)
    elif fault == "missing":
        del h.transport.objects[key]
    else:
        h.transport.objects[key] = ObjectBytes(b"tampered", h.transport.objects[key].metadata)
    before = counts(h), len(h.witness.requests)
    with pytest.raises(ResearchError):
        await command(case, "finalize_upload", target)
    assert (counts(h), len(h.witness.requests)) == before
    assert current(h, "worker_upload", target.upload_id)["state"] == "pending"
