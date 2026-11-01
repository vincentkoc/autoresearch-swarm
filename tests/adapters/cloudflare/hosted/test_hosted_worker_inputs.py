"""Scoped buffered input delivery with shared expiry and current ownership checks."""

import asyncio
from types import SimpleNamespace

import pytest

from adapters.cloudflare.platform.exclusive import exclusive
from adapters.cloudflare.research.research_router import HostedInputConfiguration
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.control.worker_artifacts import WorkerArtifactRead
from research_swarm.domain.artifacts import ArtifactRecord
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import SignedTimeWitness
from research_swarm.domain.types import new_id
from research_swarm.state.database import encode
from research_swarm.storage.async_r2 import ObjectBytes
from research_swarm.storage.r2_objects import generation_key
from tests.adapters.cloudflare.hosted.test_hosted_artifacts import current
from tests.adapters.cloudflare.hosted.test_hosted_http import send
from tests.adapters.cloudflare.hosted.test_hosted_research import admit, enroll_and_claim, sdk
from tests.adapters.cloudflare.hosted.test_hosted_research import research as research
from tests.adapters.cloudflare.hosted.test_hosted_router import counts
from tests.adapters.cloudflare.hosted.test_hosted_router import hosted as hosted
from tests.projects.test_manifest import project as project
from tests.workers.test_time_witnesses import SEED


@pytest.fixture
async def inputs(research, project, tmp_path):
    h, loop = research, asyncio.get_running_loop()
    h.config = HostedInputConfiguration(**h.config.model_dump(exclude={"profile"}))
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    credential = h.credentials.issue(
        h.principal,
        h.worker_credential.principal.model_copy(
            update={"scopes": {"worker.query", "worker.claim", "worker.inputs"}},
        ),
    )
    manifest = project / "research.toml"
    manifest.write_text(
        manifest.read_text().replace(
            'kind = "train"', 'kind = "train"\nrequired_artifacts = ["training"]'
        )
    )

    def prepare():
        data_path = tmp_path / "training.bin"
        data_path.write_bytes(b"synthetic training input\x00\xff")
        with sdk(h, loop) as operator, sdk(h, loop, token=credential.token) as worker:
            data = operator.upload_file(data_path, idempotency_key="training")
            admitted = admit(
                operator, project, input_artifacts={"training": data.model_dump(mode="json")}
            )
            claimed, _ = enroll_and_claim(
                operator, admitted, credential.principal.id, worker_client=worker
            )
        attempt = claimed["result"]
        return data, admitted, attempt

    data, admitted, attempt = await asyncio.to_thread(prepare)
    dispatch = attempt["data"]["dispatch"]
    target = WorkerArtifactRead(
        worker_id=dispatch["worker_id"],
        session_id=dispatch["worker_session_id"],
        attempt_id=attempt["id"],
        artifact_id=attempt["data"]["source_artifact"]["id"],
    )
    return SimpleNamespace(
        h=h, credential=credential, data=data, admitted=admitted, attempt=attempt, target=target
    )


def custody(h):
    return h.store.read(
        lambda sql: (
            sql.execute("SELECT COUNT(*) FROM commands").one()[0],
            sql.execute("SELECT COUNT(*) FROM records WHERE kind='artifact_read_session'").one()[0],
            sql.execute("SELECT COUNT(*) FROM records WHERE kind='artifact_reference'").one()[0],
        )
    )


async def test_restricted_sdk_downloads_source_and_named_input_without_pins(inputs, tmp_path):
    case, loop = inputs, asyncio.get_running_loop()
    credential = case.h.credentials.issue(
        case.h.principal,
        case.credential.principal.model_copy(
            update={"scopes": {"worker.inputs"}},
        ),
    )
    before = custody(case.h)

    def exercise():
        with sdk(case.h, loop, token=credential.token) as worker:
            for target, name in (
                (case.target, "source"),
                (case.target.model_copy(update={"artifact_id": case.data.id}), "training"),
            ):
                reference = worker.worker_input(target)
                output = tmp_path / name
                assert worker.worker_download(target, output) == reference
                if name == "training":
                    assert output.read_bytes() == b"synthetic training input\x00\xff"
                else:
                    assert output.read_bytes().startswith(b"PK")
            with pytest.raises(ResearchError) as denied:
                worker.download(case.data.id, tmp_path / "global")
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED

    await asyncio.to_thread(exercise)
    assert custody(case.h) == before


@pytest.mark.parametrize(
    "fault", ["principal", "session", "artifact", "settled", "admission", "policy"]
)
async def test_invalid_input_authority_refuses_before_storage_or_witness(inputs, fault):
    case, h = inputs, inputs.h
    target, token = case.target, case.credential.token
    if fault == "principal":
        token = h.operator.token
    elif fault in {"session", "artifact"}:
        target = target.model_copy(
            update={"session_id" if fault == "session" else "artifact_id": new_id()}
        )
    else:
        kind, identifier = (
            ("attempt", case.attempt["id"]) if fault == "settled" else ("worker", target.worker_id)
        )
        row = current(h, kind, identifier)
        if fault == "settled":
            row["data"]["publication_status"] = "accepted"
        else:
            row["data"]["clock_admission"] = (
                None
                if fault == "admission"
                else {
                    "profile": "hosted-native-witness-v1",
                    "policy_digest": "sha256:" + "a" * 64,
                }
            )
        h.store.write(
            lambda sql: sql.execute(
                "UPDATE records SET payload=? WHERE kind=? AND id=?",
                (encode(row["data"]), kind, identifier),
            )
        )
    before, calls = custody(h), (len(h.transport.calls), len(h.witness.requests))
    with pytest.raises(ResearchError):
        await h.router.worker_content(token, target.model_dump(mode="json"))
    assert custody(h) == before and (len(h.transport.calls), len(h.witness.requests)) == calls


@pytest.mark.parametrize("fault", ["corrupt", "missing", "retired", "oversized"])
async def test_unverified_input_bytes_never_acquire_authority_or_escape(inputs, fault):
    case, h = inputs, inputs.h
    record = ArtifactRecord.model_validate(current(h, "artifact", case.target.artifact_id)["data"])
    blob = h.router.service.artifacts._blob(record)
    key = generation_key(h.config.storage.prefix, h.principal.workspace_id, blob)
    if fault == "retired":
        await h.objects.retire(h.principal.workspace_id, blob)
    elif fault == "missing":
        del h.transport.objects[key]
    else:
        stored = h.transport.objects[key]
        h.transport.objects[key] = ObjectBytes(
            b"bad" if fault == "corrupt" else bytes(h.config.storage.max_object_bytes + 1),
            stored.metadata,
        )
    before, calls = custody(h), len(h.witness.requests)
    with pytest.raises(ResearchError):
        await h.router.worker_content(case.credential.token, case.target.model_dump(mode="json"))
    assert custody(h) == before and len(h.witness.requests) == calls


async def test_grant_expiring_during_r2_read_cannot_release_bytes(inputs, monkeypatch):
    case, h = inputs, inputs.h
    arrived, release = asyncio.Event(), asyncio.Event()
    original_read, acquire = h.objects.read, h.router.acquire

    async def read(*args):
        assert h.router.boundary._turn is None
        body = await original_read(*args)
        arrived.set()
        await release.wait()
        return body

    async def expired(request):
        value = SignedTimeWitness.model_validate_json(await acquire(request)).payload
        value = value.model_copy(update={"authority_time_ms": value.authority_time_ms + 121_000})
        return canonical_bytes(
            WitnessSigner(h.config.source, SEED).sign(value).model_dump(mode="json")
        )

    monkeypatch.setattr(h.objects, "read", read)
    monkeypatch.setattr(h.router, "acquire", expired)
    before = custody(h)
    task = asyncio.create_task(
        h.router.worker_content(case.credential.token, case.target.model_dump(mode="json"))
    )
    try:
        await asyncio.wait_for(arrived.wait(), 2)
        assert not task.done()
        release.set()
        with pytest.raises(ResearchError) as expired_error:
            await task
        assert expired_error.value.code == ErrorCode.STALE_FENCE
        assert custody(h) == before
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("fault", ["catalog", "credential", "epoch", "cancel"])
async def test_final_input_authorization_rechecks_after_verified_bytes(inputs, monkeypatch, fault):
    case, h = inputs, inputs.h
    original = h.router.acquire

    async def acquire(request):
        if fault == "catalog":
            row = current(h, "artifact", case.target.artifact_id)
            row["data"]["storage_generation"] = new_id()
            h.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET payload=? WHERE kind='artifact' AND id=?",
                    (encode(row["data"]), case.target.artifact_id),
                )
            )
        elif fault == "credential":
            h.credentials.revoke(h.principal, case.credential.id)
        elif fault == "epoch":
            h.witness.epoch = new_id()
        else:
            asyncio.current_task().cancel()
        return await original(request)

    monkeypatch.setattr(h.router, "acquire", acquire)
    before = custody(h)
    task = asyncio.create_task(
        h.router.worker_content(case.credential.token, case.target.model_dump(mode="json"))
    )
    with pytest.raises(asyncio.CancelledError if fault == "cancel" else ResearchError):
        await task
    assert custody(h) == before
    if fault == "epoch":
        assert h.registry.current().state == "unqualified"
    assert not h.gate.lock.locked() and h.router.boundary._turn is None


async def test_input_profile_is_explicit_before_request_body(research):
    h, consumed = research, []
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    for path in ("/v1/worker/input", "/v1/worker/input/content"):
        response = await send(h, "POST", path, payload={}, consume=consumed)
        assert response.status == 501 and consumed == []
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before


async def test_gated_revocation_orders_after_complete_input_read(inputs, monkeypatch):
    case, h = inputs, inputs.h
    arrived, release = asyncio.Event(), asyncio.Event()
    original = h.objects.read

    async def read(*args):
        body = await original(*args)
        arrived.set()
        await release.wait()
        return body

    monkeypatch.setattr(h.objects, "read", read)
    reading = asyncio.create_task(
        h.router.worker_content(case.credential.token, case.target.model_dump(mode="json"))
    )
    revoking = None
    before = custody(h)
    try:
        await asyncio.wait_for(arrived.wait(), 2)

        async def revoke():
            h.credentials.revoke(h.principal, case.credential.id)

        revoking = asyncio.create_task(exclusive(h.gate, revoke))
        await asyncio.wait_for(h.gate.waiting.wait(), 2)
        assert not reading.done() and not revoking.done()
        release.set()
        reference, body = await reading
        assert reference.id == case.target.artifact_id and body.startswith(b"PK")
        await revoking
        calls = len(h.transport.calls), len(h.witness.requests)
        with pytest.raises(ResearchError) as denied:
            await h.router.worker_content(
                case.credential.token, case.target.model_dump(mode="json")
            )
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        assert (len(h.transport.calls), len(h.witness.requests)) == calls and custody(h) == before
    finally:
        release.set()
        tasks = [task for task in (reading, revoking) if task is not None]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
