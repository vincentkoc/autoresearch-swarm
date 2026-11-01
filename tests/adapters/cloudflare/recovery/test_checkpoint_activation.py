"""Atomic hosted activation on SQLite/mock R2; actual workerd is separate evidence."""

import asyncio
import json

import pytest

from adapters.cloudflare.platform.runtime import open_router
from adapters.cloudflare.recovery import checkpoint_activation
from adapters.cloudflare.recovery.checkpoint_activation import activate_target
from research_swarm.control.auth import CredentialStore
from research_swarm.domain.artifacts import ArtifactRecord, UploadSpec
from research_swarm.domain.canonical import content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.database import Database, Transaction
from research_swarm.storage.async_r2 import BoundedR2Objects, ObjectBytes
from research_swarm.storage.r2_objects import generation_metadata
from tests.adapters.cloudflare.hosted import test_hosted_activation_cut as fixtures
from tests.adapters.cloudflare.hosted.test_hosted_router import Gate
from tests.adapters.cloudflare.recovery.test_checkpoint_import import make_recovery, snapshot
from tests.adapters.cloudflare.recovery.test_checkpoint_runtime import fresh_target, records
from tests.storage.test_async_r2 import Transport

deployment = fixtures.deployment
bootstrap = fixtures.bootstrap
source = fixtures.source
sealed = fixtures.sealed
configured = fixtures.configured
imported = fixtures.imported
elected = fixtures.elected
target = fixtures.target
prepared = fixtures.prepared


@pytest.fixture
async def recovery(sealed, tmp_path):
    def configured_target(raw, source):
        raw["configuration"]["storage_limit_bytes"] = 1024
        return fresh_target(raw, source)

    return make_recovery(sealed, tmp_path, configure_target=configured_target)


@pytest.fixture
async def ready(prepared):
    e = prepared
    e.objects = BoundedR2Objects(
        e.manifest.configuration.storage, Transport(e.manifest.configuration.storage)
    )
    e.activation_request = {
        "request_id": new_id(),
        "expected_revision": records(e, "workspace_gate")[0]["revision"],
        "payload": {
            "restore_id": e.intent.restore_id,
            "expected_control_epoch": 2,
            "reason": "Original obligations closed; enable elected runtime",
        },
    }
    return e


async def activate(e, *, store=None, state=None, **changes):
    options = {
        "identity": e.target.identity,
        "credential": e.target_secrets.bootstrap,
        "target_json": json.dumps(e.target_config),
        "request_json": json.dumps(e.activation_request),
        "crypto": e.source.crypto,
        "objects": e.objects,
        "acquire": e.witness,
        **changes,
    }
    return await activate_target(
        e.state if state is None else state,
        lambda: e.target if store is None else store,
        e.manifest,
        e.target_secrets,
        **options,
    )


async def opened(e, **changes):
    return await open_router(
        Gate(),
        lambda: e.target,
        e.manifest,
        e.target_secrets,
        crypto=e.source.crypto,
        acquire=e.witness,
        objects=e.objects,
        **{"recovery_binding": e.binding, **changes},
    )


async def test_activation_retires_original_credentials_and_reopens_without_changing_accounting(
    ready,
):
    e = ready
    before, original = snapshot(e.target), snapshot(e.source.store)
    raw = await activate(e)
    reply = strict_json_loads(raw)
    assert reply["disposition"] == "accepted"
    assert reply["result"]["data"]["profile"] == "hosted-recovery-activation-v1"
    assert records(e, "workspace_gate")[0]["state"] == "active"
    after = snapshot(e.target)
    for name in ("budgets", "budget_reservations", "budget_settlements", "budget_entries"):
        assert before[name] == after[name]
    assert snapshot(e.source.store) == original
    credentials = CredentialStore.open_existing(Database.from_store(e.target))
    for token in e.source.secrets.credentials.values():
        with pytest.raises(ResearchError):
            credentials.resolve(token)
    for member in e.manifest.credentials:
        assert credentials.resolve(e.target_secrets.credentials[member.id]) == member.principal
    calls = len(e.witness.requests)
    e.witness.failure = TimeoutError("Synthetic witness offline")
    router = await opened(e)
    assert router.database.authority_identity == e.target.identity
    assert len(e.witness.requests) == calls
    assert await activate(e) == raw
    assert snapshot(e.target) == after
    assert not records(e, "run_result")
    with pytest.raises(ResearchError):
        await opened(e, recovery_binding=None)


@pytest.mark.parametrize("phase", ["retirement", "deployment", "event", "generated_bounds"])
async def test_failure_rolls_back_retirement_deployment_gate_and_ack(ready, monkeypatch, phase):
    e = ready
    before = snapshot(e.target)
    original_intent = checkpoint_activation.write_deployment_intent
    original_complete = checkpoint_activation.complete_deployment_in
    original_event = Transaction.event
    original_bounds = checkpoint_activation.require_activation_bounds

    def fail_intent(*args):
        original_intent(*args)
        raise RuntimeError("Synthetic activation rollback")

    def fail_complete(*args):
        original_complete(*args)
        raise RuntimeError("Synthetic activation rollback")

    def fail_event(self, name, *args, **kwargs):
        result = original_event(self, name, *args, **kwargs)
        if name == "workspace.recovery_activated":
            raise RuntimeError("Synthetic activation rollback")
        return result

    def fail_bounds(sql):
        original_bounds(sql)
        if sql.execute("SELECT 1 FROM records WHERE kind='recovery_activation'").fetchone():
            raise RuntimeError("Synthetic activation rollback")

    with monkeypatch.context() as patch:
        if phase == "retirement":
            patch.setattr(checkpoint_activation, "write_deployment_intent", fail_intent)
        elif phase == "deployment":
            patch.setattr(checkpoint_activation, "complete_deployment_in", fail_complete)
        elif phase == "event":
            patch.setattr(Transaction, "event", fail_event)
        else:
            patch.setattr(checkpoint_activation, "require_activation_bounds", fail_bounds)
        with pytest.raises(RuntimeError, match="Synthetic activation rollback"):
            await activate(e)
    assert snapshot(e.target) == before
    assert strict_json_loads(await activate(e))["disposition"] == "accepted"


async def test_lost_acknowledgment_replays_offline_and_rejects_new_intent(ready):
    e = ready

    class LostReply:
        identity = e.target.identity
        read = e.target.read

        def write(self, callback):
            e.target.write(callback)
            raise ConnectionError("Synthetic lost accepted response")

    with pytest.raises(ConnectionError):
        await activate(e, store=LostReply())
    before, calls = snapshot(e.target), len(e.witness.requests)
    e.state = Gate()
    e.witness.failure = TimeoutError("Synthetic offline")
    e.objects.transport.hide_reads = True
    first = await activate(e)
    assert await activate(e) == first
    assert len(e.witness.requests) == calls and snapshot(e.target) == before
    changed = dict(e.activation_request, request_id=new_id())
    with pytest.raises(ResearchError) as failure:
        await activate(e, request_json=json.dumps(changed))
    assert failure.value.code == ErrorCode.IDEMPOTENCY_CONFLICT


@pytest.mark.parametrize("change", ["revision", "epoch", "restore", "bootstrap", "admission"])
async def test_invalid_activation_never_acquires_a_final_witness(ready, change):
    e = ready
    options = {}
    if change == "revision":
        e.activation_request["expected_revision"] += 1
    elif change in {"epoch", "restore"}:
        e.activation_request["payload"][
            "expected_control_epoch" if change == "epoch" else "restore_id"
        ] = 3 if change == "epoch" else new_id()
    elif change == "bootstrap":
        options["credential"] = "wrong"
    else:
        e.target.write(
            lambda sql: sql.execute(
                "UPDATE records SET payload=json_set(payload,'$.target_signature',?) "
                "WHERE kind='recovery_admission'",
                ("0" * 128,),
            )
        )
    before, calls = snapshot(e.target), len(e.witness.requests)
    with pytest.raises(ResearchError):
        await activate(e, **options)
    assert snapshot(e.target) == before and len(e.witness.requests) == calls


@pytest.mark.parametrize("change", ["provider", "credential", "oversize", "column", "admission"])
async def test_final_transaction_rechecks_changes_during_witness_io(ready, change):
    e = ready
    damaged = None

    async def acquire(request):
        nonlocal damaged
        response = await e.witness(request)

        def mutate(sql):
            if change == "provider":
                sql.execute("UPDATE records SET revision=revision+1 WHERE kind='time_provider'")
            elif change == "credential":
                sql.execute(
                    "UPDATE credentials SET revoked=1 WHERE id=?", (e.manifest.credentials[0].id,)
                )
            elif change == "oversize":
                sql.execute(
                    "UPDATE records SET payload=payload || ? WHERE kind='recovery_runtime_plan'",
                    (" " * 65536,),
                )
            elif change == "column":
                sql.execute("ALTER TABLE records ADD COLUMN unexpected TEXT")
            else:
                sql.execute("UPDATE records SET state='mutable' WHERE kind='recovery_admission'")

        e.target.write(mutate)
        damaged = snapshot(e.target)
        return response

    with pytest.raises(ResearchError):
        await activate(e, acquire=acquire)
    assert snapshot(e.target) == damaged
    assert records(e, "workspace_gate")[0]["state"] == "recovering"


async def test_witness_loss_and_epoch_change_preserve_activation_intent(ready):
    e = ready
    before = snapshot(e.target)
    e.witness.failure = TimeoutError("Synthetic witness loss")
    with pytest.raises(ResearchError):
        await activate(e)
    assert snapshot(e.target) == before
    e.witness.failure = None
    original_epoch = e.witness.epoch
    e.witness.epoch = new_id()
    with pytest.raises(ResearchError):
        await activate(e)
    fenced = snapshot(e.target)
    assert fenced["commands"] == before["commands"]
    assert fenced["credentials"] == before["credentials"]
    assert len(records(e, "time_source_retirement")) == 1
    assert records(e, "workspace_gate")[0]["state"] == "recovering"
    e.witness.epoch = original_epoch
    with pytest.raises(ResearchError):
        await activate(e)
    assert snapshot(e.target) == fenced


async def test_concurrent_exact_requests_share_one_activation_and_ack(ready):
    e = ready
    before = len(e.witness.requests)
    first, second = await asyncio.gather(activate(e), activate(e))
    assert first == second and len(records(e, "recovery_activation")) == 1
    assert len(e.witness.requests) == before + 1


async def publish(e, body=b"verified activation bytes"):
    upload = UploadSpec(
        id=new_id(),
        workspace_id=e.context.principal.workspace_id,
        actor_id=e.context.principal.id,
        digest=content_digest(body),
        size_bytes=len(body),
    )
    blob = await e.objects.publish(upload, body)
    record = ArtifactRecord(
        reference=blob.reference,
        storage_generation=blob.generation,
        workspace_id=upload.workspace_id,
        published_by=upload.actor_id,
        verified_at=utc_now(),
    )
    e.target.write(
        lambda sql: Transaction(sql, e.context).insert(
            "artifact", blob.reference.id, record.model_dump(mode="json"), state="verified"
        )
    )
    return blob, body


@pytest.mark.parametrize("damage", [None, "missing", "body", "quota", "changed_cut"])
async def test_actual_artifact_bytes_and_target_quota_precede_final_witness(
    ready, monkeypatch, damage
):
    e = ready
    blob, body = await publish(e, b"x" * (1025 if damage == "quota" else 32))
    key = next(iter(e.objects.transport.objects))
    if damage == "missing":
        e.objects.transport.hide_reads = True
    elif damage == "body":
        e.objects.transport.objects[key] = ObjectBytes(b"z" * len(body), generation_metadata(blob))
    elif damage == "changed_cut":
        original = e.objects.read

        async def read(*args):
            value = await original(*args)
            e.target.write(
                lambda sql: sql.execute(
                    "UPDATE records SET revision=revision+1 WHERE kind='artifact'"
                )
            )
            return value

        monkeypatch.setattr(e.objects, "read", read)
    before, calls = snapshot(e.target), len(e.witness.requests)
    if damage is not None:
        with pytest.raises(ResearchError):
            await activate(e)
        assert len(e.witness.requests) == calls
        assert not records(e, "recovery_activation")
        if damage != "changed_cut":
            assert snapshot(e.target) == before
        return
    raw = await activate(e)
    assert len(e.witness.requests) == calls + 1
    assert e.objects.transport.calls[-1][0] == "get"
    e.objects.transport.hide_reads = True
    e.witness.failure = TimeoutError("Synthetic offline")
    assert await activate(e) == raw


async def test_cancelled_activation_does_not_publish_partial_deployment(ready):
    e = ready
    before = snapshot(e.target)
    e.witness.entered.clear()
    e.witness.release = asyncio.Event()
    pending = asyncio.create_task(activate(e))
    await e.witness.entered.wait()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert snapshot(e.target) == before


async def test_independent_operation_gates_converge_on_exact_committed_ack(ready):
    e = ready
    arrived, both = 0, asyncio.Event()

    async def acquire(request):
        nonlocal arrived
        arrived += 1
        if arrived == 2:
            both.set()
        await both.wait()
        return await e.witness(request)

    results = await asyncio.gather(
        activate(e, state=Gate(), acquire=acquire), activate(e, state=Gate(), acquire=acquire)
    )
    assert results[0] == results[1]
    assert len(records(e, "recovery_activation")) == 1
