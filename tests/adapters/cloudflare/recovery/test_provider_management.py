"""Active deployment clock management; native SQLite proof, not actual workerd."""

import asyncio
import json

import pytest

from adapters.cloudflare.platform import provider_management
from adapters.cloudflare.platform.provider_management import manage_provider
from adapters.cloudflare.platform.provisioning import require_deployment
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.time_providers import RequalifyTimeProvider, TimeProviderRequalification
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction
from research_swarm.state.time_providers import ProviderChanged, TimeProviders
from tests.adapters.cloudflare.hosted.test_hosted_router import Gate
from tests.adapters.cloudflare.recovery.test_checkpoint_import import snapshot
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.adapters.cloudflare.test_cloudflare_provisioning import run as provision


async def manage(e, *, request=True, store=None, state=None, **changes):
    return await manage_provider(
        e.state if state is None else state,
        lambda: e.store if store is None else store,
        e.manifest,
        e.secrets,
        **{
            "identity": e.store.identity,
            "credential": e.secrets.bootstrap,
            "crypto": e.crypto,
            "acquire": e.witness,
            "request_json": json.dumps(e.request) if request else None,
            "recovery_binding": getattr(e, "binding", None),
            **changes,
        },
    )


async def retire(e):
    registry = TimeProviders(e.store.identity, crypto_provider=e.crypto)
    principal = next(
        item.principal
        for item in e.manifest.credentials
        if item.principal.id == e.manifest.provisioning_actor_id
    )
    context = CommandContext(
        principal=principal, idempotency_key="native-discontinuity:" + new_id()
    )
    current = e.store.read(
        lambda sql: registry.load(Transaction(sql, context), e.manifest.configuration.authority)
    )
    context = context.model_copy(update={"expected_revision": current.revision})
    operation, pending = registry.prepare(context, current, "fixture.clock", {})
    e.old_epoch, e.witness.epoch = e.witness.epoch, new_id()
    proof = pending.complete(await e.witness(pending.request))
    with pytest.raises(ProviderChanged) as changed:
        e.store.read(
            lambda sql: registry.check(
                Transaction(sql, context), operation, proof, name="fixture.clock", payload={}
            )
        )
    fenced = e.store.write(
        lambda sql: registry.fence(
            Transaction(sql, context), changed.value.evidence, name="fixture.clock", payload={}
        )
    )
    assert fenced.state == "unqualified"
    e.request = RequalifyTimeProvider(
        request_id=new_id(),
        expected_provider_revision=fenced.revision,
        expected_provider_generation=fenced.data.scope.provider_generation,
        reason="Explicit recovery after pinned clock discontinuity",
    ).model_dump(mode="json")
    return fenced


@pytest.fixture
async def active(bootstrap):
    e = bootstrap
    e.initial_receipt = await provision(e)
    return e


@pytest.fixture
async def fenced(active):
    await retire(active)
    return active


async def test_status_is_persisted_state_and_does_not_acquire_or_change_anything(active):
    e = active
    before, calls = snapshot(e.store), len(e.witness.requests)
    e.witness.failure = TimeoutError("Synthetic offline source")
    status = strict_json_loads(await manage(e, request=False))
    assert status["manifest_digest"] == e.manifest.digest
    assert status["provider"]["state"] == "qualified"
    assert (
        status["provider"]["data"]["scope"]["provider_generation"]
        == e.initial_receipt["provider_generation"]
    )
    assert snapshot(e.store) == before and len(e.witness.requests) == calls


async def test_requalification_preserves_deployment_and_unrelated_credential_revocations(fenced):
    e = fenced
    other = next(
        item
        for item in e.manifest.credentials
        if item.principal.id != e.manifest.provisioning_actor_id
    )
    e.store.write(
        lambda sql: sql.execute("UPDATE credentials SET revoked=1 WHERE id=?", (other.id,))
    )
    before = snapshot(e.store)
    reply = strict_json_loads(await manage(e))
    assert reply["disposition"] == "accepted"
    receipt = TimeProviderRequalification.model_validate(reply["result"]["data"])
    assert receipt.provider.source_epoch == e.witness.epoch
    assert receipt.provider_revision == e.request["expected_provider_revision"] + 1
    assert require_deployment(e.store, e.manifest, crypto=e.crypto) == e.initial_receipt
    after = snapshot(e.store)
    for table in (
        "hosted_provisioning",
        "hosted_authority_config",
        "credentials",
        "budgets",
        "budget_reservations",
        "budget_settlements",
        "budget_entries",
    ):
        assert after[table] == before[table]


async def test_lost_reply_and_later_fence_replay_original_history_without_witness(fenced):
    e = fenced

    class LostReply:
        identity = e.store.identity
        read = e.store.read

        def write(self, callback):
            e.store.write(callback)
            raise ConnectionError("Synthetic lost accepted acknowledgment")

    with pytest.raises(ConnectionError):
        await manage(e, store=LostReply())
    e.witness.failure = TimeoutError("Synthetic offline")
    calls = len(e.witness.requests)
    first = await manage(e, state=Gate())
    assert strict_json_loads(first)["disposition"] == "accepted"
    assert len(e.witness.requests) == calls
    previous_request = dict(e.request)
    e.witness.failure = None
    await retire(e)
    before, calls = snapshot(e.store), len(e.witness.requests)
    e.witness.failure = TimeoutError("Synthetic offline after second retirement")
    assert await manage(e, request_json=json.dumps(previous_request)) == first
    assert snapshot(e.store) == before and len(e.witness.requests) == calls
    e.witness.failure = None
    assert strict_json_loads(await manage(e))["disposition"] == "accepted"


@pytest.mark.parametrize(
    "change", ["revision", "generation", "bootstrap", "retired_epoch", "source"]
)
async def test_invalid_or_retired_selection_cannot_qualify(fenced, change):
    e = fenced
    options = {}
    if change == "revision":
        e.request["expected_provider_revision"] += 1
    elif change == "generation":
        e.request["expected_provider_generation"] = new_id()
    elif change == "bootstrap":
        options["credential"] = "wrong"
    elif change == "retired_epoch":
        e.witness.epoch = e.old_epoch
    else:
        e.store.write(
            lambda sql: sql.execute(
                "UPDATE records SET payload=json_set(payload,'$.source.id',?) "
                "WHERE kind='time_provider'",
                (new_id(),),
            )
        )
    before = snapshot(e.store)
    with pytest.raises(ResearchError):
        await manage(e, **options)
    assert snapshot(e.store) == before


async def test_qualified_provider_cannot_be_replaced_without_retirement(active):
    e = active
    status = strict_json_loads(await manage(e, request=False))["provider"]
    e.request = RequalifyTimeProvider(
        request_id=new_id(),
        expected_provider_revision=status["revision"],
        expected_provider_generation=status["data"]["scope"]["provider_generation"],
        reason="Must refuse implicit replacement",
    ).model_dump(mode="json")
    before, calls = snapshot(e.store), len(e.witness.requests)
    with pytest.raises(ResearchError):
        await manage(e)
    assert snapshot(e.store) == before and len(e.witness.requests) == calls


@pytest.mark.parametrize("change", ["operator", "other_identity", "provider", "configuration"])
async def test_await_rechecks_operator_identity_and_provider_authority(fenced, change):
    e = fenced
    changed = None

    async def acquire(request):
        nonlocal changed
        response = await e.witness(request)

        def mutate(sql):
            if change == "operator":
                member = next(
                    x
                    for x in e.manifest.credentials
                    if x.principal.id == e.manifest.provisioning_actor_id
                )
                sql.execute("UPDATE credentials SET revoked=1 WHERE id=?", (member.id,))
            elif change == "other_identity":
                member = next(
                    x
                    for x in e.manifest.credentials
                    if x.principal.id != e.manifest.provisioning_actor_id
                )
                sql.execute(
                    "UPDATE credentials SET token_digest=? WHERE id=?", ("0" * 64, member.id)
                )
            elif change == "provider":
                sql.execute("UPDATE records SET revision=revision+1 WHERE kind='time_provider'")
            else:
                sql.execute("UPDATE hosted_authority_config SET digest=?", ("sha256:" + "0" * 64,))

        e.store.write(mutate)
        changed = snapshot(e.store)
        return response

    with pytest.raises(ResearchError):
        await manage(e, acquire=acquire)
    assert snapshot(e.store) == changed


@pytest.mark.parametrize("phase", ["event", "receipt", "verification"])
async def test_failure_rolls_back_generation_receipt_and_ack(fenced, monkeypatch, phase):
    e = fenced
    before = snapshot(e.store)
    original_event, original_insert = Transaction.event, Transaction.insert
    original_verify = provider_management.bounded_record

    def event(self, name, *args, **kwargs):
        result = original_event(self, name, *args, **kwargs)
        if name == "workspace.time_provider_requalified":
            raise RuntimeError("Synthetic management rollback")
        return result

    def insert(self, kind, *args, **kwargs):
        result = original_insert(self, kind, *args, **kwargs)
        if kind == provider_management.RECEIPT_KIND:
            raise RuntimeError("Synthetic management rollback")
        return result

    def verify(*args, **kwargs):
        original_verify(*args, **kwargs)
        raise RuntimeError("Synthetic management rollback")

    with monkeypatch.context() as patch:
        if phase == "event":
            patch.setattr(Transaction, "event", event)
        elif phase == "receipt":
            patch.setattr(Transaction, "insert", insert)
        else:
            patch.setattr(provider_management, "bounded_record", verify)
        with pytest.raises(RuntimeError, match="Synthetic management rollback"):
            await manage(e)
    assert snapshot(e.store) == before
    assert strict_json_loads(await manage(e))["disposition"] == "accepted"


@pytest.mark.parametrize("same_intent", [False, True])
async def test_competing_connections_advance_one_generation(fenced, same_intent):
    e = fenced
    entered, release = asyncio.Event(), asyncio.Event()

    async def acquire(request):
        entered.set()
        await release.wait()
        return await e.witness(request)

    first = asyncio.create_task(manage(e, acquire=acquire))
    await asyncio.wait_for(entered.wait(), 5)
    other = dict(e.request) if same_intent else dict(e.request, request_id=new_id())
    winner = await manage(e, state=Gate(), request_json=json.dumps(other))
    release.set()
    if same_intent:
        assert await first == winner
    else:
        with pytest.raises(ResearchError):
            await first
    rows = snapshot(e.store)["records"]
    assert len([row for row in rows if row["kind"] == provider_management.RECEIPT_KIND]) == 1


@pytest.mark.parametrize("operation", ["status", "command"])
async def test_sealed_workspace_refuses_management_without_witness(fenced, operation):
    e = fenced
    principal = next(
        item.principal
        for item in e.manifest.credentials
        if item.principal.id == e.manifest.provisioning_actor_id
    )
    context = CommandContext(principal=principal, idempotency_key="fixture-inactive")
    e.store.write(
        lambda sql: Transaction(sql, context).insert(
            "workspace_gate", principal.workspace_id, {}, state="sealed"
        )
    )
    before, calls = snapshot(e.store), len(e.witness.requests)
    with pytest.raises(ResearchError):
        await manage(e, request=operation == "command")
    assert snapshot(e.store) == before and len(e.witness.requests) == calls


@pytest.mark.parametrize(
    "damage", ["operator", "receipt", "generation", "signature", "ack_size", "record_size"]
)
async def test_completed_replay_reauthenticates_and_verifies_bounded_immutable_evidence(
    fenced, damage
):
    e = fenced
    accepted = strict_json_loads(await manage(e))
    generation = accepted["result"]["id"]

    def mutate(sql):
        if damage == "operator":
            member = next(
                x
                for x in e.manifest.credentials
                if x.principal.id == e.manifest.provisioning_actor_id
            )
            sql.execute("UPDATE credentials SET revoked=1 WHERE id=?", (member.id,))
        elif damage == "ack_size":
            sql.execute(
                "UPDATE commands SET response=response || ? WHERE idempotency_key=?",
                (" " * 65536, "provider-requalify:" + e.request["request_id"]),
            )
        elif damage == "record_size":
            sql.execute(
                "UPDATE records SET created_at=? WHERE kind=? AND id=?",
                ("x" * 65536, provider_management.RECEIPT_KIND, generation),
            )
        elif damage == "signature":
            sql.execute(
                "UPDATE records SET payload=json_set(payload,'$.qualification.signature',?) "
                "WHERE kind='time_provider_generation' AND id=?",
                ("0" * 128, generation),
            )
        else:
            kind = (
                provider_management.RECEIPT_KIND
                if damage == "receipt"
                else "time_provider_generation"
            )
            sql.execute(
                "UPDATE records SET revision=revision+1 WHERE kind=? AND id=?", (kind, generation)
            )

    e.store.write(mutate)
    before, calls = snapshot(e.store), len(e.witness.requests)
    e.witness.failure = TimeoutError("Synthetic offline")
    with pytest.raises(ResearchError):
        await manage(e)
    assert snapshot(e.store) == before and len(e.witness.requests) == calls


async def test_cancelled_acquisition_leaves_provider_and_intent_available(fenced):
    e = fenced
    before = snapshot(e.store)
    entered = asyncio.Event()

    async def acquire(request):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(manage(e, acquire=acquire))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert snapshot(e.store) == before
    assert strict_json_loads(await manage(e))["disposition"] == "accepted"
