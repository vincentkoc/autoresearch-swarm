"""Immutable readiness lineage from real provider retirement and qualification effects."""

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from adapters.cloudflare import runtime_history
from adapters.cloudflare.checkpoint_runtime import guarded
from adapters.cloudflare.runtime_history import (
    REQUALIFICATION_KIND,
    fence_digest,
    read_qualification,
    read_retirement,
)
from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.crypto import resolve_crypto
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.recovery_runtime import (
    RequalifyRecoveryRuntime,
    RuntimePreparationReceipt,
    RuntimeRequalificationReceipt,
)
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.database import Database, Transaction
from research_swarm.state.time_providers import TimeProviders, qualification_payload
from tests import test_checkpoint_activation as fixtures
from tests.test_checkpoint_activation import activate
from tests.test_checkpoint_import import snapshot
from tests.test_checkpoint_runtime import records
from tests.test_time_providers import registry as registry
from tests.test_time_witnesses import identifier
from tests.test_time_witnesses import source as witness_source

deployment = fixtures.deployment
bootstrap = fixtures.bootstrap
source = fixtures.source
sealed = fixtures.sealed
recovery = fixtures.recovery
configured = fixtures.configured
imported = fixtures.imported
elected = fixtures.elected
target = fixtures.target
prepared = fixtures.prepared
ready = fixtures.ready


def test_shared_origin_retirement_keeps_its_original_provider_evidence(registry):
    r = registry
    r.qualify()
    first = r.authority
    r.authority = first.model_copy(update={"activation_id": new_id()})
    r.write(
        r.context(revision=0), lambda tx: r.providers.register(tx, r.authority, witness_source())
    )
    current, second = r.qualify(), r.authority
    previous = RuntimePreparationReceipt(
        plan_digest="sha256:" + "1" * 64,
        restoration_digest="sha256:" + "2" * 64,
        admission_digest="sha256:" + "3" * 64,
        provider_revision=current.revision,
        provider=current.data,
        prepared_at=utc_now(),
    )
    r.authority = first
    context, changed = r.discontinuity()
    r.write(
        context,
        lambda tx: r.providers.fence(tx, changed.evidence, name="fixture.operation", payload={}),
    )
    original = [
        row
        for row in snapshot(r.database.store)["records"]
        if row["kind"] == "time_source_retirement"
    ]
    r.authority = second
    context, changed = r.discontinuity(epoch=identifier(7))
    r.write(
        context,
        lambda tx: r.providers.fence(tx, changed.evidence, name="fixture.operation", payload={}),
    )
    fence = r.database.store.read(
        lambda sql: read_retirement(
            sql,
            SimpleNamespace(authority=second),
            previous,
            crypto=resolve_crypto(None),
        )
    )
    assert fence.provider == previous.provider
    assert [
        row
        for row in snapshot(r.database.store)["records"]
        if row["kind"] == "time_source_retirement"
    ] == original


def selected(e, generation=None, **options):
    return e.target.read(
        lambda sql: read_qualification(
            sql,
            e.runtime.plan,
            generation=generation or e.successor.provider.scope.provider_generation,
            restoration_digest=e.initial.restoration_digest,
            admission_digest=e.initial.admission_digest,
            crypto=e.source.crypto,
            **options,
        )
    )


@pytest.fixture
async def requalified(ready):
    e = ready
    e.initial = RuntimePreparationReceipt.model_validate_json(
        records(e, "recovery_runtime_preparation")[0]["payload"]
    )
    e.witness.epoch = new_id()
    with pytest.raises(ResearchError):
        await activate(e)
    database = Database.from_store(e.target, recovery_verifier=e.verifier)
    authority = recovery_authority(database, e.context, crypto_provider=e.source.crypto)
    registry = TimeProviders(e.target.identity, crypto_provider=e.source.crypto)
    current = e.target.read(
        lambda sql: registry.load(Transaction(sql, e.context), e.runtime.plan.authority)
    )
    request = RequalifyRecoveryRuntime(
        request_id=new_id(),
        expected_provider_revision=current.revision,
        previous_qualification_digest=e.initial.digest,
        reason="Constructed successor history; private command is qualified separately",
    )
    context = e.context.model_copy(
        update={"command_id": new_id(), "expected_revision": current.revision}
    )
    payload, name = (
        {"plan_digest": e.runtime.plan.digest, "request": request.model_dump(mode="json")},
        "workspace.requalify_runtime",
    )
    operation, pending = registry.prepare(context, current, name, payload)
    proof = pending.complete(await e.witness(pending.request))
    owned = guarded(
        context,
        authority,
        e.runtime.plan,
        "time_provider.requalify",
        qualification_payload(operation, name, payload),
    )

    def complete(sql):
        fence = read_retirement(sql, e.runtime.plan, e.initial, crypto=e.source.crypto)
        result = registry.requalify(
            Transaction(sql, owned), operation, proof, name=name, payload=payload
        )
        receipt = RuntimeRequalificationReceipt(
            request=request,
            plan_digest=e.runtime.plan.digest,
            restoration_digest=e.initial.restoration_digest,
            admission_digest=e.initial.admission_digest,
            previous_generation=e.initial.provider.scope.provider_generation,
            fence_digest=fence_digest(fence),
            provider_revision=result.revision,
            provider=result.data,
            qualified_at=utc_now(),
        )
        Transaction(sql, context).insert(
            REQUALIFICATION_KIND,
            result.data.scope.provider_generation,
            receipt.model_dump(mode="json"),
            state="immutable",
        )
        return receipt

    e.successor = e.target.write(complete)
    return e


async def test_selected_generation_preserves_initial_and_ignores_current_provider(requalified):
    e = requalified
    before = snapshot(e.target)
    assert selected(e) == e.successor
    assert selected(e, e.initial.provider.scope.provider_generation) == e.initial
    assert snapshot(e.target) == before
    e.target.write(
        lambda sql: sql.execute(
            "UPDATE records SET revision=revision+1,state='unqualified' WHERE kind='time_provider'"
        )
    )
    assert selected(e) == e.successor


@pytest.mark.parametrize(
    "kind,field,value",
    [
        (REQUALIFICATION_KIND, "request.previous_qualification_digest", "sha256:" + "0" * 64),
        (REQUALIFICATION_KIND, "fence_digest", "sha256:" + "0" * 64),
        (REQUALIFICATION_KIND, "previous_generation", "00000000-0000-4000-8000-000000000099"),
        ("time_provider_fence", "provider_revision", 19),
        ("time_source_retirement", "observed.signature", "0" * 128),
        ("recovery_runtime_preparation", "plan_digest", "sha256:" + "0" * 64),
    ],
)
async def test_lineage_refuses_changed_predecessor_or_retirement(requalified, kind, field, value):
    e = requalified
    e.target.write(
        lambda sql: sql.execute(
            "UPDATE records SET payload=json_set(payload,?,?) WHERE kind=?",
            ("$." + field, value, kind),
        )
    )
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        selected(e)
    assert snapshot(e.target) == before


@pytest.mark.parametrize("damage", ["missing", "cycle", "old_epoch", "oversized", "revision"])
async def test_lineage_requires_complete_bounded_successors(requalified, damage):
    e = requalified

    def change(sql):
        if damage == "missing":
            sql.execute("DELETE FROM records WHERE kind='time_provider_fence'")
        elif damage == "cycle":
            sql.execute(
                "UPDATE records SET payload=json_set(payload,'$.previous_generation',?) "
                "WHERE kind=?",
                (e.successor.provider.scope.provider_generation, REQUALIFICATION_KIND),
            )
        elif damage == "old_epoch":
            sql.execute(
                "UPDATE records SET payload=json_set(payload,'$.provider.source_epoch',?) "
                "WHERE kind=?",
                (e.initial.provider.source_epoch, REQUALIFICATION_KIND),
            )
        elif damage == "oversized":
            sql.execute(
                "UPDATE records SET payload=payload || ? WHERE kind=?",
                (" " * 65536, REQUALIFICATION_KIND),
            )
        else:
            sql.execute("UPDATE records SET revision=2 WHERE kind=?", (REQUALIFICATION_KIND,))

    e.target.write(change)
    with pytest.raises((ResearchError, ValidationError)):
        selected(e)


@pytest.mark.parametrize("change", ["revision", "generation", "epoch", "reason", "extra"])
async def test_requalification_record_contract_rejects_invalid_successor(requalified, change):
    raw = requalified.successor.model_dump(mode="json")
    if change == "revision":
        raw["provider_revision"] += 1
    elif change == "generation":
        raw["provider"]["scope"]["provider_generation"] = raw["previous_generation"]
    elif change == "epoch":
        raw["provider"]["source_epoch"] = None
    elif change == "reason":
        raw["request"]["reason"] = "x" * 4097
    else:
        raw["request"]["source"] = {}
    with pytest.raises(ValidationError):
        RuntimeRequalificationReceipt.model_validate(raw)


async def test_readiness_admission_can_reserve_one_lineage_slot(requalified, monkeypatch):
    e = requalified
    monkeypatch.setattr(runtime_history, "MAX_REQUALIFICATIONS", 1)
    assert selected(e) == e.successor
    before = snapshot(e.target)
    with pytest.raises(ResearchError) as overflow:
        selected(e, reserve_successor=True)
    assert overflow.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert (
        selected(e, e.initial.provider.scope.provider_generation, reserve_successor=True)
        == e.initial
    )
    assert snapshot(e.target) == before


async def test_seventeen_successors_refuse_before_any_retirement_projection(
    requalified, monkeypatch
):
    e = requalified
    previous = e.successor

    def append(sql):
        nonlocal previous
        tx = Transaction(sql, e.context)
        for _ in range(16):
            successor = previous.model_copy(
                update={
                    "request": previous.request.model_copy(
                        update={
                            "request_id": new_id(),
                            "expected_provider_revision": previous.provider_revision + 1,
                            "previous_qualification_digest": previous.digest,
                        }
                    ),
                    "previous_generation": previous.provider.scope.provider_generation,
                    "provider_revision": previous.provider_revision + 2,
                    "provider": previous.provider.model_copy(
                        update={
                            "scope": previous.provider.scope.model_copy(
                                update={"provider_generation": new_id()}
                            ),
                            "source_epoch": new_id(),
                        }
                    ),
                }
            )
            tx.insert(
                REQUALIFICATION_KIND,
                successor.provider.scope.provider_generation,
                successor.model_dump(mode="json"),
                state="immutable",
            )
            previous = successor

    e.target.write(append)
    monkeypatch.setattr(
        runtime_history,
        "read_retirement",
        lambda *a, **k: pytest.fail("unbounded lineage projected"),
    )
    with pytest.raises(ResearchError) as overflow:
        selected(e, previous.provider.scope.provider_generation)
    assert overflow.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
