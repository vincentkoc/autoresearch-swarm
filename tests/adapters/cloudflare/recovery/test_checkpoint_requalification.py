"""Private retired-clock recovery on SQLite; actual workerd is qualified separately."""

import asyncio
import json

import pytest

from adapters.cloudflare.recovery import checkpoint_requalification
from adapters.cloudflare.recovery.checkpoint_requalification import requalify_target_runtime
from adapters.cloudflare.recovery.runtime_history import REQUALIFICATION_KIND
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.recovery_runtime import (
    RequalifyRecoveryRuntime,
    RuntimePreparationReceipt,
    RuntimeRequalificationReceipt,
)
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction
from tests.adapters.cloudflare.hosted.test_hosted_router import Gate
from tests.adapters.cloudflare.recovery import test_checkpoint_activation as fixtures
from tests.adapters.cloudflare.recovery.test_checkpoint_activation import activate, opened
from tests.adapters.cloudflare.recovery.test_checkpoint_import import snapshot
from tests.adapters.cloudflare.recovery.test_checkpoint_runtime import records
from tests.adapters.cloudflare.recovery.test_checkpoint_runtime import run as prepare

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


@pytest.fixture
async def retired(ready):
    e = ready
    e.initial = RuntimePreparationReceipt.model_validate_json(
        records(e, "recovery_runtime_preparation")[0]["payload"]
    )
    e.old_epoch = e.witness.epoch
    e.witness.epoch = new_id()
    with pytest.raises(ResearchError):
        await activate(e)
    current = next(
        row
        for row in records(e, "time_provider")
        if row["id"] == e.runtime.plan.authority.activation_id
    )
    assert current["state"] == "unqualified"
    e.requalification_request = RequalifyRecoveryRuntime(
        request_id=new_id(),
        expected_provider_revision=current["revision"],
        previous_qualification_digest=e.initial.digest,
        reason="Requalify the elected target after its pinned witness restarted",
    ).model_dump(mode="json")
    return e


async def requalify(e, *, store=None, state=None, **changes):
    return await requalify_target_runtime(
        e.state if state is None else state,
        lambda: e.target if store is None else store,
        e.manifest,
        e.target_secrets,
        **{
            "identity": e.target.identity,
            "credential": e.target_secrets.bootstrap,
            "target_json": json.dumps(e.target_config),
            "request_json": json.dumps(e.requalification_request),
            "crypto": e.source.crypto,
            "acquire": e.witness,
            **changes,
        },
    )


async def test_retired_clock_requalifies_once_then_activates_with_immutable_lineage(retired):
    e = retired
    before = snapshot(e.target)
    original_receipt = records(e, "recovery_runtime_preparation")
    original_retirement = records(e, "time_source_retirement")
    raw = await requalify(e)
    reply = strict_json_loads(raw)
    assert reply["disposition"] == "accepted"
    receipt = RuntimeRequalificationReceipt.model_validate(reply["result"]["data"])
    assert receipt.provider.source_epoch == e.witness.epoch
    assert receipt.provider_revision == e.initial.provider_revision + 2
    assert receipt.previous_generation == e.initial.provider.scope.provider_generation
    assert receipt.provider.scope.provider_generation != receipt.previous_generation
    assert records(e, "workspace_gate")[0]["state"] == "recovering"
    after = snapshot(e.target)
    for name in (
        "credentials",
        "budgets",
        "budget_reservations",
        "budget_settlements",
        "budget_entries",
    ):
        assert after[name] == before[name]
    calls = len(e.witness.requests)
    e.witness.failure = TimeoutError("Synthetic witness offline")
    assert await requalify(e, state=Gate()) == raw
    assert snapshot(e.target) == after and len(e.witness.requests) == calls
    # The original preparation operation cannot silently become clock repair.
    with pytest.raises(ResearchError):
        await prepare(e)
    e.witness.failure = None
    activation = strict_json_loads(await activate(e))["result"]["data"]
    assert activation["runtime_digest"] != e.runtime.digest
    assert (
        activation["final_witness"]["payload"]["request"]["scope"]["provider_generation"]
        == receipt.provider.scope.provider_generation
    )
    assert (await opened(e)).database.authority_identity == e.target.identity
    assert records(e, "recovery_runtime_preparation") == original_receipt
    assert records(e, "time_source_retirement") == original_retirement
    assert not records(e, "run_result")
    with pytest.raises(ResearchError):
        await requalify(e)


async def test_lost_reply_is_replayed_without_a_new_generation_or_witness(retired):
    e = retired

    class LostReply:
        identity = e.target.identity
        read = e.target.read

        def write(self, callback):
            e.target.write(callback)
            raise ConnectionError("Synthetic accepted reply lost")

    with pytest.raises(ConnectionError):
        await requalify(e, store=LostReply())
    before, calls = snapshot(e.target), len(e.witness.requests)
    e.witness.failure = TimeoutError("Synthetic witness offline")
    first = await requalify(e, state=Gate())
    assert strict_json_loads(first)["disposition"] == "accepted"
    assert await requalify(e, state=Gate()) == first
    assert snapshot(e.target) == before and len(e.witness.requests) == calls
    changed = dict(e.requalification_request, reason="A different intent")
    with pytest.raises(ResearchError):
        await requalify(e, request_json=json.dumps(changed))


async def test_second_restart_extends_history_and_preserves_the_first_acknowledgment(retired):
    e = retired
    original_request = dict(e.requalification_request)
    first_reply = await requalify(e)
    first = RuntimeRequalificationReceipt.model_validate(
        strict_json_loads(first_reply)["result"]["data"]
    )
    e.witness.epoch = new_id()
    with pytest.raises(ResearchError):
        await activate(e)
    e.requalification_request = RequalifyRecoveryRuntime(
        request_id=new_id(),
        expected_provider_revision=first.provider_revision + 1,
        previous_qualification_digest=first.digest,
        reason="A second restart of the same pinned witness",
    ).model_dump(mode="json")
    second_reply = await requalify(e)
    second = RuntimeRequalificationReceipt.model_validate(
        strict_json_loads(second_reply)["result"]["data"]
    )
    assert second.previous_generation == first.provider.scope.provider_generation
    assert len(records(e, "time_source_retirement")) == 2
    assert len(records(e, REQUALIFICATION_KIND)) == 2
    before, calls = snapshot(e.target), len(e.witness.requests)
    e.witness.failure = TimeoutError("Synthetic witness offline")
    assert await requalify(e, request_json=json.dumps(original_request)) == first_reply
    assert await requalify(e) == second_reply
    assert snapshot(e.target) == before and len(e.witness.requests) == calls
    e.witness.failure = None
    activation = await activate(e)
    e.witness.failure = TimeoutError("Synthetic witness offline after activation")
    assert await activate(e) == activation
    assert (await opened(e)).database.authority_identity == e.target.identity


@pytest.mark.parametrize("independent_gate", [False, True])
async def test_exact_concurrent_requests_qualify_one_generation(retired, independent_gate):
    e = retired
    entered, release = asyncio.Event(), asyncio.Event()

    async def acquire(request):
        entered.set()
        await release.wait()
        return await e.witness(request)

    first = asyncio.create_task(requalify(e, acquire=acquire))
    await asyncio.wait_for(entered.wait(), 5)
    second = asyncio.create_task(requalify(e, state=Gate() if independent_gate else e.state))
    if independent_gate:
        second_reply = await second
        release.set()
        assert await first == second_reply
    else:
        release.set()
        assert await first == await second
    assert len(records(e, REQUALIFICATION_KIND)) == 1


async def test_competing_intents_cannot_both_qualify_the_fenced_revision(retired):
    e = retired
    entered, release = asyncio.Event(), asyncio.Event()

    async def acquire(request):
        entered.set()
        await release.wait()
        return await e.witness(request)

    first = asyncio.create_task(requalify(e, acquire=acquire))
    await asyncio.wait_for(entered.wait(), 5)
    other = dict(e.requalification_request, request_id=new_id())
    winner = await requalify(e, state=Gate(), request_json=json.dumps(other))
    release.set()
    with pytest.raises(ResearchError):
        await first
    assert strict_json_loads(winner)["disposition"] == "accepted"
    assert len(records(e, REQUALIFICATION_KIND)) == 1


@pytest.mark.parametrize("change", ["revision", "digest", "bootstrap", "predecessor", "fence"])
async def test_invalid_selection_refuses_before_witness_io(retired, change):
    e = retired
    options = {}
    if change == "revision":
        e.requalification_request["expected_provider_revision"] += 1
    elif change == "digest":
        e.requalification_request["previous_qualification_digest"] = "sha256:" + "0" * 64
    elif change == "bootstrap":
        options["credential"] = "wrong"
    else:
        kind = "recovery_runtime_preparation" if change == "predecessor" else "time_provider_fence"
        e.target.write(lambda sql: sql.execute("DELETE FROM records WHERE kind=?", (kind,)))
    before, calls = snapshot(e.target), len(e.witness.requests)
    with pytest.raises(ResearchError):
        await requalify(e, **options)
    assert snapshot(e.target) == before and len(e.witness.requests) == calls


@pytest.mark.parametrize("change", ["provider", "roster", "plan", "admission", "predecessor"])
async def test_witness_await_rechecks_every_selected_authority_input(retired, change):
    e = retired
    changed = None

    async def acquire(request):
        nonlocal changed
        response = await e.witness(request)

        def mutate(sql):
            if change == "roster":
                sql.execute(
                    "UPDATE credentials SET revoked=1 WHERE id=?", (e.manifest.credentials[0].id,)
                )
            else:
                kind = {
                    "provider": "time_provider",
                    "plan": "recovery_runtime_plan",
                    "admission": "recovery_admission",
                    "predecessor": "recovery_runtime_preparation",
                }[change]
                sql.execute("UPDATE records SET revision=revision+1 WHERE kind=?", (kind,))

        e.target.write(mutate)
        changed = snapshot(e.target)
        return response

    with pytest.raises(ResearchError):
        await requalify(e, acquire=acquire)
    assert snapshot(e.target) == changed
    assert not records(e, REQUALIFICATION_KIND)


@pytest.mark.parametrize("phase", ["receipt", "event", "bounds"])
async def test_write_failure_rolls_back_provider_receipt_event_and_ack(retired, monkeypatch, phase):
    e = retired
    before = snapshot(e.target)
    original_insert, original_event = Transaction.insert, Transaction.event
    original_bounds = checkpoint_requalification.require_activation_bounds

    def fail_insert(self, kind, *args, **kwargs):
        result = original_insert(self, kind, *args, **kwargs)
        if kind == REQUALIFICATION_KIND:
            raise RuntimeError("Synthetic requalification rollback")
        return result

    def fail_event(self, name, *args, **kwargs):
        result = original_event(self, name, *args, **kwargs)
        if name == "workspace.runtime_requalified":
            raise RuntimeError("Synthetic requalification rollback")
        return result

    def fail_bounds(sql):
        original_bounds(sql)
        if sql.execute("SELECT 1 FROM records WHERE kind=?", (REQUALIFICATION_KIND,)).fetchone():
            raise RuntimeError("Synthetic requalification rollback")

    with monkeypatch.context() as patch:
        if phase == "receipt":
            patch.setattr(Transaction, "insert", fail_insert)
        elif phase == "event":
            patch.setattr(Transaction, "event", fail_event)
        else:
            patch.setattr(checkpoint_requalification, "require_activation_bounds", fail_bounds)
        with pytest.raises(RuntimeError, match="Synthetic requalification rollback"):
            await requalify(e)
    assert snapshot(e.target) == before
    assert strict_json_loads(await requalify(e))["disposition"] == "accepted"


async def test_retired_source_epoch_cannot_return_and_timeout_does_not_consume_intent(retired):
    e = retired
    before = snapshot(e.target)
    e.witness.failure = TimeoutError("Synthetic witness offline")
    with pytest.raises(ResearchError):
        await requalify(e)
    assert snapshot(e.target) == before
    e.witness.failure = None
    fresh_epoch, e.witness.epoch = e.witness.epoch, e.old_epoch
    with pytest.raises(ResearchError):
        await requalify(e)
    assert snapshot(e.target) == before
    e.witness.epoch = fresh_epoch
    assert strict_json_loads(await requalify(e))["disposition"] == "accepted"


async def test_cancellation_during_witness_io_preserves_the_fenced_state(retired):
    e = retired
    before = snapshot(e.target)
    entered = asyncio.Event()

    async def acquire(request):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(requalify(e, acquire=acquire))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert snapshot(e.target) == before
