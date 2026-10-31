"""Timer readiness uses the transaction's witness, never the hosted process clock."""

import asyncio
from datetime import datetime

import pytest

from adapters.cloudflare.delivery_router import DeliveryRegistration, HostedDelivery
from adapters.cloudflare.hosted_http import capabilities
from adapters.cloudflare.research_router import HostedTimerConfiguration
from adapters.cloudflare.wait_delivery import wait_delivery_key
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.domain.backup import WorkspaceSeal
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import OrchestrationBinding
from research_swarm.domain.time_providers import source_origin_id
from research_swarm.domain.time_witnesses import SignedTimeWitness
from research_swarm.domain.types import new_id
from research_swarm.orchestration.delivery_contract import binding_reference
from research_swarm.orchestration.gateway_models import GatewayBinding
from research_swarm.state.database import Transaction, encode
from tests.test_hosted_artifacts import current
from tests.test_hosted_delivery import send, work
from tests.test_hosted_orchestration import CONFIG, campaign, domain_counts, drive, fence
from tests.test_hosted_orchestration import evaluation as evaluation
from tests.test_hosted_orchestration import hosted as hosted
from tests.test_hosted_orchestration import lifecycle as lifecycle
from tests.test_hosted_orchestration import orchestrated as orchestrated
from tests.test_hosted_orchestration import project as project
from tests.test_hosted_orchestration import publication as publication
from tests.test_hosted_orchestration import research as research
from tests.test_hosted_orchestration import scientific as scientific
from tests.test_time_witnesses import SEED

pytestmark = pytest.mark.parametrize("publication", ["evaluation"], indirect=True)
THRESHOLD = "2099-01-01T00:00:00.000Z"
THRESHOLD_MS = int(datetime.fromisoformat(THRESHOLD.replace("Z", "+00:00")).timestamp() * 1000)


@pytest.fixture
async def timers(orchestrated, monkeypatch):
    e, h = orchestrated, orchestrated.h
    h.config = HostedTimerConfiguration(**h.config.model_dump(exclude={"profile"}))
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    e.timer_ms, e.timer_epoch = THRESHOLD_MS - 1, None

    async def acquire(request):
        original = SignedTimeWitness.model_validate_json(await e.case.acquire(request)).payload
        payload = original.model_copy(
            update={
                "authority_time_ms": e.timer_ms,
                **({"source_epoch": e.timer_epoch} if e.timer_epoch is not None else {}),
            }
        )
        return canonical_bytes(
            WitnessSigner(h.config.source, SEED).sign(payload).model_dump(mode="json")
        )

    e.timer_acquire = acquire
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=acquire)
    monkeypatch.setattr("research_swarm.state.waits.utc_now", lambda: "2200-01-01T00:00:00.000Z")
    return e


async def record(e, *, events=("fixture.never",)):
    binding = current(e.h, "orchestration_binding", campaign(e)["id"])
    request = {
        "wait_id": new_id(),
        "campaign_id": binding["id"],
        "epoch": 1,
        "configuration_digest": CONFIG,
        "condition": {"observed_watermark": 0, "event_types": list(events), "wake_at": THRESHOLD},
    }
    return (
        await drive(
            e, "orchestration.record_wait", request, key="timer", revision=binding["revision"]
        )
    )["result"]


async def reconcile(e, identifier, *, key="timer-check"):
    return await drive(e, "orchestration.reconcile_wait", {"wait_id": identifier}, key=key)


async def test_timer_uses_one_witness_instant_and_replays_after_reopen(timers):
    e, h = timers, timers.h
    assert capabilities(h.config)["orchestration"]["timer_clock"] == "transaction-native-witness"
    saved = await record(e)
    identifier = saved["wait"]["id"]
    assert saved["wait"]["state"] == "waiting"
    with pytest.raises(ResearchError) as pending:
        await reconcile(e, identifier)
    assert pending.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=e.timer_acquire)
    e.timer_ms = THRESHOLD_MS
    ready = await reconcile(e, identifier)
    assert ready["result"]["readiness"]["data"]["cause"] == "timer"
    assert ready["result"]["readiness"]["data"]["ready_at"] == THRESHOLD
    assert ready["result"]["wait"]["data"]["condition"]["wake_at"] == THRESHOLD
    before = domain_counts(h), len(h.witness.requests)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture offline")
    assert await reconcile(e, identifier) == ready
    assert (domain_counts(h), len(h.witness.requests)) == before


async def test_timer_outage_does_not_consume_pending_intent(timers):
    e, h = timers, timers.h
    identifier = (await record(e))["wait"]["id"]
    e.timer_ms = THRESHOLD_MS
    before = domain_counts(h)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture offline")
    with pytest.raises(ResearchError) as unavailable:
        await reconcile(e, identifier)
    assert unavailable.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    assert domain_counts(h) == before
    assert current(h, "orchestration_wait", identifier)["state"] == "waiting"
    h.witness.failure = None
    assert (await reconcile(e, identifier))["result"]["readiness"]["data"]["cause"] == "timer"


async def test_delivery_accepts_original_retirement_from_another_activation(timer_delivery):
    e, h = timer_delivery, timer_delivery.h
    identifier = (await record(e))["wait"]["id"]
    pending = await work(e)
    registry = h.registry
    authority, original = registry.authority, registry.current()
    registry.authority = authority.model_copy(update={"activation_id": new_id()})
    registry.write(
        registry.context(revision=0),
        lambda tx: registry.providers.register(tx, registry.authority, h.config.source),
    )
    registry.qualify(epoch=original.data.source_epoch)
    context, changed = registry.discontinuity(epoch=new_id())
    registry.write(
        context,
        lambda tx: registry.providers.fence(
            tx, changed.evidence, name="fixture.operation", payload={}
        ),
    )
    registry.authority = authority
    origin = source_origin_id(original.data.source, original.data.source_epoch)
    retired = current(h, "time_source_retirement", origin)
    assert (await send(e, "wait", pending))[1]["state"] == "pending"
    assert current(h, "time_source_retirement", origin) == retired
    assert registry.current().state == "unqualified"
    assert current(h, "orchestration_wait", identifier)["state"] == "waiting"
    assert (await send(e, "wait", pending))[1]["state"] == "pending"


@pytest.mark.parametrize("ready", [True, False])
async def test_delivery_preserves_ready_history_but_refuses_new_work_after_seal(
    timer_delivery, ready
):
    e, h = timer_delivery, timer_delivery.h
    identifier = (await record(e))["wait"]["id"]
    pending = await work(e)
    if ready:
        e.timer_ms = THRESHOLD_MS
        settled = await send(e, "wait", pending)
        assert settled[0] == 200 and settled[1]["state"] == "settled"
    seal = WorkspaceSeal(
        backup_id=new_id(),
        workspace_id=h.principal.workspace_id,
        controller_id=h.config.authority.controller_id,
        control_epoch=h.config.authority.control_epoch,
        operator_id=h.principal.id,
        reason="Synthetic timer custody transfer",
        sealed_at="2026-10-31T00:00:00.000Z",
        evidence_through=0,
        unresolved_attempt_ids=(),
        worker_ids=(),
    )
    h.store.write(
        lambda sql: Transaction(sql, h.registry.context()).insert(
            "workspace_gate", h.principal.workspace_id, seal.model_dump(mode="json"), state="sealed"
        )
    )
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture offline")
    before = domain_counts(h), len(h.witness.requests)
    response = await send(e, "wait", pending)
    if ready:
        assert response == settled
    else:
        assert response[0] == 409 and response[1]["error"]["code"] == "STALE_FENCE"
        assert current(h, "orchestration_wait", identifier)["state"] == "waiting"
    assert (domain_counts(h), len(h.witness.requests)) == before


@pytest.mark.parametrize("event_first", [True, False])
async def test_event_wins_without_sampling_timer_clock_in_shared_wait_kernel(
    timers, monkeypatch, event_first
):
    e, h = timers, timers.h

    async def report():
        await drive(
            e,
            "report.create",
            {"campaign_id": campaign(e)["id"]},
            key="event",
            credential=h.operator,
        )

    if event_first:
        await report()
    else:
        saved = await record(e, events=("report.created",))
        await report()
    e.timer_ms = THRESHOLD_MS

    def forbidden(transaction):
        raise AssertionError("An existing event must win before timer comparison")

    monkeypatch.setattr(h.router.service.orchestration_waits, "clock", forbidden)
    if event_first:
        saved = await record(e, events=("report.created",))
    ready = await reconcile(e, saved["wait"]["id"])
    assert ready["result"]["readiness"]["data"]["cause"] == "event"


@pytest.mark.parametrize("fault", ["credential", "cancel"])
async def test_timer_rechecks_owner_after_the_witness(timers, monkeypatch, fault):
    e, h = timers, timers.h
    identifier = (await record(e))["wait"]["id"]
    e.timer_ms = THRESHOLD_MS

    async def acquire(request):
        response = await e.timer_acquire(request)
        if fault == "credential":
            h.credentials.revoke(h.principal, e.driver.id)
        else:
            asyncio.current_task().cancel()
        return response

    monkeypatch.setattr(h.router, "acquire", acquire)
    task = asyncio.create_task(reconcile(e, identifier))
    with pytest.raises((ResearchError, asyncio.CancelledError)):
        await task
    assert current(h, "orchestration_wait", identifier)["state"] == "waiting"


async def test_fencing_supersedes_timer_without_consuming_evidence(timers):
    e, h = timers, timers.h
    identifier = (await record(e))["wait"]["id"]
    await fence(e)
    e.timer_ms = THRESHOLD_MS
    ready = await reconcile(e, identifier)
    assert ready["result"]["readiness"]["data"]["cause"] == "fenced"
    assert current(h, "orchestration_wait", identifier)["state"] == "superseded"


async def test_explicit_clock_requalification_preserves_the_original_timer_threshold(timers):
    e, h = timers, timers.h
    identifier = (await record(e))["wait"]["id"]
    e.timer_epoch, e.timer_ms = new_id(), THRESHOLD_MS
    with pytest.raises(ResearchError) as changed:
        await reconcile(e, identifier)
    assert changed.value.code == ErrorCode.STALE_FENCE
    assert current(h, "orchestration_wait", identifier)["state"] == "waiting"
    snapshot = h.registry.current()
    assert snapshot.state == "unqualified"
    await requalify(e)
    ready = await reconcile(e, identifier, key="timer-after-requalification")
    assert ready["result"]["readiness"]["data"]["cause"] == "timer"
    assert ready["result"]["wait"]["data"]["condition"]["wake_at"] == THRESHOLD


async def requalify(e):
    h = e.h
    snapshot = h.registry.current()
    context = h.registry.context(revision=snapshot.revision)
    operation, payload = "fixture.requalify-timer", {}
    binding, waiting = h.registry.providers.prepare(
        context, snapshot, operation, payload, source=h.config.source
    )
    proof = waiting.complete(await e.timer_acquire(waiting.request))
    h.registry.write(
        context,
        lambda tx: h.registry.providers.requalify(
            tx, binding, proof, name=operation, payload=payload
        ),
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=e.timer_acquire)


@pytest.fixture
async def timer_delivery(timers):
    e, h = timers, timers.h
    e.gateway = h.credentials.issue(
        h.principal,
        h.principal.model_copy(update={"id": new_id(), "scopes": {"orchestration.deliver"}}),
    )
    e.reference = GatewayBinding.from_reference(
        binding_reference(OrchestrationBinding.model_validate(e.binding["data"]))
    )
    e.registrations = {
        e.gateway.principal.id: DeliveryRegistration(
            e.reference, {"control": e.driver.token, "evaluator": e.evaluator.token}
        )
    }
    e.facade = HostedDelivery(h.router, e.registrations)
    return e


async def test_delivery_survives_clock_fencing_without_rewriting_rejected_command(timer_delivery):
    e, h = timer_delivery, timer_delivery.h
    identifier = (await record(e))["wait"]["id"]
    pending = await work(e)
    old = h.registry.current()
    old_key = wait_delivery_key(identifier, old.data.scope.provider_generation)
    assert (await send(e, "wait", pending))[1]["state"] == "pending"
    e.timer_epoch = new_id()
    assert (await send(e, "wait", pending))[1]["state"] == "pending"
    assert h.registry.current().state == "unqualified"
    with pytest.raises(ResearchError) as rejected:
        await reconcile(e, identifier, key=old_key)
    assert rejected.value.code == ErrorCode.STALE_FENCE
    before = domain_counts(h), len(h.witness.requests)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture offline")
    for _ in range(2):
        assert (await send(e, "wait", pending))[1]["state"] == "pending"
    assert (domain_counts(h), len(h.witness.requests)) == before
    h.witness.failure = None
    await requalify(e)
    assert h.registry.current().data.scope.provider_generation != old.data.scope.provider_generation
    e.facade = HostedDelivery(h.router, e.registrations)
    e.timer_ms = THRESHOLD_MS
    ready = await send(e, "wait", pending)
    assert ready[0] == 200 and ready[1]["state"] == "settled"
    assert ready[1]["disposition"] == "timer"
    with pytest.raises(ResearchError) as replay:
        await reconcile(e, identifier, key=old_key)
    assert replay.value.code == ErrorCode.STALE_FENCE
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture offline")
    before = domain_counts(h), len(h.witness.requests)
    assert await send(e, "wait", pending) == ready
    assert (domain_counts(h), len(h.witness.requests)) == before
    assert current(h, "orchestration_wait", identifier)["data"]["condition"]["wake_at"] == THRESHOLD
    assert current(h, "wait_readiness", identifier)["data"]["ready_at"] == THRESHOLD
    for kind in ("wait_readiness", "wakeup_delivery"):
        count = h.store.read(
            lambda sql, kind=kind: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind=?", (kind,)
            ).one()[0]
        )
        assert count == 1


@pytest.mark.parametrize("fault", ["gateway", "route", "fence", "binding"])
async def test_unqualified_wait_does_not_hide_revocation_or_unrelated_fences(timer_delivery, fault):
    e, h = timer_delivery, timer_delivery.h
    identifier = (await record(e))["wait"]["id"]
    pending = await work(e)
    e.timer_epoch = new_id()
    assert (await send(e, "wait", pending))[1]["state"] == "pending"
    if fault in {"gateway", "route"}:
        h.credentials.revoke(h.principal, e.gateway.id if fault == "gateway" else e.driver.id)
    elif fault == "fence":
        h.store.write(
            lambda sql: sql.execute("DELETE FROM records WHERE kind='time_provider_fence'")
        )
    else:
        row = current(h, "orchestration_binding", campaign(e)["id"])
        row["data"]["configuration_digest"] = "sha256:" + "f" * 64
        h.store.write(
            lambda sql: sql.execute(
                "UPDATE records SET payload=? WHERE kind='orchestration_binding' AND id=?",
                (encode(row["data"]), row["id"]),
            )
        )
    status, response = await send(e, "wait", pending)
    assert status in {401, 403, 409} and "error" in response
    assert current(h, "orchestration_wait", identifier)["state"] == "waiting"
