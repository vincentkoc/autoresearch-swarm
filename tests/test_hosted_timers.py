"""Timer readiness uses the transaction's witness, never the hosted process clock."""

import asyncio
from datetime import datetime

import pytest

from adapters.cloudflare.hosted_http import capabilities
from adapters.cloudflare.research_router import HostedTimerConfiguration
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import SignedTimeWitness
from research_swarm.domain.types import new_id
from research_swarm.state.database import encode
from tests.test_hosted_artifacts import current
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
    ready = await reconcile(e, identifier, key="timer-after-requalification")
    assert ready["result"]["readiness"]["data"]["cause"] == "timer"
    assert ready["result"]["wait"]["data"]["condition"]["wake_at"] == THRESHOLD
