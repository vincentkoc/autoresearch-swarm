"""Saved target effects, independent receipts and durable event waits on one ledger."""

import asyncio

import pytest

from adapters.cloudflare.research.research_router import HostedOrchestrationConfiguration
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DriveAction
from research_swarm.domain.types import new_id
from research_swarm.orchestration.turns import DurableTurnDriver
from research_swarm.state.database import encode
from research_swarm.workers.local import LocalWorker
from tests.adapters.cloudflare.hosted.test_hosted_artifacts import current
from tests.adapters.cloudflare.hosted.test_hosted_http import send
from tests.adapters.cloudflare.hosted.test_hosted_research import sdk
from tests.adapters.cloudflare.hosted.test_hosted_scientific import campaign, domain_counts
from tests.adapters.cloudflare.hosted.test_hosted_scientific import evaluation as evaluation
from tests.adapters.cloudflare.hosted.test_hosted_scientific import hosted as hosted
from tests.adapters.cloudflare.hosted.test_hosted_scientific import lifecycle as lifecycle
from tests.adapters.cloudflare.hosted.test_hosted_scientific import project as project
from tests.adapters.cloudflare.hosted.test_hosted_scientific import publication as publication
from tests.adapters.cloudflare.hosted.test_hosted_scientific import research as research
from tests.adapters.cloudflare.hosted.test_hosted_scientific import scientific as scientific

CONFIG = content_digest(b"hosted-driver-configuration")
pytestmark = pytest.mark.parametrize("publication", ["evaluation"], indirect=True)


@pytest.fixture
async def orchestrated(scientific):
    e, h = scientific, scientific.h
    h.config = HostedOrchestrationConfiguration(**h.config.model_dump(exclude={"profile"}))
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=e.case.acquire)
    e.driver = h.credentials.issue(
        h.principal,
        h.principal.model_copy(
            update={
                "id": new_id(),
                "scopes": {
                    "read",
                    "orchestration.drive",
                    "report.create",
                    "campaign.manage",
                    "evaluation.request",
                    "artifact.upload",
                },
            }
        ),
    )
    e.evaluator = h.credentials.issue(
        h.principal,
        e.evaluator.principal.model_copy(
            update={"id": new_id(), "scopes": {"read", "evaluation.publish", "orchestration.drive"}}
        ),
    )
    e.intent = e.intent.model_copy(update={"evaluator_id": e.evaluator.principal.id})
    e.binding = (
        await h.router.command(
            h.operator.token,
            "orchestration.bind",
            {
                "campaign_id": campaign(e)["id"],
                "adapter": "cloudflare-workflows",
                "configuration_digest": CONFIG,
                "routes": {"control": e.driver.principal.id, "evaluator": e.evaluator.principal.id},
            },
            idempotency_key="bind",
            expected_revision=campaign(e)["revision"],
        )
    )["result"]
    return e


async def drive(e, operation, payload, *, key, revision=None, credential=None):
    return await e.h.router.command(
        (credential or e.driver).token,
        operation,
        payload,
        idempotency_key=key,
        expected_revision=revision,
    )


async def prepare(e, operation="report.create", payload=None, *, revision=None, credential=None):
    def call():
        with sdk(e.h, e.loop, token=(credential or e.driver).token) as client:
            binding = client.get("orchestration_binding", campaign(e)["id"])
            ordinal = binding["data"]["next_ordinal"]
            driver = DurableTurnDriver(client, binding["id"], epoch=1, configuration_digest=CONFIG)
            return driver.prepare_turn(
                ordinal,
                lambda: DriveAction(
                    operation=operation,
                    payload=payload or {"campaign_id": binding["id"]},
                    expected_revision=revision,
                    idempotency_key=f"target-{ordinal}",
                ),
            )

    return await asyncio.to_thread(call)


async def execute(e, turn, *, key="execute", credential=None):
    return await drive(
        e, "orchestration.execute_turn", {"turn_id": turn["id"]}, key=key, credential=credential
    )


async def fence(e):
    binding = current(e.h, "orchestration_binding", campaign(e)["id"])
    return await drive(
        e,
        "orchestration.fence",
        {
            "campaign_id": binding["id"],
            "epoch": 1,
            "reason": "Fixture fence",
        },
        key="fence",
        revision=binding["revision"],
        credential=e.h.operator,
    )


@pytest.mark.parametrize("recovery", ["execute", "reconcile"])
async def test_target_commit_recovers_only_missing_receipt_with_a_new_witness(
    orchestrated, monkeypatch, recovery
):
    e, h = orchestrated, orchestrated.h
    turn = await prepare(e)
    acquire, calls = h.router.acquire, 0

    async def interrupt_receipt(request):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ResearchError(
                ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture receipt witness unavailable"
            )
        return await acquire(request)

    monkeypatch.setattr(h.router, "acquire", interrupt_receipt)
    with pytest.raises(ResearchError) as interrupted:
        await execute(e, turn)
    assert interrupted.value.code == ErrorCode.TRANSPORT_UNAVAILABLE and calls == 2
    reports = await h.router.query(h.operator.token, "list", {"kind": "report"})
    assert len(reports["items"]) == 1
    assert current(h, "drive_turn", turn["id"])["state"] == "prepared"
    assert (await h.router.query(h.operator.token, "list", {"kind": "drive_receipt"}))[
        "items"
    ] == []
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=e.case.acquire)

    async def cannot_deliver(*args, **kwargs):
        raise AssertionError("Committed target must not be delivered again")

    monkeypatch.setattr(h.router.scientific, "command", cannot_deliver)
    before = len(h.witness.requests), len(h.transport.calls)

    async def recover(credential=None):
        return await drive(
            e,
            f"orchestration.{recovery}_turn",
            {"turn_id": turn["id"]},
            key="execute",
            credential=credential,
        )

    accepted = await recover()
    assert accepted["result"]["response"]["disposition"] == "accepted"
    assert accepted["result"]["response"]["result"]["id"] == reports["items"][0]["id"]
    assert (len(h.witness.requests), len(h.transport.calls)) == (before[0] + 1, before[1])
    assert current(h, "orchestration_binding", campaign(e)["id"])["data"]["pending_turn_id"] is None
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Source unavailable")
    h.transport.hide_reads = True
    before = domain_counts(h), len(h.witness.requests), len(h.transport.calls)
    assert await recover() == accepted
    assert (domain_counts(h), len(h.witness.requests), len(h.transport.calls)) == before
    restricted = h.credentials.issue(
        h.principal,
        e.driver.principal.model_copy(update={"scopes": {"read", "orchestration.drive"}}),
    )
    with pytest.raises(ResearchError) as denied:
        await recover(credential=restricted)
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert (domain_counts(h), len(h.witness.requests), len(h.transport.calls)) == before


async def test_saved_evaluator_targets_preserve_private_artifact_authority(orchestrated):
    e, h = orchestrated, orchestrated.h
    authorize = await prepare(
        e, "evaluation.authorize", e.intent.model_dump(mode="json"), revision=e.job["revision"]
    )
    before = len(h.witness.requests)
    authorized = (await execute(e, authorize))["result"]["response"]["result"]
    # Artifact begin + finalize, the target authorization, then its delivery receipt.
    assert len(h.witness.requests) == before + 4
    assert len({request.nonce for request in h.witness.requests[before:]}) == 4
    assert len({request.request_digest for request in h.witness.requests[before:]}) == 4

    def score():
        with sdk(h, e.loop) as operator, sdk(h, e.loop, token=e.case.credential.token) as client:
            runner = LocalWorker(
                client,
                e.case.runner.worker,
                root=e.case.runner.root,
                machine_registry=e.case.runner.resources.path,
            )
            attempt = runner.claim(operator.get("job", e.job["id"]))
            assert runner.execute(attempt["id"])["state"] == "succeeded"
            return attempt

    attempt = await asyncio.to_thread(score)
    completion = {"request_id": authorized["id"], "attempt_id": attempt["id"]}
    turn = await prepare(e, "evaluation.complete", completion, credential=e.evaluator)
    with pytest.raises(ResearchError) as bare:
        await drive(
            e, "evaluation.complete", completion, key="bare-completion", credential=e.evaluator
        )
    assert bare.value.code == ErrorCode.AUTHORIZATION_DENIED
    before = len(h.witness.requests)
    receipt = (await execute(e, turn, key="evaluator-execute", credential=e.evaluator))["result"][
        "response"
    ]["result"]
    assert receipt["state"] == "valid"
    assert receipt["data"]["receipt"]["evaluator_id"] == e.evaluator.principal.id
    assert len(h.witness.requests) == before + 2
    assert (
        current(h, "trial", e.case.admitted.trial["result"]["id"])["data"]["evaluation_status"]
        == "valid"
    )


async def test_target_preflight_failure_remains_pending_but_durable_rejection_settles(orchestrated):
    e, h = orchestrated, orchestrated.h
    row = campaign(e)
    turn = await prepare(
        e,
        "campaign.complete",
        {"campaign_id": row["id"], "reason": "Fixture"},
        revision=row["revision"],
    )
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Source unavailable")
    with pytest.raises(ResearchError) as unavailable:
        await execute(e, turn)
    assert unavailable.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    assert current(h, "drive_turn", turn["id"])["state"] == "prepared"
    h.witness.failure = None
    before = len(h.witness.requests)
    rejected = (await execute(e, turn))["result"]
    assert rejected["response"]["disposition"] == "rejected"
    assert rejected["response"]["error"]["code"] == ErrorCode.INVALID_TRANSITION
    assert len(h.witness.requests) == before + 2
    assert campaign(e) == row


@pytest.mark.parametrize("interrupted", [False, True])
async def test_saved_authorization_retires_duplicate_manifest_with_only_derived_authority(
    orchestrated, monkeypatch, interrupted
):
    e, h = orchestrated, orchestrated.h
    invalid = await prepare(
        e,
        "evaluation.authorize",
        e.intent.model_copy(update={"exposure_class": "confirmation"}).model_dump(mode="json"),
        revision=e.job["revision"],
    )
    rejected = (await execute(e, invalid, key="invalid-authorization"))["result"]
    assert rejected["response"]["disposition"] == "rejected"
    assert rejected["response"]["error"]["code"] == ErrorCode.COMPARISON_INELIGIBLE
    corrected = await prepare(
        e, "evaluation.authorize", e.intent.model_dump(mode="json"), revision=e.job["revision"]
    )
    if interrupted:
        retire = h.objects.retire

        async def lose_retirement(*args):
            await retire(*args)
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture retirement reply lost")

        monkeypatch.setattr(h.objects, "retire", lose_retirement)
        with pytest.raises(ResearchError) as lost:
            await execute(e, corrected, key="corrected-authorization")
        assert lost.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
        cleanup = h.store.read(
            lambda sql: sql.execute(
                "SELECT id FROM records WHERE kind='upload' AND state='cleanup'"
            ).one()[0]
        )
        before = len(h.transport.calls), len(h.witness.requests)
        with pytest.raises(ResearchError) as bare:
            await drive(e, "artifact.cleanup_upload", {"upload_id": cleanup}, key="bare-cleanup")
        assert bare.value.code == ErrorCode.AUTHORIZATION_DENIED
        assert (len(h.transport.calls), len(h.witness.requests)) == before
        assert current(h, "drive_turn", corrected["id"])["state"] == "prepared"
        monkeypatch.setattr(h.objects, "retire", retire)
    accepted = (await execute(e, corrected, key="corrected-authorization"))["result"]
    assert accepted["response"]["result"]["state"] == "authorized"
    assert (
        h.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='upload' AND state='cleanup'"
            ).one()[0]
        )
        == 0
    )
    assert (
        len((await h.router.query(h.operator.token, "list", {"kind": "drive_receipt"}))["items"])
        == 2
    )


@pytest.mark.parametrize("event_first", [False, True])
async def test_event_wait_catches_both_orders_and_ack_does_not_consume_readiness(
    orchestrated, event_first
):
    e, h = orchestrated, orchestrated.h
    context = await h.router.query(h.operator.token, "context", {"campaign_id": campaign(e)["id"]})
    report = None

    async def publish_report():
        return (
            await drive(
                e,
                "report.create",
                {"campaign_id": campaign(e)["id"]},
                key="event-report",
                credential=h.operator,
            )
        )["result"]

    if event_first:
        report = await publish_report()
    binding = current(h, "orchestration_binding", campaign(e)["id"])
    request = {
        "wait_id": new_id(),
        "campaign_id": binding["id"],
        "epoch": 1,
        "configuration_digest": CONFIG,
        "condition": {
            "observed_watermark": context["watermark"],
            "event_types": ["report.created"],
        },
    }
    waited = (
        await drive(
            e, "orchestration.record_wait", request, key="wait", revision=binding["revision"]
        )
    )["result"]
    assert waited["wait"]["state"] == ("ready" if event_first else "waiting")
    if not event_first:
        with pytest.raises(ResearchError) as pending:
            await drive(
                e, "orchestration.reconcile_wait", {"wait_id": request["wait_id"]}, key="reconcile"
            )
        assert pending.value.code == ErrorCode.RESOURCE_UNAVAILABLE
        report = await publish_report()
    ready = (
        await drive(
            e, "orchestration.reconcile_wait", {"wait_id": request["wait_id"]}, key="reconcile"
        )
    )["result"]
    assert ready["readiness"]["data"]["cause"] == "event"
    assert ready["readiness"]["data"]["watermark"] > context["watermark"]
    assert report is not None
    delivery = ready["delivery"]
    ack = {"delivery_id": delivery["id"], "campaign_id": binding["id"], "epoch": 1}
    received = await drive(
        e, "orchestration.acknowledge_wakeup", ack, key="ack", revision=delivery["revision"]
    )
    assert (
        await drive(
            e, "orchestration.acknowledge_wakeup", ack, key="ack", revision=delivery["revision"]
        )
        == received
    )
    assert current(h, "orchestration_wait", request["wait_id"])["state"] == "ready"
    assert (
        current(h, "orchestration_binding", binding["id"])["data"]["active_wait_id"]
        == request["wait_id"]
    )
    turn = await prepare(e)
    assert current(h, "orchestration_wait", request["wait_id"])["state"] == "consumed"
    assert (
        current(h, "orchestration_binding", binding["id"])["data"]["pending_turn_id"] == turn["id"]
    )


async def test_unqualified_actions_and_timers_do_not_reserve_work(orchestrated):
    e, h = orchestrated, orchestrated.h
    before = domain_counts(h), len(h.witness.requests), len(h.transport.calls)
    with pytest.raises(ResearchError) as unavailable:
        await prepare(e, "search.request_proposal", {"campaign_id": campaign(e)["id"]})
    assert unavailable.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    with pytest.raises(ResearchError) as timer:
        await drive(
            e,
            "orchestration.record_wait",
            {
                "wait_id": new_id(),
                "campaign_id": campaign(e)["id"],
                "epoch": 1,
                "configuration_digest": CONFIG,
                "condition": {"observed_watermark": 0, "wake_at": "2000-01-01T00:00:00.000Z"},
            },
            key="timer",
            revision=e.binding["revision"],
        )
    assert timer.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert (domain_counts(h), len(h.witness.requests), len(h.transport.calls)) == before


async def test_fenced_unacknowledged_turn_closes_without_creating_a_target_result(orchestrated):
    e, h = orchestrated, orchestrated.h
    turn = await prepare(e)
    await fence(e)
    closed = await drive(
        e,
        "orchestration.close_turn",
        {
            "turn_id": turn["id"],
            "epoch": 1,
            "finding": "Fenced before delivery",
        },
        key="close",
        revision=turn["revision"],
        credential=h.operator,
    )
    assert closed["result"]["closure"]["state"] == "immutable"
    assert current(h, "drive_turn", turn["id"])["state"] == "closed_unacknowledged"
    assert (await h.router.query(h.operator.token, "list", {"kind": "report"}))["items"] == []
    assert (await h.router.query(h.operator.token, "list", {"kind": "drive_receipt"}))[
        "items"
    ] == []


async def test_old_profile_refuses_orchestration_and_context_before_reading_body(scientific):
    h = scientific.h
    for method, path in (
        ("POST", "/v1/commands/orchestration.bind"),
        ("GET", f"/v1/campaigns/{campaign(scientific)['id']}/context"),
    ):
        consumed = []
        response = await send(
            h, method, path, body=b"unused", token=h.operator.token, consume=consumed
        )
        assert response.status == 501 and consumed == []


@pytest.mark.parametrize("fault", ["fence", "revoke", "cancel"])
async def test_target_rechecks_driver_after_witness_without_inventing_effects(
    orchestrated, monkeypatch, fault
):
    e, h = orchestrated, orchestrated.h
    turn = await prepare(e)
    acquire, first = h.router.acquire, True

    async def change_owner(request):
        nonlocal first
        response = await acquire(request)
        if first:
            first = False
            if fault == "fence":
                h.store.write(
                    lambda sql: sql.execute(
                        "UPDATE records SET state='fenced' "
                        "WHERE kind='orchestration_binding' AND id=?",
                        (e.binding["id"],),
                    )
                )
            elif fault == "revoke":
                h.credentials.revoke(h.principal, e.driver.id)
            else:
                asyncio.current_task().cancel()
        return response

    monkeypatch.setattr(h.router, "acquire", change_owner)
    task = asyncio.create_task(execute(e, turn))
    if fault == "fence":
        result = (await task)["result"]
        assert result["response"]["disposition"] == "rejected"
        assert result["response"]["error"]["code"] == ErrorCode.STALE_FENCE
    else:
        with pytest.raises(ResearchError if fault == "revoke" else asyncio.CancelledError):
            await task
        assert (await h.router.query(h.operator.token, "list", {"kind": "drive_receipt"}))[
            "items"
        ] == []
        assert current(h, "drive_turn", turn["id"])["state"] == "prepared"
    assert (await h.router.query(h.operator.token, "list", {"kind": "report"}))["items"] == []
