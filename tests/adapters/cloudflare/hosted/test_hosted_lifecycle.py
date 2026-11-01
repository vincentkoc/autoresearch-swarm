"""Hosted lifecycle intent preserves native closure and independent admission rules."""

import asyncio

import pytest

from adapters.cloudflare.research.research_router import HostedLifecycleConfiguration
from research_swarm.control.acceptance_models import LeaseRenewal
from research_swarm.control.worker_lifecycle import WorkerHeartbeat
from research_swarm.domain.controls import ControlAcknowledgment
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import encode
from tests.adapters.cloudflare.hosted.test_hosted_artifacts import current
from tests.adapters.cloudflare.hosted.test_hosted_evaluations import (
    authorize,
    complete,
    execute_evaluation,
)
from tests.adapters.cloudflare.hosted.test_hosted_evaluations import evaluation as evaluation
from tests.adapters.cloudflare.hosted.test_hosted_http import send
from tests.adapters.cloudflare.hosted.test_hosted_research import research as research
from tests.adapters.cloudflare.hosted.test_hosted_result_publication import (
    publication as publication,
)
from tests.adapters.cloudflare.hosted.test_hosted_router import counts
from tests.adapters.cloudflare.hosted.test_hosted_router import hosted as hosted
from tests.projects.test_manifest import project as project

pytestmark = pytest.mark.parametrize("publication", ["evaluation"], indirect=True)


@pytest.fixture
async def lifecycle(evaluation):
    e, h = evaluation, evaluation.h
    h.config = HostedLifecycleConfiguration(**h.config.model_dump(exclude={"profile"}))
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=e.case.acquire)
    return e


async def command(e, operation, payload, *, key, revision=None, worker=False):
    return await e.h.router.command(
        e.case.credential.token if worker else e.h.operator.token,
        operation,
        payload.model_dump(mode="json") if hasattr(payload, "model_dump") else payload,
        idempotency_key=key,
        expected_revision=revision,
    )


async def claim(e, *, key="lifecycle-claim"):
    await authorize(e)
    job = current(e.h, "job", e.job["id"])
    binding = e.case.runner.worker.binding
    return await command(
        e,
        "worker.claim_job",
        {
            "worker_id": binding.worker_id,
            "session_id": binding.worker_session_id,
            "job_id": job["id"],
        },
        key=key,
        revision=job["revision"],
        worker=True,
    )


async def test_campaign_drain_allows_active_trial_scoring_then_resume(lifecycle):
    e = lifecycle
    campaign = current(e.h, "campaign", e.case.admitted.campaign["id"])
    paused = await command(
        e,
        "campaign.pause",
        {"campaign_id": campaign["id"], "mode": "drain"},
        key="pause",
        revision=campaign["revision"],
    )
    assert paused["result"]["state"] == "pausing"
    with pytest.raises(ResearchError) as refused:
        await command(
            e,
            "trial.submit",
            {**e.case.admitted.submission, "seed": 43},
            key="new-trial-while-pausing",
            revision=paused["result"]["revision"],
        )
    assert refused.value.code == ErrorCode.INVALID_TRANSITION
    _, _, intent = await execute_evaluation(e)
    settled = current(e.h, "campaign", campaign["id"])
    assert settled["state"] == "paused"
    assert (await complete(e, intent))["result"]["state"] == "valid"
    resumed = await command(
        e,
        "campaign.start",
        {"campaign_id": campaign["id"]},
        key="resume",
        revision=settled["revision"],
    )
    assert resumed["result"]["state"] == "running"


async def test_worker_drain_blocks_continuation_until_admission_reopens(lifecycle):
    e = lifecycle
    binding = e.case.runner.worker.binding.model_dump(mode="json")
    request = {"binding": binding, "admission": "draining", "reason": "Fixture drain"}
    draining = await command(e, "worker.set_admission", request, key="drain", revision=0)
    with pytest.raises(ResearchError) as refused:
        await claim(e, key="claim-while-draining")
    assert refused.value.code == ErrorCode.INVALID_TRANSITION
    await command(
        e,
        "worker.set_admission",
        {**request, "admission": "active"},
        key="reopen",
        revision=draining["result"]["revision"],
    )
    _, _, intent = await execute_evaluation(e)
    assert (await complete(e, intent))["result"]["state"] == "valid"


async def test_control_ack_and_acceptance_renewal_do_not_prove_execution_closure(lifecycle):
    e, h = lifecycle, lifecycle.h
    attempt = (await claim(e))["result"]
    target = {"attempt_id": attempt["id"], "reason": "Fixture cancellation"}
    cancelled = await command(
        e, "attempt.request_cancel", target, key="cancel", revision=attempt["revision"]
    )
    control = cancelled["result"]["control"]
    assert control["state"] == "requested"
    lease = current(h, "acceptance_lease", attempt["id"])
    beat = WorkerHeartbeat(
        binding=e.case.runner.worker.binding,
        driver_instance_id=new_id(),
        sequence=1,
        acknowledgments=(
            ControlAcknowledgment(
                attempt_id=attempt["id"],
                control_sequence=control["data"]["control_sequence"],
                dispatch_digest=control["data"]["dispatch_digest"],
            ),
        ),
        lease_renewals=(
            LeaseRenewal(
                attempt_id=attempt["id"],
                dispatch_digest=lease["data"]["dispatch_digest"],
                expected_lease_revision=lease["revision"],
            ),
        ),
    )
    before_settlements = h.store.read(
        lambda sql: sql.execute("SELECT COUNT(*) FROM budget_settlements").one()[0]
    )
    received = await command(e, "worker.heartbeat", beat, key="heartbeat", worker=True)
    assert received["result"]["presence"]["data"]["heartbeat_sequence"] == 1
    assert current(h, "attempt_control", attempt["id"])["state"] == "acknowledged"
    assert current(h, "attempt", attempt["id"]) == attempt
    renewed = current(h, "acceptance_lease", attempt["id"])
    assert renewed["revision"] == lease["revision"] + 1
    assert renewed["data"]["expires_at"] <= renewed["data"]["renew_until"]
    assert (
        h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM budget_settlements").one()[0])
        == before_settlements
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=e.case.acquire)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture source unavailable")
    h.transport.hide_reads = True
    before = counts(h), len(h.witness.requests), len(h.transport.calls)
    assert await command(e, "worker.heartbeat", beat, key="heartbeat", worker=True) == received
    assert (
        await command(
            e, "attempt.request_cancel", target, key="cancel", revision=attempt["revision"]
        )
        == cancelled
    )
    assert (counts(h), len(h.witness.requests), len(h.transport.calls)) == before


async def test_evaluation_profile_refuses_lifecycle_before_consuming_body(evaluation):
    consume = []
    response = await send(
        evaluation.h,
        "POST",
        "/v1/commands/campaign.pause",
        body=b"unused",
        token=evaluation.h.operator.token,
        consume=consume,
    )
    assert response.status == 501 and consume == []


async def test_invalid_control_ack_cannot_partially_renew_or_record_presence(lifecycle):
    e, h = lifecycle, lifecycle.h
    attempt = (await claim(e))["result"]
    lease = current(h, "acceptance_lease", attempt["id"])
    beat = WorkerHeartbeat(
        binding=e.case.runner.worker.binding,
        driver_instance_id=new_id(),
        sequence=1,
        acknowledgments=(
            ControlAcknowledgment(
                attempt_id=new_id(),
                control_sequence=1,
                dispatch_digest=lease["data"]["dispatch_digest"],
            ),
        ),
        lease_renewals=(
            LeaseRenewal(
                attempt_id=attempt["id"],
                dispatch_digest=lease["data"]["dispatch_digest"],
                expected_lease_revision=lease["revision"],
            ),
        ),
    )
    before_events = h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM events").one()[0])
    with pytest.raises(ResearchError) as denied:
        await command(e, "worker.heartbeat", beat, key="invalid-ack", worker=True)
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert current(h, "acceptance_lease", attempt["id"]) == lease
    assert current(h, "attempt", attempt["id"]) == attempt
    assert (
        h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM events").one()[0])
        == before_events
    )
    assert (
        h.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='worker_presence'"
            ).one()[0]
        )
        == 0
    )


@pytest.mark.parametrize("fault", ["credential", "worker", "cancel"])
async def test_heartbeat_rechecks_owner_after_witness(lifecycle, monkeypatch, fault):
    e, h = lifecycle, lifecycle.h
    original = h.router.acquire

    async def acquire(request):
        response = await original(request)
        if fault == "credential":
            h.credentials.revoke(h.principal, e.case.credential.id)
        elif fault == "worker":
            h.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET state='revoked' WHERE kind='worker' AND id=?",
                    (e.case.runner.worker.binding.worker_id,),
                )
            )
        else:
            asyncio.current_task().cancel()
        return response

    monkeypatch.setattr(h.router, "acquire", acquire)
    heartbeat = WorkerHeartbeat(
        binding=e.case.runner.worker.binding,
        driver_instance_id=new_id(),
        sequence=1,
    )
    task = asyncio.create_task(command(e, "worker.heartbeat", heartbeat, key="fenced", worker=True))
    with pytest.raises((ResearchError, asyncio.CancelledError)):
        await task
    assert (
        h.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='worker_presence'"
            ).one()[0]
        )
        == 0
    )
