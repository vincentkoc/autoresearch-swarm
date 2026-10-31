"""Shared scientific decisions over actual hosted-adapter CPU trial evidence."""

import asyncio

import pytest

from adapters.cloudflare.research_router import HostedScientificConfiguration
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import encode
from research_swarm.workers.local import LocalWorker
from tests.test_hosted_artifacts import current
from tests.test_hosted_evaluations import complete, execute_evaluation
from tests.test_hosted_evaluations import evaluation as evaluation
from tests.test_hosted_http import send
from tests.test_hosted_lifecycle import command
from tests.test_hosted_lifecycle import lifecycle as lifecycle
from tests.test_hosted_research import research as research
from tests.test_hosted_research import sdk
from tests.test_hosted_result_publication import publication as publication
from tests.test_hosted_router import counts
from tests.test_hosted_router import hosted as hosted
from tests.test_manifest import project as project

pytestmark = pytest.mark.parametrize("publication", ["evaluation"], indirect=True)


@pytest.fixture
async def scientific(lifecycle):
    e, h = lifecycle, lifecycle.h
    h.config = HostedScientificConfiguration(**h.config.model_dump(exclude={"profile"}))
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=e.case.acquire)
    return e


def campaign(e):
    return current(e.h, "campaign", e.case.admitted.campaign["id"])


async def evaluated(e):
    _, _, intent = await execute_evaluation(e)
    return (await complete(e, intent))["result"]


def assessment(e, receipt):
    return {
        "campaign_id": campaign(e)["id"],
        "candidate_evaluation_id": receipt["id"],
        "expected_incumbent_revision": campaign(e)["data"]["incumbent_revision"],
    }


async def select(e, receipt):
    assessed = await command(e, "promotion.assess", assessment(e, receipt), key="assess")
    return await command(
        e, "promotion.commit", {"decision_id": assessed["result"]["id"]}, key="select"
    )


async def correction(e, receipt):
    return await command(
        e,
        "evidence.invalidate",
        {
            "target": {"kind": "evaluation_receipt", "id": receipt["id"]},
            "reason": "Synthetic evidence correction",
        },
        key="correction",
    )


def domain_counts(h):
    return h.store.read(
        lambda sql: tuple(
            sql.execute(f"SELECT COUNT(*) FROM {table}").one()[0]
            for table in ("records", "events", "budget_settlements")
        )
    )


async def test_observed_selection_cas_completion_correction_and_exact_replay(scientific):
    e, h = scientific, scientific.h
    receipt = await evaluated(e)
    history = []

    async def record(operation, payload, key, revision=None):
        before = len(h.transport.calls), len(h.witness.requests)
        response = await command(e, operation, payload, key=key, revision=revision)
        assert (len(h.transport.calls), len(h.witness.requests)) == (before[0], before[1] + 1)
        history.append((operation, payload, key, revision, response))
        return response

    assessments = [
        await record("promotion.assess", assessment(e, receipt), f"assess-{index}")
        for index in range(2)
    ]
    for item in assessments:
        data = item["result"]["data"]
        assert data["evidence_level"] == "observed"
        assert data["decision"]["reason_codes"] == ["baseline_initialized"]
        assert data["decision"]["improvement"] is None
    requests = [{"decision_id": item["result"]["id"]} for item in assessments]
    before = len(h.transport.calls), len(h.witness.requests)
    results = await asyncio.gather(
        *(
            command(e, "promotion.commit", request, key=f"commit-{i}")
            for i, request in enumerate(requests)
        ),
        return_exceptions=True,
    )
    assert (len(h.transport.calls), len(h.witness.requests)) == (before[0], before[1] + 2)
    winning = next(i for i, result in enumerate(results) if isinstance(result, dict))
    losing = 1 - winning
    assert isinstance(results[losing], ResearchError)
    assert results[losing].code == ErrorCode.INCUMBENT_REVISION_CONFLICT
    history.append(
        ("promotion.commit", requests[winning], f"commit-{winning}", None, results[winning])
    )
    assert campaign(e)["data"]["incumbent_revision"] == 1
    for index, item in enumerate(assessments):
        stored = current(h, "promotion", item["result"]["id"])
        assert stored["state"] == ("committed" if index == winning else "assessed")
        assert stored["data"] == item["result"]["data"]

    before_completion = campaign(e)
    completed = await record(
        "campaign.complete",
        {"campaign_id": before_completion["id"], "reason": "Fixture research completed"},
        "complete",
        before_completion["revision"],
    )
    finished, report = completed["result"]["campaign"], completed["result"]["report"]
    assert finished["state"] == "completed"
    assert finished["data"]["final_report_id"] == report["id"]
    data = report["data"]
    assert report["state"] == "immutable"
    assert data["report_digest"] == record_digest(
        "research.report", {key: value for key, value in data.items() if key != "report_digest"}
    )
    snapshot = data["snapshot"]
    assert snapshot["campaign_state"] == "completed"
    assert snapshot["campaign_revision"] == finished["revision"]
    assert snapshot["incumbent"]["evidence_level"] == "observed"
    assert snapshot["recent_evaluations"][0]["eligible"]
    assert snapshot["evaluation_claim_support"]["protected"] is False
    assert snapshot["evaluation_claim_support"]["official"] is False
    assert all(row["reserved"] == row["uncertain"] == "0" for row in snapshot["budget"])
    watermark_event = h.store.read(
        lambda sql: sql.execute(
            "SELECT json_extract(payload,'$.type') FROM events WHERE sequence=?",
            (snapshot["evidence_through"],),
        ).one()[0]
    )
    assert watermark_event == "campaign.completed"
    frozen = canonical_bytes(report)
    await record(
        "evidence.invalidate",
        {
            "target": {"kind": "evaluation_receipt", "id": receipt["id"]},
            "reason": "Synthetic late correction",
        },
        "late-correction",
    )
    assert canonical_bytes(current(h, "report", report["id"])) == frozen
    assert current(h, "evaluation_receipt", receipt["id"]) == receipt
    assert current(h, "promotion", requests[winning]["decision_id"])["state"] == "affected"
    assert campaign(e)["state"] == "completed"
    assert campaign(e)["data"]["incumbent_revision"] == 2
    assert campaign(e)["data"]["incumbent_eligibility"] == "suspended"
    recent = await record(
        "report.create", {"campaign_id": finished["id"], "recent_limit": 1}, "current-report"
    )
    corrected = recent["result"]["data"]["snapshot"]
    assert corrected["incumbent"]["eligibility"] == "suspended"
    assert not corrected["recent_evaluations"][0]["eligible"]
    assert corrected["recent_evaluations"][0]["data"] == snapshot["recent_evaluations"][0]["data"]
    with pytest.raises(ResearchError) as invalidated:
        await command(e, "promotion.commit", requests[winning], key="fresh-commit")
    assert invalidated.value.code == ErrorCode.EVIDENCE_INVALIDATED

    def query():
        with sdk(h, e.loop) as client:
            assert client.get("report", report["id"]) == report
            assert len(client.list(RecordQuery(kind="report"))["items"]) == 2
            assert len(client.list(RecordQuery(kind="run_result"))["items"]) == 2
            assert len(client.list(RecordQuery(kind="evidence_correction"))["items"]) == 1
            assert (
                client.capabilities()["scientific_closeout"]["confirmation_plans"] == "unavailable"
            )

    await asyncio.to_thread(query)
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=e.case.acquire)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Source unavailable")
    h.transport.hide_reads = True
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    for operation, payload, key, revision, response in history:
        assert await command(e, operation, payload, key=key, revision=revision) == response
    for key, expected, request in (
        (f"commit-{losing}", ErrorCode.INCUMBENT_REVISION_CONFLICT, requests[losing]),
        ("fresh-commit", ErrorCode.EVIDENCE_INVALIDATED, requests[winning]),
    ):
        with pytest.raises(ResearchError) as denied:
            await command(e, "promotion.commit", request, key=key)
        assert denied.value.code == expected
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before


async def test_completion_requires_native_settlement_and_independent_evaluation(scientific):
    e = scientific
    payload = {"campaign_id": campaign(e)["id"], "reason": "Fixture completion"}
    before = domain_counts(e.h)
    with pytest.raises(ResearchError) as queued:
        await command(
            e, "campaign.complete", payload, key="queued", revision=campaign(e)["revision"]
        )
    assert queued.value.code == ErrorCode.INVALID_TRANSITION
    assert domain_counts(e.h) == before
    _, _, intent = await execute_evaluation(e)
    before = domain_counts(e.h)
    with pytest.raises(ResearchError) as pending:
        await command(
            e, "campaign.complete", payload, key="pending", revision=campaign(e)["revision"]
        )
    assert pending.value.code == ErrorCode.EVALUATION_INCOMPLETE
    assert domain_counts(e.h) == before
    await complete(e, intent)
    assert (
        await command(
            e, "campaign.complete", payload, key="finish", revision=campaign(e)["revision"]
        )
    )["result"]["campaign"]["state"] == "completed"


async def test_reinitialization_requires_fresh_evidence_and_preserves_the_correction(scientific):
    e, h = scientific, scientific.h
    old = await evaluated(e)
    await select(e, old)
    corrected = await correction(e, old)
    with pytest.raises(ResearchError) as stale:
        await command(e, "promotion.reinitialize", assessment(e, old), key="old-baseline")
    assert stale.value.code == ErrorCode.COMPARISON_INELIGIBLE

    def fresh_trial():
        with sdk(h, e.loop) as operator, sdk(h, e.loop, token=e.case.credential.token) as worker:
            runner = LocalWorker(
                worker,
                e.case.runner.worker,
                root=e.case.runner.root,
                machine_registry=e.case.runner.resources.path,
            )
            operator.command(
                "trial.submit",
                {**e.case.admitted.submission, "seed": 43},
                idempotency_key="fresh-trial",
                expected_revision=campaign(e)["revision"],
            )

            def ready():
                rows = operator.list(RecordQuery(kind="job", state="ready"))["items"]
                assert len(rows) == 1
                return operator.get("job", rows[0]["id"])

            training = runner.claim(ready())
            assert runner.execute(training["id"])["state"] == "succeeded"
            scoring = ready()
            authorized = operator.command(
                "evaluation.authorize",
                {"job_id": scoring["id"], "evaluator_id": e.evaluator.principal.id},
                idempotency_key="fresh-evaluation",
                expected_revision=scoring["revision"],
            )["result"]
            attempt = runner.claim(ready())
            assert runner.execute(attempt["id"])["state"] == "succeeded"
            return authorized, attempt

    authorized, attempt = await asyncio.to_thread(fresh_trial)
    fresh = (
        await h.router.command(
            e.evaluator.token,
            "evaluation.complete",
            {"request_id": authorized["id"], "attempt_id": attempt["id"]},
            idempotency_key="fresh-receipt",
        )
    )["result"]
    with pytest.raises(ResearchError) as suspended:
        await command(e, "promotion.assess", assessment(e, fresh), key="ordinary-assess")
    assert suspended.value.code == ErrorCode.EVIDENCE_INVALIDATED
    assessed = (
        await command(e, "promotion.reinitialize", assessment(e, fresh), key="reinitialize")
    )["result"]
    assert assessed["data"]["decision"]["reason_codes"] == ["baseline_reinitialized"]
    assert assessed["data"]["decision"]["improvement"] is None
    assert assessed["data"]["evidence_level"] == "observed"
    await command(e, "promotion.commit", {"decision_id": assessed["id"]}, key="reinitialize-commit")
    assert campaign(e)["data"]["incumbent_eligibility"] == "eligible"
    assert campaign(e)["data"]["incumbent_revision"] == 3
    assert current(h, "evaluation_receipt", old["id"]) == old
    assert current(h, "evidence_correction", corrected["result"]["id"])["state"] == "effective"


async def test_report_failure_rolls_back_completion_then_same_key_can_retry(
    scientific, monkeypatch
):
    e = scientific
    await evaluated(e)
    before, original = domain_counts(e.h), campaign(e)
    payload = {"campaign_id": original["id"], "reason": "Fixture completion"}

    def fail(*args, **kwargs):
        raise RuntimeError("Fixture report write failure")

    with monkeypatch.context() as patch:
        patch.setattr("research_swarm.control.campaigns.create_report", fail)
        with pytest.raises(RuntimeError, match="report write failure"):
            await command(
                e, "campaign.complete", payload, key="finish", revision=original["revision"]
            )
    assert campaign(e) == original and domain_counts(e.h) == before
    assert (
        await command(e, "campaign.complete", payload, key="finish", revision=original["revision"])
    )["result"]["campaign"]["state"] == "completed"


async def test_old_profile_refuses_closeout_before_body(lifecycle):
    h = lifecycle.h
    consumed = []
    response = await send(
        h,
        "POST",
        "/v1/commands/report.create",
        body=b"unused",
        token=h.operator.token,
        consume=consumed,
    )
    assert response.status == 501 and consumed == []


async def test_closeout_requires_its_own_scopes_before_witness(scientific):
    e, h = scientific, scientific.h
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    for credential in (e.case.credential, e.evaluator):
        for operation, payload, revision in (
            ("promotion.assess", assessment(e, {"id": e.job["id"]}), None),
            ("promotion.reinitialize", assessment(e, {"id": e.job["id"]}), None),
            ("promotion.commit", {"decision_id": e.job["id"]}, None),
            (
                "evidence.invalidate",
                {
                    "target": {"kind": "evaluation_receipt", "id": e.job["id"]},
                    "reason": "Unauthorized fixture correction",
                },
                None,
            ),
            ("report.create", {"campaign_id": campaign(e)["id"]}, None),
            (
                "campaign.complete",
                {"campaign_id": campaign(e)["id"], "reason": "Unauthorized"},
                campaign(e)["revision"],
            ),
        ):
            with pytest.raises(ResearchError) as denied:
                await h.router.command(
                    credential.token,
                    operation,
                    payload,
                    idempotency_key=operation,
                    expected_revision=revision,
                )
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before


@pytest.mark.parametrize("fault", ["credential", "cancel"])
async def test_closeout_rechecks_identity_after_witness(scientific, monkeypatch, fault):
    e, h = scientific, scientific.h
    original = h.router.acquire

    async def acquire(request):
        response = await original(request)
        if fault == "credential":
            h.credentials.revoke(h.principal, h.operator.id)
        else:
            asyncio.current_task().cancel()
        return response

    monkeypatch.setattr(h.router, "acquire", acquire)
    before = domain_counts(h)
    operation = asyncio.create_task(
        command(e, "report.create", {"campaign_id": campaign(e)["id"]}, key="report")
    )
    with pytest.raises(ResearchError if fault == "credential" else asyncio.CancelledError):
        await operation
    assert domain_counts(h) == before
