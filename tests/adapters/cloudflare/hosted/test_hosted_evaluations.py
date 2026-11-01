"""Independent hosted evaluation with actual fixture-owned CPU execution."""

import asyncio
from types import SimpleNamespace

import pytest

from adapters.cloudflare.research.research_router import HostedEvaluationConfiguration
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.artifacts import ArtifactRecord, StoredBlob
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import encode
from research_swarm.storage.async_r2 import ObjectBytes
from research_swarm.storage.r2_objects import generation_key
from research_swarm.workers.local import LocalWorker
from tests.adapters.cloudflare.hosted.test_hosted_artifacts import current
from tests.adapters.cloudflare.hosted.test_hosted_http import send
from tests.adapters.cloudflare.hosted.test_hosted_research import research as research
from tests.adapters.cloudflare.hosted.test_hosted_research import sdk
from tests.adapters.cloudflare.hosted.test_hosted_result_publication import (
    publication as publication,
)
from tests.adapters.cloudflare.hosted.test_hosted_result_publication import (
    publication_diagnostic,
    publish,
)
from tests.adapters.cloudflare.hosted.test_hosted_router import counts
from tests.adapters.cloudflare.hosted.test_hosted_router import hosted as hosted
from tests.projects.test_manifest import project as project


@pytest.fixture
async def evaluation(publication):
    case, h = publication, publication.h
    attempt = (await publish(case))["result"]
    assert attempt["state"] == "succeeded", publication_diagnostic(case, attempt)
    h.config = HostedEvaluationConfiguration(**h.config.model_dump(exclude={"profile"}))
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=case.acquire)
    evaluator = h.credentials.issue(
        h.principal,
        h.principal.model_copy(update={"id": new_id(), "scopes": {"read", "evaluation.publish"}}),
    )
    loop = asyncio.get_running_loop()

    def ready():
        with sdk(h, loop) as client:
            jobs = client.list(RecordQuery(kind="job", state="ready"))["items"]
            assert len(jobs) == 1
            return client.get("job", jobs[0]["id"])

    job = await asyncio.to_thread(ready)
    return SimpleNamespace(
        case=case,
        h=h,
        loop=loop,
        evaluator=evaluator,
        job=job,
        intent=AuthorizeEvaluation(job_id=job["id"], evaluator_id=evaluator.principal.id),
    )


async def authorize(e):
    return await e.h.router.command(
        e.h.operator.token,
        "evaluation.authorize",
        e.intent.model_dump(mode="json"),
        idempotency_key="authorize-evaluation",
        expected_revision=e.job["revision"],
    )


async def execute_evaluation(e):
    authorized = await authorize(e)

    def execute():
        with (
            sdk(e.h, e.loop) as operator,
            sdk(e.h, e.loop, token=e.case.credential.token) as client,
        ):
            runner = LocalWorker(
                client,
                e.case.runner.worker,
                root=e.case.runner.root,
                machine_registry=e.case.runner.resources.path,
            )
            attempt = runner.claim(operator.get("job", e.job["id"]))
            accepted = runner.execute(attempt["id"])
            assert accepted["state"] == "succeeded"
            return accepted

    attempt = await asyncio.to_thread(execute)
    return (
        authorized,
        attempt,
        CompleteEvaluation(request_id=authorized["result"]["id"], attempt_id=attempt["id"]),
    )


async def complete(e, intent, *, key="complete-evaluation", token=None):
    return await e.h.router.command(
        token or e.evaluator.token,
        "evaluation.complete",
        intent.model_dump(mode="json"),
        idempotency_key=key,
    )


@pytest.mark.parametrize("publication", ["evaluation"], indirect=True)
async def test_independent_evaluator_completes_native_stage_and_replays_offline(evaluation):
    e, h = evaluation, evaluation.h
    authorized, attempt, intent = await execute_evaluation(e)
    assert (
        current(h, "trial", e.case.admitted.trial["result"]["id"])["data"]["evaluation_status"]
        == "pending"
    )
    before = len(h.transport.calls), len(h.witness.requests)
    with pytest.raises(ResearchError) as denied:
        await complete(e, intent, token=e.case.credential.token)
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert (len(h.transport.calls), len(h.witness.requests)) == before
    receipt = await complete(e, intent)
    assert receipt["result"]["state"] == "valid"
    assert receipt["result"]["data"]["receipt"]["evaluator_id"] == e.evaluator.principal.id
    assert receipt["result"]["data"]["attempt_id"] == attempt["id"]
    assert (
        current(h, "trial", e.case.admitted.trial["result"]["id"])["data"]["evaluation_status"]
        == "valid"
    )
    assert (
        h.store.read(
            lambda sql: sql.execute("SELECT COUNT(*) FROM records WHERE kind='promotion'").one()[0]
        )
        == 0
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=e.case.acquire)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture source unavailable")
    h.transport.hide_reads = True
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    assert await authorize(e) == authorized
    assert await complete(e, intent) == receipt
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before

    def query():
        with sdk(h, e.loop, token=e.evaluator.token) as client:
            assert client.get("evaluation_receipt", receipt["result"]["id"]) == receipt["result"]
            assert len(client.list(RecordQuery(kind="evaluation_receipt"))["items"]) == 1

    await asyncio.to_thread(query)


@pytest.mark.parametrize(
    "publication", ["evaluation-incomplete", "evaluation-invalid"], indirect=True
)
async def test_ineligible_evaluation_is_recorded_without_validating_trial(evaluation):
    e = evaluation
    _, _, intent = await execute_evaluation(e)
    receipt = (await complete(e, intent))["result"]
    assert receipt["state"] in {"incomplete", "invalid"}
    assert (
        current(e.h, "trial", e.case.admitted.trial["result"]["id"])["data"]["evaluation_status"]
        == receipt["state"]
    )


@pytest.mark.parametrize("publication", ["evaluation"], indirect=True)
async def test_manifest_transfer_recovers_lost_put_without_duplicate_preparation(evaluation):
    e, h = evaluation, evaluation.h
    before = len(h.witness.requests)
    h.transport.lose_put_response = True
    with pytest.raises(ResearchError) as lost:
        await authorize(e)
    assert lost.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    assert (
        h.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='evaluation_request'"
            ).one()[0]
        )
        == 0
    )
    h.transport.lose_put_response = False
    accepted = await authorize(e)
    assert accepted["result"]["state"] == "authorized"
    requests = h.witness.requests[before:]
    assert len(requests) == 3 and len({request.nonce for request in requests}) == 3
    assert len({request.request_digest for request in requests}) == 3


@pytest.mark.parametrize("publication", ["evaluation"], indirect=True)
@pytest.mark.parametrize("interrupted", [False, True])
async def test_corrected_authorization_retires_duplicate_inline_generation(
    evaluation, monkeypatch, interrupted
):
    e, h = evaluation, evaluation.h
    with pytest.raises(ResearchError) as rejected:
        await h.router.command(
            h.operator.token,
            "evaluation.authorize",
            e.intent.model_copy(update={"exposure_class": "confirmation"}).model_dump(mode="json"),
            idempotency_key="unplanned-confirmation",
            expected_revision=e.job["revision"],
        )
    assert rejected.value.code == ErrorCode.COMPARISON_INELIGIBLE
    if interrupted:
        retire = h.objects.retire

        async def lose_retirement(*args):
            await retire(*args)
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture cleanup response lost")

        monkeypatch.setattr(h.objects, "retire", lose_retirement)
        with pytest.raises(ResearchError) as lost:
            await authorize(e)
        assert lost.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
        assert (
            h.store.read(
                lambda sql: sql.execute(
                    "SELECT COUNT(*) FROM records WHERE kind='upload' AND state='cleanup'"
                ).one()[0]
            )
            == 1
        )
        assert (
            h.store.read(
                lambda sql: sql.execute(
                    "SELECT COUNT(*) FROM records WHERE kind='evaluation_request'"
                ).one()[0]
            )
            == 0
        )
        monkeypatch.setattr(h.objects, "retire", retire)
    authorized = (await authorize(e))["result"]
    assert authorized["state"] == "authorized"
    assert (
        h.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='upload' AND state='cleanup'"
            ).one()[0]
        )
        == 0
    )
    manifest = ArtifactRecord.model_validate(
        current(h, "artifact", authorized["data"]["request"]["input_artifact"]["id"])["data"]
    )
    generations = h.store.read(
        lambda sql: [
            row[0]
            for row in sql.execute(
                "SELECT id FROM records WHERE kind='upload' AND json_extract(payload,'$.digest')=?",
                (manifest.reference.digest,),
            )
        ]
    )
    assert len(generations) == 2
    duplicate = next(value for value in generations if value != manifest.storage_generation)
    with pytest.raises(ResearchError) as retired:
        await h.objects.read(
            h.principal.workspace_id, StoredBlob(reference=manifest.reference, generation=duplicate)
        )
    assert retired.value.code == ErrorCode.INVALID_TRANSITION


@pytest.mark.parametrize("publication", ["evaluation"], indirect=True)
@pytest.mark.parametrize("operation", ["evaluation.authorize", "evaluation.complete"])
@pytest.mark.parametrize("fault", ["credential", "record", "cancel"])
async def test_evaluation_rechecks_after_final_witness(evaluation, monkeypatch, operation, fault):
    e, h = evaluation, evaluation.h
    intent = None
    if operation == "evaluation.complete":
        _, attempt, intent = await execute_evaluation(e)
    original = h.router._witnessed
    before_settlements = h.store.read(
        lambda sql: sql.execute("SELECT COUNT(*) FROM budget_settlements").one()[0]
    )

    def changed(perform):
        if fault == "credential":
            h.credentials.revoke(
                h.principal,
                h.operator.id if operation == "evaluation.authorize" else e.evaluator.id,
            )
        elif fault == "cancel":
            asyncio.current_task().cancel()
        else:
            kind = "trial" if operation == "evaluation.authorize" else "artifact"
            identifier = (
                e.case.admitted.trial["result"]["id"]
                if kind == "trial"
                else attempt["data"]["report_artifact"]["id"]
            )
            row = current(h, kind, identifier)
            row["data"].update(
                {"execution_outcome": "closed_unaccepted"}
                if kind == "trial"
                else {"storage_generation": new_id()}
            )
            h.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET payload=? WHERE kind=? AND id=?",
                    (encode(row["data"]), kind, identifier),
                )
            )
        return perform()

    async def witnessed(context, name, payload, perform):
        return await original(
            context, name, payload, (lambda: changed(perform)) if name == operation else perform
        )

    monkeypatch.setattr(h.router, "_witnessed", witnessed)
    task = asyncio.create_task(authorize(e) if intent is None else complete(e, intent))
    with pytest.raises((ResearchError, asyncio.CancelledError)):
        await task
    kind = "evaluation_request" if operation == "evaluation.authorize" else "evaluation_receipt"
    assert (
        h.store.read(
            lambda sql: sql.execute("SELECT COUNT(*) FROM records WHERE kind=?", (kind,)).one()[0]
        )
        == 0
    )
    assert (
        h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM budget_settlements").one()[0])
        == before_settlements
    )


@pytest.mark.parametrize("publication", ["evaluation"], indirect=True)
async def test_wrong_designated_evaluator_cannot_read_or_publish_report(evaluation):
    e, h = evaluation, evaluation.h
    _, _, intent = await execute_evaluation(e)
    impostor = h.credentials.issue(
        h.principal, e.evaluator.principal.model_copy(update={"id": new_id()})
    )
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    with pytest.raises(ResearchError) as denied:
        await complete(e, intent, token=impostor.token)
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before


@pytest.mark.parametrize("publication", ["evaluation"], indirect=True)
async def test_publication_profile_refuses_evaluation_before_body_consumption(publication):
    consume = []
    response = await send(
        publication.h,
        "POST",
        "/v1/commands/evaluation.authorize",
        body=b"unused",
        token=publication.h.operator.token,
        consume=consume,
    )
    assert response.status == 501 and consume == []


@pytest.mark.parametrize("publication", ["evaluation"], indirect=True)
@pytest.mark.parametrize("fault", ["missing", "corrupt"])
async def test_completion_verifies_report_bytes_before_witness(evaluation, fault):
    e, h = evaluation, evaluation.h
    _, attempt, intent = await execute_evaluation(e)
    record = ArtifactRecord.model_validate(
        current(h, "artifact", attempt["data"]["report_artifact"]["id"])["data"]
    )
    key = generation_key(
        h.config.storage.prefix, record.workspace_id, h.router.service.artifacts._blob(record)
    )
    saved = h.transport.objects.pop(key)
    if fault == "corrupt":
        h.transport.objects[key] = ObjectBytes(b"corrupt report", saved.metadata)
    before = counts(h), len(h.witness.requests)
    with pytest.raises(ResearchError) as refused:
        await complete(e, intent)
    assert refused.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert (counts(h), len(h.witness.requests)) == before


@pytest.mark.parametrize("publication", ["evaluation"], indirect=True)
async def test_authorization_rechecks_manifest_catalog_after_final_witness(evaluation, monkeypatch):
    e, h = evaluation, evaluation.h
    publish_bytes, witnessed = h.router.artifacts.publish_bytes, h.router._witnessed
    saved = {}

    async def capture(*args, **kwargs):
        reference = await publish_bytes(*args, **kwargs)
        saved["reference"] = reference
        return reference

    def change(perform):
        identifier = saved["reference"].id
        row = current(h, "artifact", identifier)
        row["data"]["storage_generation"] = new_id()
        h.store.write(
            lambda sql: sql.execute(
                "UPDATE records SET payload=? WHERE kind='artifact' AND id=?",
                (encode(row["data"]), identifier),
            )
        )
        return perform()

    async def acquire(context, operation, payload, perform):
        return await witnessed(
            context,
            operation,
            payload,
            (lambda: change(perform)) if operation == "evaluation.authorize" else perform,
        )

    monkeypatch.setattr(h.router.artifacts, "publish_bytes", capture)
    monkeypatch.setattr(h.router, "_witnessed", acquire)
    with pytest.raises(ResearchError) as refused:
        await authorize(e)
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert (
        h.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='evaluation_request'"
            ).one()[0]
        )
        == 0
    )
