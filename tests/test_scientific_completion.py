from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest
from mcp import Client

from research_swarm import ResearchClient
from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport
from research_swarm.control.commands import CampaignTarget, SubmitTrial
from research_swarm.control.completion_models import (
    CaptureCampaignObligations,
    CloseCampaignResources,
    CompleteScientificCampaign,
)
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.models import StoppingRule, TrialData
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import ReportCampaign
from research_swarm.control.results import PublishAttempt
from research_swarm.control.workers import AttemptData
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from research_swarm.workers.journal import JournalState
from tests.test_campaign_completion import complete
from tests.test_child_campaigns import OPTIONS, accounts, child_trial, create, start
from tests.test_cli_mcp import cli, stdio
from tests.test_closure import capture as capture_execution
from tests.test_closure import close_operationally
from tests.test_evaluation_control import prepare_evaluation
from tests.test_evaluation_driver import role
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_promotions import assess, commit, evaluated
from tests.test_reconciliation import reconcile, submit
from tests.test_worker_sessions import advance_epoch


def review(pipeline: Pipeline, client: ResearchClient | None = None) -> dict[str, Any]:
    client = client or pipeline.client
    campaign = client.get("campaign", pipeline.campaign["id"])
    return client.command(
        "campaign.capture_obligations",
        CaptureCampaignObligations(campaign_id=campaign["id"], review_id=new_id()),
        idempotency_key=new_id(),
        expected_revision=campaign["revision"],
    )["result"]


def finish(pipeline: Pipeline, captured: dict[str, Any] | None = None) -> dict[str, Any]:
    captured = captured or review(pipeline)
    return pipeline.client.command(
        "campaign.complete_scientific",
        CompleteScientificCampaign(
            campaign_id=pipeline.campaign["id"],
            obligations_id=captured["id"],
            reason="Finish declared research with retained operational obligations",
        ),
        idempotency_key=new_id(),
        expected_revision=captured["data"]["campaign_revision"],
    )["result"]


def close_resources(
    pipeline: Pipeline,
    captured: dict[str, Any] | None = None,
    *,
    client: ResearchClient | None = None,
) -> dict[str, Any]:
    captured = captured or review(pipeline)
    return (client or pipeline.client).command(
        "campaign.close_resources",
        CloseCampaignResources(
            campaign_id=pipeline.campaign["id"],
            obligations_id=captured["id"],
            reason="All original custody has reconciled",
        ),
        idempotency_key=new_id(),
        expected_revision=captured["data"]["campaign_revision"],
    )["result"]


def unknown(pipeline: Pipeline, attempt: dict[str, Any], tmp_path: Path) -> None:
    try:
        entry = pipeline.worker.journal.get(attempt["id"])
    except ResearchError as error:
        assert error.code == ErrorCode.NOT_FOUND
        entry = pipeline.worker._prepare(
            AttemptData.model_validate(attempt["data"]),
            TrialData.model_validate(pipeline.trial["data"]),
        )
    observed = entry.model_copy(update={"state": JournalState.UNKNOWN, "outcome": None})
    path = tmp_path / (attempt["id"] + ".json")
    path.write_text(observed.model_dump_json(), encoding="utf-8")
    artifact = pipeline.worker.client.upload_file(path, idempotency_key=new_id())
    pipeline.worker.client.command(
        "worker.publish_attempt",
        PublishAttempt(
            attempt_id=attempt["id"], journal_artifact=artifact, finding="Closure is not yet known"
        ),
        idempotency_key=new_id(),
    )


def entries(pipeline: Pipeline, captured: dict[str, Any]) -> list[dict[str, Any]]:
    chunks = [
        pipeline.client.get("campaign_obligation_chunk", identifier)["data"]
        for identifier in captured["data"]["chunk_ids"]
    ]
    assert [chunk["ordinal"] for chunk in chunks] == list(range(1, len(chunks) + 1))
    assert all(chunk["review_id"] == captured["id"] for chunk in chunks)
    result = [entry for chunk in chunks for entry in chunk["entries"]]
    assert len(result) == captured["data"]["entry_count"]
    assert captured["data"]["obligations_digest"] == record_digest(
        "research.campaign-obligations-v1", {"entries": result}
    )
    return result


def test_unknown_completion_preserves_custody_then_original_closure_settles_once(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    unknown(pipeline, attempt, tmp_path)
    with pytest.raises(ResearchError) as strict:
        complete(pipeline)
    assert strict.value.code == ErrorCode.EXECUTION_UNKNOWN
    before = pipeline.client.get("attempt", attempt["id"])
    budget = pipeline.client.budget(pipeline.campaign["id"])
    native = pipeline.worker.journal.get(attempt["id"])
    captured = review(pipeline)
    obligations = entries(pipeline, captured)
    assert any(
        item["category"] == "attempt" and item["id"] == attempt["id"] for item in obligations
    )
    completion = finish(pipeline, captured)
    assert finish(pipeline, captured) == completion  # A new delivery key, same frozen intent.
    assert pipeline.client.get("campaign", pipeline.campaign["id"])["state"] == "completed"
    assert pipeline.client.get("attempt", attempt["id"]) == before
    assert pipeline.client.budget(pipeline.campaign["id"])["accounts"] == budget["accounts"]
    assert pipeline.worker.journal.get(attempt["id"]) == native
    report_id = completion["data"]["final_report_id"]
    report = pipeline.client.get("report", report_id)
    frozen = canonical_bytes(report)
    assert report["data"]["snapshot"]["operational"]["status"] == "reconciliation_pending"
    assert pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"] == []
    jobs = pipeline.client.list(RecordQuery(kind="job"))["items"]
    assert {job["state"] for job in jobs} == {"execution_unknown", "not_started"}
    with pytest.raises(ResearchError) as held:
        close_resources(pipeline)
    assert held.value.code == ErrorCode.RESOURCE_UNAVAILABLE

    pipeline.worker.journal.request_cancel(attempt["id"], 1)
    result = pipeline.worker.execute(attempt["id"])
    assert result["state"] == "not_started"
    captured = review(pipeline)
    closure = close_resources(pipeline, captured)
    assert close_resources(pipeline, captured) == closure
    assert closure["data"]["final_report_id"] == report_id
    assert closure["data"]["funding_settlement_id"] is None
    assert canonical_bytes(pipeline.client.get("report", report_id)) == frozen
    current = pipeline.client.context(pipeline.campaign["id"])["operational"]
    assert current["status"] == "closed" and current["resource_closure_id"] == closure["id"]
    assert all(
        item["reserved"] == item["uncertain"] == "0"
        for item in accounts(pipeline, pipeline.campaign["id"]).values()
    )


def test_obligation_review_rejects_changed_attempt_without_completing(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    unknown(pipeline, attempt, tmp_path)
    captured = review(pipeline)
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.worker.journal.request_cancel(attempt["id"], 1)
    pipeline.worker.execute(attempt["id"])
    with pytest.raises(ResearchError) as stale:
        finish(pipeline, captured)
    assert stale.value.code == ErrorCode.REVISION_CONFLICT
    assert pipeline.client.get("campaign", pipeline.campaign["id"]) == campaign
    assert pipeline.client.list(RecordQuery(kind="scientific_completion"))["items"] == []
    assert pipeline.client.list(RecordQuery(kind="report"))["items"] == []


def test_known_active_attempt_must_drain_before_scientific_completion(pipeline: Pipeline) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    with pytest.raises(ResearchError) as active:
        finish(pipeline)
    assert active.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert pipeline.client.get("attempt", attempt["id"]) == attempt


@pytest.mark.parametrize(
    "pipeline",
    [{"stopping": StoppingRule(max_trials=2, permit_manual_completion=False)}],
    indirect=True,
)
def test_scientific_completion_obeys_frozen_stopping_rule(pipeline: Pipeline) -> None:
    with pytest.raises(ResearchError) as stopping:
        finish(pipeline)
    assert stopping.value.code == ErrorCode.INVALID_TRANSITION
    assert "stopping condition" in stopping.value.message


@pytest.mark.parametrize("closure_only", [False, True])
def test_late_native_evidence_after_scientific_completion_never_launches_successor(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, closure_only: bool
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    closed = capture_execution(pipeline.worker, attempt["id"], monkeypatch)
    unknown(pipeline, attempt, tmp_path)
    completion = finish(pipeline)
    report = pipeline.client.get("report", completion["data"]["final_report_id"])
    publication = (
        closed.publication.model_copy(update={"report_artifact": None, "outputs": {}})
        if closure_only
        else closed.publication
    )
    submission = submit(closed, publication)
    advance_epoch(pipeline)
    result = (close_operationally if closure_only else reconcile)(pipeline, submission)
    assert result["attempt"]["state"] == ("closed_unaccepted" if closure_only else "succeeded")
    assert result["attempt"]["data"]["dispatch"] == attempt["data"]["dispatch"]
    trial = pipeline.client.get("trial", pipeline.trial["id"])
    assert trial["data"]["resource_status"] == "closed"
    assert trial["data"]["execution_outcome"] == (
        "closed_unaccepted" if closure_only else "cancelled"
    )
    assert pipeline.client.list(RecordQuery(kind="job", state="ready"))["items"] == []
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1
    assert len(pipeline.client.list(RecordQuery(kind="run_result"))["items"]) == (
        0 if closure_only else 1
    )
    assert pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"] == []
    close_resources(pipeline)
    assert pipeline.client.get("report", report["id"]) == report
    assert (
        pipeline.client.get("campaign", pipeline.campaign["id"])["data"]["incumbent_candidate_id"]
        is None
    )


def test_already_authorized_evaluation_can_finish_without_reopening_selection(
    pipeline: Pipeline,
) -> None:
    authorization, evaluator, _ = prepare_evaluation(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker.execute(attempt["id"])
    completion = finish(pipeline)
    report = pipeline.client.get("report", completion["data"]["final_report_id"])
    receipt = evaluator.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authorization["id"], attempt_id=attempt["id"]),
        idempotency_key=new_id(),
    )["result"]
    assert receipt["kind"] == "evaluation_receipt"
    assert pipeline.client.get("report", report["id"]) == report
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == "valid"
    )
    close_resources(pipeline)


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_completion_blocks_new_admission_authorization_promotion_and_restart(
    pipeline: Pipeline,
) -> None:
    assessment = assess(pipeline, evaluated(pipeline))
    finish(pipeline)
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    trial = TrialData.model_validate(pipeline.trial["data"])
    with pytest.raises(ResearchError) as admission:
        pipeline.client.command(
            "trial.submit",
            SubmitTrial(
                campaign_id=campaign["id"],
                candidate_id=trial.spec.candidate_id,
                validation_id=trial.validation_id,
                seed=99,
            ),
            idempotency_key=new_id(),
            expected_revision=campaign["revision"],
        )
    assert admission.value.code == ErrorCode.INVALID_TRANSITION
    with pytest.raises(ResearchError) as child:
        create(pipeline)
    assert child.value.code == ErrorCode.INVALID_TRANSITION
    with pytest.raises(ResearchError) as promotion:
        commit(pipeline, assessment)
    assert promotion.value.code == ErrorCode.INVALID_TRANSITION
    with pytest.raises(ResearchError) as restart:
        pipeline.client.command(
            "campaign.start",
            CampaignTarget(campaign_id=campaign["id"]),
            idempotency_key=new_id(),
            expected_revision=campaign["revision"],
        )
    assert restart.value.code == ErrorCode.INVALID_TRANSITION
    job = next(
        item
        for item in pipeline.client.list(RecordQuery(kind="job"))["items"]
        if item["data"]["stage_id"] == "evaluate"
    )
    with pytest.raises(ResearchError) as authorization:
        pipeline.client.command(
            "evaluation.authorize",
            AuthorizeEvaluation(job_id=job["id"], evaluator_id=new_id()),
            idempotency_key=new_id(),
            expected_revision=job["revision"],
        )
    assert authorization.value.code == ErrorCode.INVALID_TRANSITION


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_child_funding_remains_held_until_original_native_closure(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    branch = child_trial(pipeline, start(pipeline, create(pipeline)["campaign"]))
    attempt = branch.worker.claim(branch.ready())
    closed = capture_execution(branch.worker, attempt["id"], monkeypatch)
    unknown(branch, attempt, tmp_path)
    before = accounts(pipeline, pipeline.campaign["id"])
    with pytest.raises(ResearchError) as active_child:
        finish(pipeline)
    assert active_child.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    completion = finish(branch)
    assert accounts(pipeline, pipeline.campaign["id"]) == before
    assert pipeline.client.get("funding_projection", branch.campaign["id"])["state"] == "open"
    finish(pipeline)  # Finished child science may retain its funding obligation.
    with pytest.raises(ResearchError) as child_held:
        close_resources(pipeline)
    assert child_held.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    reconcile(branch, submit(closed))
    spent = accounts(branch, branch.campaign["id"])
    mid = accounts(pipeline, pipeline.campaign["id"])
    assert (
        Decimal(mid["execution_ms"]["consumed"]) == Decimal(spent["execution_ms"]["consumed"]) > 0
    )
    assert Decimal(mid["execution_ms"]["reserved"]) == 50_000 - Decimal(
        spent["execution_ms"]["consumed"]
    )
    captured = review(branch)
    closure = close_resources(branch, captured)
    assert close_resources(branch, captured) == closure
    assert closure["data"]["funding_settlement_id"] == branch.campaign["id"]
    final = accounts(pipeline, pipeline.campaign["id"])
    assert final["execution_ms"]["reserved"] == "0"
    assert final["execution_ms"]["consumed"] == mid["execution_ms"]["consumed"]
    assert Decimal(final["execution_ms"]["released_total"]) == Decimal(
        mid["execution_ms"]["reserved"]
    )
    close_resources(pipeline)
    assert (
        pipeline.client.get("report", completion["data"]["final_report_id"])["data"]["snapshot"][
            "funding"
        ]["incoming"]["state"]
        == "open"
    )


@pytest.mark.parametrize("failure_at", ["report", "completion_event", "closure_event"])
def test_scientific_completion_and_closure_are_atomic(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, failure_at: str
) -> None:
    if failure_at == "closure_event":
        finish(pipeline)
    captured = review(pipeline)
    before = pipeline.client.get("campaign", pipeline.campaign["id"])
    budget = pipeline.client.budget(pipeline.campaign["id"])
    jobs = pipeline.client.list(RecordQuery(kind="job"))["items"]
    original = Transaction.event

    def fail_report(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("Synthetic atomic completion failure")

    def fail_event(self: Transaction, name: str, *args: Any, **kwargs: Any) -> Any:
        target = (
            "campaign.resources_closed"
            if failure_at == "closure_event"
            else "campaign.scientifically_completed"
        )
        if name == target:
            fail_report()
        return original(self, name, *args, **kwargs)

    with monkeypatch.context() as patch:
        if failure_at == "report":
            patch.setattr("research_swarm.control.scientific_completion.create_report", fail_report)
        else:
            patch.setattr(Transaction, "event", fail_event)
        with pytest.raises(RuntimeError, match="atomic completion failure"):
            (close_resources if failure_at == "closure_event" else finish)(pipeline, captured)
    assert pipeline.client.get("campaign", pipeline.campaign["id"]) == before
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    assert pipeline.client.list(RecordQuery(kind="job"))["items"] == jobs
    kind = "campaign_resource_closure" if failure_at == "closure_event" else "scientific_completion"
    assert pipeline.client.list(RecordQuery(kind=kind))["items"] == []
    (close_resources if failure_at == "closure_event" else finish)(pipeline, captured)


def test_concurrent_resource_closure_from_independent_connections_creates_one_receipt(
    pipeline: Pipeline,
) -> None:
    finish(pipeline)
    captured = review(pipeline)
    barrier = Barrier(2)

    def settle() -> dict[str, Any]:
        remote = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
        barrier.wait(timeout=10)
        return close_resources(pipeline, captured, client=remote)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = list(pool.map(lambda _: settle(), range(2)))
    assert first == second
    assert len(pipeline.client.list(RecordQuery(kind="campaign_resource_closure"))["items"]) == 1
    events = pipeline.client.events(after=0, limit=100)["events"]
    assert sum(event["type"] == "campaign.resources_closed" for event in events) == 1


async def test_http_capture_mcp_completion_and_cli_closure_share_contract(
    pipeline: Pipeline,
) -> None:
    _, _, token = role(pipeline, "*")
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, token) as remote,
    ):
        captured = review(pipeline, remote)
    request = CompleteScientificCampaign(
        campaign_id=pipeline.campaign["id"],
        obligations_id=captured["id"],
        reason="Cross-transport completion",
    )
    async with Client(stdio(pipeline.runtime.root), read_timeout_seconds=20, cache=None) as agent:
        response = await agent.call_tool(
            "research_campaign_complete_scientific",
            {
                "request": request.model_dump(mode="json"),
                "idempotency_key": "agent-finish",
                "expected_revision": captured["data"]["campaign_revision"],
            },
        )
        assert not response.is_error
    captured = review(pipeline)
    result = cli(
        pipeline.runtime.root,
        "command",
        "campaign.close_resources",
        "--key",
        "cli-close",
        "--revision",
        str(captured["data"]["campaign_revision"]),
        payload=CloseCampaignResources(
            campaign_id=pipeline.campaign["id"],
            obligations_id=captured["id"],
            reason="Resources reconciled",
        ).model_dump(mode="json"),
    )
    assert result["result"]["kind"] == "campaign_resource_closure"
    assert pipeline.client.context(pipeline.campaign["id"])["operational"]["status"] == "closed"
    report = pipeline.client.command(
        "report.create",
        ReportCampaign(campaign_id=pipeline.campaign["id"]),
        idempotency_key=new_id(),
    )["result"]
    assert report["data"]["snapshot"]["operational"]["status"] == "closed"


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_funding_settlement_rolls_back_with_resource_closure_event(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    branch = child_trial(pipeline, start(pipeline, create(pipeline)["campaign"]))
    finish(branch)
    captured = review(branch)
    projection = pipeline.client.get("funding_projection", branch.campaign["id"])
    before = accounts(pipeline, pipeline.campaign["id"])
    original = Transaction.event

    def fail(self: Transaction, name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "campaign.resources_closed":
            raise RuntimeError("Synthetic post-settlement failure")
        return original(self, name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="post-settlement"):
            close_resources(branch, captured)
    assert pipeline.client.get("funding_projection", branch.campaign["id"]) == projection
    assert accounts(pipeline, pipeline.campaign["id"]) == before
    assert pipeline.client.list(RecordQuery(kind="funding_settlement"))["items"] == []
    close_resources(branch, captured)
    assert accounts(pipeline, pipeline.campaign["id"])["execution_ms"]["reserved"] == "0"


@pytest.mark.parametrize("bound", ["MAX_ENTRIES", "MAX_BYTES"])
def test_oversized_review_has_no_partial_chunks_or_root(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, bound: str
) -> None:
    monkeypatch.setattr("research_swarm.control.obligations." + bound, 1)
    with pytest.raises(ResearchError) as oversized:
        review(pipeline)
    assert oversized.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    for kind in ("campaign_obligations", "campaign_obligation_chunk"):
        assert pipeline.client.list(RecordQuery(kind=kind))["items"] == []
