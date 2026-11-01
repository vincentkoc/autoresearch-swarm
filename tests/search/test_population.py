from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import CampaignTarget, CreateCampaign
from research_swarm.control.population import MAX_ACTIVE_MEMBERS
from research_swarm.control.population_models import RetainPopulationMember, RetirePopulationMember
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import ReportCampaign
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from tests.control.test_promotions import assess, commit, evaluated, second_evaluation
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.support import LIMITS


def retention(pipeline: Pipeline, receipt: dict[str, Any]) -> RetainPopulationMember:
    authorization = pipeline.client.get(
        "evaluation_request", receipt["data"]["receipt"]["request_id"]
    )["data"]["request"]
    trial = pipeline.client.get("trial", authorization["trial_id"])
    return RetainPopulationMember(
        campaign_id=pipeline.campaign["id"],
        candidate_id=trial["data"]["candidate_id"],
        validation_id=trial["data"]["validation_id"],
        evaluation_id=receipt["id"],
        reason="Preserve a useful alternative independently of its primary score",
        labels=("optimizer", "control"),
    )


def retain(
    pipeline: Pipeline,
    request: RetainPopulationMember,
    *,
    revision: int = 0,
    key: str | None = None,
) -> dict[str, Any]:
    return pipeline.client.command(
        "population.retain",
        request,
        idempotency_key=key or new_id(),
        expected_revision=revision,
    )


def retire(
    pipeline: Pipeline, member_id: str, revision: int, *, key: str | None = None
) -> dict[str, Any]:
    return pipeline.client.command(
        "population.retire",
        RetirePopulationMember(member_id=member_id, reason="Retire this branch"),
        idempotency_key=key or new_id(),
        expected_revision=revision,
    )


def test_worse_branch_survives_retirement_reactivation_and_replay_without_promotion(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    incumbent = evaluated(pipeline)
    commit(pipeline, assess(pipeline, incumbent))
    alternative = second_evaluation(pipeline, tmp_path)
    rejected = assess(pipeline, alternative, "worse-archive-control", 1)
    assert rejected["data"]["decision"]["outcome"] == "reject"
    request = retention(pipeline, alternative)
    before_campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    before_budget = pipeline.client.budget(pipeline.campaign["id"])
    first = retain(pipeline, request, key="retain-alternative")
    member = first["result"]
    assert member["id"] == request.candidate_id and member["state"] == "active"
    with ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root))) as reopened:
        assert (
            reopened.command(
                "population.retain",
                request,
                idempotency_key="retain-alternative",
                expected_revision=0,
            )
            == first
        )
    with pytest.raises(ResearchError) as duplicate:
        retain(pipeline, request, revision=1)
    assert duplicate.value.code == ErrorCode.INVALID_TRANSITION
    history_query = RecordQuery(kind="population_change", parent_id=member["id"])
    original_history = pipeline.client.list(history_query)["items"][0]
    report = pipeline.client.command(
        "report.create",
        ReportCampaign(campaign_id=pipeline.campaign["id"]),
        idempotency_key="retained-branch-report",
    )["result"]
    archived = report["data"]["snapshot"]["research"]["population"]["items"]
    assert archived[0]["id"] == member["id"]
    retired = retire(pipeline, member["id"], 1, key="retire-alternative")
    assert retired["result"]["state"] == "retired"
    assert retired["result"]["data"] == member["data"]
    reactivated = retain(pipeline, request, revision=2)["result"]
    assert reactivated["revision"] == 3 and reactivated["id"] == member["id"]
    history = pipeline.client.list(history_query)["items"]
    assert [item["data"]["state"] for item in history] == ["active", "retired", "active"]
    assert [item["data"]["after_revision"] for item in history] == [1, 2, 3]
    assert history[0] == original_history
    assert pipeline.client.get("report", report["id"]) == report
    assert pipeline.client.get("campaign", pipeline.campaign["id"]) == before_campaign
    assert pipeline.client.budget(pipeline.campaign["id"]) == before_budget
    page = pipeline.client.list(RecordQuery(kind="population_member", state="active"))
    assert len(page["items"]) == 1


def test_archive_requires_matching_current_evidence_and_fresh_reactivation(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    original = retention(pipeline, evaluated(pipeline))
    other = retention(pipeline, second_evaluation(pipeline, tmp_path))
    for changes, code in (
        ({"validation_id": other.validation_id}, ErrorCode.PROJECT_CONTRACT_INVALID),
        ({"evaluation_id": other.evaluation_id}, ErrorCode.COMPARISON_INELIGIBLE),
    ):
        with pytest.raises(ResearchError) as mismatch:
            retain(pipeline, original.model_copy(update=changes))
        assert mismatch.value.code == code
    campaign = pipeline.client.command(
        "campaign.create",
        CreateCampaign(
            project_id=pipeline.client.context(original.campaign_id)["contract"]["project_id"],
            name="Other research",
            primary_metric="val_bpb",
            limits=LIMITS,
        ),
        idempotency_key="other-campaign",
    )["result"]
    campaign = pipeline.client.command(
        "campaign.seal",
        CampaignTarget(campaign_id=campaign["id"]),
        idempotency_key="seal-other-campaign",
        expected_revision=campaign["revision"],
    )["result"]
    with pytest.raises(ResearchError) as cross_campaign:
        retain(pipeline, original.model_copy(update={"campaign_id": campaign["id"]}))
    assert cross_campaign.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
    member = retain(pipeline, original)["result"]
    pipeline.client.command(
        "evidence.invalidate",
        InvalidateEvidence(
            target=EvidenceReference(kind="evaluation_receipt", id=original.evaluation_id),
            reason="Correct archive evidence without deleting its history",
        ),
        idempotency_key="archive-correction",
    )
    assert pipeline.client.get("population_member", member["id"]) == member
    retire(pipeline, member["id"], 1)
    before = pipeline.client.budget(original.campaign_id)
    with pytest.raises(ResearchError) as stale:
        retain(pipeline, original, revision=2)
    assert stale.value.code in {ErrorCode.COMPARISON_INELIGIBLE, ErrorCode.EVIDENCE_INVALIDATED}
    assert pipeline.client.get("population_member", member["id"])["state"] == "retired"
    assert len(pipeline.client.list(RecordQuery(kind="population_change"))["items"]) == 2
    assert pipeline.client.budget(original.campaign_id) == before


def test_concurrent_capacity_and_retirement_use_atomic_membership_revisions(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = (
        retention(pipeline, evaluated(pipeline)),
        retention(pipeline, second_evaluation(pipeline, tmp_path)),
    )
    assert MAX_ACTIVE_MEMBERS == 64
    # A one-slot profile exercises the same transaction boundary with two real candidates.
    monkeypatch.setattr("research_swarm.control.population.MAX_ACTIVE_MEMBERS", 1)
    barrier = Barrier(2)

    def enter(index: int) -> dict[str, Any] | ErrorCode:
        barrier.wait(timeout=10)
        try:
            return retain(pipeline, requests[index])
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(enter, range(2)))
    assert results.count(ErrorCode.RESOURCE_UNAVAILABLE) == 1
    winner = next(index for index, result in enumerate(results) if isinstance(result, dict))
    member_id = requests[winner].candidate_id
    barrier = Barrier(2)

    def leave(index: int) -> dict[str, Any] | ErrorCode:
        barrier.wait(timeout=10)
        try:
            return retire(pipeline, member_id, 1, key=f"retire-race-{index}")
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(leave, range(2)))
    assert results.count(ErrorCode.REVISION_CONFLICT) == 1
    assert len(pipeline.client.list(RecordQuery(kind="population_change"))["items"]) == 2
    assert retain(pipeline, requests[1 - winner])["result"]["state"] == "active"


@pytest.mark.parametrize("scopes", [{"read"}, {"search.manage"}])
def test_archive_service_requires_both_management_and_read_scope(
    pipeline: Pipeline,
    scopes: set[str],
) -> None:
    actor = pipeline.runtime.principal().model_copy(
        update={"id": new_id(), "scopes": frozenset(scopes)}
    )
    context = CommandContext(principal=actor, idempotency_key=new_id(), expected_revision=0)
    request = RetainPopulationMember(
        campaign_id=pipeline.campaign["id"],
        candidate_id=new_id(),
        validation_id=new_id(),
        evaluation_id=new_id(),
        reason="Denied",
    )
    with pytest.raises(ResearchError) as denied:
        pipeline.runtime.service.retain_population_member(context, request)
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert pipeline.client.list(RecordQuery(kind="population_member"))["items"] == []
