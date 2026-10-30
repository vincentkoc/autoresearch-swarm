from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest
from pydantic import ValidationError

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.child_models import (
    CancelChildCampaign,
    CreateChildCampaign,
    SettleChildCampaign,
)
from research_swarm.control.commands import (
    CampaignTarget,
    CompleteCampaign,
    ProposeCandidate,
    SubmitTrial,
    ValidateCandidate,
)
from research_swarm.control.models import (
    DelegationConfiguration,
    SearchConfiguration,
    StoppingRule,
    TrialData,
)
from research_swarm.control.queries import RecordQuery
from research_swarm.control.results import PublishAttempt
from research_swarm.control.workers import AttemptData
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from research_swarm.workers.journal import JournalState
from tests.support import LIMITS
from tests.test_campaign_completion import complete
from tests.test_cli_mcp import cli
from tests.test_confirmation import create_plan, options, setup_plan
from tests.test_evaluation_driver import role
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_portfolio_control import answer, issue, portfolio_options
from tests.test_proposals import baseline
from tests.test_search_admission import admit
from tests.test_search_observations import evaluate

OPTIONS = {
    "limits": (
        *LIMITS,
        BudgetLimit(dimension="child_campaigns", limit=4, unit="count"),
        BudgetLimit(dimension="trial_admissions", limit=6, unit="count"),
    ),
    "delegation": DelegationConfiguration(max_depth=2),
    "stopping": StoppingRule(max_trials=6),
}
CHILD_LIMITS = (
    BudgetLimit(dimension="attempts", limit=4, unit="count"),
    BudgetLimit(dimension="execution_ms", limit=50_000, unit="ms"),
    BudgetLimit(dimension="trial_admissions", limit=1, unit="count"),
)


def accounts(pipeline: Pipeline, campaign_id: str) -> dict[str, Any]:
    return {item["dimension"]: item for item in pipeline.client.budget(campaign_id)["accounts"]}


def create(
    pipeline: Pipeline,
    *,
    parent_id: str | None = None,
    limits: tuple[BudgetLimit, ...] = CHILD_LIMITS,
    remaining_depth: int = 0,
    max_trials: int = 1,
    key: str | None = None,
    client: ResearchClient | None = None,
) -> dict[str, Any]:
    client = client or pipeline.client
    parent = client.get("campaign", parent_id or pipeline.campaign["id"])
    return client.command(
        "campaign.create_child",
        CreateChildCampaign(
            parent_campaign_id=parent["id"],
            name="Funded optimizer research",
            limits=limits,
            remaining_depth=remaining_depth,
            max_trials=max_trials,
            search=SearchConfiguration(policy="external", version="external-v1"),
        ),
        idempotency_key=key or new_id(),
        expected_revision=parent["revision"],
    )["result"]


def start(pipeline: Pipeline, campaign: dict[str, Any]) -> dict[str, Any]:
    for operation in ("campaign.seal", "campaign.start"):
        campaign = pipeline.client.command(
            operation,
            CampaignTarget(campaign_id=campaign["id"]),
            idempotency_key=new_id(),
            expected_revision=campaign["revision"],
        )["result"]
    return campaign


def child_trial(pipeline: Pipeline, campaign: dict[str, Any]) -> Pipeline:
    revision = pipeline.client.get(
        "project_revision", campaign["data"]["contract"]["project_revision_id"]
    )
    candidate = pipeline.client.command(
        "candidate.propose",
        ProposeCandidate(
            campaign_id=campaign["id"],
            source_artifact=revision["data"]["source_artifact"],
            hypothesis="Run the inherited frozen baseline under a funded child",
        ),
        idempotency_key=new_id(),
        expected_revision=campaign["revision"],
    )["result"]
    validation = pipeline.client.command(
        "candidate.validate",
        ValidateCandidate(candidate_id=candidate["id"]),
        idempotency_key=new_id(),
        expected_revision=candidate["revision"],
    )["result"]
    trial = pipeline.client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=candidate["id"],
            validation_id=validation["id"],
            seed=19,
        ),
        idempotency_key=new_id(),
        expected_revision=campaign["revision"],
    )["result"]
    return replace(pipeline, campaign=campaign, trial=trial)


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_http_child_executes_real_work_and_completion_settles_once_with_final_report(
    pipeline: Pipeline,
) -> None:
    _, _, token = role(pipeline, "*")
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, token) as remote,
    ):
        created = create(pipeline, client=remote, key="http-child")
        assert create(pipeline, client=remote, key="http-child") == created
    parent_id = pipeline.campaign["id"]
    campaign = created["campaign"]
    parent_contract = pipeline.client.get("campaign", parent_id)["data"]["contract"]
    for name in (
        "project_id",
        "project_revision_id",
        "protocol_digest",
        "primary_metric",
        "promotion",
        "input_artifacts",
    ):
        assert campaign["data"]["contract"][name] == parent_contract[name]
    assert campaign["data"]["parent_campaign_id"] == parent_id
    assert accounts(pipeline, parent_id)["child_campaigns"]["consumed"] == "1"
    assert accounts(pipeline, parent_id)["execution_ms"]["reserved"] == "50000"
    branch = child_trial(pipeline, start(pipeline, campaign))
    evaluate(branch, branch.trial)
    spent = accounts(pipeline, branch.campaign["id"])
    before = accounts(pipeline, parent_id)
    assert (
        Decimal(before["execution_ms"]["consumed"])
        == Decimal(spent["execution_ms"]["consumed"])
        > 0
    )
    assert Decimal(before["execution_ms"]["reserved"]) == 50_000 - Decimal(
        spent["execution_ms"]["consumed"]
    )
    assert before["trial_admissions"]["consumed"] == "2"  # Root baseline and one child trial.
    before_finish = pipeline.client.get("campaign", campaign["id"])
    finished = complete(branch, "finish-http-child")
    assert (
        pipeline.client.command(
            "campaign.complete",
            CompleteCampaign(
                campaign_id=campaign["id"], reason="Declared fixture research finished"
            ),
            idempotency_key="finish-http-child",
            expected_revision=before_finish["revision"],
        )
        == finished
    )
    output = finished["result"]
    final = accounts(pipeline, parent_id)
    assert final["execution_ms"]["consumed"] == before["execution_ms"]["consumed"]
    assert final["execution_ms"]["reserved"] == "0"
    assert Decimal(final["execution_ms"]["released_total"]) == Decimal(
        before["execution_ms"]["reserved"]
    )
    report = output["report"]["data"]
    assert report["snapshot"]["funding"]["incoming"]["state"] == "settled"
    assert report["snapshot"]["funding"]["incoming"]["settlement_id"] == campaign["id"]
    assert "do not add child totals again" in report["markdown"]
    reopened = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
    for _ in range(3):
        receipt = reopened.command(
            "campaign.settle_child",
            SettleChildCampaign(child_campaign_id=campaign["id"]),
            expected_revision=created["allocation"]["revision"],
            idempotency_key=new_id(),
        )["result"]
        assert receipt == output["funding_settlement"]
    assert accounts(pipeline, parent_id) == final
    assert len(reopened.list(RecordQuery(kind="funding_settlement"))["items"]) == 1
    assert reopened.context(parent_id)["funding"]["open_child_count"] == 0


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_unstarted_child_cancellation_refunds_funding_but_not_creation_counter(
    pipeline: Pipeline,
) -> None:
    created = create(pipeline)
    child = created["campaign"]
    result = cli(
        pipeline.runtime.root,
        "command",
        "campaign.cancel_child",
        "--key",
        "cancel-unused",
        "--revision",
        str(child["revision"]),
        payload=CancelChildCampaign(
            child_campaign_id=child["id"], reason="No longer allocating this research branch"
        ).model_dump(mode="json"),
    )["result"]
    assert result["campaign"]["state"] == "cancelled"
    assert result["report"]["data"]["snapshot"]["funding"]["incoming"]["state"] == "settled"
    budget = accounts(pipeline, pipeline.campaign["id"])
    assert budget["child_campaigns"]["consumed"] == "1"
    assert budget["execution_ms"]["consumed"] == "0" and budget["execution_ms"]["reserved"] == "0"
    assert budget["trial_admissions"]["consumed"] == "1"
    assert all(item["reserved"] == "0" for item in budget.values())


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_parallel_child_creation_preserves_parent_budget_and_does_not_leak_failed_children(
    pipeline: Pipeline,
) -> None:
    barrier = Barrier(2)
    limits = (
        *CHILD_LIMITS[:1],
        BudgetLimit(dimension="execution_ms", limit=200_000, unit="ms"),
        CHILD_LIMITS[-1],
    )
    clients = [role(pipeline, "*")[1] for _ in range(2)]

    def attempt(client: ResearchClient) -> Any:
        barrier.wait(timeout=5)
        try:
            return create(pipeline, client=client, limits=limits)
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, clients))
    assert results.count(ErrorCode.BUDGET_EXHAUSTED) == 1
    assert len(pipeline.client.list(RecordQuery(kind="campaign_funding"))["items"]) == 1
    assert len(pipeline.client.list(RecordQuery(kind="campaign"))["items"]) == 2
    budget = accounts(pipeline, pipeline.campaign["id"])
    assert budget["execution_ms"]["reserved"] == "200000"
    assert budget["child_campaigns"]["consumed"] == "1"


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_descendant_trials_and_child_creation_are_charged_once_at_each_ancestor(
    pipeline: Pipeline,
) -> None:
    limits = (
        *CHILD_LIMITS[:2],
        BudgetLimit(dimension="trial_admissions", limit=2, unit="count"),
        BudgetLimit(dimension="child_campaigns", limit=1, unit="count"),
    )
    child = start(
        pipeline, create(pipeline, limits=limits, max_trials=2, remaining_depth=1)["campaign"]
    )
    grandchild = create(pipeline, parent_id=child["id"])["campaign"]
    branch = child_trial(pipeline, start(pipeline, grandchild))
    evaluate(branch, branch.trial)
    root_budget = accounts(pipeline, pipeline.campaign["id"])
    child_budget = accounts(pipeline, child["id"])
    assert root_budget["child_campaigns"]["consumed"] == "2"
    assert child_budget["child_campaigns"]["consumed"] == "1"
    assert root_budget["trial_admissions"]["consumed"] == "2"
    assert child_budget["trial_admissions"]["consumed"] == "1"
    with pytest.raises(ResearchError) as open_descendant:
        complete(replace(pipeline, campaign=child), "premature-parent")
    assert open_descendant.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    complete(branch)
    root_before = accounts(pipeline, pipeline.campaign["id"])
    assert Decimal(root_before["execution_ms"]["reserved"]) > 0
    complete(replace(pipeline, campaign=child), "finish-parent")
    assert accounts(pipeline, pipeline.campaign["id"])["execution_ms"]["reserved"] == "0"
    assert (
        accounts(pipeline, pipeline.campaign["id"])["execution_ms"]["consumed"]
        == root_before["execution_ms"]["consumed"]
    )


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_child_cannot_widen_depth_stopping_or_protocol_and_running_cancel_is_refused(
    pipeline: Pipeline,
) -> None:
    before = pipeline.client.budget(pipeline.campaign["id"])
    for arguments in ({"remaining_depth": 2}, {"max_trials": 7}):
        with pytest.raises(ResearchError) as widened:
            create(pipeline, **arguments)
        assert widened.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    with pytest.raises(ValidationError):
        CreateChildCampaign(
            parent_campaign_id=pipeline.campaign["id"],
            name="Changed protocol",
            limits=CHILD_LIMITS,
            project_id=new_id(),
        )
    child = start(pipeline, create(pipeline)["campaign"])
    with pytest.raises(ResearchError) as running:
        pipeline.client.command(
            "campaign.cancel_child",
            CancelChildCampaign(
                child_campaign_id=child["id"], reason="Must not bypass running-work custody"
            ),
            expected_revision=child["revision"],
            idempotency_key=new_id(),
        )
    assert running.value.code == ErrorCode.INVALID_TRANSITION


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_child_report_failure_rolls_back_ancestor_release_and_recovery_closes_once(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    branch = child_trial(pipeline, start(pipeline, create(pipeline)["campaign"]))
    evaluate(branch, branch.trial)
    before = pipeline.client.get("campaign", branch.campaign["id"])
    parent_budget = accounts(pipeline, pipeline.campaign["id"])
    allocation = pipeline.client.get("funding_projection", branch.campaign["id"])

    def failed(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("Fixture child report failure")

    with monkeypatch.context() as patch:
        patch.setattr("research_swarm.control.campaigns.create_report", failed)
        with pytest.raises(RuntimeError, match="child report failure"):
            complete(branch, "report-rollback")
    assert pipeline.client.get("campaign", branch.campaign["id"]) == before
    assert pipeline.client.get("funding_projection", branch.campaign["id"]) == allocation
    assert accounts(pipeline, pipeline.campaign["id"]) == parent_budget
    assert pipeline.client.list(RecordQuery(kind="funding_settlement"))["items"] == []
    assert pipeline.client.list(RecordQuery(kind="report"))["items"] == []
    assert complete(branch, "report-rollback")["result"]["campaign"]["state"] == "completed"


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_child_unknown_holds_ancestor_funding_until_original_native_closure(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    created = create(pipeline)
    branch = child_trial(pipeline, start(pipeline, created["campaign"]))
    attempt = branch.worker.claim(branch.ready())
    prepared = branch.worker._prepare(
        AttemptData.model_validate(attempt["data"]),
        TrialData.model_validate(branch.trial["data"]),
    )
    path = tmp_path / "unknown-child.json"
    path.write_text(
        prepared.model_copy(update={"state": JournalState.UNKNOWN}).model_dump_json(),
        encoding="utf-8",
    )
    artifact = branch.worker.client.upload_file(path, idempotency_key="unknown-child")
    branch.worker.client.command(
        "worker.publish_attempt",
        PublishAttempt(
            attempt_id=attempt["id"],
            journal_artifact=artifact,
            finding="Original native execution cannot yet be established",
        ),
        idempotency_key="unknown-child-publication",
    )
    budget = accounts(pipeline, pipeline.campaign["id"])
    assert budget["attempts"]["uncertain"] == "1"
    assert Decimal(budget["execution_ms"]["uncertain"]) > 0
    with pytest.raises(ResearchError) as unresolved:
        complete(branch, "unknown-child-completion")
    assert unresolved.value.code == ErrorCode.EXECUTION_UNKNOWN
    allocation = pipeline.client.get("funding_projection", branch.campaign["id"])
    with pytest.raises(ResearchError) as not_terminal:
        pipeline.client.command(
            "campaign.settle_child",
            SettleChildCampaign(child_campaign_id=branch.campaign["id"]),
            idempotency_key="unknown-child-settlement",
            expected_revision=allocation["revision"],
        )
    assert not_terminal.value.code == ErrorCode.INVALID_TRANSITION
    assert accounts(pipeline, pipeline.campaign["id"]) == budget
    branch.worker.journal.request_cancel(attempt["id"], 1)
    assert branch.worker.execute(attempt["id"])["state"] == "not_started"
    complete(branch, "original-closure-complete")
    final = accounts(pipeline, pipeline.campaign["id"])
    assert all(item["uncertain"] == "0" and item["reserved"] == "0" for item in final.values())
    assert final["attempts"]["consumed"] == "0"  # Original closure proves it never started.
    assert final["trial_admissions"]["consumed"] == "2"


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_trial_counter_bounds_direct_child_submission_and_graph_failure_is_atomic(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign = start(pipeline, create(pipeline, max_trials=2)["campaign"])
    branch = child_trial(pipeline, campaign)
    existing = TrialData.model_validate(branch.trial["data"])
    request = SubmitTrial(
        campaign_id=campaign["id"],
        candidate_id=existing.candidate_id,
        validation_id=existing.validation_id,
        seed=31,
    )
    before = accounts(pipeline, pipeline.campaign["id"])
    with pytest.raises(ResearchError) as exhausted:
        pipeline.client.command(
            "trial.submit",
            request,
            idempotency_key="extra-child-trial",
            expected_revision=campaign["revision"],
        )
    assert exhausted.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert accounts(pipeline, pipeline.campaign["id"]) == before
    assert (
        len(pipeline.client.list(RecordQuery(kind="trial", campaign_id=campaign["id"]))["items"])
        == 1
    )

    root_trial = TrialData.model_validate(pipeline.trial["data"])
    root_request = SubmitTrial(
        campaign_id=pipeline.campaign["id"],
        candidate_id=root_trial.candidate_id,
        validation_id=root_trial.validation_id,
        seed=37,
    )
    original = Transaction.insert

    def fail_graph(self: Transaction, kind: str, *args: Any, **kwargs: Any) -> Any:
        if kind == "job":
            raise RuntimeError("Fixture trial graph write failure")
        return original(self, kind, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "insert", fail_graph)
        with pytest.raises(RuntimeError, match="graph write failure"):
            pipeline.client.command(
                "trial.submit",
                root_request,
                idempotency_key="rolled-back-admission",
                expected_revision=pipeline.campaign["revision"],
            )
    assert accounts(pipeline, pipeline.campaign["id"]) == before
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2


@pytest.mark.parametrize("pipeline", [OPTIONS], indirect=True)
def test_invalid_child_funding_leaves_no_campaign_or_admission_charge(pipeline: Pipeline) -> None:
    invalid = (
        (CHILD_LIMITS[:-1], ErrorCode.PROJECT_CONTRACT_INVALID),
        (
            (*CHILD_LIMITS[:-1], BudgetLimit(dimension="trial_admissions", unit="tokens", limit=1)),
            ErrorCode.PROJECT_CONTRACT_INVALID,
        ),
        (
            (*CHILD_LIMITS, BudgetLimit(dimension="new_money", unit="usd", limit=1)),
            ErrorCode.BUDGET_EXHAUSTED,
        ),
        (
            (*CHILD_LIMITS[:-1], BudgetLimit(dimension="trial_admissions", unit="count", limit=2)),
            ErrorCode.PROJECT_CONTRACT_INVALID,
        ),
    )
    before = accounts(pipeline, pipeline.campaign["id"])
    for limits, expected in invalid:
        with pytest.raises(ResearchError) as denied:
            create(pipeline, limits=limits)
        assert denied.value.code == expected
        assert accounts(pipeline, pipeline.campaign["id"]) == before
    assert len(pipeline.client.list(RecordQuery(kind="campaign"))["items"]) == 1
    assert pipeline.client.list(RecordQuery(kind="campaign_funding"))["items"] == []
    with pytest.raises(ValidationError, match="unique"):
        CreateChildCampaign(
            parent_campaign_id=pipeline.campaign["id"],
            name="Duplicate funding",
            limits=(*CHILD_LIMITS, CHILD_LIMITS[0]),
        )


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            **OPTIONS,
            "stopping": StoppingRule(
                max_trials=6, target_metric=0.001, permit_manual_completion=False
            ),
        }
    ],
    indirect=True,
)
def test_descendant_spending_can_satisfy_nonmanual_parent_stopping_rule(
    pipeline: Pipeline,
) -> None:
    child = start(
        pipeline,
        create(
            pipeline,
            limits=(*CHILD_LIMITS, BudgetLimit(dimension="child_campaigns", unit="count", limit=1)),
            max_trials=2,
            remaining_depth=1,
        )["campaign"],
    )
    assert child["data"]["contract"]["stopping"] == {
        "schema_version": "1.0",
        "max_trials": 2,
        "target_metric": 0.001,
        "permit_manual_completion": False,
    }
    branch = child_trial(
        pipeline, start(pipeline, create(pipeline, parent_id=child["id"])["campaign"])
    )
    evaluate(branch, branch.trial)
    complete(branch, "strict-grandchild")
    assert pipeline.client.list(RecordQuery(kind="trial", campaign_id=child["id"]))["items"] == []
    assert accounts(pipeline, child["id"])["trial_admissions"]["available"] == "0"
    assert (
        complete(replace(pipeline, campaign=child), "strict-parent")["result"]["campaign"]["state"]
        == "completed"
    )


def test_legacy_campaign_omits_optional_delegation_and_funding_views(pipeline: Pipeline) -> None:
    contract = pipeline.campaign["data"]["contract"]
    assert "delegation" not in contract
    context = pipeline.client.context(pipeline.campaign["id"])
    assert "funding" not in context
    assert "delegation" not in context["contract"]


PORTFOLIO_OPTIONS = {
    **portfolio_options(max_trials=6),
    "delegation": DelegationConfiguration(max_depth=1),
    "limits": (
        *portfolio_options()["limits"],
        BudgetLimit(dimension="trial_admissions", unit="count", limit=4),
        BudgetLimit(dimension="child_campaigns", unit="count", limit=2),
    ),
}


@pytest.mark.parametrize("pipeline", [PORTFOLIO_OPTIONS], indirect=True)
def test_child_and_direct_admission_cannot_spend_portfolio_promises(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    baseline(pipeline)
    first, second = issue(pipeline, 0, 0)["request"], issue(pipeline, 1, 1)["request"]
    before = accounts(pipeline, pipeline.campaign["id"])
    with pytest.raises(ResearchError) as promised:
        create(
            pipeline,
            limits=(
                *CHILD_LIMITS[:-1],
                BudgetLimit(dimension="trial_admissions", limit=2, unit="count"),
            ),
            max_trials=2,
        )
    assert promised.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert accounts(pipeline, pipeline.campaign["id"]) == before
    assert len(pipeline.client.list(RecordQuery(kind="campaign"))["items"]) == 1
    create(pipeline)  # Exactly the one remaining unpromised admission.
    existing = TrialData.model_validate(pipeline.trial["data"])
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    budget = accounts(pipeline, campaign["id"])
    with pytest.raises(ResearchError) as direct:
        pipeline.client.command(
            "trial.submit",
            SubmitTrial(
                campaign_id=campaign["id"],
                candidate_id=existing.candidate_id,
                validation_id=existing.validation_id,
                seed=37,
            ),
            idempotency_key="direct-cannot-steal",
            expected_revision=campaign["revision"],
        )
    assert direct.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert accounts(pipeline, campaign["id"]) == budget
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 1
    for index, (proposal, rate) in enumerate(((first, 0.15), (second, 0.35))):
        answer(pipeline, proposal, tmp_path, rate)
        assert admit(pipeline, proposal["id"], revision=2 + index)["result"]["trial"] is not None
    final = accounts(pipeline, campaign["id"])["trial_admissions"]
    assert final["consumed"] == "3" and final["reserved"] == "1"


@pytest.mark.parametrize("pipeline", [PORTFOLIO_OPTIONS], indirect=True)
def test_proposal_issuance_counts_funding_already_held_by_child(pipeline: Pipeline) -> None:
    baseline(pipeline)
    create(
        pipeline,
        limits=(
            *CHILD_LIMITS[:-1],
            BudgetLimit(dimension="trial_admissions", limit=2, unit="count"),
        ),
        max_trials=2,
    )
    issue(pipeline, 0, 0)
    budget = accounts(pipeline, pipeline.campaign["id"])
    with pytest.raises(ResearchError) as denied:
        issue(pipeline, 1, 1)
    assert denied.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert accounts(pipeline, pipeline.campaign["id"]) == budget
    assert len(pipeline.client.list(RecordQuery(kind="proposal_request"))["items"]) == 1


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            **options(),
            "limits": (
                *options()["limits"],
                BudgetLimit(dimension="trial_admissions", unit="count", limit=5),
            ),
        }
    ],
    indirect=True,
)
def test_confirmation_panel_cannot_bypass_funded_trial_counter(pipeline: Pipeline) -> None:
    intent, campaign = setup_plan(pipeline)
    budget = accounts(pipeline, campaign["id"])
    with pytest.raises(ResearchError) as exhausted:
        create_plan(pipeline, intent, campaign)
    assert exhausted.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert accounts(pipeline, campaign["id"]) == budget
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 1
    assert len(pipeline.client.list(RecordQuery(kind="job"))["items"]) == 2
    assert pipeline.client.list(RecordQuery(kind="confirmation_plan"))["items"] == []
