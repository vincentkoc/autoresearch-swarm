from __future__ import annotations

import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import CampaignTarget, SubmitTrial
from research_swarm.control.models import (
    PortfolioConfiguration,
    PromotionConfiguration,
    ResearchTheme,
    SearchConfiguration,
    StoppingRule,
)
from research_swarm.control.observation_models import (
    AbandonObservationSelection,
    SelectTrialObservation,
)
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.search_models import RequestProposal, RespondToProposal, SearchState
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from tests.control.test_evaluation_driver import role
from tests.control.test_promotions import assess, commit
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_http_delivery import serving
from tests.paths import REPO_ROOT
from tests.search.test_proposals import PROPOSAL_LIMITS, baseline, cancel
from tests.search.test_search_admission import admit
from tests.search.test_search_driver import pin
from tests.search.test_search_observations import evaluate, observe, resolve
from tests.support import ResearchFixture

THEMES = (
    ResearchTheme(
        theme_id="optimizer",
        proposer_id="11111111-1111-4111-8111-111111111111",
        instructions="Explore optimizer changes",
    ),
    ResearchTheme(
        theme_id="architecture",
        proposer_id="22222222-2222-4222-8222-222222222222",
        instructions="Explore architecture changes",
    ),
)


def portfolio_options(max_trials: int = 6) -> dict[str, Any]:
    return {
        "limits": (
            *PROPOSAL_LIMITS[:-1],
            BudgetLimit(dimension="proposal_requests", limit=6, unit="count"),
        ),
        "search": SearchConfiguration(
            policy="portfolio",
            version="portfolio-v1",
            max_parallel_trials=2,
            portfolio=PortfolioConfiguration(themes=THEMES, first_seed=19),
        ),
        "source_learning_rate": 0.0,
        "stopping": StoppingRule(max_trials=max_trials),
    }


def issue(pipeline: Pipeline, ordinal: int, revision: int) -> dict[str, Any]:
    authority = pipeline.client.context(pipeline.campaign["id"])["authority"]
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    theme = THEMES[ordinal % len(THEMES)]
    intent = RequestProposal(
        campaign_id=campaign["id"],
        proposer_id=theme.proposer_id,
        instructions=theme.instructions,
        theme_id=theme.theme_id,
        seed=19 + ordinal,
        expected_incumbent_revision=campaign["data"]["incumbent_revision"],
        expected_controller_id=authority["controller_id"],
        expected_control_epoch=authority["control_epoch"],
    )
    return pipeline.client.command(
        "search.request_proposal", intent, idempotency_key=new_id(), expected_revision=revision
    )["result"]


def answer(
    pipeline: Pipeline, request: dict[str, Any], tmp_path: Path, rate: float
) -> dict[str, Any]:
    actor = Principal(
        id=request["data"]["proposer_id"],
        workspace_id=pipeline.runtime.identity.workspace_id,
        scopes=frozenset({"proposal.respond"}),
    )
    credential = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), actor)
    client = ResearchClient(
        LocalTransport(
            pipeline.runtime, lambda: pipeline.runtime.credentials.resolve(credential.token)
        )
    )
    source = tmp_path / new_id()
    shutil.copytree(REPO_ROOT / "examples" / "cpu_bigram", source)
    train = source / "train.py"
    train.write_text(
        train.read_text().replace("LEARNING_RATE = 0.35", f"LEARNING_RATE = {rate}"),
        encoding="utf-8",
    )
    artifact = pipeline.client.capture_project(source, idempotency_key=new_id())
    return client.command(
        "search.respond",
        RespondToProposal(
            request_id=request["id"],
            outcome="candidate",
            source_artifact=artifact,
            hypothesis=f"Evaluate optimizer setting {rate}",
        ),
        idempotency_key=new_id(),
    )["result"]


def two_candidates(pipeline: Pipeline, tmp_path: Path) -> list[dict[str, Any]]:
    baseline(pipeline)
    requests = [issue(pipeline, 0, 0)["request"], issue(pipeline, 1, 1)["request"]]
    for request, rate in zip(requests, (0.35, 0.15), strict=True):
        answer(pipeline, request, tmp_path, rate)
    return [
        admit(pipeline, request["id"], revision=2 + index)["result"]["trial"]
        for index, request in enumerate(requests)
    ]


@pytest.mark.parametrize("pipeline", [portfolio_options()], indirect=True)
def test_independent_agents_submit_and_replay_concurrent_http_responses(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    baseline(pipeline)
    requests = [issue(pipeline, 0, 0)["request"], issue(pipeline, 1, 1)["request"]]
    credentials, responses = [], []
    for index, request in enumerate(requests):
        actor = Principal(
            id=request["data"]["proposer_id"],
            workspace_id=pipeline.runtime.identity.workspace_id,
            scopes=frozenset({"proposal.respond"}),
        )
        credentials.append(pipeline.runtime.credentials.issue(pipeline.runtime.principal(), actor))
        source = tmp_path / f"agent-{index}"
        shutil.copytree(pipeline.source, source)
        train = source / "train.py"
        content = train.read_text(encoding="utf-8")
        assert content.count("LEARNING_RATE = 0.0") == 1
        train.write_text(
            content.replace("LEARNING_RATE = 0.0", f"LEARNING_RATE = {(index + 1) / 10}"),
            encoding="utf-8",
        )
        artifact = pipeline.client.capture_project(source, idempotency_key=f"source-{index}")
        responses.append(
            RespondToProposal(
                request_id=request["id"],
                outcome="candidate",
                source_artifact=artifact,
                hypothesis=f"Independent agent hypothesis {index}",
            )
        )
    assert requests[0]["data"]["proposer_id"] != requests[1]["data"]["proposer_id"]
    barrier = threading.Barrier(2)
    with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:

        def submit(index: int) -> dict[str, Any]:
            with ResearchClient.connect(url, credentials[index].token) as agent:
                barrier.wait(timeout=10)
                return agent.command(
                    "search.respond", responses[index], idempotency_key="independent-response"
                )

        with ResearchClient.connect(url, credentials[0].token) as first_agent:
            with pytest.raises(ResearchError) as foreign:
                first_agent.command(
                    "search.respond", responses[1], idempotency_key="foreign-response"
                )
            assert foreign.value.code == ErrorCode.AUTHORIZATION_DENIED
        with ThreadPoolExecutor(max_workers=2) as pool:
            accepted = list(pool.map(submit, range(2)))
        budget = pipeline.client.budget(pipeline.campaign["id"])
        with ThreadPoolExecutor(max_workers=2) as pool:
            replayed = list(pool.map(submit, range(2)))
        assert replayed == accepted
        assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    assert accepted[0]["command_id"] != accepted[1]["command_id"]
    for index, response in enumerate(accepted):
        assert response["result"]["id"] == requests[index]["id"]
        assert response["result"]["data"]["responder_id"] == requests[index]["data"]["proposer_id"]
        assert response["result"]["data"]["request"] == responses[index].model_dump(mode="json")
    assert len(pipeline.client.list(RecordQuery(kind="proposal_response"))["items"]) == 2
    admitted = [
        admit(pipeline, request["id"], revision=2 + index)["result"]
        for index, request in enumerate(requests)
    ]
    assert all(item["transition"]["data"]["outcome"] == "admitted" for item in admitted)
    assert len({item["candidate"]["id"] for item in admitted}) == 2
    assert set(admitted[-1]["search"]["data"]["active_trial_ids"]) == {
        item["trial"]["id"] for item in admitted
    }


def test_existing_search_configuration_bytes_and_ordered_portfolio_identity() -> None:
    assert canonical_bytes(SearchConfiguration().model_dump(mode="json")) == (
        b'{"max_parallel_trials":1,"policy":"greedy","schema_version":"1.0","version":"greedy-v1"}'
    )
    first = PortfolioConfiguration(themes=THEMES)
    second = PortfolioConfiguration(themes=tuple(reversed(THEMES)))
    assert record_digest("portfolio-test", first.model_dump(mode="json")) != record_digest(
        "portfolio-test", second.model_dump(mode="json")
    )
    with pytest.raises(ValidationError):
        PortfolioConfiguration(themes=(THEMES[0], THEMES[0]))


@pytest.mark.parametrize("pipeline", [portfolio_options()], indirect=True)
def test_themes_are_sealed_and_cancelled_work_does_not_refund_round_robin_ordinal(
    pipeline: Pipeline,
) -> None:
    baseline(pipeline)
    first = issue(pipeline, 0, 0)["request"]
    second = issue(pipeline, 1, 1)["request"]
    assert [first["data"]["theme_id"], second["data"]["theme_id"]] == ["optimizer", "architecture"]
    with pytest.raises(ResearchError) as full:
        issue(pipeline, 2, 2)
    assert full.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    cancel(pipeline, first, revision=2)
    with pytest.raises(ResearchError) as stale_theme:
        issue(pipeline, 1, 3)
    assert stale_theme.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
    third = issue(pipeline, 2, 3)
    assert third["request"]["data"]["seed"] == 21
    assert third["search"]["data"]["request_count"] == 3
    budget = next(
        item
        for item in pipeline.client.budget(pipeline.campaign["id"])["accounts"]
        if item["dimension"] == "proposal_requests"
    )
    assert budget["consumed"] == "3"


@pytest.mark.parametrize("pipeline", [portfolio_options(max_trials=2)], indirect=True)
def test_pending_proposal_reserves_the_remaining_trial_allowance(pipeline: Pipeline) -> None:
    baseline(pipeline)
    issue(pipeline, 0, 0)
    before = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError) as exhausted:
        issue(pipeline, 1, 1)
    assert exhausted.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    with pytest.raises(ResearchError) as manual:
        pipeline.client.command(
            "trial.submit",
            SubmitTrial(
                campaign_id=campaign["id"],
                candidate_id=pipeline.trial["data"]["candidate_id"],
                validation_id=pipeline.trial["data"]["validation_id"],
                seed=77,
            ),
            idempotency_key="manual-overbook",
            expected_revision=campaign["revision"],
        )
    assert manual.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 1


@pytest.mark.parametrize("pipeline", [portfolio_options()], indirect=True)
def test_parallel_trial_selection_has_one_owner_and_cannot_be_bypassed_by_promotion(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    trials = two_candidates(pipeline, tmp_path)
    receipts = [evaluate(pipeline, trial) for trial in trials]
    observations = [observe(pipeline, trial)["result"] for trial in trials]
    decisions = [assess(pipeline, receipt, new_id(), 1) for receipt in receipts]
    with pytest.raises(ResearchError) as no_owner:
        commit(pipeline, decisions[0], "unowned-promotion")
    assert no_owner.value.code == ErrorCode.INVALID_TRANSITION
    authority = pipeline.client.context(pipeline.campaign["id"])["authority"]
    clients = [role(pipeline, "read", "search.manage")[1] for _ in range(2)]
    barrier = threading.Barrier(2)

    def contend(index: int) -> str:
        barrier.wait(timeout=10)
        try:
            clients[index].command(
                "search.select_observation",
                SelectTrialObservation(
                    campaign_id=pipeline.campaign["id"],
                    observation_id=observations[index]["id"],
                    expected_controller_id=authority["controller_id"],
                    expected_control_epoch=authority["control_epoch"],
                ),
                idempotency_key=new_id(),
                expected_revision=4,
            )
            return "accepted"
        except ResearchError as error:
            return error.code.value

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(contend, range(2)))
    assert sorted(results) == [ErrorCode.REVISION_CONFLICT.value, "accepted"]
    winner, loser = results.index("accepted"), results.index(ErrorCode.REVISION_CONFLICT.value)
    with pytest.raises(ResearchError) as occupied:
        pin(pipeline, observations[loser], revision=5)
    assert occupied.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    with pytest.raises(ResearchError) as bypass:
        commit(pipeline, decisions[loser], "bypassed-owner")
    assert bypass.value.code == ErrorCode.COMPARISON_INELIGIBLE
    commit(pipeline, decisions[winner], "owned-promotion")
    resolve(pipeline, observations[winner], decisions[winner], revision=5)
    state = pipeline.client.get("search_state", pipeline.campaign["id"])
    assert "selection_trial_id" not in state["data"]
    assert state["data"]["active_trial_ids"] == [trials[loser]["id"]]
    refreshed = observe(pipeline, trials[loser])["result"]
    assert refreshed["data"]["comparator_id"] == trials[winner]["data"]["candidate_id"]
    pin(pipeline, refreshed, revision=6)
    current_decision = assess(pipeline, receipts[loser], "fresh-comparison", 2)
    if current_decision["data"]["decision"]["outcome"] == "promote":
        commit(pipeline, current_decision, "fresh-promotion")
    resolve(pipeline, refreshed, current_decision, revision=7)
    parents = [
        pipeline.client.get("candidate", trial["data"]["candidate_id"])["data"]["parent_ids"]
        for trial in trials
    ]
    assert parents == [[pipeline.trial["data"]["candidate_id"]]] * 2


@pytest.mark.parametrize("pipeline", [portfolio_options()], indirect=True)
def test_explicit_correction_abandonment_releases_only_the_owned_selection(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    trial = two_candidates(pipeline, tmp_path)[0]
    receipt = evaluate(pipeline, trial)
    measured = observe(pipeline, trial)["result"]
    selected = pin(pipeline, measured, revision=4)
    pipeline.client.command(
        "evidence.invalidate",
        InvalidateEvidence(
            target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
            reason="Corrected measurement",
        ),
        idempotency_key="portfolio-correction",
    )
    corrected = observe(pipeline, trial)["result"]
    authority = pipeline.client.context(pipeline.campaign["id"])["authority"]
    pipeline.client.command(
        "search.abandon_selection",
        AbandonObservationSelection(
            campaign_id=pipeline.campaign["id"],
            observation_id=corrected["id"],
            selection_id=selected["transition"]["id"],
            reason="Release invalidated selection for review",
            expected_controller_id=authority["controller_id"],
            expected_control_epoch=authority["control_epoch"],
        ),
        idempotency_key="abandon-portfolio-pin",
        expected_revision=5,
    )
    state = pipeline.client.get("search_state", pipeline.campaign["id"])["data"]
    assert "selection_trial_id" not in state and len(state["active_trial_ids"]) == 1
    assert (
        pipeline.client.get("policy_transition", selected["transition"]["id"])
        == selected["transition"]
    )
    assert len(pipeline.client.list(RecordQuery(kind="promotion"))["items"]) == 1


@pytest.mark.parametrize("invalid", ["missing", "version", "slots", "paired", "foreign"])
def test_validation_and_sealing_reject_unsupported_portfolio_contracts(
    research: ResearchFixture,
    invalid: str,
) -> None:
    search = portfolio_options()["search"]
    promotion = PromotionConfiguration()
    if invalid == "missing":
        search = search.model_copy(update={"portfolio": None})
    elif invalid == "version":
        search = search.model_copy(update={"version": "portfolio-future"})
    elif invalid == "slots":
        search = search.model_copy(update={"max_parallel_trials": 17})
    elif invalid == "paired":
        promotion = PromotionConfiguration(
            mode="paired_confirmation",
            version="paired-sign-v1",
            confirmation_seeds=(101, 102, 103, 104, 105),
        )
    else:
        search = search.model_copy(update={"policy": "greedy", "version": "greedy-v1"})
    campaign = research.campaign(start=False, search=search, promotion=promotion)
    for operation in (research.service.validate_campaign, research.service.seal_campaign):
        with pytest.raises(ResearchError) as unsupported:
            operation(
                research.context(revision=campaign["revision"]),
                CampaignTarget(campaign_id=campaign["id"]),
            )
        assert unsupported.value.code == (
            ErrorCode.PROJECT_CONTRACT_INVALID
            if invalid == "foreign"
            else ErrorCode.CAPABILITY_UNAVAILABLE
        )
    assert (
        research.service.database.read_record(research.context(), "campaign", campaign["id"])
        == campaign
    )


def test_absent_portfolio_fields_preserve_legacy_policy_and_request_bytes() -> None:
    identifier = "11111111-1111-4111-8111-111111111111"
    digest = "sha256:" + "0" * 64
    legacy_state = {
        "schema_version": "1.0",
        "campaign_id": identifier,
        "campaign_contract_digest": digest,
        "policy": "external",
        "policy_version": "external-v1",
        "generation": 0,
        "request_count": 0,
        "pending_request_ids": [],
        "active_trial_ids": [],
    }
    assert canonical_bytes(
        SearchState.model_validate(legacy_state).model_dump(mode="json")
    ) == canonical_bytes(legacy_state)
    legacy_request = {
        "schema_version": "1.0",
        "campaign_id": identifier,
        "proposer_id": identifier,
        "seed": 19,
        "expected_incumbent_revision": 1,
        "expected_controller_id": identifier,
        "expected_control_epoch": 0,
        "instructions": "Explore",
    }
    assert canonical_bytes(
        RequestProposal.model_validate(legacy_request).model_dump(mode="json")
    ) == canonical_bytes(legacy_request)


@pytest.mark.parametrize("pipeline", [portfolio_options()], indirect=True)
def test_late_proposal_is_preserved_but_requires_cancellation_after_another_trial_promotes(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    baseline(pipeline)
    fast = issue(pipeline, 0, 0)["request"]
    slow = issue(pipeline, 1, 1)["request"]
    answer(pipeline, fast, tmp_path, 0.35)
    trial = admit(pipeline, fast["id"], revision=2)["result"]["trial"]
    receipt = evaluate(pipeline, trial)
    observed = observe(pipeline, trial)["result"]
    pin(pipeline, observed, revision=3)
    decision = assess(pipeline, receipt, "fast-selection", 1)
    commit(pipeline, decision, "fast-winner")
    resolve(pipeline, observed, decision, revision=4)
    late = answer(pipeline, slow, tmp_path, 0.15)
    assert late["data"]["late_at_recording"]
    with pytest.raises(ResearchError) as stale:
        admit(pipeline, slow["id"], revision=5)
    assert stale.value.code == ErrorCode.INCUMBENT_REVISION_CONFLICT
    cancel(pipeline, slow, revision=5)
    assert pipeline.client.get("proposal_response", slow["id"]) == late
    replacement = issue(pipeline, 2, 6)["request"]
    assert replacement["data"]["parent_candidate_id"] == trial["data"]["candidate_id"]
    assert replacement["id"] != slow["id"]
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2
