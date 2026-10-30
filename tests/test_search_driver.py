from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import PauseCampaign
from research_swarm.control.models import StoppingRule
from research_swarm.control.observation_models import (
    AbandonObservationSelection,
    SelectTrialObservation,
    observation_selection_id,
)
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.search_models import RespondToProposal
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.orchestration.search import SearchDriver
from research_swarm.search.greedy import GreedyConfiguration, GreedyPolicy
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_promotions import assess, commit
from tests.test_proposals import PROPOSAL_LIMITS, baseline, proposer
from tests.test_search_admission import changed_source
from tests.test_search_observations import evaluate, observe, paired_options, policy_trial, resolve


def pin(pipeline: Pipeline, observation: dict[str, Any], revision: int = 2) -> dict[str, Any]:
    authority = pipeline.client.context(pipeline.campaign["id"])["authority"]
    return pipeline.client.command(
        "search.select_observation",
        SelectTrialObservation(
            campaign_id=pipeline.campaign["id"],
            observation_id=observation["id"],
            expected_controller_id=authority["controller_id"],
            expected_control_epoch=authority["control_epoch"],
        ),
        idempotency_key=new_id(),
        expected_revision=revision,
    )["result"]


@pytest.mark.parametrize(
    "pipeline", [{"limits": PROPOSAL_LIMITS, "source_learning_rate": 0.0}], indirect=True
)
def test_greedy_driver_completes_real_local_and_http_research_cycle(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    principal, agent, _ = proposer(pipeline)
    config = GreedyConfiguration(
        proposer_id=principal.id, baseline_trial_id=pipeline.trial["id"], first_seed=19
    )
    local = SearchDriver(pipeline.client, pipeline.campaign["id"], config)
    assert local.advance().state == "awaiting_execution"
    evaluate(pipeline, pipeline.trial)
    baseline_ready = local.advance()
    assert baseline_ready.state == "awaiting_proposal"
    assert [step.operation for step in baseline_ready.actions] == [
        "promotion.assess",
        "promotion.commit",
        "search.request_proposal",
    ]
    request_id = baseline_ready.evidence_ids[0]
    artifact = changed_source(pipeline, tmp_path)
    agent.command(
        "search.respond",
        RespondToProposal(
            request_id=request_id,
            outcome="candidate",
            source_artifact=artifact,
            hypothesis="Train a nonzero optimizer against the frozen zero-learning baseline",
        ),
        idempotency_key="agent-answer",
    )
    credential = pipeline.runtime.credentials.issue(
        pipeline.runtime.principal(), pipeline.runtime.principal()
    )
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, credential.token) as remote,
    ):
        driver = SearchDriver(remote, pipeline.campaign["id"], config)
        snapshot = driver.snapshot()
        assert GreedyPolicy.plan(snapshot, config) == local.plan()
        admitted = driver.advance()
        assert admitted.state == "awaiting_execution"
        trial_id = admitted.evidence_ids[0]
        trial = pipeline.client.get("trial", trial_id)
        evaluate(pipeline, trial)
        # Persist the pin, then recreate the driver with another client after assessment.
        progress = driver.advance(max_steps=3)
        assert progress.state == "step_limit"
        assert [step.operation for step in progress.actions] == [
            "search.observe_trial",
            "search.select_observation",
            "promotion.assess",
        ]
        selected = SearchDriver(pipeline.client, pipeline.campaign["id"], config).advance(
            max_steps=1
        )
        assert selected.actions[0].operation == "promotion.commit"
        resumed = SearchDriver(remote, pipeline.campaign["id"], config).advance()
        assert resumed.state == "awaiting_proposal"
        assert [step.operation for step in resumed.actions] == [
            "search.resolve_trial",
            "search.request_proposal",
        ]
        next_request = remote.get("proposal_request", resumed.evidence_ids[0])
        assert next_request["data"]["parent_candidate_id"] == trial["data"]["candidate_id"]
        assert next_request["data"]["seed"] == 20
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    assert campaign["data"]["incumbent_revision"] == 2
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2
    assert len(pipeline.client.list(RecordQuery(kind="observation_consumption"))["items"]) == 1


@pytest.mark.parametrize(
    "pipeline", [{"limits": PROPOSAL_LIMITS, "source_learning_rate": 0.0}], indirect=True
)
def test_pin_cannot_be_replaced_by_a_post_promotion_observation(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    trial = policy_trial(pipeline, tmp_path)
    receipt = evaluate(pipeline, trial)
    original = observe(pipeline, trial)["result"]
    selected = pin(pipeline, original)
    assert selected["search"]["revision"] == 3
    identifier = observation_selection_id(
        pipeline.runtime.identity.workspace_id, pipeline.campaign["id"], trial["id"]
    )
    assert selected["transition"]["id"] == identifier
    decision = assess(pipeline, receipt, "pin-selection", 1)
    commit(pipeline, decision, "pin-commit")
    newer = observe(pipeline, trial)["result"]
    assert newer["id"] != original["id"]
    with pytest.raises(ResearchError) as immutable:
        pin(pipeline, newer, 3)
    assert immutable.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    with pytest.raises(ResearchError) as wrong_observation:
        resolve(pipeline, newer, decision, revision=3)
    assert wrong_observation.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert (
        resolve(pipeline, original, decision, revision=3)["result"]["transition"]["data"]["outcome"]
        == "promoted"
    )
    assert pipeline.client.get("policy_transition", identifier) == selected["transition"]


@pytest.mark.parametrize(
    "pipeline", [{"limits": PROPOSAL_LIMITS, "source_learning_rate": 0.0}], indirect=True
)
def test_two_principals_share_one_research_transition_at_each_race(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    principal, agent, _ = proposer(pipeline)
    config = GreedyConfiguration(proposer_id=principal.id, baseline_trial_id=pipeline.trial["id"])
    evaluate(pipeline, pipeline.trial)
    assert (
        SearchDriver(pipeline.client, pipeline.campaign["id"], config).advance(max_steps=2).state
        == "step_limit"
    )
    clients = []
    for _ in range(2):
        actor = Principal(
            id=new_id(),
            workspace_id=pipeline.runtime.identity.workspace_id,
            scopes=frozenset({"*"}),
        )
        clients.append(ResearchClient(LocalTransport(pipeline.runtime, lambda actor=actor: actor)))

    def race(operation: str, states: list[str]) -> None:
        barrier = threading.Barrier(2)

        def synchronized(original: Any) -> Any:
            def command(name: str, *args: Any, **kwargs: Any) -> Any:
                assert name == operation
                barrier.wait(timeout=10)
                return original(name, *args, **kwargs)

            return command

        with monkeypatch.context() as patch, ThreadPoolExecutor(max_workers=2) as pool:
            for client in clients:
                patch.setattr(client, "command", synchronized(client.command))
            futures = [
                pool.submit(
                    SearchDriver(client, pipeline.campaign["id"], config).advance, max_steps=1
                )
                for client in clients
            ]
            assert sorted(future.result(timeout=20).state for future in futures) == sorted(states)

    race("search.request_proposal", ["step_limit", "contended"])
    request = pipeline.client.get("search_state", pipeline.campaign["id"])["data"][
        "pending_request_ids"
    ][0]
    agent.command(
        "search.respond",
        RespondToProposal(
            request_id=request,
            outcome="candidate",
            source_artifact=changed_source(pipeline, tmp_path),
            hypothesis="Race independent controllers over one proposed optimizer change",
        ),
        idempotency_key="race-response",
    )
    race("search.admit_response", ["step_limit", "contended"])
    trial_id = pipeline.client.get("search_state", pipeline.campaign["id"])["data"][
        "active_trial_ids"
    ][0]
    evaluate(pipeline, pipeline.client.get("trial", trial_id))
    race("search.observe_trial", ["step_limit", "step_limit"])
    race("search.select_observation", ["step_limit", "contended"])
    race("promotion.assess", ["step_limit", "step_limit"])
    race("promotion.commit", ["step_limit", "step_limit"])
    race("search.resolve_trial", ["step_limit", "contended"])
    state = pipeline.client.get("search_state", pipeline.campaign["id"])["data"]
    assert state["generation"] == 1 and state["active_trial_ids"] == []
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2
    assert len(pipeline.client.list(RecordQuery(kind="search_observation"))["items"]) == 1
    assert len(pipeline.client.list(RecordQuery(kind="observation_consumption"))["items"]) == 1
    assert (
        pipeline.client.get("campaign", pipeline.campaign["id"])["data"]["incumbent_revision"] == 2
    )


@pytest.mark.parametrize(
    "pipeline", [{"limits": PROPOSAL_LIMITS, "source_learning_rate": 0.0}], indirect=True
)
def test_recreated_driver_recovers_lost_ack_at_every_mutating_boundary(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    principal, agent, _ = proposer(pipeline)
    config = GreedyConfiguration(proposer_id=principal.id, baseline_trial_id=pipeline.trial["id"])
    evaluate(pipeline, pipeline.trial)
    client = ResearchClient(LocalTransport(pipeline.runtime, pipeline.runtime.principal))
    original = client.command
    lost: set[tuple[str, str]] = set()

    def lose_ack(operation: str, *args: Any, **kwargs: Any) -> Any:
        result = original(operation, *args, **kwargs)
        identity = operation, kwargs["idempotency_key"]
        if identity not in lost:
            lost.add(identity)
            raise ResearchError(ErrorCode.DELIVERY_UNCERTAIN, "Accepted reply lost in transit")
        return result

    monkeypatch.setattr(client, "command", lose_ack)
    response_id = None
    executed = set()
    for _ in range(32):
        result = SearchDriver(client, pipeline.campaign["id"], config).advance()
        if result.state == "delivery_uncertain":
            continue
        if result.state == "awaiting_proposal":
            if response_id is not None:
                assert result.evidence_ids != (response_id,)
                break
            response_id = result.evidence_ids[0]
            agent.command(
                "search.respond",
                RespondToProposal(
                    request_id=response_id,
                    outcome="candidate",
                    source_artifact=changed_source(pipeline, tmp_path),
                    hypothesis="Recover proposal and selection acknowledgments from durable state",
                ),
                idempotency_key="recovery-response",
            )
        elif result.state == "awaiting_execution":
            trial_id = result.evidence_ids[0]
            assert trial_id not in executed
            executed.add(trial_id)
            evaluate(pipeline, pipeline.client.get("trial", trial_id))
        else:
            pytest.fail(f"Unexpected recovery state: {result}")
    else:
        pytest.fail("Research cycle did not recover within the action bound")
    assert {operation for operation, _ in lost} == {
        "promotion.assess",
        "promotion.commit",
        "search.request_proposal",
        "search.admit_response",
        "search.observe_trial",
        "search.select_observation",
        "search.resolve_trial",
    }
    assert len(executed) == 1
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2
    assert len(pipeline.client.list(RecordQuery(kind="observation_consumption"))["items"]) == 1
    assert (
        pipeline.client.get("campaign", pipeline.campaign["id"])["data"]["incumbent_revision"] == 2
    )


@pytest.mark.parametrize(
    "pipeline", [{"limits": PROPOSAL_LIMITS, "source_learning_rate": 0.0}], indirect=True
)
@pytest.mark.parametrize("correct_comparator", [False, True])
def test_correction_explicitly_abandons_pin_without_erasing_evidence_or_moving_incumbent(
    pipeline: Pipeline, tmp_path: Path, correct_comparator: bool
) -> None:
    trial = policy_trial(pipeline, tmp_path)
    receipt = evaluate(pipeline, trial)
    original = observe(pipeline, trial)["result"]
    selected = pin(pipeline, original)
    decision = assess(pipeline, receipt, "corrected-selection", 1)
    authority = pipeline.client.context(pipeline.campaign["id"])["authority"]

    def abandon(observation_id: str, selection_id: str) -> dict[str, Any]:
        return pipeline.client.command(
            "search.abandon_selection",
            AbandonObservationSelection(
                campaign_id=pipeline.campaign["id"],
                observation_id=observation_id,
                selection_id=selection_id,
                reason="Evaluator evidence corrected after the observation was selected",
                expected_controller_id=authority["controller_id"],
                expected_control_epoch=authority["control_epoch"],
            ),
            idempotency_key=new_id(),
            expected_revision=3,
        )["result"]

    with pytest.raises(ResearchError) as valid_measurement:
        abandon(original["id"], selected["transition"]["id"])
    assert valid_measurement.value.code == ErrorCode.COMPARISON_INELIGIBLE
    pipeline.client.command(
        "evidence.invalidate",
        InvalidateEvidence(
            target=EvidenceReference(
                kind="evaluation_receipt",
                id=original["data"]["comparator_evaluation_id"]
                if correct_comparator
                else receipt["id"],
            ),
            reason="Test correction after immutable observation selection",
        ),
        idempotency_key="correct-selected-evidence",
    )
    with pytest.raises(ResearchError) as stale:
        resolve(pipeline, original, decision, revision=3)
    assert stale.value.code == (
        ErrorCode.EVIDENCE_INVALIDATED if correct_comparator else ErrorCode.STALE_FENCE
    )
    fresh = observe(pipeline, trial)["result"]
    assert fresh["data"]["facts"]["outcome"] == (
        "measurement" if correct_comparator else "evidence_invalidated"
    )
    with pytest.raises(ResearchError) as wrong_pin:
        abandon(fresh["id"], new_id())
    assert wrong_pin.value.code == ErrorCode.COMPARISON_INELIGIBLE
    budget = pipeline.client.budget(pipeline.campaign["id"])
    campaign_before = pipeline.client.get("campaign", pipeline.campaign["id"])
    resolved = abandon(fresh["id"], selected["transition"]["id"])
    transition = resolved["transition"]["data"]
    assert transition["outcome"] == "non_scientific" and transition["decision_id"] is None
    assert transition["abandonment"]["original_observation_id"] == original["id"]
    assert transition["abandonment"]["basis"] == (
        "comparator_invalidated" if correct_comparator else "trial_evidence_changed"
    )
    assert resolved["search"]["data"]["active_trial_ids"] == []
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    assert pipeline.client.get("search_observation", original["id"]) == original
    assert (
        pipeline.client.get("policy_transition", selected["transition"]["id"])
        == selected["transition"]
    )
    assert pipeline.client.get("campaign", pipeline.campaign["id"]) == campaign_before


@pytest.mark.parametrize(
    "pipeline", [{"limits": PROPOSAL_LIMITS, "source_learning_rate": 0.0}], indirect=True
)
@pytest.mark.parametrize("already_committed", [False, True])
def test_pause_stops_selection_but_allows_committed_bookkeeping(
    pipeline: Pipeline, tmp_path: Path, already_committed: bool
) -> None:
    trial = policy_trial(pipeline, tmp_path)
    receipt = evaluate(pipeline, trial)
    original = observe(pipeline, trial)["result"]
    pin(pipeline, original)
    decision = assess(pipeline, receipt, "pause-selection", 1)
    if already_committed:
        commit(pipeline, decision, "pause-commit")
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "campaign.pause",
        PauseCampaign(campaign_id=campaign["id"]),
        idempotency_key="operator-pause",
        expected_revision=campaign["revision"],
    )
    driver = SearchDriver(
        pipeline.client, campaign["id"], GreedyConfiguration(proposer_id=new_id())
    )
    paused = driver.advance()
    assert paused.state == "paused"
    assert [item.operation for item in paused.actions] == (
        ["search.resolve_trial"] if already_committed else []
    )
    state = pipeline.client.get("search_state", campaign["id"])["data"]
    assert state["active_trial_ids"] == ([] if already_committed else [trial["id"]])


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "limits": (
                *PROPOSAL_LIMITS[:-1],
                BudgetLimit(dimension="proposal_requests", limit=0, unit="count"),
            )
        },
        {"limits": PROPOSAL_LIMITS, "stopping": StoppingRule(target_metric=20.0)},
    ],
    indirect=True,
)
def test_declared_stop_prevents_another_proposal(pipeline: Pipeline) -> None:
    baseline(pipeline)
    config = GreedyConfiguration(proposer_id=new_id())
    before = pipeline.client.budget(pipeline.campaign["id"])
    result = SearchDriver(pipeline.client, pipeline.campaign["id"], config).advance()
    expected = (
        "target_reached"
        if pipeline.campaign["data"]["contract"]["stopping"]["target_metric"] is not None
        else "budget_exhausted"
    )
    assert result.state == expected and result.actions == ()
    assert pipeline.client.list(RecordQuery(kind="proposal_request"))["items"] == []
    assert pipeline.client.budget(pipeline.campaign["id"]) == before


@pytest.mark.parametrize("pipeline", [paired_options()], indirect=True)
@pytest.mark.parametrize("failed_panel", [False, True])
def test_greedy_panel_finishes_or_records_ineligibility_without_another_panel(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_panel: bool
) -> None:
    from research_swarm.control.confirmation import CreateConfirmationPlan
    from research_swarm.control.confirmation_models import ConfirmationData

    trial = policy_trial(pipeline, tmp_path)
    evaluate(pipeline, trial)
    config = GreedyConfiguration(proposer_id=new_id())
    driver = SearchDriver(pipeline.client, pipeline.campaign["id"], config)
    assert driver.advance().state == "awaiting_confirmation"
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    assert pipeline.worker.profile is not None
    fingerprint = pipeline.worker.profile.execution_fingerprint
    planned = pipeline.client.command(
        "confirmation.create",
        CreateConfirmationPlan(
            campaign_id=campaign["id"],
            candidate_id=trial["data"]["candidate_id"],
            validation_id=trial["data"]["validation_id"],
            expected_incumbent_revision=1,
            evaluator_id=pipeline.runtime.principal().id,
            execution_fingerprints={"train": fingerprint, "evaluate": fingerprint},
        ),
        idempotency_key="driver-panel",
        expected_revision=campaign["revision"],
    )["result"]
    plan = ConfirmationData.model_validate(planned["data"]).plan
    assert driver.advance().state == "awaiting_confirmation"
    for index, binding in enumerate(plan.bindings):
        realized = pipeline.client.get("trial", binding.trial_id)
        if index == 0 and failed_panel:
            attempt = pipeline.worker.claim(
                pipeline.client.get("job", realized["data"]["job_ids"][0])
            )

            def broken(*args: Any, **kwargs: Any) -> Any:
                raise ValueError("Synthetic invalid report from one required realization")

            with monkeypatch.context() as patch:
                patch.setattr("research_swarm.workers.local.read_report", broken)
                pipeline.worker.execute(attempt["id"])
            assert driver.advance().state == "awaiting_confirmation"
        else:
            evaluate(pipeline, realized)
    result = SearchDriver(pipeline.client, campaign["id"], config).advance()
    if failed_panel:
        assert result.state == "needs_review"
        assert [item.operation for item in result.actions] == ["confirmation.assess"]
        assert pipeline.client.get("confirmation_plan", plan.id)["state"] == "ineligible"
        assert pipeline.client.get("campaign", campaign["id"])["data"]["incumbent_revision"] == 1
    else:
        assert result.state == "budget_exhausted"
        assert [item.operation for item in result.actions] == [
            "confirmation.assess",
            "promotion.commit",
            "search.resolve_trial",
        ]
        assert (
            pipeline.client.get("campaign", campaign["id"])["data"]["incumbent_evidence_level"]
            == "confirmed"
        )
    assert len(pipeline.client.list(RecordQuery(kind="confirmation_plan"))["items"]) == 1
    assert len(pipeline.client.list(RecordQuery(kind="proposal_request"))["items"]) == 1
