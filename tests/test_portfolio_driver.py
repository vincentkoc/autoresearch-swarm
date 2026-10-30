from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import ResearchClient
from research_swarm.control.commands import PauseCampaign
from research_swarm.control.queries import RecordQuery
from research_swarm.control.results import PublishAttempt
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.orchestration.search import SearchDriver
from research_swarm.search.greedy import SearchAction
from research_swarm.search.portfolio import PortfolioDriverConfiguration
from research_swarm.workers.journal import JournalState
from tests.test_cli_mcp import cli
from tests.test_evaluation_driver import role
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_portfolio_control import answer, portfolio_options
from tests.test_search_observations import evaluate


def driver(pipeline: Pipeline, client: ResearchClient | None = None) -> SearchDriver:
    return SearchDriver(
        client or pipeline.client,
        pipeline.campaign["id"],
        PortfolioDriverConfiguration(baseline_trial_id=pipeline.trial["id"]),
    )


def requests(pipeline: Pipeline) -> list[dict[str, Any]]:
    state = pipeline.client.get("search_state", pipeline.campaign["id"])
    return [
        pipeline.client.get("proposal_request", item)
        for item in state["data"]["pending_request_ids"]
    ]


def admit_pair(pipeline: Pipeline, tmp_path: Path) -> list[dict[str, Any]]:
    evaluate(pipeline, pipeline.trial)
    result = driver(pipeline).advance()
    assert result.state == "awaiting_proposal" and len(result.evidence_ids) == 2
    for request, rate in zip(requests(pipeline), (0.15, 0.35), strict=True):
        answer(pipeline, request, tmp_path, rate)
    assert driver(pipeline).advance().state == "awaiting_execution"
    state = pipeline.client.get("search_state", pipeline.campaign["id"])
    return [pipeline.client.get("trial", item) for item in state["data"]["active_trial_ids"]]


@pytest.mark.parametrize(
    ("operation", "payload", "revision", "basis", "digest"),
    [
        (
            "search.request_proposal",
            {
                "campaign_id": "ID",
                "proposer_id": "ID",
                "seed": 19,
                "expected_incumbent_revision": 1,
                "expected_controller_id": "ID",
                "expected_control_epoch": 0,
                "instructions": "Explore",
            },
            0,
            {},
            "d9ce14633409b0d415486fb9b1e4e2d786655de7a79110642de7f14e75239062",
        ),
        (
            "search.observe_trial",
            {"campaign_id": "ID", "trial_id": "ID"},
            2,
            {"incumbent_revision": 1},
            "19222e9ab9bb87f437fd6917b40054a3c9d64fc56b55b5ec66e9a01ca04da6c5",
        ),
        (
            "promotion.commit",
            {"decision_id": "ID"},
            None,
            {},
            "02a1030b5fac188e56fc226cc617f6897b01ca3964f66a435fa1595aa5a35f93",
        ),
    ],
)
def test_greedy_command_keys_survive_shared_planner_extraction(
    operation: str,
    payload: dict[str, Any],
    revision: int | None,
    basis: dict[str, Any],
    digest: str,
) -> None:
    identifier = "11111111-1111-4111-8111-111111111111"
    action = SearchAction(
        operation=operation,
        payload={
            "schema_version": "1.0",
            **{key: identifier if value == "ID" else value for key, value in payload.items()},
        },
        expected_revision=revision,
        basis=basis,
    )
    assert action.command_key(identifier) == "sha256:" + digest


@pytest.mark.parametrize("pipeline", [portfolio_options(max_trials=3)], indirect=True)
def test_ready_sibling_promotes_through_http_and_lost_commit_recovers_while_paused(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = admit_pair(pipeline, tmp_path)
    original_parent = pipeline.client.get("candidate", first["data"]["candidate_id"])["data"][
        "parent_ids"
    ]
    evaluate(pipeline, second)
    _, _, token = role(pipeline, "*")
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, token) as remote,
    ):
        original = remote.command

        def lose_commit(operation: str, *args: Any, **kwargs: Any) -> Any:
            reply = original(operation, *args, **kwargs)
            if operation == "promotion.commit":
                raise ResearchError(ErrorCode.DELIVERY_UNCERTAIN, "Accepted commit reply was lost")
            return reply

        with monkeypatch.context() as patch:
            patch.setattr(remote, "command", lose_commit)
            lost = driver(pipeline, remote).advance()
        assert lost.state == "delivery_uncertain"
        state = remote.get("search_state", pipeline.campaign["id"])
        assert state["data"]["selection_trial_id"] == second["id"]
        campaign = remote.get("campaign", pipeline.campaign["id"])
        assert campaign["data"]["incumbent_revision"] == 2
        assert campaign["data"]["incumbent_candidate_id"] == second["data"]["candidate_id"]
        remote.command(
            "campaign.pause",
            PauseCampaign(campaign_id=campaign["id"]),
            idempotency_key="pause-after-selection",
            expected_revision=campaign["revision"],
        )
        resumed = driver(pipeline, remote).advance()
        assert resumed.state == "paused"
        assert [item.operation for item in resumed.actions] == ["search.resolve_trial"]
        assert remote.get("search_state", campaign["id"])["data"]["active_trial_ids"] == [
            first["id"]
        ]
    # The admitted old-parent trial remains a valid hypothesis; ancestry is never rewritten.
    from research_swarm.control.commands import CampaignTarget

    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "campaign.start",
        CampaignTarget(campaign_id=campaign["id"]),
        idempotency_key="resume-portfolio",
        expected_revision=campaign["revision"],
    )
    evaluate(pipeline, first)
    result = driver(pipeline).advance()
    assert result.state == "budget_exhausted"
    observations = pipeline.client.list(
        RecordQuery(kind="search_observation", parent_id=first["id"])
    )["items"]
    assert observations[-1]["data"]["comparator_id"] == second["data"]["candidate_id"]
    assert (
        pipeline.client.get("candidate", first["data"]["candidate_id"])["data"]["parent_ids"]
        == original_parent
    )
    consumptions = pipeline.client.list(RecordQuery(kind="observation_consumption"))["items"]
    assert len(consumptions) == 2
    final = pipeline.client.get("campaign", campaign["id"])["data"]
    promotions = pipeline.client.list(
        RecordQuery(kind="promotion", parent_id=first["data"]["candidate_id"])
    )["items"]
    assert len(promotions) == 1
    won = promotions[0]["state"] == "committed"
    assert final["incumbent_revision"] == 2 + int(won)
    assert final["incumbent_candidate_id"] == (first if won else second)["data"]["candidate_id"]


@pytest.mark.parametrize("pipeline", [portfolio_options(max_trials=4)], indirect=True)
def test_unanswered_first_request_does_not_block_second_and_late_response_is_retained(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    evaluate(pipeline, pipeline.trial)
    assert driver(pipeline).advance().state == "awaiting_proposal"
    first, second = requests(pipeline)
    answer(pipeline, second, tmp_path, 0.35)
    assert driver(pipeline).advance().state == "awaiting_execution"
    state = pipeline.client.get("search_state", pipeline.campaign["id"])
    trial = pipeline.client.get("trial", state["data"]["active_trial_ids"][0])
    evaluate(pipeline, trial)
    finished = driver(pipeline).advance()
    assert [step.operation for step in finished.actions][-3:] == [
        "search.cancel_proposal",
        "search.request_proposal",
        "search.request_proposal",
    ]
    assert pipeline.client.get("proposal_request", first["id"])["state"] == "cancelled"
    response = answer(pipeline, first, tmp_path, 0.15)
    assert response["data"]["late_at_recording"]
    assert pipeline.client.get("proposal_response", first["id"]) == response
    fresh = requests(pipeline)
    assert [item["data"]["seed"] for item in fresh] == [21, 22]
    assert all(
        item["data"]["parent_candidate_id"] == trial["data"]["candidate_id"] for item in fresh
    )
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2
    assert driver(pipeline).advance().actions == ()


@pytest.mark.parametrize("pipeline", [portfolio_options()], indirect=True)
def test_unknown_execution_retains_slot_but_ready_sibling_and_vacancy_progress(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = admit_pair(pipeline, tmp_path)
    evaluate(pipeline, second)
    attempt = pipeline.worker.claim(pipeline.client.get("job", first["data"]["job_ids"][0]))
    original = pipeline.worker.client.command

    def lost_publication(operation: str, *args: Any, **kwargs: Any) -> Any:
        if operation == "worker.publish_attempt":
            raise RuntimeError("Lost native receipt")
        return original(operation, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(pipeline.worker.client, "command", lost_publication)
        with pytest.raises(RuntimeError, match="Lost native receipt"):
            pipeline.worker.execute(attempt["id"])
    closed = pipeline.worker.journal.get(attempt["id"])
    unknown = closed.model_copy(update={"state": JournalState.UNKNOWN, "outcome": None})
    artifact_path = tmp_path / "unknown-native-receipt.json"
    artifact_path.write_text(unknown.model_dump_json(), encoding="utf-8")
    artifact = pipeline.worker.client.upload_file(artifact_path, idempotency_key="unknown-receipt")
    original(
        "worker.publish_attempt",
        PublishAttempt(
            attempt_id=attempt["id"],
            journal_artifact=artifact,
            finding="Original closure receipt unavailable",
        ),
        idempotency_key="unknown-publication",
    )
    before = pipeline.client.get("trial", first["id"])
    try:
        result = driver(pipeline).advance()
        assert result.state == "awaiting_execution"
        assert [step.operation for step in result.actions][-2:] == [
            "search.resolve_trial",
            "search.request_proposal",
        ]
        state = pipeline.client.get("search_state", pipeline.campaign["id"])["data"]
        assert state["active_trial_ids"] == [first["id"]] and len(state["pending_request_ids"]) == 1
        assert pipeline.client.get("trial", first["id"]) == before
        assert len(pipeline.client.list(RecordQuery(kind="observation_consumption"))["items"]) == 1
        assert (
            len(
                pipeline.client.list(
                    RecordQuery(kind="attempt", parent_id=first["data"]["job_ids"][0])
                )["items"]
            )
            == 1
        )
    finally:
        pipeline.worker.execute(attempt["id"])


@pytest.mark.parametrize("pipeline", [portfolio_options(max_trials=3)], indirect=True)
def test_independent_drivers_race_one_selection_and_limited_scan_does_not_block_sibling(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trials = admit_pair(pipeline, tmp_path)
    for trial in trials:
        evaluate(pipeline, trial)
    limited = driver(pipeline)
    original_records = limited._records

    def incomplete(kind: str, *, parent_id: str | None = None) -> Any:
        if kind == "search_observation" and parent_id == trials[0]["id"]:
            return [], False
        return original_records(kind, parent_id=parent_id)

    monkeypatch.setattr(limited, "_records", incomplete)
    observed = limited.advance(max_steps=1)
    assert observed.actions[0].operation == "search.observe_trial"
    # A bounded scan of A cannot suppress a complete selection of B.
    assert (
        limited.plan().payload["observation_id"]
        == pipeline.client.list(RecordQuery(kind="search_observation", parent_id=trials[1]["id"]))[
            "items"
        ][0]["id"]
    )
    clients = [role(pipeline, "*")[1] for _ in range(2)]
    barrier = threading.Barrier(2)

    def race(client: ResearchClient) -> Any:
        action = limited.plan()
        barrier.wait(timeout=10)
        try:
            return client.command(
                action.operation,
                action.payload,
                idempotency_key=action.command_key(pipeline.campaign["id"]),
                expected_revision=action.expected_revision,
            )["disposition"]
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(race, clients))
    assert sorted(map(str, outcomes)) == sorted(map(str, ["accepted", ErrorCode.REVISION_CONFLICT]))
    state = pipeline.client.get("search_state", pipeline.campaign["id"])
    assert state["data"]["selection_trial_id"] == trials[1]["id"]
    result = driver(pipeline).advance()
    assert result.state == "budget_exhausted"
    assert len(pipeline.client.list(RecordQuery(kind="observation_consumption"))["items"]) == 2


@pytest.mark.parametrize("pipeline", [portfolio_options(max_trials=2)], indirect=True)
def test_portfolio_cli_uses_sealed_theme_and_reserves_last_trial(pipeline: Pipeline) -> None:
    evaluate(pipeline, pipeline.trial)
    result = cli(
        pipeline.runtime.root,
        "search",
        "advance",
        pipeline.campaign["id"],
        "--baseline-trial",
        pipeline.trial["id"],
    )
    assert result["state"] == "awaiting_proposal"
    assert [step["operation"] for step in result["actions"]] == [
        "promotion.assess",
        "promotion.commit",
        "search.request_proposal",
    ]
    assert len(requests(pipeline)) == 1
    assert cli(pipeline.runtime.root, "search", "advance", pipeline.campaign["id"])["actions"] == []


@pytest.mark.parametrize("pipeline", [portfolio_options()], indirect=True)
@pytest.mark.parametrize("change", ["search", "epoch"])
def test_plural_snapshot_detects_changes_before_issuing_any_action(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from tests.test_portfolio_control import issue
    from tests.test_worker_sessions import advance_epoch

    evaluate(pipeline, pipeline.trial)
    assert driver(pipeline).advance(max_steps=3).state == "step_limit"
    selected = driver(pipeline)
    original = selected._request

    def changing(identifier: str) -> Any:
        result = original(identifier)
        if change == "search":
            issue(pipeline, 1, 1)
        else:
            advance_epoch(pipeline)
        return result

    monkeypatch.setattr(selected, "_request", changing)
    result = selected.advance()
    assert result.state == "contended" and result.actions == ()
    if change == "epoch":
        recovered = driver(pipeline).advance()
        assert recovered.actions[0].operation == "search.cancel_proposal"
        assert len(requests(pipeline)) == 2
        epoch = pipeline.client.context(pipeline.campaign["id"])["authority"]["control_epoch"]
        assert all(item["data"]["control_epoch"] == epoch for item in requests(pipeline))


@pytest.mark.parametrize("pipeline", [portfolio_options()], indirect=True)
def test_paused_response_keeps_matching_context_and_cli_rejects_theme_overrides(
    pipeline: Pipeline, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from research_swarm.cli import main
    from research_swarm.control.commands import CampaignTarget

    evaluate(pipeline, pipeline.trial)
    assert driver(pipeline).advance().state == "awaiting_proposal"
    first, second = requests(pipeline)
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    paused = pipeline.client.command(
        "campaign.pause",
        PauseCampaign(campaign_id=campaign["id"]),
        idempotency_key="pause-agents",
        expected_revision=campaign["revision"],
    )["result"]
    response = answer(pipeline, second, tmp_path, 0.35)
    assert response["data"]["late_at_recording"]
    assert driver(pipeline).advance().state == "paused"
    pipeline.client.command(
        "campaign.start",
        CampaignTarget(campaign_id=campaign["id"]),
        idempotency_key="continue-agents",
        expected_revision=paused["revision"],
    )
    admitted = driver(pipeline).advance()
    assert [item.operation for item in admitted.actions] == ["search.admit_response"]
    assert requests(pipeline) == [first]
    before = pipeline.client.get("search_state", campaign["id"])
    for option, value in (("--proposer", new_id()), ("--first-seed", "0"), ("--instructions", "")):
        assert (
            main(
                [
                    "--state-dir",
                    str(pipeline.runtime.root),
                    "search",
                    "advance",
                    campaign["id"],
                    option,
                    value,
                ]
            )
            == 2
        )
        assert "sealed themes" in capsys.readouterr().err
    assert pipeline.client.get("search_state", campaign["id"]) == before


@pytest.mark.parametrize("pipeline", [portfolio_options()], indirect=True)
def test_target_with_unknown_sibling_does_not_admit_or_request_more_work(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    from dataclasses import replace

    from research_swarm.search.common import SearchWait
    from research_swarm.search.portfolio import PortfolioPolicy

    first, second = admit_pair(pipeline, tmp_path)
    evaluate(pipeline, second)
    selected = driver(pipeline)
    assert (
        selected.advance(max_steps=4).state == "step_limit"
    )  # commit; owner still needs resolution
    snapshot = selected.portfolio_snapshot()
    unknown = replace(
        snapshot.trials[0],
        trial={
            **snapshot.trials[0].trial,
            "data": {**first["data"], "execution_outcome": "unknown", "resource_status": "unknown"},
        },
    )
    snapshot = replace(
        snapshot,
        header=replace(
            snapshot.header,
            context={
                **snapshot.header.context,
                "stopping_status": {"target_met": True},
            },
        ),
        trials=(unknown, snapshot.trials[1]),
    )
    assert PortfolioPolicy.plan(snapshot).operation == "search.resolve_trial"
    assert selected.advance(max_steps=1).actions[0].operation == "search.resolve_trial"
    snapshot = selected.portfolio_snapshot()
    snapshot = replace(
        snapshot,
        header=replace(
            snapshot.header,
            context={
                **snapshot.header.context,
                "stopping_status": {"target_met": True},
            },
        ),
        trials=(unknown,),
    )
    waiting = PortfolioPolicy.plan(snapshot)
    assert isinstance(waiting, SearchWait) and waiting.state == "awaiting_execution"
    assert waiting.evidence_ids == (first["id"],)


@pytest.mark.parametrize("pipeline", [portfolio_options()], indirect=True)
def test_missing_owner_pin_requires_review_without_stealing_the_selection(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    from dataclasses import replace

    from research_swarm.search.common import SearchWait
    from research_swarm.search.portfolio import PortfolioPolicy

    trials = admit_pair(pipeline, tmp_path)
    for trial in trials:
        evaluate(pipeline, trial)
    selected = driver(pipeline)
    assert selected.advance(max_steps=2).actions[-1].operation == "search.select_observation"
    snapshot = selected.portfolio_snapshot()
    waiting = PortfolioPolicy.plan(
        replace(snapshot, trials=(replace(snapshot.trials[0], pin=None), snapshot.trials[1]))
    )
    assert isinstance(waiting, SearchWait) and waiting.state == "needs_review"
    assert waiting.evidence_ids == (trials[0]["id"],)


@pytest.mark.parametrize(
    "pipeline",
    [{"limits": portfolio_options()["limits"], "source_learning_rate": 0.0}],
    indirect=True,
)
def test_greedy_planner_produces_the_original_command_preimages(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    from research_swarm.search.greedy import GreedyConfiguration
    from tests.test_proposals import baseline

    baseline(pipeline)
    greedy = SearchDriver(
        pipeline.client,
        pipeline.campaign["id"],
        GreedyConfiguration(proposer_id=new_id(), first_seed=19, instructions="Explore"),
    )
    identifier = "11111111-1111-4111-8111-111111111111"

    def original_key(action: Any, revision: int | None) -> str:
        assert isinstance(action, SearchAction)
        values = action.model_dump(mode="json")
        values["expected_revision"] = revision
        values["payload"] = {
            key: identifier if key.endswith("_id") else value
            for key, value in action.payload.items()
        }
        if "expected_control_epoch" in values["payload"]:
            values["payload"]["expected_control_epoch"] = 0
        return SearchAction.model_validate(values).command_key(identifier)

    assert (
        original_key(greedy.plan(), 0)
        == "sha256:d9ce14633409b0d415486fb9b1e4e2d786655de7a79110642de7f14e75239062"
    )
    assert greedy.advance().state == "awaiting_proposal"
    answer(pipeline, requests(pipeline)[0], tmp_path, 0.35)
    admitted = greedy.advance()
    trial = pipeline.client.get("trial", admitted.evidence_ids[0])
    evaluate(pipeline, trial)
    observation = greedy.plan()
    assert observation.expected_revision == pipeline.client.get("trial", trial["id"])["revision"]
    assert (
        original_key(observation, 2)
        == "sha256:19222e9ab9bb87f437fd6917b40054a3c9d64fc56b55b5ec66e9a01ca04da6c5"
    )
    assert greedy.advance(max_steps=3).state == "step_limit"
    assert (
        original_key(greedy.plan(), None)
        == "sha256:02a1030b5fac188e56fc226cc617f6897b01ca3964f66a435fa1595aa5a35f93"
    )
