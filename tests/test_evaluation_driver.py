from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import PauseCampaign, SubmitTrial
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.queries import RecordQuery
from research_swarm.control.search_models import RespondToProposal
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.orchestration.evaluation import EvaluationDriver
from research_swarm.orchestration.scoped import ScopedWorkerDriver
from research_swarm.orchestration.search import SearchDriver
from research_swarm.runtime import write_private
from research_swarm.search.greedy import GreedyConfiguration
from tests.test_cli_mcp import cli
from tests.test_confirmation import create_plan, options, setup_plan
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_proposals import PROPOSAL_LIMITS, proposer
from tests.test_scoped_driver import enrollment
from tests.test_search_admission import changed_source


def role(pipeline: Pipeline, *scopes: str) -> tuple[Principal, ResearchClient, str]:
    principal = Principal(
        id=new_id(),
        workspace_id=pipeline.runtime.identity.workspace_id,
        scopes=frozenset(scopes),
    )
    credential = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), principal)
    client = ResearchClient(
        LocalTransport(
            pipeline.runtime,
            lambda: pipeline.runtime.credentials.resolve(credential.token),
        )
    )
    return principal, client, credential.token


def trained_trials(pipeline: Pipeline, count: int) -> list[dict[str, Any]]:
    trials = [pipeline.trial]
    for seed in range(1, count):
        campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
        trials.append(
            pipeline.client.command(
                "trial.submit",
                SubmitTrial(
                    campaign_id=campaign["id"],
                    candidate_id=pipeline.trial["data"]["candidate_id"],
                    validation_id=pipeline.trial["data"]["validation_id"],
                    seed=seed,
                ),
                idempotency_key=f"replica-{seed}",
                expected_revision=campaign["revision"],
            )["result"]
        )
    for trial in trials:
        attempt = pipeline.worker.claim(pipeline.client.get("job", trial["data"]["job_ids"][0]))
        pipeline.worker.execute(attempt["id"])
    return trials


def execute_scoring(pipeline: Pipeline, trials: list[dict[str, Any]]) -> None:
    for trial in trials:
        attempt = pipeline.worker.claim(pipeline.client.get("job", trial["data"]["job_ids"][1]))
        pipeline.worker.execute(attempt["id"])


def test_missing_campaign_is_not_reported_as_an_idle_scan(pipeline: Pipeline) -> None:
    result = EvaluationDriver(
        pipeline.client,
        pipeline.client,
        new_id(),
        pipeline.runtime.principal().id,
    ).advance()
    assert result.state == "needs_review" and result.error_code == ErrorCode.NOT_FOUND
    assert result.actions == ()


@pytest.mark.parametrize(
    "pipeline",
    [{"limits": PROPOSAL_LIMITS, "source_learning_rate": 0.0}],
    indirect=True,
)
async def test_http_control_and_distinct_evaluator_complete_an_independent_research_cycle(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    _, _, controller_token = role(pipeline, "read", "evaluation.request", "artifact.upload")
    evaluator, _, evaluator_token = role(pipeline, "read", "evaluation.publish")
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, controller_token) as controller,
        ResearchClient.connect(url, evaluator_token) as publisher,
        enrollment(pipeline, tmp_path / "independent-worker", url) as connected,
    ):
        driver = EvaluationDriver(controller, publisher, pipeline.campaign["id"], evaluator.id)
        assert (await driver.advance_async()).state == "idle"
        assert (await ScopedWorkerDriver(connected).run(once=True)).observation_count == 1
        authorized = await driver.advance_async()
        assert [item.operation for item in authorized.actions] == ["evaluation.authorize"]
        request = controller.get("evaluation_request", authorized.actions[0].record_id)
        assert request["data"]["request"]["evaluator_id"] == evaluator.id
        assert (await ScopedWorkerDriver(connected).run(once=True)).observation_count == 1
        completed = EvaluationDriver(
            controller, publisher, pipeline.campaign["id"], evaluator.id
        ).advance()
        assert [item.operation for item in completed.actions] == ["evaluation.complete"]
        assert controller.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == "valid"
        assert driver.advance().state == "idle"
        assert len(controller.list(RecordQuery(kind="attempt"))["items"]) == 2
        assert len(controller.list(RecordQuery(kind="evaluation_receipt"))["items"]) == 1
        assert (
            controller.get("campaign", pipeline.campaign["id"])["data"]["incumbent_candidate_id"]
            is None
        )
        agent_identity, agent, _ = proposer(pipeline)
        operator_token = pipeline.runtime.credentials.issue(
            pipeline.runtime.principal(),
            pipeline.runtime.principal(),
        ).token
        with ResearchClient.connect(url, operator_token) as operator:
            search = SearchDriver(
                operator,
                pipeline.campaign["id"],
                GreedyConfiguration(
                    proposer_id=agent_identity.id,
                    baseline_trial_id=pipeline.trial["id"],
                ),
            )
            request = search.advance()
            assert request.state == "awaiting_proposal"
            agent.command(
                "search.respond",
                RespondToProposal(
                    request_id=request.evidence_ids[0],
                    outcome="candidate",
                    source_artifact=changed_source(pipeline, tmp_path),
                    hypothesis="Train a nonzero optimizer with independent evaluation control",
                ),
                idempotency_key="independent-cycle-response",
            )
            assert search.advance().state == "awaiting_execution"
            assert (await ScopedWorkerDriver(connected).run(once=True)).observation_count == 1
            assert len(driver.advance().actions) == 1
            assert (await ScopedWorkerDriver(connected).run(once=True)).observation_count == 1
            assert len(driver.advance().actions) == 1
            progressed = search.advance()
            assert progressed.state == "awaiting_proposal"
            assert (
                operator.get("campaign", pipeline.campaign["id"])["data"]["incumbent_revision"] == 2
            )
            assert len(operator.list(RecordQuery(kind="attempt"))["items"]) == 4
            assert len(operator.list(RecordQuery(kind="observation_consumption"))["items"]) == 1


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "search": SearchConfiguration(
                policy="external",
                version="external-v1",
                max_parallel_trials=3,
            )
        }
    ],
    indirect=True,
)
@pytest.mark.parametrize("operation", ["evaluation.authorize", "evaluation.complete"])
def test_lost_reply_recovers_original_effect_and_unprocessed_page_tail(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    trials = trained_trials(pipeline, 3)
    _, controller, _ = role(pipeline, "read", "evaluation.request", "artifact.upload")
    evaluator, publisher, _ = role(pipeline, "read", "evaluation.publish")
    driver = EvaluationDriver(controller, publisher, pipeline.campaign["id"], evaluator.id)
    if operation == "evaluation.complete":
        assert len(driver.advance().actions) == 3
        execute_scoring(pipeline, trials)
    target = controller if operation == "evaluation.authorize" else publisher
    original, calls = target.command, 0

    def lose_second(name: str, *args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        result = original(name, *args, **kwargs)
        calls += 1
        if calls == 2:
            raise ResearchError(ErrorCode.DELIVERY_UNCERTAIN, "Synthetic lost accepted response")
        return result

    monkeypatch.setattr(target, "command", lose_second)
    interrupted = driver.advance()
    assert interrupted.state == "delivery_uncertain" and len(interrupted.actions) == 1
    resumed = EvaluationDriver(
        controller, publisher, pipeline.campaign["id"], evaluator.id
    ).advance(
        continuation=interrupted.continuation,
    )
    assert resumed.state == "advanced" and len(resumed.actions) == 1
    kind = "evaluation_request" if operation == "evaluation.authorize" else "evaluation_receipt"
    assert len(controller.list(RecordQuery(kind=kind))["items"]) == 3
    assert driver.advance().actions == ()


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "search": SearchConfiguration(
                policy="external",
                version="external-v1",
                max_parallel_trials=3,
            )
        }
    ],
    indirect=True,
)
def test_bounded_cursor_passes_waiting_requests_without_starving_finished_trials(
    pipeline: Pipeline,
) -> None:
    trials = trained_trials(pipeline, 3)
    evaluator, publisher, _ = role(pipeline, "read", "evaluation.publish")
    driver = EvaluationDriver(pipeline.client, publisher, pipeline.campaign["id"], evaluator.id)
    cursor, actions = None, []
    for _ in range(5):
        result = driver.advance(continuation=cursor, max_pages=1, page_size=1)
        assert result.examined_count <= 1
        actions.extend(result.actions)
        cursor = result.continuation
        if cursor is None:
            break
    assert len(actions) == 3 and cursor is None
    execute_scoring(pipeline, trials[1:])
    waiting = driver.advance(max_pages=1, page_size=1)
    assert waiting.actions == () and waiting.continuation is not None
    for _ in range(5):
        advanced = driver.advance(continuation=waiting.continuation, max_pages=1, page_size=1)
        waiting = advanced
        if advanced.continuation is None:
            break
    assert pipeline.client.get("trial", trials[0]["id"])["data"]["evaluation_status"] == "pending"
    assert all(
        pipeline.client.get("trial", trial["id"])["data"]["evaluation_status"] == "valid"
        for trial in trials[1:]
    )
    wrong = driver.initial.model_copy(update={"evaluator_id": new_id()})
    with pytest.raises(ResearchError) as denied:
        driver.advance(continuation=wrong)
    assert denied.value.code == ErrorCode.PROJECT_CONTRACT_INVALID


def test_distinct_controllers_race_one_authority_and_wrong_publisher_cannot_score(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trials = trained_trials(pipeline, 1)
    controllers = [
        role(pipeline, "read", "evaluation.request", "artifact.upload")[1] for _ in range(2)
    ]
    evaluator, publisher, _ = role(pipeline, "read", "evaluation.publish")
    barrier = threading.Barrier(2)

    def synchronize(original: Any) -> Any:
        def call(*args: Any, **kwargs: Any) -> Any:
            barrier.wait(timeout=10)
            return original(*args, **kwargs)

        return call

    with monkeypatch.context() as patch, ThreadPoolExecutor(max_workers=2) as pool:
        for client in controllers:
            patch.setattr(client, "command", synchronize(client.command))
        futures = [
            pool.submit(
                EvaluationDriver(
                    client,
                    publisher,
                    pipeline.campaign["id"],
                    evaluator.id,
                ).advance
            )
            for client in controllers
        ]
        results = [future.result(timeout=20) for future in futures]
    assert sum(len(result.actions) for result in results) == 1
    assert [finding.code for result in results for finding in result.findings] == [
        ErrorCode.REVISION_CONFLICT
    ]
    assert len(pipeline.client.list(RecordQuery(kind="evaluation_request"))["items"]) == 1
    execute_scoring(pipeline, trials)
    denied = EvaluationDriver(
        controllers[0], controllers[0], pipeline.campaign["id"], evaluator.id
    ).advance()
    assert (
        denied.state == "needs_review" and denied.findings[0].code == ErrorCode.AUTHORIZATION_DENIED
    )
    assert pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"] == []
    accepted = EvaluationDriver(
        controllers[0], publisher, pipeline.campaign["id"], evaluator.id
    ).advance()
    assert len(accepted.actions) == 1


def test_cli_distinct_evaluator_drains_a_pausing_trial_and_completes_after_pause(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    trials = trained_trials(pipeline, 1)
    evaluator, _, token = role(pipeline, "read", "evaluation.publish")
    credential = tmp_path / "evaluator-token"
    write_private(credential, token.encode("ascii"))
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "campaign.pause",
        PauseCampaign(campaign_id=campaign["id"]),
        idempotency_key="evaluation-drain",
        expected_revision=campaign["revision"],
    )
    arguments = (
        "evaluation",
        "advance",
        campaign["id"],
        "--evaluator",
        evaluator.id,
        "--evaluator-token-file",
        str(credential),
    )
    authorized = cli(pipeline.runtime.root, *arguments)
    assert authorized["actions"][0]["operation"] == "evaluation.authorize"
    execute_scoring(pipeline, trials)
    assert pipeline.client.get("campaign", campaign["id"])["state"] == "paused"
    completed = cli(pipeline.runtime.root, *arguments)
    assert completed["actions"][0]["operation"] == "evaluation.complete"
    assert pipeline.client.get("trial", trials[0]["id"])["data"]["evaluation_status"] == "valid"


@pytest.mark.parametrize("pipeline", [options()], indirect=True)
def test_confirmation_authority_keeps_sealed_evaluator_and_consumes_original_exposure(
    pipeline: Pipeline,
) -> None:
    from research_swarm.control.confirmation_models import ConfirmationData

    intent, campaign = setup_plan(pipeline)
    planned = create_plan(pipeline, intent, campaign)
    plan = ConfirmationData.model_validate(planned["data"]).plan
    binding = plan.bindings[0]
    trial = pipeline.client.get("trial", binding.trial_id)
    attempt = pipeline.worker.claim(pipeline.client.get("job", trial["data"]["job_ids"][0]))
    pipeline.worker.execute(attempt["id"])
    foreign, publisher, _ = role(pipeline, "read", "evaluation.publish")
    refused = EvaluationDriver(pipeline.client, publisher, campaign["id"], foreign.id).advance()
    assert refused.state == "needs_review" and refused.actions == ()
    assert refused.findings[0].code == ErrorCode.AUTHORIZATION_DENIED
    driver = EvaluationDriver(pipeline.client, pipeline.client, campaign["id"], intent.evaluator_id)
    authorized = driver.advance()
    assert len(authorized.actions) == 1
    request = pipeline.client.get("evaluation_request", authorized.actions[0].record_id)
    assert request["data"]["request"]["exposure_class"] == "confirmation"
    assert request["data"]["request"]["exposure_authorization_id"] == binding.exposure_id
    execute_scoring(pipeline, [trial])
    completed = driver.advance()
    assert len(completed.actions) == 1
    assert pipeline.client.get("evaluation_exposure", binding.exposure_id)["state"] == "consumed"
    assert pipeline.client.get("confirmation_plan", plan.id)["data"]["decision_id"] is None
    assert len(pipeline.client.list(RecordQuery(kind="promotion"))["items"]) == 1


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "search": SearchConfiguration(
                policy="external",
                version="external-v1",
                max_parallel_trials=3,
            )
        }
    ],
    indirect=True,
)
def test_credential_failure_stops_the_route_without_fallback_or_skipping_the_page(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trained_trials(pipeline, 3)
    _, restricted, _ = role(pipeline, "read")
    evaluator, publisher, _ = role(pipeline, "read", "evaluation.publish")
    original, calls = restricted.command, []

    def counted(operation: str, *args: Any, **kwargs: Any) -> Any:
        calls.append(operation)
        return original(operation, *args, **kwargs)

    monkeypatch.setattr(restricted, "command", counted)
    blocked = EvaluationDriver(
        restricted, publisher, pipeline.campaign["id"], evaluator.id
    ).advance()
    assert blocked.error_code == ErrorCode.AUTHORIZATION_DENIED
    assert calls == ["evaluation.authorize"] and blocked.continuation is not None
    assert pipeline.client.list(RecordQuery(kind="evaluation_request"))["items"] == []
    admitted = EvaluationDriver(
        pipeline.client, publisher, pipeline.campaign["id"], evaluator.id
    ).advance(
        continuation=blocked.continuation,
    )
    assert len(admitted.actions) == 3


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "search": SearchConfiguration(
                policy="external",
                version="external-v1",
                max_parallel_trials=3,
            )
        }
    ],
    indirect=True,
)
def test_failed_scoring_is_retained_while_another_trial_gets_its_receipt(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trials = trained_trials(pipeline, 2)
    evaluator, publisher, _ = role(pipeline, "read", "evaluation.publish")
    driver = EvaluationDriver(pipeline.client, publisher, pipeline.campaign["id"], evaluator.id)
    assert len(driver.advance().actions) == 2

    def broken(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("Synthetic invalid scoring report")

    with monkeypatch.context() as patch:
        patch.setattr("research_swarm.workers.local.read_report", broken)
        execute_scoring(pipeline, trials[:1])
    execute_scoring(pipeline, trials[1:])
    budget = pipeline.client.budget(pipeline.campaign["id"])
    result = driver.advance()
    assert result.state == "needs_review" and len(result.actions) == 1
    assert result.findings[0].code == ErrorCode.EVALUATION_INCOMPLETE
    assert len(pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"]) == 1
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 4
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget


@pytest.mark.parametrize("pipeline", [True], indirect=True)
def test_unknown_sibling_stage_does_not_authorize_a_ready_scoring_job(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from research_swarm.control.results import PublishAttempt
    from research_swarm.workers.journal import JournalState

    trial = trained_trials(pipeline, 1)[0]
    jobs = [pipeline.client.get("job", identifier) for identifier in trial["data"]["job_ids"]]
    sibling = next(job for job in jobs if job["data"]["stage"]["id"] == "probe")
    scoring = next(job for job in jobs if job["data"]["stage"]["kind"] == "evaluate")
    assert scoring["state"] == "ready"
    attempt = pipeline.worker.claim(sibling)
    original = pipeline.worker.client.command

    def unavailable(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "worker.publish_attempt":
            raise RuntimeError("Synthetic unavailable closure reply")
        return original(name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(pipeline.worker.client, "command", unavailable)
        with pytest.raises(RuntimeError):
            pipeline.worker.execute(attempt["id"])
    closed = pipeline.worker.journal.get(attempt["id"])
    unknown = closed.model_copy(update={"state": JournalState.UNKNOWN, "outcome": None})
    path = tmp_path / "unknown.json"
    path.write_text(unknown.model_dump_json(), encoding="utf-8")
    artifact = pipeline.worker.client.upload_file(path, idempotency_key="unknown-stage")
    original(
        "worker.publish_attempt",
        PublishAttempt(
            attempt_id=attempt["id"],
            journal_artifact=artifact,
            finding="Closure unproven",
        ),
        idempotency_key="unknown-stage-evidence",
    )
    evaluator, publisher, _ = role(pipeline, "read", "evaluation.publish")
    driver = EvaluationDriver(pipeline.client, publisher, pipeline.campaign["id"], evaluator.id)
    before = pipeline.client.budget(pipeline.campaign["id"])
    assert driver.advance().actions == ()
    assert pipeline.client.get("job", scoring["id"])["data"]["evaluation_request_id"] is None
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
