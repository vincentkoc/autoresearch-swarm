"""Integrated acceptance cases extending existing unit and native contract proof."""

from pathlib import Path

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import SubmitTrial
from research_swarm.control.confirmation_models import ConfirmationData
from research_swarm.control.evaluations import AuthorizeEvaluation
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.orchestration.search import SearchDriver
from research_swarm.runtime import LocalRuntime
from research_swarm.search.greedy import GreedyConfiguration
from research_swarm.workers.journal import JournalState
from tests.control.test_campaign_completion import complete
from tests.control.test_confirmation import create_plan, exposure_budget, options, setup_plan
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.search.test_proposals import baseline


@pytest.mark.parametrize("pipeline", [{"source_network_policy": "denied"}], indirect=True)
def test_actual_claim_rejects_unavailable_isolation_without_reservation(pipeline: Pipeline):
    before = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError) as unavailable:
        pipeline.worker.claim(pipeline.ready())
    assert unavailable.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert pipeline.client.list(RecordQuery(kind="attempt"))["items"] == []
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    assert pipeline.worker.resources.availability() == {"cpu": 1}


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "limits": (
                BudgetLimit(dimension="attempts", limit=2, unit="count"),
                BudgetLimit(dimension="execution_ms", limit=50_000, unit="ms"),
                BudgetLimit(dimension="trial_admissions", limit=1, unit="count"),
                BudgetLimit(dimension="proposal_requests", limit=2, unit="count"),
            ),
        }
    ],
    indirect=True,
)
def test_reopened_controller_preserves_spent_budget_for_search_and_direct_admission(
    pipeline: Pipeline,
):
    baseline(pipeline)
    before = pipeline.client.budget(pipeline.campaign["id"])
    assert (
        next(row for row in before["accounts"] if row["dimension"] == "attempts")["consumed"] == "2"
    )
    reopened = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
    progress = SearchDriver(
        reopened, pipeline.campaign["id"], GreedyConfiguration(proposer_id=new_id())
    ).advance()
    assert progress.state == "budget_exhausted" and progress.actions == ()
    campaign = reopened.get("campaign", pipeline.campaign["id"])
    with pytest.raises(ResearchError) as spent:
        reopened.command(
            "trial.submit",
            SubmitTrial(
                campaign_id=campaign["id"],
                candidate_id=pipeline.trial["data"]["candidate_id"],
                validation_id=pipeline.trial["data"]["validation_id"],
                seed=99,
            ),
            idempotency_key="cannot-spend-again",
            expected_revision=campaign["revision"],
        )
    assert spent.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert reopened.budget(campaign["id"]) == before
    assert len(reopened.list(RecordQuery(kind="trial"))["items"]) == 1
    assert reopened.list(RecordQuery(kind="proposal_request"))["items"] == []


def test_completed_campaign_retains_checkpoint_bytes_across_controller_reopen(
    pipeline: Pipeline, tmp_path: Path
):
    baseline(pipeline)
    context = CommandContext(
        principal=pipeline.runtime.principal(), idempotency_key="storage-occupancy"
    )
    occupied = pipeline.runtime.service.artifacts.occupancy(context)
    assert occupied["retained_bytes"] > 0 and occupied["staging_reserved_bytes"] == 0
    complete(pipeline)
    assert pipeline.runtime.service.artifacts.occupancy(context) == occupied
    training = pipeline.client.get("job", pipeline.trial["data"]["job_ids"][0])
    artifact = training["data"]["outputs"]["model"]
    reopened_runtime = LocalRuntime.open(pipeline.runtime.root)
    reopened = ResearchClient(LocalTransport(reopened_runtime))
    assert reopened_runtime.service.artifacts.occupancy(context) == occupied
    path = tmp_path / "retained-checkpoint.json"
    downloaded = reopened.download(artifact["id"], path)
    assert downloaded.digest == artifact["digest"] and path.stat().st_size == artifact["size_bytes"]
    with pytest.raises(ResearchError, match="retained references"):
        reopened_runtime.service.artifacts.delete(
            CommandContext(
                principal=reopened_runtime.principal(),
                idempotency_key="completed-still-retained",
                expected_revision=1,
            ),
            artifact["id"],
            "Fixture lifecycle qualification",
        )
    assert reopened.get("campaign", pipeline.campaign["id"])["state"] == "completed"
    assert reopened.download(artifact["id"], tmp_path / "still-retained.json") == downloaded
    assert reopened_runtime.service.artifacts.occupancy(context) == occupied


@pytest.mark.parametrize(
    "pipeline", [{**options(), "source_scoring_delay_seed": 101}], indirect=True
)
def test_confirmation_timeout_after_input_access_cannot_refund_exposure(pipeline: Pipeline):
    intent, campaign = setup_plan(pipeline)
    plan = ConfirmationData.model_validate(create_plan(pipeline, intent, campaign)["data"]).plan
    binding = plan.bindings[0]
    trial = pipeline.client.get("trial", binding.trial_id)
    training = pipeline.worker.claim(pipeline.client.get("job", trial["data"]["job_ids"][0]))
    pipeline.worker.execute(training["id"])
    scoring = pipeline.client.get("job", binding.scoring_job_id)
    pipeline.client.command(
        "evaluation.authorize",
        AuthorizeEvaluation(
            job_id=scoring["id"],
            evaluator_id=intent.evaluator_id,
        ),
        idempotency_key="timed-confirmation",
        expected_revision=scoring["revision"],
    )
    attempt = pipeline.worker.claim(pipeline.client.get("job", scoring["id"]))
    assert exposure_budget(pipeline)["uncertain"] == "1"
    result = pipeline.worker.execute(attempt["id"])
    assert result["state"] == "failed"
    entry = pipeline.worker.journal.get(attempt["id"])
    assert entry.state == JournalState.CLOSED and entry.outcome.reason == "deadline"
    assert entry.outcome.closure.scope_closed
    markers = list(pipeline.worker.root.rglob("access.marker"))
    assert len(markers) == 1 and markers[0].read_text() == "101"
    before = exposure_budget(pipeline)
    assert before["consumed"] == "1" and before["uncertain"] == "0" and before["reserved"] == "9"
    reopened = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
    assert reopened.get("evaluation_exposure", binding.exposure_id)["state"] == "consumed"
    assert pipeline.worker.execute(attempt["id"]) == result
    assert pipeline.worker.journal.get(attempt["id"]) == entry
    after = next(
        item
        for item in reopened.budget(campaign["id"])["accounts"]
        if item["dimension"] == "evaluation_exposures"
    )
    assert after == before
    assert reopened.get("trial", binding.trial_id)["data"]["evaluation_status"] != "valid"
