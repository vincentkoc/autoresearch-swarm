"""Integrated acceptance cases extending existing unit and native contract proof."""

from pathlib import Path

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import SubmitTrial
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.runtime import LocalRuntime
from tests.control.test_campaign_completion import complete
from tests.control.test_evaluation_control import evaluated
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline


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
            ),
        }
    ],
    indirect=True,
)
def test_reopened_controller_preserves_spent_budget_for_direct_admission(
    pipeline: Pipeline,
):
    evaluated(pipeline)
    before = pipeline.client.budget(pipeline.campaign["id"])
    assert (
        next(row for row in before["accounts"] if row["dimension"] == "attempts")["consumed"] == "2"
    )
    reopened = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
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


def test_completed_campaign_retains_checkpoint_bytes_across_controller_reopen(
    pipeline: Pipeline, tmp_path: Path
):
    evaluated(pipeline)
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
