from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.confirmation_models import ConfirmationData
from research_swarm.control.evaluations import AuthorizeEvaluation
from research_swarm.workers.journal import JournalEntry, JournalState, WorkerJournal
from tests.control.test_confirmation import create_plan, exposure_budget, options, setup_plan
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.workers.test_timed_execution import native_worker


@pytest.mark.parametrize("pipeline", [options()], indirect=True)
@pytest.mark.parametrize("withhold_closure", [False, True])
def test_unreleased_scoring_requires_closure_before_refunding_exposure(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, withhold_closure: bool
) -> None:
    intent, campaign = setup_plan(pipeline)
    binding = ConfirmationData.model_validate(
        create_plan(pipeline, intent, campaign)["data"]
    ).plan.bindings[0]
    trial = pipeline.client.get("trial", binding.trial_id)
    train = pipeline.worker.claim(pipeline.client.get("job", trial["data"]["job_ids"][0]))
    assert pipeline.worker.execute(train["id"])["state"] == "succeeded"
    scoring = pipeline.client.get("job", binding.scoring_job_id)
    pipeline.client.command(
        "evaluation.authorize",
        AuthorizeEvaluation(job_id=scoring["id"], evaluator_id=intent.evaluator_id),
        idempotency_key="held-scoring-authority",
        expected_revision=scoring["revision"],
    )
    worker, _ = native_worker(pipeline, tmp_path)
    attempt = worker.claim(pipeline.client.get("job", scoring["id"]))
    before_attempts = next(
        row
        for row in pipeline.client.budget(campaign["id"])["accounts"]
        if row["dimension"] == "attempts"
    )
    assert exposure_budget(pipeline)["uncertain"] == "1"
    authorize = WorkerJournal.authorize_timed_release

    def lost(self: WorkerJournal, *args: Any) -> Any:
        authorize(self, *args)
        raise OSError("Synthetic lost release reply")

    publish = worker._publish
    captured: list[JournalEntry] = []

    def partial(entry: JournalEntry) -> dict[str, Any]:
        assert entry.outcome is not None
        captured.append(entry)
        unknown = entry.model_copy(
            update={
                "state": JournalState.UNKNOWN,
                "outcome": entry.outcome.model_copy(
                    update={
                        "closure": entry.outcome.closure.model_copy(update={"scope_closed": False}),
                        "exit_code": None,
                    }
                ),
            }
        )
        return publish(unknown)

    with monkeypatch.context() as patch:
        patch.setattr(WorkerJournal, "authorize_timed_release", lost)
        if withhold_closure:
            patch.setattr(worker, "_publish", partial)
        result = worker.execute(attempt["id"])
    entry = worker.journal.get(attempt["id"])
    assert entry.outcome is not None and entry.outcome.released_at is None
    assert entry.outcome.closure.scope_closed and entry.outcome.reason == "supervisor_error"
    assert "OSError" in entry.finding
    if withhold_closure:
        budget = exposure_budget(pipeline)
        assert budget["consumed"] == "0" and budget["uncertain"] == "1"
        uncertain = next(
            row
            for row in pipeline.client.budget(campaign["id"])["accounts"]
            if row["dimension"] == "attempts"
        )
        assert uncertain["consumed"] == before_attempts["consumed"]
        assert uncertain["uncertain"] == "1"
        result = publish(captured[0])
    assert result["state"] == "not_started"
    budget = exposure_budget(pipeline)
    assert budget["consumed"] == budget["uncertain"] == "0"
    assert budget["released_total"] == "1"
    assert pipeline.client.get("evaluation_exposure", binding.exposure_id)["state"] == "released"
    attempts = next(
        row
        for row in pipeline.client.budget(campaign["id"])["accounts"]
        if row["dimension"] == "attempts"
    )
    assert attempts["consumed"] == before_attempts["consumed"]
    assert attempts["uncertain"] == "0"
    assert worker.execute(attempt["id"]) == result
