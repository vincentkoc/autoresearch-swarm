import re
from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.commands import CampaignTarget, CompleteCampaign, PauseCampaign
from research_swarm.control.evaluations import CompleteEvaluation
from research_swarm.control.models import StoppingRule, TrialData
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import ReportCampaign, literal
from research_swarm.control.results import PublishAttempt
from research_swarm.control.validity import EvidenceReference
from research_swarm.control.workers import AttemptData, ClaimJob
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.workers.journal import JournalState
from tests.test_evaluation_control import prepare_evaluation
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_promotions import assess, commit, evaluated


def complete(pipeline: Pipeline, key: str = "finish") -> dict[str, Any]:
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    return pipeline.client.command(
        "campaign.complete",
        CompleteCampaign(campaign_id=campaign["id"], reason="Declared fixture research finished"),
        idempotency_key=key,
        expected_revision=campaign["revision"],
    )


def test_completion_and_report_are_atomic_replayable_and_preserve_later_corrections(
    pipeline: Pipeline,
) -> None:
    receipt = evaluated(pipeline)
    commit(pipeline, assess(pipeline, receipt))
    before = pipeline.client.get("campaign", pipeline.campaign["id"])
    response = complete(pipeline)
    assert (
        pipeline.client.command(
            "campaign.complete",
            CompleteCampaign(campaign_id=before["id"], reason="Declared fixture research finished"),
            idempotency_key="finish",
            expected_revision=before["revision"],
        )
        == response
    )
    report = response["result"]["report"]
    campaign = response["result"]["campaign"]
    assert campaign["state"] == "completed" and campaign["data"]["final_report_id"] == report["id"]
    content = report["data"]
    assert content["report_digest"] == record_digest(
        "research.report", {key: value for key, value in content.items() if key != "report_digest"}
    )
    snapshot = content["snapshot"]
    assert snapshot["campaign_state"] == "completed"
    assert snapshot["campaign_revision"] == campaign["revision"]
    assert snapshot["stopping_reason"] == campaign["data"]["stopping_reason"]
    assert {item["state"] for item in snapshot["counts"] if item["kind"] == "campaign"} == {
        "completed"
    }
    assert (
        pipeline.client.events(after=snapshot["evidence_through"] - 1, limit=1)["events"][0]["type"]
        == "campaign.completed"
    )
    assert snapshot["incumbent"]["evidence_level"] == "observed"
    assert snapshot["recent_evaluations"][0]["eligible"]
    assert "No incumbent" not in content["markdown"]
    assert all(item["reserved"] == "0" and item["uncertain"] == "0" for item in snapshot["budget"])
    pipeline.client.command(
        "evidence.invalidate",
        InvalidateEvidence(
            target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
            reason="Correction after completion",
        ),
        idempotency_key="late-correction",
    )
    assert pipeline.client.get("report", report["id"]) == report
    current = pipeline.client.command(
        "report.create",
        ReportCampaign(campaign_id=campaign["id"], recent_limit=1),
        idempotency_key="current-report",
    )["result"]["data"]["snapshot"]
    assert not current["recent_evaluations"][0]["eligible"]
    assert current["incumbent"]["eligibility"] == "suspended"


def test_unfinished_execution_and_pending_evaluation_cannot_complete(pipeline: Pipeline) -> None:
    with pytest.raises(ResearchError) as queued:
        complete(pipeline, "queued-cannot-finish")
    assert queued.value.code == ErrorCode.INVALID_TRANSITION
    authority, evaluator, _ = prepare_evaluation(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker.execute(attempt["id"])
    with pytest.raises(ResearchError) as pending:
        complete(pipeline, "unscored-cannot-finish")
    assert pending.value.code == ErrorCode.EVALUATION_INCOMPLETE
    assert pipeline.client.list(RecordQuery(kind="report"))["items"] == []
    evaluator.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authority["id"], attempt_id=attempt["id"]),
        idempotency_key="score-to-finish",
    )
    assert complete(pipeline, "now-finished")["result"]["campaign"]["state"] == "completed"


def test_idle_pause_stops_admission_and_resume_keeps_queued_work(pipeline: Pipeline) -> None:
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    paused = pipeline.client.command(
        "campaign.pause",
        PauseCampaign(campaign_id=campaign["id"]),
        idempotency_key="pause",
        expected_revision=campaign["revision"],
    )["result"]
    assert paused["state"] == "paused"
    job = pipeline.ready()
    with pytest.raises(ResearchError) as denied:
        pipeline.worker.client.command(
            "worker.claim_job",
            ClaimJob(
                worker_id=pipeline.worker.worker.binding.worker_id,
                session_id=pipeline.worker.worker.binding.worker_session_id,
                job_id=job["id"],
            ),
            idempotency_key="paused-claim",
            expected_revision=job["revision"],
        )
    assert denied.value.code == ErrorCode.INVALID_TRANSITION
    resumed = pipeline.client.command(
        "campaign.start",
        CampaignTarget(campaign_id=campaign["id"]),
        idempotency_key="resume",
        expected_revision=paused["revision"],
    )["result"]
    assert resumed["state"] == "running"
    assert pipeline.ready() == job


def test_drain_pause_allows_active_stage_graph_to_close(pipeline: Pipeline) -> None:
    authority, evaluator, _ = prepare_evaluation(pipeline)
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    paused = pipeline.client.command(
        "campaign.pause",
        PauseCampaign(campaign_id=campaign["id"]),
        idempotency_key="drain",
        expected_revision=campaign["revision"],
    )["result"]
    assert paused["state"] == "pausing"
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker.execute(attempt["id"])
    assert pipeline.client.get("campaign", campaign["id"])["state"] == "paused"
    evaluator.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authority["id"], attempt_id=attempt["id"]),
        idempotency_key="evaluate-drained",
    )
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == "valid"
    )


@pytest.mark.parametrize(
    "pipeline",
    [{"stopping": StoppingRule(max_trials=2, permit_manual_completion=False)}],
    indirect=True,
)
def test_completion_respects_declared_stopping_rule(pipeline: Pipeline) -> None:
    evaluated(pipeline)
    with pytest.raises(ResearchError) as failure:
        complete(pipeline)
    assert failure.value.code == ErrorCode.INVALID_TRANSITION
    assert "stopping condition" in failure.value.message


@pytest.mark.parametrize(
    "pipeline",
    [{"stopping": StoppingRule(max_trials=2, target_metric=20.0, permit_manual_completion=False)}],
    indirect=True,
)
def test_selected_target_can_complete_before_trial_count(pipeline: Pipeline) -> None:
    commit(pipeline, assess(pipeline, evaluated(pipeline)))
    assert complete(pipeline)["result"]["campaign"]["state"] == "completed"


def test_unknown_execution_blocks_completion_without_refunding_budget(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    prepared = pipeline.worker._prepare(
        AttemptData.model_validate(attempt["data"]),
        TrialData.model_validate(pipeline.trial["data"]),
    )
    unknown = prepared.model_copy(update={"state": JournalState.UNKNOWN})
    path = tmp_path / "unknown.json"
    path.write_text(unknown.model_dump_json(), encoding="utf-8")
    artifact = pipeline.worker.client.upload_file(path, idempotency_key="uncertain-fixture")
    pipeline.worker.client.command(
        "worker.publish_attempt",
        PublishAttempt(
            attempt_id=attempt["id"],
            journal_artifact=artifact,
            finding="Native state is unavailable",
        ),
        idempotency_key="uncertain-state",
    )
    with pytest.raises(ResearchError) as failure:
        complete(pipeline, "uncertain-cannot-complete")
    assert failure.value.code == ErrorCode.EXECUTION_UNKNOWN
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert next(item for item in accounts if item["dimension"] == "attempts")["uncertain"] == "1"
    assert pipeline.client.list(RecordQuery(kind="report"))["items"] == []
    pipeline.worker.journal.request_cancel(attempt["id"], 1)
    assert pipeline.worker.execute(attempt["id"])["state"] == "not_started"


def test_completion_freezes_uncommitted_selection(pipeline: Pipeline) -> None:
    assessment = assess(pipeline, evaluated(pipeline))
    complete(pipeline)
    with pytest.raises(ResearchError) as failure:
        commit(pipeline, assessment)
    assert failure.value.code == ErrorCode.INVALID_TRANSITION
    assert pipeline.client.get("promotion", assessment["id"])["state"] == "assessed"
    assert (
        pipeline.client.get("campaign", pipeline.campaign["id"])["data"]["incumbent_candidate_id"]
        is None
    )


def test_report_failure_rolls_back_completion_and_its_event(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    evaluated(pipeline)
    before = pipeline.client.get("campaign", pipeline.campaign["id"])

    def failed(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("Fixture report persistence failure")

    monkeypatch.setattr("research_swarm.control.campaigns.create_report", failed)
    with pytest.raises(RuntimeError, match="report persistence"):
        complete(pipeline)
    assert pipeline.client.get("campaign", pipeline.campaign["id"]) == before
    assert pipeline.client.list(RecordQuery(kind="report"))["items"] == []


@pytest.mark.parametrize(
    "label",
    [
        "![label](https://example.com/image)",
        "[link](https://example.com)",
        "\\backslash `backtick` ``pair``",
        "first\r\n![second](image)",
        "<script>unsafe</script>",
    ],
)
def test_report_labels_remain_inside_a_single_commonmark_code_span(label: str) -> None:
    rendered = literal(label)
    opener = re.match(r"`+", rendered)
    assert opener is not None
    delimiter = opener.group()
    assert rendered.endswith(delimiter)
    interior = rendered[len(delimiter) : -len(delimiter)]
    assert delimiter not in interior and "\n" not in interior and "\r" not in interior
    assert interior.strip() == label.replace("\r", " ").replace("\n", " ").strip()
