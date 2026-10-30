from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from research_swarm import ResearchClient
from research_swarm.client import LocalTransport
from research_swarm.control.commands import (
    CampaignTarget,
    CreateCampaign,
    ProposeCandidate,
    SubmitTrial,
    ValidateCandidate,
)
from research_swarm.control.models import TrialData
from research_swarm.control.queries import RecordQuery
from research_swarm.control.results import PublishAttempt
from research_swarm.control.workers import AttemptData, EngineCapabilities, EnrollWorker, WorkerData
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime, read_private
from research_swarm.workers.journal import JournalState
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.supervisor import run_attempt
from tests.support import LIMITS


@dataclass
class Pipeline:
    client: ResearchClient
    runtime: LocalRuntime
    worker: LocalWorker
    campaign: dict[str, Any]
    trial: dict[str, Any]

    def ready(self) -> dict[str, Any]:
        return self.client.list(
            RecordQuery(kind="job", campaign_id=self.campaign["id"], state="ready")
        )["items"][0]


@pytest.fixture
def pipeline(tmp_path: Path, request: pytest.FixtureRequest) -> Pipeline:
    runtime = LocalRuntime.open(tmp_path / "runtime")
    client = ResearchClient(LocalTransport(runtime))
    source = Path(__file__).resolve().parents[1] / "examples" / "cpu_bigram"
    options = getattr(request, "param", False)
    parallel_stage = options is True
    campaign_options = {"limits": LIMITS, **(options if isinstance(options, dict) else {})}
    if parallel_stage:
        copied = tmp_path / "parallel-source"
        shutil.copytree(source, copied)
        source = copied
        manifest = source / "research.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8") + '\n[[stages]]\nid = "probe"\nkind = "smoke"\n'
            'argv = ["python", "-c", "raise SystemExit(1)"]\ntimeout_ms = 1000\n',
            encoding="utf-8",
        )
    registered = client.register_project(source, idempotency_key="source")["result"]
    campaign = client.command(
        "campaign.create",
        CreateCampaign(
            project_id=registered["project"]["id"],
            name="Local pipeline",
            primary_metric="val_bpb",
            **campaign_options,
        ),
        idempotency_key="campaign",
    )["result"]
    for operation in ("campaign.seal", "campaign.start"):
        campaign = client.command(
            operation,
            CampaignTarget(campaign_id=campaign["id"]),
            idempotency_key=operation,
            expected_revision=campaign["revision"],
        )["result"]
    candidate = client.command(
        "candidate.propose",
        ProposeCandidate(
            campaign_id=campaign["id"],
            source_artifact=registered["project_revision"]["data"]["source_artifact"],
            hypothesis="Execute the immutable baseline",
        ),
        idempotency_key="candidate",
        expected_revision=campaign["revision"],
    )["result"]
    validation = client.command(
        "candidate.validate",
        ValidateCandidate(candidate_id=candidate["id"]),
        idempotency_key="validation",
        expected_revision=candidate["revision"],
    )["result"]
    submitted = client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=candidate["id"],
            validation_id=validation["id"],
            seed=7,
        ),
        idempotency_key="trial",
        expected_revision=campaign["revision"],
    )["result"]
    worker_principal = Principal(
        id=new_id(),
        workspace_id=runtime.identity.workspace_id,
        scopes=frozenset({"read", "artifact.upload", "worker.claim", "worker.publish"}),
    )
    credential = runtime.credentials.issue(runtime.principal(), worker_principal)
    worker_client = ResearchClient(
        LocalTransport(runtime, lambda: runtime.credentials.resolve(credential.token))
    )
    enrolled = client.command(
        "worker.enroll",
        EnrollWorker(
            worker_id=new_id(),
            session_id=new_id(),
            principal_id=worker_principal.id,
            name="Pipeline fixture",
            max_concurrency=2 if parallel_stage else 1,
            profiles=(
                EngineCapabilities(
                    name="python-cpu",
                    backend="python",
                    cpu_slots=2 if parallel_stage else 1,
                    execution_fingerprint=content_digest(b"pipeline-python-profile"),
                ),
            ),
        ),
        idempotency_key="worker",
    )["result"]
    worker = LocalWorker(
        worker_client,
        WorkerData.model_validate(enrolled["data"]),
        key=read_private(runtime.root, "dispatch.key", limit=32),
        root=tmp_path / "worker",
        machine_registry=tmp_path / "machine" / "resources.sqlite",
    )
    return Pipeline(client, runtime, worker, campaign, submitted)


def test_registered_local_worker_executes_stage_graph_and_settles_each_attempt(
    pipeline: Pipeline,
) -> None:
    completed = []
    for _ in range(2):
        job = pipeline.ready()
        attempt = pipeline.worker.claim(job)
        assert pipeline.worker.claim(job) == attempt
        result = pipeline.worker.execute(attempt["id"])
        assert result["state"] == "succeeded"
        assert pipeline.worker.execute(attempt["id"]) == result
        completed.append(result)
    trial = pipeline.client.get("trial", pipeline.trial["id"])
    assert trial["state"] == "succeeded"
    assert trial["data"]["execution_outcome"] == "succeeded"
    assert trial["data"]["resource_status"] == "closed"
    # A native report is not an authorized evaluator receipt or a promotion.
    assert trial["data"]["evaluation_status"] == "pending"
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert all(account["reserved"] == "0" and account["uncertain"] == "0" for account in accounts)
    assert (
        next(account for account in accounts if account["dimension"] == "attempts")["consumed"]
        == "2"
    )
    assert pipeline.worker.resources.availability() == {"cpu": 1}
    evaluated = pipeline.client.get("run_result", completed[1]["id"])
    assert evaluated["data"]["measurements"][0]["name"] == "val_bpb"
    assert evaluated["data"]["measurements"][0]["value"] > 0
    for result in completed:
        entry = pipeline.worker.journal.get(result["id"])
        assert entry.state == JournalState.CLOSED and entry.outcome is not None
        assert entry.outcome.closure.scope_closed


def test_delivery_restart_recovers_prepared_identity_without_reexecution(
    pipeline: Pipeline,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    result = pipeline.worker.execute(attempt["id"])
    previous = pipeline.worker.journal.get(attempt["id"])
    restarted = LocalWorker(
        pipeline.worker.client,
        pipeline.worker.worker,
        key=read_private(pipeline.runtime.root, "dispatch.key", limit=32),
        root=pipeline.worker.root,
        machine_registry=pipeline.worker.resources.path,
    )
    assert restarted.execute(attempt["id"]) == result
    assert restarted.journal.get(attempt["id"]) == previous


def test_publication_failure_preserves_closure_and_retries_without_execution(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    original = pipeline.worker.client.command

    def interrupted(operation: str, *args: Any, **kwargs: Any) -> Any:
        if operation == "worker.publish_attempt":
            raise RuntimeError("Synthetic controller disconnect")
        return original(operation, *args, **kwargs)

    monkeypatch.setattr(pipeline.worker.client, "command", interrupted)
    with pytest.raises(RuntimeError, match="disconnect"):
        pipeline.worker.execute(attempt["id"])
    closed = pipeline.worker.journal.get(attempt["id"])
    assert closed.state == JournalState.CLOSED
    assert pipeline.worker.resources.availability() == {"cpu": 1}
    monkeypatch.setattr(pipeline.worker.client, "command", original)
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    assert pipeline.worker.journal.get(attempt["id"]) == closed
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert next(item for item in accounts if item["dimension"] == "attempts")["consumed"] == "1"


def test_unknown_observation_keeps_budget_until_late_original_closure(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    original = pipeline.worker.client.command

    def interrupted(operation: str, *args: Any, **kwargs: Any) -> Any:
        if operation == "worker.publish_attempt":
            raise RuntimeError("Synthetic unpublished closure")
        return original(operation, *args, **kwargs)

    monkeypatch.setattr(pipeline.worker.client, "command", interrupted)
    with pytest.raises(RuntimeError):
        pipeline.worker.execute(attempt["id"])
    closed = pipeline.worker.journal.get(attempt["id"])
    monkeypatch.setattr(pipeline.worker.client, "command", original)
    unknown = closed.model_copy(update={"state": JournalState.UNKNOWN, "outcome": None})
    file = tmp_path / "uncertain-observation.json"
    file.write_text(unknown.model_dump_json(), encoding="utf-8")
    reference = pipeline.worker.client.upload_file(file, idempotency_key="uncertain-observation")
    receipt = original(
        "worker.publish_attempt",
        PublishAttempt(
            attempt_id=attempt["id"],
            journal_artifact=reference,
            finding="Native receipt unavailable",
        ),
        idempotency_key="uncertain-publication",
    )
    assert receipt["result"]["state"] == "execution_unknown"
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    count = next(item for item in accounts if item["dimension"] == "attempts")
    assert count["uncertain"] == "1" and count["consumed"] == "0"
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    assert pipeline.worker.journal.get(attempt["id"]) == closed
    count = next(
        item
        for item in pipeline.client.budget(pipeline.campaign["id"])["accounts"]
        if item["dimension"] == "attempts"
    )
    assert count["uncertain"] == "0" and count["consumed"] == "1"


def test_cancelled_unstarted_pipeline_has_no_fake_exit_or_measurement(pipeline: Pipeline) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker._prepare(
        AttemptData.model_validate(attempt["data"]),
        TrialData.model_validate(pipeline.trial["data"]),
    )
    pipeline.worker.journal.request_cancel(attempt["id"], 1)
    result = pipeline.worker.execute(attempt["id"])
    assert result["state"] == "not_started"
    assert pipeline.worker.resources.availability() == {"cpu": 1}
    with pytest.raises(ResearchError) as missing:
        pipeline.client.get("run_result", attempt["id"])
    assert missing.value.code == ErrorCode.NOT_FOUND
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert all(item["consumed"] == "0" and item["reserved"] == "0" for item in accounts)


def test_invalid_report_records_failure_and_blocks_dependent_evaluation(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def invalid(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("Synthetic missing report")

    monkeypatch.setattr("research_swarm.workers.local.read_report", invalid)
    attempt = pipeline.worker.claim(pipeline.ready())
    result = pipeline.worker.execute(attempt["id"])
    assert result["state"] == "failed"
    execution = pipeline.client.get("run_result", attempt["id"])["data"]
    assert execution["exit_code"] == 0 and execution["status"] == "failed"
    assert execution["measurements"] == []
    assert pipeline.client.get("trial", pipeline.trial["id"])["state"] == "failed"
    assert pipeline.client.list(RecordQuery(kind="job", state="ready"))["items"] == []


def test_materialization_failure_settles_an_authenticated_unstarted_attempt(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_source(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("Synthetic extraction failure after native reservation")

    monkeypatch.setattr("research_swarm.workers.local.extract_bundle", broken_source)
    attempt = pipeline.worker.claim(pipeline.ready())
    result = pipeline.worker.execute(attempt["id"])
    assert result["state"] == "not_started"
    entry = pipeline.worker.journal.get(attempt["id"])
    assert entry.preparation_required and entry.prepared is None and entry.native is None
    assert pipeline.worker.resources.availability() == {"cpu": 1}
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert all(item["consumed"] == "0" and item["reserved"] == "0" for item in accounts)


def test_expired_grant_settles_before_materialization_or_native_reservation(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    monkeypatch.setattr(
        "research_swarm.workers.journal.utc_now", lambda: "2999-01-01T00:00:00.000Z"
    )
    result = pipeline.worker.execute(attempt["id"])
    assert result["state"] == "not_started"
    entry = pipeline.worker.journal.get(attempt["id"])
    assert entry.prepared is None and entry.native is None
    assert pipeline.worker.resources.availability() == {"cpu": 1}


def test_output_publication_io_failure_is_retryable_without_a_new_execution(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = pipeline.worker.client.upload_file

    def interrupted(path: Path, *, idempotency_key: str, **kwargs: Any) -> Any:
        if idempotency_key.endswith(":report"):
            raise OSError("Synthetic publication storage failure")
        return original(path, idempotency_key=idempotency_key, **kwargs)

    attempt = pipeline.worker.claim(pipeline.ready())
    monkeypatch.setattr(pipeline.worker.client, "upload_file", interrupted)
    with pytest.raises(OSError, match="storage failure"):
        pipeline.worker.execute(attempt["id"])
    assert pipeline.client.get("attempt", attempt["id"])["state"] == "reserved"
    entry = pipeline.worker.journal.get(attempt["id"])
    monkeypatch.setattr(pipeline.worker.client, "upload_file", original)
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    assert pipeline.worker.journal.get(attempt["id"]) == entry


def test_held_but_unreleased_process_does_not_consume_a_launched_attempt(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker._prepare(
        AttemptData.model_validate(attempt["data"]),
        TrialData.model_validate(pipeline.trial["data"]),
    )

    def refuse(attempt_id: str, supervisor_id: str) -> bool:
        pipeline.worker.journal.request_cancel(attempt_id, 1)
        return False

    monkeypatch.setattr(pipeline.worker.journal, "authorize_release", refuse)
    held = run_attempt(pipeline.worker.journal, attempt["id"])
    assert held.state == JournalState.NOT_STARTED and held.outcome is not None
    assert held.outcome.released_at is None and held.native is not None
    assert pipeline.worker.execute(attempt["id"])["state"] == "not_started"
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert next(item for item in accounts if item["dimension"] == "attempts")["consumed"] == "0"
    assert (
        int(next(item for item in accounts if item["dimension"] == "execution_ms")["consumed"])
        == held.outcome.elapsed_ms
    )
    with pytest.raises(ResearchError) as absent:
        pipeline.client.get("run_result", attempt["id"])
    assert absent.value.code == ErrorCode.NOT_FOUND


@pytest.mark.parametrize("pipeline", [True], indirect=True)
def test_unknown_parallel_stage_keeps_trial_admission_slot_after_sibling_finishes(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    probe = pipeline.worker.claim(pipeline.ready())
    original = pipeline.worker.client.command

    def disconnected(operation: str, *args: Any, **kwargs: Any) -> Any:
        if operation == "worker.publish_attempt":
            raise RuntimeError("Synthetic unpublished closure")
        return original(operation, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(pipeline.worker.client, "command", disconnected)
        with pytest.raises(RuntimeError):
            pipeline.worker.execute(attempt["id"])
    entry = pipeline.worker.journal.get(attempt["id"])
    unknown = entry.model_copy(update={"state": JournalState.UNKNOWN, "outcome": None})
    file = tmp_path / "parallel-unknown.json"
    file.write_text(unknown.model_dump_json(), encoding="utf-8")
    reference = pipeline.worker.client.upload_file(file, idempotency_key="parallel-unknown")
    original(
        "worker.publish_attempt",
        PublishAttempt(
            attempt_id=attempt["id"],
            journal_artifact=reference,
        ),
        idempotency_key="parallel-unknown",
    )
    assert pipeline.worker.execute(probe["id"])["state"] == "failed"
    assert pipeline.client.get("trial", pipeline.trial["id"])["state"] == "execution_unknown"
    pipeline.client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=pipeline.campaign["id"],
            candidate_id=pipeline.trial["data"]["candidate_id"],
            validation_id=pipeline.trial["data"]["validation_id"],
            seed=8,
        ),
        idempotency_key="parallel-second-trial",
        expected_revision=pipeline.campaign["revision"],
    )
    with pytest.raises(ResearchError, match="Campaign trial concurrency"):
        pipeline.worker.claim(pipeline.ready())
