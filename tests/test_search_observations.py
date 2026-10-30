from __future__ import annotations

import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.observation_models import ObserveTrial, ResolveTrialObservation
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.results import PublishAttempt
from research_swarm.control.search_models import RespondToProposal
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.workers.journal import JournalState
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_promotions import assess, commit
from tests.test_proposals import PROPOSAL_LIMITS, baseline, proposer, request_proposal
from tests.test_search_admission import admit


def policy_trial(
    pipeline: Pipeline,
    tmp_path: Path,
    learning_rate: float = 0.35,
    *,
    crash: bool = False,
    finish_delay: float = 0,
) -> dict[str, Any]:
    baseline(pipeline)
    principal, agent, _ = proposer(pipeline)
    request = request_proposal(pipeline, principal)["request"]
    source = tmp_path / "candidate"
    shutil.copytree(Path(__file__).resolve().parents[1] / "examples" / "cpu_bigram", source)
    training = source / "train.py"
    training.write_text(
        training.read_text(encoding="utf-8").replace(
            "LEARNING_RATE = 0.35", f"LEARNING_RATE = {learning_rate}"
        )
        + "\n# Policy observation candidate.\n",
        encoding="utf-8",
    )
    if finish_delay:
        training.write_text(
            training.read_text(encoding="utf-8") + f"\nimport time\ntime.sleep({finish_delay})\n",
            encoding="utf-8",
        )
    if crash:
        training.write_text(
            training.read_text(encoding="utf-8") + "\nraise SystemExit(7)\n", encoding="utf-8"
        )
    artifact = pipeline.client.capture_project(source, idempotency_key="observation-source")
    agent.command(
        "search.respond",
        RespondToProposal(
            request_id=request["id"],
            outcome="candidate",
            source_artifact=artifact,
            hypothesis="Compare a frozen optimizer setting",
        ),
        idempotency_key="observation-response",
    )
    return admit(pipeline, request["id"])["result"]["trial"]


def evaluate(pipeline: Pipeline, trial: dict[str, Any]) -> dict[str, Any]:
    train = pipeline.client.get("job", trial["data"]["job_ids"][0])
    attempt = pipeline.worker.claim(train)
    pipeline.worker.execute(attempt["id"])
    job = pipeline.client.get("job", trial["data"]["job_ids"][1])
    authorization = pipeline.client.command(
        "evaluation.authorize",
        AuthorizeEvaluation(job_id=job["id"], evaluator_id=pipeline.runtime.principal().id),
        idempotency_key=new_id(),
        expected_revision=job["revision"],
    )["result"]
    attempt = pipeline.worker.claim(pipeline.client.get("job", job["id"]))
    pipeline.worker.execute(attempt["id"])
    return pipeline.client.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authorization["id"], attempt_id=attempt["id"]),
        idempotency_key=new_id(),
    )["result"]


def observe(pipeline: Pipeline, trial: dict[str, Any], key: str | None = None) -> dict[str, Any]:
    current = pipeline.client.get("trial", trial["id"])
    return pipeline.client.command(
        "search.observe_trial",
        ObserveTrial(campaign_id=pipeline.campaign["id"], trial_id=trial["id"]),
        idempotency_key=key or new_id(),
        expected_revision=current["revision"],
    )


def resolve_intent(
    pipeline: Pipeline, observation: dict[str, Any], decision: dict[str, Any] | None = None
) -> ResolveTrialObservation:
    authority = pipeline.client.context(pipeline.campaign["id"])["authority"]
    return ResolveTrialObservation(
        campaign_id=pipeline.campaign["id"],
        observation_id=observation["id"],
        decision_id=decision["id"] if decision else None,
        expected_controller_id=authority["controller_id"],
        expected_control_epoch=authority["control_epoch"],
    )


def resolve(
    pipeline: Pipeline,
    observation: dict[str, Any],
    decision: dict[str, Any] | None = None,
    *,
    revision: int = 2,
    key: str | None = None,
) -> dict[str, Any]:
    return pipeline.client.command(
        "search.resolve_trial",
        resolve_intent(pipeline, observation, decision),
        idempotency_key=key or new_id(),
        expected_revision=revision,
    )


@pytest.mark.parametrize(
    "pipeline", [{"limits": PROPOSAL_LIMITS, "source_learning_rate": 0.0}], indirect=True
)
class TestSearchObservations:
    def test_observation_and_promoted_resolution_survive_restart_and_duplicate_delivery(
        self, pipeline: Pipeline, tmp_path: Path
    ) -> None:
        trial = policy_trial(pipeline, tmp_path)
        receipt = evaluate(pipeline, trial)
        before = pipeline.client.get("search_state", pipeline.campaign["id"])
        first = observe(pipeline, trial, "observed")
        observation = first["result"]
        assert observation["data"]["facts"]["outcome"] == "measurement"
        assert observation["data"]["facts"]["evaluation_id"] == receipt["id"]
        assert observe(pipeline, trial, "redelivered")["result"] == observation
        assert pipeline.client.get("search_state", pipeline.campaign["id"]) == before
        assert pipeline.client.list(RecordQuery(kind="observation_consumption"))["items"] == []
        decision = assess(pipeline, receipt, "selection", 1)
        assert decision["data"]["decision"]["outcome"] == "promote"
        with pytest.raises(ResearchError) as not_selected:
            resolve(pipeline, observation, decision)
        assert not_selected.value.code == ErrorCode.INVALID_TRANSITION
        commit(pipeline, decision, "select")
        # Selection is durable first. Reopening cannot lose its policy slot or repeat selection.
        reopened = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
        intent = resolve_intent(pipeline, observation, decision)
        resolved = reopened.command(
            "search.resolve_trial", intent, idempotency_key="resolved", expected_revision=2
        )
        assert resolved["result"]["transition"]["data"]["outcome"] == "promoted"
        assert resolved["result"]["search"]["data"]["generation"] == 1
        assert resolved["result"]["search"]["data"]["active_trial_ids"] == []
        assert (
            reopened.command(
                "search.resolve_trial", intent, idempotency_key="resolved", expected_revision=2
            )
            == resolved
        )
        with pytest.raises(ResearchError):
            resolve(pipeline, observation, decision, revision=3)
        assert len(pipeline.client.list(RecordQuery(kind="observation_consumption"))["items"]) == 1
        principal, _, _ = proposer(pipeline)
        next_request = request_proposal(pipeline, principal, 3)["request"]
        assert next_request["data"]["parent_candidate_id"] == trial["data"]["candidate_id"]
        assert next_request["data"]["incumbent_revision"] == 2

    def test_concurrent_resolution_consumes_one_observation(
        self, pipeline: Pipeline, tmp_path: Path
    ) -> None:
        trial = policy_trial(pipeline, tmp_path)
        receipt = evaluate(pipeline, trial)
        observation = observe(pipeline, trial)["result"]
        decision = assess(pipeline, receipt, "selection", 1)
        commit(pipeline, decision, "select")
        barrier = threading.Barrier(2)
        intent = resolve_intent(pipeline, observation, decision)

        def consume(index: int) -> dict[str, Any] | ErrorCode:
            client = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
            barrier.wait(timeout=5)
            try:
                return client.command(
                    "search.resolve_trial",
                    intent,
                    idempotency_key=f"resolution-{index}",
                    expected_revision=2,
                )
            except ResearchError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(consume, range(2)))
        assert results.count(ErrorCode.REVISION_CONFLICT) == 1
        assert (
            pipeline.client.get("search_state", pipeline.campaign["id"])["data"]["generation"] == 1
        )
        assert len(pipeline.client.list(RecordQuery(kind="observation_consumption"))["items"]) == 1

    def test_corrected_evidence_becomes_a_new_unscored_observation(
        self, pipeline: Pipeline, tmp_path: Path
    ) -> None:
        trial = policy_trial(pipeline, tmp_path)
        receipt = evaluate(pipeline, trial)
        measured = observe(pipeline, trial)["result"]
        decision = assess(pipeline, receipt, "selection", 1)
        pipeline.client.command(
            "evidence.invalidate",
            InvalidateEvidence(
                target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
                reason="Correction after measurement",
            ),
            idempotency_key="correction",
        )
        with pytest.raises(ResearchError) as stale:
            resolve(pipeline, measured, decision)
        assert stale.value.code == ErrorCode.STALE_FENCE
        corrected = observe(pipeline, trial)["result"]
        assert corrected["id"] != measured["id"]
        assert corrected["data"]["facts"]["outcome"] == "evidence_invalidated"
        assert corrected["data"]["facts"]["metric"] is None
        consumed = resolve(pipeline, corrected)["result"]
        assert consumed["transition"]["data"]["outcome"] == "non_scientific"
        assert pipeline.client.get("search_observation", measured["id"]) == measured
        assert (
            pipeline.client.get("campaign", pipeline.campaign["id"])["data"]["incumbent_revision"]
            == 1
        )

    def test_real_crash_has_no_score_and_resolves_without_scientific_rejection(
        self, pipeline: Pipeline, tmp_path: Path
    ) -> None:
        trial = policy_trial(pipeline, tmp_path, crash=True)
        attempt = pipeline.worker.claim(pipeline.ready())
        pipeline.worker.execute(attempt["id"])
        execution = pipeline.client.get("run_result", attempt["id"])["data"]
        assert execution["status"] == "failed" and execution["exit_code"] == 7
        observation = observe(pipeline, trial)["result"]
        assert observation["data"]["facts"]["outcome"] == "execution_failed"
        assert observation["data"]["facts"]["metric"] is None
        assert (
            resolve(pipeline, observation)["result"]["transition"]["data"]["outcome"]
            == "non_scientific"
        )
        assert len(pipeline.client.list(RecordQuery(kind="promotion"))["items"]) == 1

    def test_unknown_evidence_retains_slot_then_waits_for_evaluation(
        self, pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        trial = policy_trial(pipeline, tmp_path)
        attempt = pipeline.worker.claim(pipeline.ready())
        original = pipeline.worker.client.command

        def disconnected(operation: str, *args: Any, **kwargs: Any) -> Any:
            if operation == "worker.publish_attempt":
                raise RuntimeError("Synthetic lost native closure delivery")
            return original(operation, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(pipeline.worker.client, "command", disconnected)
            with pytest.raises(RuntimeError):
                pipeline.worker.execute(attempt["id"])
        closed = pipeline.worker.journal.get(attempt["id"])
        unknown = closed.model_copy(update={"state": JournalState.UNKNOWN, "outcome": None})
        snapshot = tmp_path / "unknown.json"
        snapshot.write_text(unknown.model_dump_json(), encoding="utf-8")
        artifact = pipeline.worker.client.upload_file(snapshot, idempotency_key="unknown-snapshot")
        original(
            "worker.publish_attempt",
            PublishAttempt(
                attempt_id=attempt["id"],
                journal_artifact=artifact,
                finding="Native receipt unavailable",
            ),
            idempotency_key="unknown-publication",
        )
        uncertain = observe(pipeline, trial)["result"]
        assert uncertain["data"]["facts"]["outcome"] == "execution_unknown"
        assert uncertain["data"]["facts"]["metric"] is None
        before = pipeline.client.budget(pipeline.campaign["id"])
        with pytest.raises(ResearchError) as blocked:
            resolve(pipeline, uncertain)
        assert blocked.value.code == ErrorCode.EXECUTION_UNKNOWN
        assert pipeline.client.budget(pipeline.campaign["id"]) == before
        assert pipeline.client.get("search_state", pipeline.campaign["id"])["data"][
            "active_trial_ids"
        ] == [trial["id"]]
        pipeline.worker.execute(attempt["id"])
        with pytest.raises(ResearchError) as pending:
            observe(pipeline, trial)
        assert pending.value.code in {ErrorCode.INVALID_TRANSITION, ErrorCode.EVALUATION_INCOMPLETE}
        assert pipeline.client.get("search_observation", uncertain["id"]) == uncertain


@pytest.mark.parametrize("pipeline", [{"limits": PROPOSAL_LIMITS}], indirect=True)
def test_worse_measurement_requires_matching_rejection_before_resolution(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    trial = policy_trial(pipeline, tmp_path, learning_rate=0.0)
    receipt = evaluate(pipeline, trial)
    observation = observe(pipeline, trial)["result"]
    with pytest.raises(ResearchError) as absent:
        resolve(pipeline, observation)
    assert absent.value.code == ErrorCode.EVALUATION_INCOMPLETE
    decision = assess(pipeline, receipt, "worse-selection", 1)
    assert decision["data"]["decision"]["outcome"] == "reject"
    result = resolve(pipeline, observation, decision)["result"]
    assert result["transition"]["data"]["outcome"] == "rejected"
    assert (
        pipeline.client.get("campaign", pipeline.campaign["id"])["data"]["incumbent_revision"] == 1
    )


@pytest.mark.parametrize(
    "pipeline", [{"limits": PROPOSAL_LIMITS, "source_learning_rate": 0.0}], indirect=True
)
def test_native_cancellation_is_distinct_from_failed_dependents(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    import time

    trial = policy_trial(pipeline, tmp_path, finish_delay=3)
    attempt = pipeline.worker.claim(pipeline.ready())
    with ThreadPoolExecutor(max_workers=1) as pool:
        execution = pool.submit(pipeline.worker.execute, attempt["id"])
        deadline = time.monotonic() + 8
        while True:
            try:
                entry = pipeline.worker.journal.get(attempt["id"])
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
            else:
                if entry.state == JournalState.RUNNING:
                    break
            if execution.done():
                execution.result()
                raise AssertionError("Owned execution ended before cancellation")
            assert time.monotonic() < deadline, "Owned execution did not reach native running state"
            time.sleep(0.01)
        pipeline.worker.journal.request_cancel(attempt["id"], 1)
        execution.result(timeout=10)
    result = pipeline.client.get("run_result", attempt["id"])["data"]
    assert result["status"] == "cancelled"
    dependent = pipeline.client.get("job", trial["data"]["job_ids"][1])
    assert dependent["state"] == "failed" and dependent["data"]["current_attempt_id"] is None
    observation = observe(pipeline, trial)["result"]
    assert observation["data"]["facts"]["outcome"] == "cancelled"
    assert observation["data"]["facts"]["attempt_outcomes"] == {attempt["id"]: "cancelled"}
    assert observation["data"]["facts"]["metric"] is None
    assert (
        resolve(pipeline, observation)["result"]["transition"]["data"]["outcome"]
        == "non_scientific"
    )


def paired_options() -> dict[str, Any]:
    from tests.test_confirmation import options

    configured = options(attempts=30)
    return {
        **configured,
        "limits": (*configured["limits"], PROPOSAL_LIMITS[-1]),
        "source_learning_rate": 0.0,
    }


@pytest.mark.parametrize("pipeline", [paired_options()], indirect=True)
def test_paired_observation_waits_for_the_complete_sealed_panel(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    from research_swarm.control.confirmation import AssessConfirmation, CreateConfirmationPlan
    from research_swarm.control.confirmation_models import ConfirmationData

    trial = policy_trial(pipeline, tmp_path)
    receipt = evaluate(pipeline, trial)
    observation = observe(pipeline, trial)["result"]
    with pytest.raises(ResearchError) as needs_panel:
        resolve(pipeline, observation)
    assert needs_panel.value.code == ErrorCode.EVALUATION_INCOMPLETE
    with pytest.raises(ResearchError) as no_observed_fallback:
        assess(pipeline, receipt, "forbidden-observed", 1)
    assert no_observed_fallback.value.code == ErrorCode.COMPARISON_INELIGIBLE
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
        idempotency_key="sealed-panel",
        expected_revision=campaign["revision"],
    )["result"]
    plan = ConfirmationData.model_validate(planned["data"]).plan
    for binding in plan.bindings:
        evaluate(pipeline, pipeline.client.get("trial", binding.trial_id))
    decision = pipeline.client.command(
        "confirmation.assess",
        AssessConfirmation(plan_id=plan.id),
        idempotency_key="panel-assessment",
    )["result"]
    assert decision["data"]["evidence_level"] == "confirmed"
    commit(pipeline, decision, "confirmed-selection")
    resolution = resolve(pipeline, observation, decision)["result"]
    assert resolution["transition"]["data"]["outcome"] == "promoted"
    assert (
        pipeline.client.get("campaign", campaign["id"])["data"]["incumbent_evidence_level"]
        == "confirmed"
    )


@pytest.mark.parametrize(
    "pipeline", [{"limits": PROPOSAL_LIMITS, "source_learning_rate": 0.0}], indirect=True
)
def test_unstarted_native_scope_does_not_become_a_scientific_failure(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    from research_swarm.control.models import TrialData
    from research_swarm.control.workers import AttemptData

    trial = policy_trial(pipeline, tmp_path)
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker._prepare(
        AttemptData.model_validate(attempt["data"]), TrialData.model_validate(trial["data"])
    )
    pipeline.worker.journal.request_cancel(attempt["id"], 1)
    assert pipeline.worker.execute(attempt["id"])["state"] == "not_started"
    observation = observe(pipeline, trial)["result"]
    assert observation["data"]["facts"]["outcome"] == "not_started"
    assert observation["data"]["facts"]["metric"] is None
    assert (
        resolve(pipeline, observation)["result"]["transition"]["data"]["outcome"]
        == "non_scientific"
    )
