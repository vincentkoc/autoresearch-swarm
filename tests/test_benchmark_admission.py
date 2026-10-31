"""Reserved policy actions execute through ordinary source, trial and worker paths."""

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Barrier

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import ProposeCandidate
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.learning_datasets import qualified_episode
from research_swarm.control.learning_models import CommitPolicyDecision, LearningPlan
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.search_models import RespondToProposal
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from tests.test_benchmark_execution import count, direct_trial
from tests.test_benchmark_proposals import ready_arms, request
from tests.test_learning_inputs import context
from tests.test_learning_publication import invalidate
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_proposals import PROPOSAL_LIMITS
from tests.test_search_admission import admission_intent, admit


def answer_arm(pipeline, view, *, failed=False):
    prepared = request(pipeline.client, view.campaign["id"], new_id())
    request_id = prepared["data"]["proposal_request_id"]
    if failed:
        response = RespondToProposal(
            request_id=request_id, outcome="failed", finding="Agent failed"
        )
        decision = None
    else:
        decision = pipeline.client.command(
            "learning.commit_decision",
            CommitPolicyDecision(request_id=request_id),
            idempotency_key=new_id(),
        )["result"]
        response = RespondToProposal(
            request_id=request_id,
            outcome="candidate",
            source_artifact=decision["data"]["source_artifact"],
            hypothesis="Execute the precommitted held-out action",
        )
    pipeline.client.command("search.respond", response, idempotency_key=new_id())
    return prepared, decision


def finish_candidate(view):
    evaluator_client = ResearchClient(LocalTransport(view.runtime))
    trainer = view.worker.claim(view.ready())
    assert view.worker.execute(trainer["id"])["state"] == "succeeded"
    job = view.ready()
    authorization = evaluator_client.command(
        "evaluation.authorize",
        AuthorizeEvaluation(job_id=job["id"], evaluator_id=view.runtime.principal().id),
        expected_revision=job["revision"],
        idempotency_key=new_id(),
    )["result"]
    evaluator = view.worker.claim(view.ready())
    assert view.worker.execute(evaluator["id"])["state"] == "succeeded"
    return evaluator_client.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authorization["id"], attempt_id=evaluator["id"]),
        idempotency_key=new_id(),
    )["result"]


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "limits": PROPOSAL_LIMITS,
            "search": SearchConfiguration(
                policy="external",
                version="external-v1",
                max_parallel_trials=2,
            ),
        }
    ],
    indirect=True,
)
class TestBenchmarkAdmission:
    def test_both_reserved_actions_have_actual_results_and_exact_costed_trajectories(
        self, pipeline, tmp_path
    ):
        _, _, views, _ = ready_arms(pipeline, tmp_path)
        for view in views:
            prepared, decision = answer_arm(pipeline, view)
            request_id = prepared["data"]["proposal_request_id"]
            arm = pipeline.client.get("benchmark_arm", view.campaign["id"])["data"]
            campaign = pipeline.client.get("campaign", view.campaign["id"])
            duplicate = pipeline.client.command(
                "candidate.propose",
                ProposeCandidate(
                    campaign_id=campaign["id"],
                    source_artifact=decision["data"]["source_artifact"],
                    hypothesis="Existing source must not censor a fixed policy action",
                ),
                expected_revision=campaign["revision"],
                idempotency_key=new_id(),
            )["result"]
            result = admit(view, request_id)["result"]
            assert result["transition"]["data"]["outcome"] == "admitted"
            assert result["trial"]["id"] == arm["candidate_trial_id"]
            assert result["trial"]["data"]["spec"]["seed"] == arm["seed"]
            assert result["candidate"]["id"] != duplicate["id"]
            assert result["candidate"]["data"]["parent_ids"] == [arm["initial_candidate_id"]]
            assert (
                pipeline.client.get("benchmark_trial_slot", arm["candidate_trial_id"])["state"]
                == "consumed"
            )
            receipt = finish_candidate(view)
            plan = LearningPlan.model_validate(
                pipeline.client.get("learning_plan", arm["learning_plan_id"])["data"]
            )
            trajectory = pipeline.runtime.database.store.read(
                lambda sql, plan=plan: qualified_episode(
                    Transaction(sql, context(pipeline.runtime)), plan, plan.episodes[0]
                )
            )
            assert trajectory.split == "held_out"
            assert trajectory.trial_id == arm["candidate_trial_id"]
            assert trajectory.candidate_evaluation_id == receipt["id"]
            assert trajectory.decision.behavior_policy_digest == arm["policy_digest"]
            assert trajectory.consumed["proposal_requests"] == Decimal(1)
            assert trajectory.consumed["attempts"] == Decimal(2)
            assert trajectory.consumed["trial_admissions"] == Decimal(1)
            with pytest.raises(ResearchError):
                direct_trial(pipeline.client, campaign, arm, arm["seed"])

    def test_final_admission_failure_rolls_back_slot_trial_jobs_and_allowance(
        self, pipeline, tmp_path, monkeypatch
    ):
        _, _, views, _ = ready_arms(pipeline, tmp_path)
        view = views[0]
        prepared, _ = answer_arm(pipeline, view)
        request_id = prepared["data"]["proposal_request_id"]
        arm = pipeline.client.get("benchmark_arm", view.campaign["id"])["data"]
        before = {kind: count(pipeline.runtime, kind) for kind in ("candidate", "trial", "job")}
        budget = pipeline.client.budget(view.campaign["id"])
        event = Transaction.event

        def fail(transaction, event_type, *args, **kwargs):
            if event_type == "search.proposal_consumed":
                raise RuntimeError("admission event failed")
            return event(transaction, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="admission event failed"):
            admit(view, request_id)
        assert all(count(pipeline.runtime, kind) == value for kind, value in before.items())
        assert pipeline.client.budget(view.campaign["id"]) == budget
        assert (
            pipeline.client.get("benchmark_trial_slot", arm["candidate_trial_id"])["state"]
            == "reserved"
        )
        assert pipeline.client.get("proposal_request", request_id)["state"] == "responded"
        monkeypatch.setattr(Transaction, "event", event)
        assert admit(view, request_id)["result"]["trial"]["id"] == arm["candidate_trial_id"]

    def test_source_verification_rechecks_training_before_spending_candidate_slot(
        self, pipeline, tmp_path, monkeypatch
    ):
        _, _, views, receipt = ready_arms(pipeline, tmp_path)
        view = views[0]
        prepared, decision = answer_arm(pipeline, view)
        fetch = pipeline.runtime.artifacts.store.fetch

        def corrected(workspace, blob, destination):
            result = fetch(workspace, blob, destination)
            if blob.reference.id == decision["data"]["source_artifact"]["id"]:
                invalidate(pipeline.client, "evaluation_receipt", receipt["id"])
            return result

        monkeypatch.setattr(pipeline.runtime.artifacts.store, "fetch", corrected)
        before = pipeline.client.budget(view.campaign["id"])
        with pytest.raises(ResearchError):
            admit(view, prepared["data"]["proposal_request_id"])
        arm = pipeline.client.get("benchmark_arm", view.campaign["id"])["data"]
        assert pipeline.client.budget(view.campaign["id"]) == before
        assert (
            pipeline.client.get("benchmark_trial_slot", arm["candidate_trial_id"])["state"]
            == "reserved"
        )

    def test_independent_admission_race_and_lost_reply_preserve_one_trial(
        self, pipeline, tmp_path, monkeypatch
    ):
        _, _, views, _ = ready_arms(pipeline, tmp_path)
        view = views[0]
        prepared, _ = answer_arm(pipeline, view)
        request_id = prepared["data"]["proposal_request_id"]
        intent = admission_intent(view, request_id)
        barrier = Barrier(2)
        runtimes = [LocalRuntime.open(pipeline.runtime.root) for _ in range(2)]
        clients = [ResearchClient(LocalTransport(runtime)) for runtime in runtimes]
        faults = []

        def lose_reply_from(execute):
            def lose_reply(context, operation, *args, **kwargs):
                result = execute(context, operation, *args, **kwargs)
                if operation == "search.admit_response":
                    faults.append(operation)
                    raise OSError("lost admission reply")
                return result

            return lose_reply

        for runtime in runtimes:
            monkeypatch.setattr(
                runtime.database, "execute", lose_reply_from(runtime.database.execute)
            )

        def issue(client):
            barrier.wait(timeout=10)
            return client.command(
                "search.admit_response",
                intent,
                expected_revision=1,
                idempotency_key="one-reserved-admission",
            )

        before = count(pipeline.runtime, "trial")
        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = pool.map(issue, clients)
        assert first == second and count(pipeline.runtime, "trial") == before + 1
        assert faults
        with pytest.raises(ResearchError) as consumed:
            admit(view, request_id, revision=2)
        assert consumed.value.code == ErrorCode.INVALID_TRANSITION

    def test_failed_agent_is_preserved_without_replacement_or_candidate_charge(
        self, pipeline, tmp_path
    ):
        _, _, views, _ = ready_arms(pipeline, tmp_path)
        view = views[0]
        prepared, _ = answer_arm(pipeline, view, failed=True)
        before = pipeline.client.budget(view.campaign["id"])
        result = admit(view, prepared["data"]["proposal_request_id"])["result"]
        assert result["transition"]["data"]["outcome"] == "agent_failed"
        assert result["trial"] is None
        assert pipeline.client.budget(view.campaign["id"]) == before
        assert request(pipeline.client, view.campaign["id"], new_id()) == prepared
        arm = pipeline.client.get("benchmark_arm", view.campaign["id"])["data"]
        assert (
            pipeline.client.get("benchmark_trial_slot", arm["candidate_trial_id"])["state"]
            == "reserved"
        )
