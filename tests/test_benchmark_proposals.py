"""Held-out requests become visible only with their fixed policy and split."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest

from research_swarm.control.benchmark_execution_models import RequestBenchmarkProposal
from research_swarm.control.dispatch import COMMANDS
from research_swarm.control.learning_models import (
    CommitPolicyDecision,
    FreezeLearningPlan,
    LearningPlan,
    QualifyTrajectoryDataset,
)
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.search_models import RespondToProposal
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from tests.test_benchmark_execution import count, ready_benchmark, start
from tests.test_learning_profiles import scoped_client
from tests.test_learning_publication import invalidate
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_proposals import PROPOSAL_LIMITS, baseline, cancel


def arm_view(pipeline, arm_id):
    arm = pipeline.client.get("benchmark_arm", arm_id)["data"]
    return replace(
        pipeline,
        client=scoped_client(pipeline.runtime, {"*"}),
        campaign=pipeline.client.get("campaign", arm_id),
        trial=pipeline.client.get("trial", arm["comparator_trial_id"]),
    )


def ready_arms(pipeline, tmp_path):
    benchmark, intent, research_receipt = ready_benchmark(pipeline, tmp_path)
    execution = start(pipeline.client, intent)
    views = [arm_view(pipeline, arm_id) for arm_id in execution["data"]["arm_ids"][:2]]
    for view in views:
        baseline(view)
    return benchmark, execution, views, research_receipt


def request(client, arm_id, key="benchmark-proposal"):
    return client.command(
        "learning.request_benchmark_proposal",
        RequestBenchmarkProposal(arm_id=arm_id),
        idempotency_key=key,
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
class TestBenchmarkProposals:
    def test_both_policies_have_fixed_held_out_plans_before_agents_respond(
        self, pipeline, tmp_path
    ):
        benchmark, execution, views, _ = ready_arms(pipeline, tmp_path)
        definition = COMMANDS["learning.request_benchmark_proposal"]
        scopes = {definition.scope, *definition.additional_scopes}
        assert scopes == {"learning.manage", "search.manage", "read"}
        for missing in scopes:
            with pytest.raises(ResearchError) as denied:
                request(
                    scoped_client(pipeline.runtime, scopes - {missing}), views[0].campaign["id"]
                )
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        before_trials = count(pipeline.runtime, "trial")
        for view, expected in zip(
            views,
            (benchmark["data"]["baseline_policy"], execution["data"]["learned_policy"]),
            strict=True,
        ):
            arm_id = view.campaign["id"]
            arm = pipeline.client.get("benchmark_arm", arm_id)["data"]
            prepared = request(scoped_client(pipeline.runtime, scopes), arm_id)
            assert prepared["data"]["proposal_request_id"] == arm["proposal_request_id"]
            plan = LearningPlan.model_validate(
                pipeline.client.get("learning_plan", arm["learning_plan_id"])["data"]
            )
            assert prepared["data"]["learning_plan_digest"] == plan.digest
            assert plan.request.policy.model_dump(mode="json") == expected
            assert len(plan.episodes) == 1 and plan.episodes[0].split == "held_out"
            assert plan.episodes[0].seed == arm["seed"]
            assert plan.episodes[0].proposer_id == pipeline.runtime.principal().id
            assert plan.episodes[0].comparator_candidate_id == arm["initial_candidate_id"]
            response = RespondToProposal(
                request_id=arm["proposal_request_id"],
                outcome="candidate",
                source_artifact=next(iter(plan.request.action_artifacts.values())),
                hypothesis="Use the assigned held-out action",
            )
            with pytest.raises(ResearchError, match="committed policy decision"):
                pipeline.client.command("search.respond", response, idempotency_key=new_id())
            decision = pipeline.client.command(
                "learning.commit_decision",
                CommitPolicyDecision(request_id=arm["proposal_request_id"]),
                idempotency_key=new_id(),
            )["result"]
            pipeline.client.command(
                "search.respond",
                response.model_copy(
                    update={
                        "source_artifact": plan.request.action_artifacts[
                            decision["data"]["decision"]["action_id"]
                        ]
                    }
                ),
                idempotency_key=new_id(),
            )
            assert request(pipeline.client, arm_id, new_id()) == prepared
            replacement = plan.request.model_copy(
                update={
                    "episodes": (
                        plan.request.episodes[0].model_copy(update={"split": "development"}),
                    )
                }
            )
            with pytest.raises(ResearchError, match="immutable"):
                pipeline.client.command(
                    "learning.freeze_plan", replacement, idempotency_key=new_id()
                )
            with pytest.raises(ResearchError):
                pipeline.client.command(
                    "learning.qualify_dataset",
                    QualifyTrajectoryDataset(plan_id=arm["learning_plan_id"], dataset_id=new_id()),
                    idempotency_key=new_id(),
                )
        assert count(pipeline.runtime, "trial") == before_trials

    def test_final_event_failure_rolls_back_plan_request_charge_and_visible_state(
        self, pipeline, tmp_path, monkeypatch
    ):
        _, _, views, _ = ready_arms(pipeline, tmp_path)
        arm_id = views[0].campaign["id"]
        arm = pipeline.client.get("benchmark_arm", arm_id)["data"]
        before = pipeline.client.budget(arm_id)
        kinds = ("proposal_request", "learning_plan", "learning_episode", "benchmark_proposal")
        inventory = {kind: count(pipeline.runtime, kind) for kind in kinds}
        independent = LocalRuntime.open(pipeline.runtime.root)
        observer = scoped_client(independent, {"read"})
        event = Transaction.event
        observations = []

        def fail(transaction, event_type, *args, **kwargs):
            if event_type == "learning.benchmark_proposal_requested":
                with pytest.raises(ResearchError) as absent:
                    observer.get("proposal_request", arm["proposal_request_id"])
                observations.append(absent.value.code)
                raise RuntimeError("benchmark event failed")
            return event(transaction, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="benchmark event failed"):
            request(pipeline.client, arm_id)
        assert observations == [ErrorCode.NOT_FOUND]
        assert pipeline.client.budget(arm_id) == before
        assert {kind: count(pipeline.runtime, kind) for kind in kinds} == inventory
        monkeypatch.setattr(Transaction, "event", event)
        assert request(pipeline.client, arm_id)["id"] == arm_id

    def test_independent_requests_and_lost_reply_create_one_pair_per_arm(
        self, pipeline, tmp_path, monkeypatch
    ):
        _, _, views, _ = ready_arms(pipeline, tmp_path)
        barrier = Barrier(2)
        clients = [scoped_client(LocalRuntime.open(pipeline.runtime.root), {"*"}) for _ in range(2)]

        def concurrent(client):
            barrier.wait(timeout=10)
            return request(client, views[0].campaign["id"], new_id())

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(concurrent, clients))
        assert results[0] == results[1]
        assert count(pipeline.runtime, "benchmark_proposal") == 1
        execute = pipeline.runtime.database.execute

        def lose_reply(context, operation, *args, **kwargs):
            result = execute(context, operation, *args, **kwargs)
            if operation == "learning.request_benchmark_proposal":
                raise OSError("committed reply lost")
            return result

        monkeypatch.setattr(pipeline.runtime.database, "execute", lose_reply)
        with pytest.raises(OSError, match="committed reply lost"):
            request(pipeline.client, views[1].campaign["id"])
        replay = request(clients[0], views[1].campaign["id"], "retry-after-lost-reply")
        assert replay["id"] == views[1].campaign["id"]
        assert count(pipeline.runtime, "benchmark_proposal") == 2
        for view in views:
            account = next(
                item
                for item in pipeline.client.budget(view.campaign["id"])["accounts"]
                if item["dimension"] == "proposal_requests"
            )
            assert account["consumed"] == "1"

    def test_current_training_required_and_comparator_must_be_ready(self, pipeline, tmp_path):
        _, execution, views, receipt = ready_arms(pipeline, tmp_path)
        unready = execution["data"]["arm_ids"][2]
        with pytest.raises(ResearchError, match="starting comparator"):
            request(pipeline.client, unready)
        assert count(pipeline.runtime, "benchmark_proposal") == 0
        invalidate(pipeline.client, "evaluation_receipt", receipt["id"])
        with pytest.raises(ResearchError):
            request(pipeline.client, views[0].campaign["id"], new_id())
        assert count(pipeline.runtime, "benchmark_proposal") == 0

    def test_cancellation_cannot_issue_a_replacement_or_rewrite_frozen_policy(
        self, pipeline, tmp_path
    ):
        _, _, views, _ = ready_arms(pipeline, tmp_path)
        view = views[0]
        prepared = request(pipeline.client, view.campaign["id"])
        request_id = prepared["data"]["proposal_request_id"]
        row = pipeline.client.get("proposal_request", request_id)
        cancel(view, row)
        assert request(pipeline.client, view.campaign["id"], new_id()) == prepared
        with pytest.raises(ResearchError, match="pending live proposal"):
            pipeline.client.command(
                "learning.commit_decision",
                CommitPolicyDecision(request_id=request_id),
                idempotency_key=new_id(),
            )
        plan = pipeline.client.get("learning_plan", prepared["data"]["learning_plan_id"])["data"]
        changed = FreezeLearningPlan.model_validate(plan["request"]).model_copy(
            update={
                "policy": LearningPlan.model_validate(plan).request.policy.model_copy(
                    update={"logits": (0.1, -0.1)}
                )
            }
        )
        with pytest.raises(ResearchError, match="immutable"):
            pipeline.client.command("learning.freeze_plan", changed, idempotency_key=new_id())
