"""Adoption mechanics use synthetic passing gates; native panels keep their actual outcome."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control import policy_adoption as adoption_module
from research_swarm.control.benchmark_assessment_models import PolicyBenchmarkAssessment
from research_swarm.control.benchmark_models import PolicyBenchmark
from research_swarm.control.dispatch import COMMANDS
from research_swarm.control.learning_models import (
    CommitPolicyDecision,
    FreezeLearningPlan,
    LearningPlan,
    PlanEpisode,
)
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.policy_guard import require_plan_binding
from research_swarm.control.policy_models import (
    AdoptPolicy,
    CreatePolicyBinding,
    PolicyBinding,
    PolicyBindingRef,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import Direction
from research_swarm.domain.types import new_id, utc_now
from research_swarm.evaluation.statistics import PairedObservation, paired_sign_gate
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.learning.test_benchmark_assessment import assess, complete_panel
from tests.learning.test_benchmark_execution import count, ready_benchmark
from tests.learning.test_learning_inputs import context
from tests.learning.test_learning_profiles import scoped_client
from tests.search.test_proposals import PROPOSAL_LIMITS, cancel, proposal_intent


def create_binding(client, benchmark_id, binding_id=None):
    return client.command(
        "learning.create_policy_binding",
        CreatePolicyBinding(binding_id=binding_id or new_id(), benchmark_id=benchmark_id),
        idempotency_key=new_id(),
    )["result"]


def adopt(client, binding_id, benchmark_id, revision=1, key=None):
    return client.command(
        "learning.adopt_policy",
        AdoptPolicy(binding_id=binding_id, benchmark_id=benchmark_id),
        expected_revision=revision,
        idempotency_key=key or new_id(),
    )["result"]


def synthetic_passing_assessment(pipeline, benchmark_row, monkeypatch):
    """Exercise the adoption CAS, not a claim about measured policy improvement."""
    benchmark = PolicyBenchmark.model_validate(benchmark_row["data"])
    trained = pipeline.client.get("learning_publication", benchmark.request.training_admission_id)[
        "data"
    ]
    from research_swarm.domain.learning import CategoricalPolicy

    pairs = tuple(
        PairedObservation(seed=seed, candidate=1.0, comparator=0.0)
        for seed in benchmark.request.gate.seeds
    )
    data = PolicyBenchmarkAssessment(
        benchmark_id=benchmark_row["id"],
        execution_digest="sha256:" + "0" * 64,
        baseline_policy_digest=benchmark.baseline_policy.digest,
        challenger_policy_digest=CategoricalPolicy.model_validate(trained["policy"]).digest,
        case_digests={new_id(): "sha256:" + "1" * 64 for _ in range(2 * len(pairs))},
        pairs=pairs,
        gate=paired_sign_gate(benchmark.request.gate, pairs, Direction.MAXIMIZE),
        total_consumed={
            "attempts": "40",
            "execution_ms": "1000",
            "proposal_requests": "10",
            "trial_admissions": "20",
        },
        evidence_through=0,
        actor_id=pipeline.runtime.principal().id,
        recorded_at=utc_now(),
    )
    assert data.gate.outcome == "promote"
    monkeypatch.setattr(adoption_module, "require_current_assessment", lambda tx, identifier: data)
    return data


def learning_intent(pipeline, binding_id):
    row = pipeline.client.get("policy_binding", binding_id)
    binding = PolicyBinding.model_validate(row["data"])
    state = pipeline.client.get("search_state", pipeline.campaign["id"])
    proposal = pipeline.client.command(
        "search.request_proposal",
        proposal_intent(pipeline, pipeline.runtime.principal()).model_copy(
            update={"seed": pipeline.trial["data"]["spec"]["seed"]}
        ),
        expected_revision=state["revision"],
        idempotency_key=new_id(),
    )["result"]["request"]
    intent = FreezeLearningPlan(
        plan_id=new_id(),
        policy=binding.policy,
        reward_spec=binding.family.reward_spec,
        action_artifacts=binding.action_artifacts,
        episodes=(PlanEpisode(request_id=proposal["id"], split="development"),),
        policy_binding=PolicyBindingRef(
            binding_id=binding_id, revision=row["revision"], policy_digest=binding.policy.digest
        ),
    )
    return proposal, intent


def freeze(pipeline, intent):
    return pipeline.client.command("learning.freeze_plan", intent, idempotency_key=new_id())[
        "result"
    ]


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "limits": tuple(
                limit.model_copy(update={"limit": limit.limit * 2})
                if limit.dimension == "proposal_requests"
                else limit
                for limit in PROPOSAL_LIMITS
            ),
            "search": SearchConfiguration(
                policy="external",
                version="external-v1",
                max_parallel_trials=2,
            ),
        }
    ],
    indirect=True,
)
class TestPolicyAdoption:
    def test_binding_origin_and_plan_reference_are_immutable_and_family_scoped(
        self, pipeline, tmp_path
    ):
        benchmark, _, _ = ready_benchmark(pipeline, tmp_path)
        definition = COMMANDS["learning.create_policy_binding"]
        scopes = {definition.scope, *definition.additional_scopes}
        assert scopes == {"learning.manage", "read"}
        for missing in scopes:
            with pytest.raises(ResearchError) as denied:
                create_binding(scoped_client(pipeline.runtime, scopes - {missing}), benchmark["id"])
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        origin = create_binding(pipeline.client, benchmark["id"])
        assert create_binding(pipeline.client, benchmark["id"], origin["id"]) == origin
        proposal, intent = learning_intent(pipeline, origin["id"])
        wrong = intent.model_copy(
            update={"reward_spec": intent.reward_spec.model_copy(update={"scale": 2.0})}
        )
        with pytest.raises(ResearchError, match="family differs"):
            freeze(pipeline, wrong)
        stale = intent.model_copy(
            update={"policy_binding": intent.policy_binding.model_copy(update={"revision": 2})}
        )
        with pytest.raises(ResearchError, match="revision changed"):
            freeze(pipeline, stale)
        row = freeze(pipeline, intent)
        plan = LearningPlan.model_validate(row["data"])
        assert plan.policy_binding.reference == intent.policy_binding
        assert plan.policy_binding.adoption_id is None
        decision = pipeline.client.command(
            "learning.commit_decision",
            CommitPolicyDecision(request_id=proposal["id"]),
            idempotency_key=new_id(),
        )["result"]
        assert decision["data"]["decision"]["behavior_policy_digest"] == intent.policy.digest

    def test_synthetic_pass_updates_binding_and_future_plans_without_rewriting_old_plan(
        self, pipeline, tmp_path, monkeypatch
    ):
        benchmark, _, _ = ready_benchmark(pipeline, tmp_path)
        origin = create_binding(pipeline.client, benchmark["id"])
        proposal, old_intent = learning_intent(pipeline, origin["id"])
        old = freeze(pipeline, old_intent)
        cancel(
            pipeline,
            proposal,
            pipeline.client.get("search_state", pipeline.campaign["id"])["revision"],
        )
        synthetic_passing_assessment(pipeline, benchmark, monkeypatch)
        definition = COMMANDS["learning.adopt_policy"]
        scopes = {definition.scope, *definition.additional_scopes}
        assert scopes == {"learning.adopt", "read"} and definition.expected_revision
        for missing in scopes:
            with pytest.raises(ResearchError) as denied:
                adopt(
                    scoped_client(pipeline.runtime, scopes - {missing}),
                    origin["id"],
                    benchmark["id"],
                )
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        before = pipeline.client.get("campaign", pipeline.campaign["id"])
        adopted = adopt(scoped_client(pipeline.runtime, scopes), origin["id"], benchmark["id"])
        assert adopted["data"]["before_revision"] == 1 and adopted["data"]["after_revision"] == 2
        assert pipeline.client.get("campaign", pipeline.campaign["id"]) == before
        assert pipeline.client.get("learning_plan", old["id"]) == old
        pipeline.runtime.database.store.read(
            lambda sql: require_plan_binding(
                Transaction(sql, context(pipeline.runtime)),
                LearningPlan.model_validate(old["data"]),
            )
        )
        assert create_binding(pipeline.client, benchmark["id"], origin["id"]) == origin
        proposal, new_intent = learning_intent(pipeline, origin["id"])
        new = freeze(pipeline, new_intent)
        assert new["data"]["policy_binding"]["adoption_id"] == adopted["id"]
        decision = pipeline.client.command(
            "learning.commit_decision",
            CommitPolicyDecision(request_id=proposal["id"]),
            idempotency_key=new_id(),
        )["result"]
        assert decision["data"]["decision"]["behavior_policy_digest"] == new_intent.policy.digest
        assert new_intent.policy.digest != old_intent.policy.digest

    def test_adoption_event_failure_rolls_back_pointer_receipt_and_retentions(
        self, pipeline, tmp_path, monkeypatch
    ):
        benchmark, _, _ = ready_benchmark(pipeline, tmp_path)
        origin = create_binding(pipeline.client, benchmark["id"])
        synthetic_passing_assessment(pipeline, benchmark, monkeypatch)
        before = pipeline.client.get("policy_binding", origin["id"])
        retained = count(pipeline.runtime, "artifact_reference")
        event = Transaction.event

        def fail(transaction, event_type, *args, **kwargs):
            if event_type == "learning.policy_adopted":
                raise RuntimeError("adoption event failed")
            return event(transaction, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="adoption event failed"):
            adopt(pipeline.client, origin["id"], benchmark["id"])
        assert pipeline.client.get("policy_binding", origin["id"]) == before
        assert count(pipeline.runtime, "policy_adoption") == 0
        assert count(pipeline.runtime, "artifact_reference") == retained
        monkeypatch.setattr(Transaction, "event", event)
        assert adopt(pipeline.client, origin["id"], benchmark["id"])["data"]["after_revision"] == 2

    def test_independent_adopters_have_one_cas_winner_and_historical_ack_replay(
        self, pipeline, tmp_path, monkeypatch
    ):
        benchmark, _, _ = ready_benchmark(pipeline, tmp_path)
        origin = create_binding(pipeline.client, benchmark["id"])
        synthetic_passing_assessment(pipeline, benchmark, monkeypatch)
        clients = [
            ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
            for _ in range(2)
        ]
        barrier = Barrier(2)

        def issue(index):
            barrier.wait(timeout=10)
            try:
                return adopt(clients[index], origin["id"], benchmark["id"], key=f"adopt-{index}")
            except ResearchError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(issue, range(2)))
        assert sum(isinstance(result, dict) for result in results) == 1
        assert ErrorCode.REVISION_CONFLICT in results
        winner = next(index for index, result in enumerate(results) if isinstance(result, dict))
        assert count(pipeline.runtime, "policy_adoption") == 1

        def corrected(*args):
            raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Synthetic evidence corrected")

        monkeypatch.setattr(adoption_module, "require_current_assessment", corrected)
        assert (
            adopt(clients[winner], origin["id"], benchmark["id"], key=f"adopt-{winner}")
            == results[winner]
        )
        _, intent = learning_intent(pipeline, origin["id"])
        with pytest.raises(ResearchError, match="corrected"):
            freeze(pipeline, intent)

    def test_native_panel_adopts_only_its_actual_gate_outcome(self, pipeline, tmp_path):
        benchmark, _, _ = complete_panel(pipeline, tmp_path)
        origin = create_binding(pipeline.client, benchmark["id"])
        assessment = assess(pipeline.client, benchmark["id"])
        if assessment["data"]["gate"]["outcome"] == "promote":
            result = adopt(pipeline.client, origin["id"], benchmark["id"])
            assert result["data"]["after_revision"] == 2
        else:
            with pytest.raises(ResearchError) as rejected:
                adopt(pipeline.client, origin["id"], benchmark["id"])
            assert rejected.value.code == ErrorCode.COMPARISON_INELIGIBLE
            assert pipeline.client.get("policy_binding", origin["id"])["revision"] == 1

    def test_refused_new_ancestry_cannot_advance_pointer_or_retain_checkpoint(
        self, pipeline, tmp_path, monkeypatch
    ):
        benchmark, _, _ = ready_benchmark(pipeline, tmp_path)
        origin = create_binding(pipeline.client, benchmark["id"])
        synthetic_passing_assessment(pipeline, benchmark, monkeypatch)
        before = pipeline.client.get("policy_binding", origin["id"])
        retained = count(pipeline.runtime, "artifact_reference")
        checked = []

        def beyond_profile(transaction, identifier):
            # The candidate receipt exists only inside this uncommitted transaction.
            checked.append(transaction.get("policy_adoption", identifier)["id"])
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "New ancestry exceeds depth bound"
            )

        monkeypatch.setattr(adoption_module, "require_current_adoption", beyond_profile)
        with pytest.raises(ResearchError, match="depth bound"):
            adopt(pipeline.client, origin["id"], benchmark["id"])
        assert len(checked) == 1
        assert pipeline.client.get("policy_binding", origin["id"]) == before
        assert count(pipeline.runtime, "policy_adoption") == 0
        assert count(pipeline.runtime, "artifact_reference") == retained
