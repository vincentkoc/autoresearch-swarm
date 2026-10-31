"""Complete native panels, immutable assessment and current-evidence revalidation."""

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Barrier

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.benchmark_assessment import require_current_assessment
from research_swarm.control.benchmark_assessment_models import (
    AssessPolicyBenchmark,
    BenchmarkCase,
    PolicyBenchmarkAssessment,
)
from research_swarm.control.dispatch import COMMANDS
from research_swarm.control.models import SearchConfiguration
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import Direction
from research_swarm.domain.types import new_id
from research_swarm.evaluation.statistics import FixedPairedPolicy, paired_sign_gate
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from tests.test_benchmark_admission import answer_arm, finish_candidate
from tests.test_benchmark_execution import count
from tests.test_benchmark_proposals import arm_view, ready_arms
from tests.test_learning_inputs import context
from tests.test_learning_profiles import scoped_client
from tests.test_learning_publication import invalidate
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_proposals import PROPOSAL_LIMITS, baseline
from tests.test_search_admission import admit


def complete_panel(pipeline, tmp_path):
    benchmark, execution, views, _ = ready_arms(pipeline, tmp_path)
    receipts = []
    for index, arm_id in enumerate(execution["data"]["arm_ids"]):
        view = views[index] if index < len(views) else arm_view(pipeline, arm_id)
        if index >= len(views):
            baseline(view)
        prepared, _ = answer_arm(pipeline, view)
        admit(view, prepared["data"]["proposal_request_id"])
        receipts.append(finish_candidate(view))
    return benchmark, execution, receipts


def assess(client, benchmark_id, key=None):
    return client.command(
        "learning.assess_benchmark",
        AssessPolicyBenchmark(benchmark_id=benchmark_id),
        idempotency_key=key or new_id(),
    )["result"]


def current(pipeline, benchmark_id):
    return pipeline.runtime.database.store.read(
        lambda sql: require_current_assessment(
            Transaction(sql, context(pipeline.runtime)), benchmark_id
        )
    )


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
class TestBenchmarkAssessment:
    def test_complete_native_panel_has_exact_rewards_costs_and_scoped_immutable_assessment(
        self, pipeline, tmp_path
    ):
        benchmark, execution, _ = complete_panel(pipeline, tmp_path)
        definition = COMMANDS["learning.assess_benchmark"]
        scopes = {definition.scope, *definition.additional_scopes}
        assert scopes == {"learning.manage", "read"}
        for missing in scopes:
            with pytest.raises(ResearchError) as denied:
                assess(scoped_client(pipeline.runtime, scopes - {missing}), benchmark["id"])
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        result = assess(scoped_client(pipeline.runtime, scopes), benchmark["id"])
        data = PolicyBenchmarkAssessment.model_validate(result["data"])
        assert len(data.pairs) == 5 and len(data.case_digests) == 10
        assert tuple(pair.seed for pair in data.pairs) == tuple(
            benchmark["data"]["request"]["gate"]["seeds"]
        )
        assert data.gate == paired_sign_gate(
            FixedPairedPolicy.model_validate(benchmark["data"]["request"]["gate"]),
            data.pairs,
            Direction.MAXIMIZE,
        )
        totals = {}
        for arm_id in execution["data"]["arm_ids"]:
            case = BenchmarkCase.model_validate(
                pipeline.client.get("benchmark_case", arm_id)["data"]
            )
            assert data.case_digests[arm_id] == case.digest
            assert case.trajectory.split == "held_out"
            assert case.trajectory.consumed["attempts"] == 2
            assert case.comparator_consumed["attempts"] == 2
            assert case.total_consumed["attempts"] == 4
            assert case.total_consumed["trial_admissions"] == 2
            assert case.total_consumed["proposal_requests"] == 1
            accounts = pipeline.client.budget(arm_id)["accounts"]
            assert case.total_consumed == {
                item["dimension"]: Decimal(item["consumed"]) for item in accounts
            }
            for dimension, value in case.total_consumed.items():
                totals[dimension] = totals.get(dimension, Decimal(0)) + value
        assert data.total_consumed == totals
        assert current(pipeline, benchmark["id"]) == data
        before = count(pipeline.runtime, "trial")
        assert assess(pipeline.client, benchmark["id"]) == result
        assert count(pipeline.runtime, "trial") == before

    def test_missing_or_failed_case_cannot_produce_partial_panel(self, pipeline, tmp_path):
        benchmark, _, views, _ = ready_arms(pipeline, tmp_path)
        with pytest.raises(ResearchError) as incomplete:
            assess(pipeline.client, benchmark["id"])
        assert incomplete.value.code == ErrorCode.EVALUATION_INCOMPLETE
        prepared, _ = answer_arm(pipeline, views[0], failed=True)
        admit(views[0], prepared["data"]["proposal_request_id"])
        with pytest.raises(ResearchError) as failed:
            assess(pipeline.client, benchmark["id"])
        assert failed.value.code == ErrorCode.EVALUATION_INCOMPLETE
        assert count(pipeline.runtime, "benchmark_case") == 0
        assert count(pipeline.runtime, "benchmark_assessment") == 0

    def test_final_event_failure_keeps_all_case_records_and_assessment_atomic(
        self, pipeline, tmp_path, monkeypatch
    ):
        benchmark, execution, _ = complete_panel(pipeline, tmp_path)
        budgets = {
            arm_id: pipeline.client.budget(arm_id) for arm_id in execution["data"]["arm_ids"]
        }
        event = Transaction.event

        def fail(transaction, event_type, *args, **kwargs):
            if event_type == "learning.benchmark_assessed":
                raise RuntimeError("assessment event failed")
            return event(transaction, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="assessment event failed"):
            assess(pipeline.client, benchmark["id"])
        assert count(pipeline.runtime, "benchmark_case") == 0
        assert count(pipeline.runtime, "benchmark_assessment") == 0
        assert {arm_id: pipeline.client.budget(arm_id) for arm_id in budgets} == budgets
        monkeypatch.setattr(Transaction, "event", event)
        assert assess(pipeline.client, benchmark["id"])["id"] == benchmark["id"]

    def test_independent_assessment_race_replays_history_but_correction_blocks_current_use(
        self, pipeline, tmp_path
    ):
        benchmark, _, receipts = complete_panel(pipeline, tmp_path)
        clients = [
            ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
            for _ in range(2)
        ]
        barrier = Barrier(2)

        def issue(client):
            barrier.wait(timeout=10)
            return assess(client, benchmark["id"])

        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = pool.map(issue, clients)
        assert first == second and count(pipeline.runtime, "benchmark_assessment") == 1
        assert count(pipeline.runtime, "benchmark_case") == 10
        invalidate(pipeline.client, "evaluation_receipt", receipts[-1]["id"])
        assert assess(pipeline.client, benchmark["id"]) == first
        with pytest.raises(ResearchError):
            current(pipeline, benchmark["id"])
