"""Assess every frozen policy case once; historical replay never grants adoption."""

from decimal import Decimal

from research_swarm.control.benchmark_assessment_models import (
    AssessPolicyBenchmark,
    BenchmarkCase,
    PolicyBenchmarkAssessment,
)
from research_swarm.control.benchmark_execution_models import (
    BenchmarkArm,
    BenchmarkExecution,
    BenchmarkProposal,
    BenchmarkTrialSlot,
)
from research_swarm.control.benchmark_models import PolicyBenchmark
from research_swarm.control.benchmark_proposals import current_arm
from research_swarm.control.benchmark_trials import execution_digest
from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.learning_costs import summarize_costs, trial_settlements
from research_swarm.control.learning_datasets import qualified_episode
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_models import LearningPlan
from research_swarm.control.models import CampaignData
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import Direction
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.learning import LearningEvidence
from research_swarm.domain.types import amount_sum, utc_now
from research_swarm.evaluation.statistics import PairedObservation, paired_sign_gate
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.ledger import BudgetLedger


def collect_case(
    transaction: Transaction,
    arm: BenchmarkArm,
    execution: BenchmarkExecution,
    benchmark: PolicyBenchmark,
) -> BenchmarkCase:
    prepared_row = transaction.get("benchmark_proposal", arm.campaign_id)
    prepared = BenchmarkProposal.model_validate(prepared_row["data"])
    plan = LearningPlan.model_validate(
        transaction.get("learning_plan", arm.learning_plan_id)["data"]
    )
    policy = benchmark.baseline_policy if arm.side == "baseline" else execution.learned_policy
    if (
        prepared_row["state"] != "immutable"
        or prepared.request.arm_id != arm.campaign_id
        or prepared.benchmark_id != arm.benchmark_id
        or prepared.execution_digest != execution_digest(execution)
        or prepared.proposal_request_id != arm.proposal_request_id
        or prepared.learning_plan_id != arm.learning_plan_id
        or prepared.learning_plan_digest != plan.digest
        or plan.request.plan_id != arm.learning_plan_id
        or plan.request.policy != policy
        or plan.request.reward_spec != benchmark.reward_spec
        or len(plan.episodes) != 1
    ):
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Benchmark case plan differs")
    episode = plan.episodes[0]
    if (
        episode.episode_id != arm.proposal_request_id
        or episode.campaign_id != arm.campaign_id
        or episode.split != "held_out"
        or episode.seed != arm.seed
        or episode.comparator_candidate_id != arm.initial_candidate_id
        or episode.comparison_digest != benchmark.comparison_digest
    ):
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Benchmark case identity differs")
    for role, trial_id in (
        ("comparator", arm.comparator_trial_id),
        ("candidate", arm.candidate_trial_id),
    ):
        slot_row = transaction.get("benchmark_trial_slot", trial_id)
        slot = BenchmarkTrialSlot.model_validate(slot_row["data"])
        if slot_row["state"] != "consumed" or slot != BenchmarkTrialSlot(
            benchmark_id=arm.benchmark_id, arm_id=arm.campaign_id, trial_id=trial_id, role=role
        ):
            raise ResearchError(
                ErrorCode.EVALUATION_INCOMPLETE, "Benchmark trial slot is incomplete"
            )
    trajectory = qualified_episode(transaction, plan, episode)
    if trajectory.trial_id != arm.candidate_trial_id:
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Benchmark candidate trial differs")
    campaign = CampaignData.model_validate(transaction.get("campaign", arm.campaign_id)["data"])
    comparator = eligible_evaluation(
        transaction, episode.comparator_evaluation_id, arm.campaign_id, campaign
    )
    if comparator.trial.spec.trial_id != arm.comparator_trial_id:
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Benchmark comparator trial differs")
    evidence: list[LearningEvidence] = []
    comparator_consumed, settlements = summarize_costs(
        trial_settlements(transaction, arm.comparator_trial_id, comparator.trial, evidence)
    )
    total = {
        dimension: amount_sum(
            trajectory.consumed.get(dimension, Decimal(0)),
            comparator_consumed.get(dimension, Decimal(0)),
        )
        for dimension in {**trajectory.consumed, **comparator_consumed}
    }
    if set(total) != {item.dimension for item in benchmark.request.per_arm_limits}:
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Benchmark cost dimensions differ")
    for dimension, consumed in total.items():
        account = BudgetLedger.account(transaction, arm.campaign_id, dimension)
        if (
            account.consumed != consumed
            or account.reserved != 0
            or account.uncertain != 0
            or consumed > account.limit
        ):
            raise ResearchError(
                ErrorCode.COMPARISON_INELIGIBLE,
                "Benchmark costs are unresolved, exceed the cap or differ from the arm ledger",
            )
    return BenchmarkCase(
        benchmark_id=arm.benchmark_id,
        execution_digest=execution_digest(execution),
        arm_id=arm.campaign_id,
        arm_digest=record_digest("research.benchmark-arm.v1", arm.model_dump(mode="json")),
        trajectory=trajectory,
        comparator_consumed=comparator_consumed,
        comparator_cost_evidence=(*evidence, *settlements),
        total_consumed=total,
    )


def collect_panel(
    transaction: Transaction, benchmark_id: str
) -> tuple[
    PolicyBenchmark, BenchmarkExecution, tuple[BenchmarkCase, ...], tuple[PairedObservation, ...]
]:
    execution = BenchmarkExecution.model_validate(
        transaction.get("benchmark_execution", benchmark_id)["data"]
    )
    expected = None
    cases = []
    indexed = {}
    for arm_id in execution.arm_ids:
        arm, current, benchmark = current_arm(transaction, arm_id)
        if current != execution or arm.benchmark_id != benchmark_id:
            raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Benchmark panel binding differs")
        expected = benchmark
        key = (arm.seed, arm.side)
        if key in indexed:
            raise ResearchError(
                ErrorCode.COMPARISON_INELIGIBLE, "Benchmark contains duplicate cases"
            )
        try:
            case = collect_case(transaction, arm, execution, benchmark)
        except ResearchError as error:
            if error.code != ErrorCode.NOT_FOUND:
                raise
            raise ResearchError(
                ErrorCode.EVALUATION_INCOMPLETE,
                "Benchmark case lacks required evidence",
                details={"arm_id": arm_id},
            ) from error
        cases.append(case)
        indexed[key] = case
    assert expected is not None
    if set(indexed) != {
        (seed, side) for seed in expected.request.gate.seeds for side in ("baseline", "challenger")
    }:
        raise ResearchError(ErrorCode.EVALUATION_INCOMPLETE, "Benchmark fixed panel is incomplete")
    pairs = []
    for seed in expected.request.gate.seeds:
        baseline = indexed[seed, "baseline"].trajectory.reward
        challenger = indexed[seed, "challenger"].trajectory.reward
        assert baseline is not None and challenger is not None
        pairs.append(PairedObservation(seed=seed, candidate=challenger, comparator=baseline))
    return expected, execution, tuple(cases), tuple(pairs)


def total_cost(cases: tuple[BenchmarkCase, ...]) -> dict[str, Decimal]:
    total: dict[str, Decimal] = {}
    for case in cases:
        for dimension, consumed in case.total_consumed.items():
            total[dimension] = amount_sum(total.get(dimension, Decimal(0)), consumed)
    return total


def require_current_assessment(
    transaction: Transaction, benchmark_id: str
) -> PolicyBenchmarkAssessment:
    row = transaction.get("benchmark_assessment", benchmark_id)
    assessment = PolicyBenchmarkAssessment.model_validate(row["data"])
    benchmark, execution, cases, pairs = collect_panel(transaction, benchmark_id)
    if (
        row["state"] != "immutable"
        or assessment.benchmark_id != benchmark_id
        or assessment.execution_digest != execution_digest(execution)
        or assessment.baseline_policy_digest != benchmark.baseline_policy.digest
        or assessment.challenger_policy_digest != execution.learned_policy.digest
        or assessment.case_digests != {case.arm_id: case.digest for case in cases}
        or assessment.pairs != pairs
        or assessment.total_consumed != total_cost(cases)
        or assessment.gate != paired_sign_gate(benchmark.request.gate, pairs, Direction.MAXIMIZE)
    ):
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Policy benchmark assessment changed")
    for case in cases:
        stored = transaction.get("benchmark_case", case.arm_id)
        if stored["state"] != "immutable" or BenchmarkCase.model_validate(stored["data"]) != case:
            raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Policy benchmark case changed")
    return assessment


class BenchmarkAssessmentControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def assess(self, context: CommandContext, intent: AssessPolicyBenchmark) -> JsonObject:
        context.principal.require("read")

        def commit(transaction: Transaction) -> JsonObject:
            previous = optional_record(transaction, "benchmark_assessment", intent.benchmark_id)
            if previous is not None:
                return previous
            benchmark, execution, cases, pairs = collect_panel(transaction, intent.benchmark_id)
            watermark = transaction.connection.execute(
                "SELECT next_sequence-1 FROM workspaces WHERE id=?", (transaction.workspace_id,)
            ).one()[0]
            assessment = PolicyBenchmarkAssessment(
                benchmark_id=intent.benchmark_id,
                execution_digest=execution_digest(execution),
                baseline_policy_digest=benchmark.baseline_policy.digest,
                challenger_policy_digest=execution.learned_policy.digest,
                case_digests={case.arm_id: case.digest for case in cases},
                pairs=pairs,
                gate=paired_sign_gate(benchmark.request.gate, pairs, Direction.MAXIMIZE),
                total_consumed=total_cost(cases),
                evidence_through=watermark,
                actor_id=context.principal.id,
                recorded_at=utc_now(),
            )
            for case in cases:
                row = transaction.insert(
                    "benchmark_case",
                    case.arm_id,
                    case.model_dump(mode="json"),
                    state="immutable",
                    campaign_id=case.arm_id,
                    parent_id=intent.benchmark_id,
                )
                transaction.event("learning.benchmark_case_qualified", row, {"arm_id": case.arm_id})
            result = transaction.insert(
                "benchmark_assessment",
                intent.benchmark_id,
                assessment.model_dump(mode="json"),
                state="immutable",
                parent_id=intent.benchmark_id,
            )
            transaction.event(
                "learning.benchmark_assessed",
                result,
                {"outcome": assessment.gate.outcome, "pairs": len(pairs)},
            )
            return result

        return self.database.execute(
            context,
            "learning.assess_benchmark",
            intent.model_dump(mode="json"),
            "learning.manage",
            commit,
        )
