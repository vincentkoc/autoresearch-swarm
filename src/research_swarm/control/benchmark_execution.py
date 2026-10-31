"""Bind one learned checkpoint and atomically admit all fixed comparator arms."""

from collections.abc import Callable
from typing import Literal

from research_swarm.control.benchmark_execution_models import (
    BenchmarkArm,
    BenchmarkExecution,
    BenchmarkTrialSlot,
    StartPolicyBenchmark,
)
from research_swarm.control.benchmark_guard import benchmark_seed_key
from research_swarm.control.benchmark_models import BenchmarkSeed, PolicyBenchmark
from research_swarm.control.benchmark_trials import BenchmarkTrialAuthority, execution_digest
from research_swarm.control.candidates import propose_candidate, validate_candidate
from research_swarm.control.commands import CreateCampaign, ProposeCandidate, SubmitTrial
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_publication import require_current_training
from research_swarm.control.learning_training_models import PublishedPolicyTraining
from research_swarm.control.models import CampaignData, SearchConfiguration, StoppingRule
from research_swarm.control.preflight import verified_source
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id, utc_now
from research_swarm.projects.source import SourceManifest
from research_swarm.state.artifacts import ArtifactService
from research_swarm.state.database import Database, JsonObject, Transaction

START_SCOPES = (
    "learning.manage",
    "campaign.manage",
    "candidate.propose",
    "candidate.validate",
    "trial.submit",
    "read",
)
SIDES: tuple[Literal["baseline", "challenger"], ...] = ("baseline", "challenger")


def setup_evidence(
    transaction: Transaction, benchmark_id: str
) -> tuple[PolicyBenchmark, PublishedPolicyTraining]:
    row = transaction.get("policy_benchmark", benchmark_id)
    benchmark = PolicyBenchmark.model_validate(row["data"])
    training = require_current_training(transaction, benchmark.request.training_admission_id)
    if (
        row["state"] != "immutable"
        or benchmark.request.benchmark_id != benchmark_id
        or training.training.dataset_digest != benchmark.dataset_digest
        or training.training.behavior_policy_digest != benchmark.baseline_policy.digest
        or training.policy.parent_digest != benchmark.baseline_policy.digest
        or training.policy.actions != benchmark.baseline_policy.actions
    ):
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Benchmark training binding differs")
    for seed in benchmark.request.gate.seeds:
        row = transaction.get(
            "benchmark_seed", benchmark_seed_key(benchmark.research_contract.protocol_digest, seed)
        )
        case = BenchmarkSeed.model_validate(row["data"])
        if (
            row["state"] != "reserved"
            or row["parent_id"] != benchmark_id
            or case.benchmark_id != benchmark_id
            or case.benchmark_digest != benchmark.digest
            or case.protocol_digest != benchmark.research_contract.protocol_digest
            or case.seed != seed
        ):
            raise ResearchError(
                ErrorCode.EVIDENCE_INVALIDATED, "Benchmark seed reservation differs"
            )
    return benchmark, training


class BenchmarkExecutionControl:
    def __init__(
        self,
        database: Database,
        artifacts: ArtifactService,
        create_campaign: Callable[[Transaction, CreateCampaign, str], JsonObject],
        seal_campaign: Callable[[Transaction, str], JsonObject],
        start_campaign: Callable[[Transaction, str], JsonObject],
        submit_trial: Callable[[Transaction, SubmitTrial, BenchmarkTrialAuthority], JsonObject],
    ) -> None:
        self.database, self.artifacts = database, artifacts
        self.create_campaign, self.seal_campaign = create_campaign, seal_campaign
        self.start_campaign, self.submit_trial = start_campaign, submit_trial

    def start(self, context: CommandContext, intent: StartPolicyBenchmark) -> JsonObject:
        for scope in START_SCOPES:
            context.principal.require(scope)
        operation, payload = "learning.start_benchmark", intent.model_dump(mode="json")

        def snapshot(
            transaction: Transaction,
        ) -> JsonObject | tuple[PolicyBenchmark, PublishedPolicyTraining]:
            row = optional_record(transaction, "benchmark_execution", intent.benchmark_id)
            if row is not None:
                if row["data"]["request"] != payload:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Benchmark execution is immutable"
                    )
                return row
            return setup_evidence(transaction, intent.benchmark_id)

        def preflight() -> JsonObject:
            evidence = self.database.store.read(lambda sql: snapshot(Transaction(sql, context)))
            if isinstance(evidence, dict):
                return self.database.execute(
                    context,
                    operation,
                    payload,
                    "learning.manage",
                    lambda tx: tx.get("benchmark_execution", intent.benchmark_id),
                )
            benchmark, training = evidence
            with verified_source(self.artifacts, context, benchmark.initial_source) as (_, source):

                def commit(transaction: Transaction) -> JsonObject:
                    current = snapshot(transaction)
                    if isinstance(current, dict):
                        return current
                    if current != evidence:
                        raise ResearchError(
                            ErrorCode.EVIDENCE_INVALIDATED,
                            "Benchmark changed during source verification",
                        )
                    execution = BenchmarkExecution(
                        request=intent,
                        benchmark_digest=benchmark.digest,
                        training_publication_digest=record_digest(
                            "research.training-publication.v1", training.model_dump(mode="json")
                        ),
                        learned_policy=training.policy,
                        arm_ids=tuple(
                            new_id() for _ in range(2 * len(benchmark.request.gate.seeds))
                        ),
                        actor_id=context.principal.id,
                        recorded_at=utc_now(),
                    )
                    row = transaction.insert(
                        "benchmark_execution",
                        intent.benchmark_id,
                        execution.model_dump(mode="json"),
                        state="immutable",
                        parent_id=intent.benchmark_id,
                    )
                    index = 0
                    for seed in benchmark.request.gate.seeds:
                        for side in SIDES:
                            self._arm(
                                transaction,
                                benchmark,
                                execution,
                                execution.arm_ids[index],
                                seed,
                                side,
                                source,
                            )
                            index += 1
                    transaction.event(
                        "learning.benchmark_started", row, {"arm_count": len(execution.arm_ids)}
                    )
                    return row

                return self.database.execute(context, operation, payload, "learning.manage", commit)

        return self.database.with_preflight(
            context, operation, payload, "learning.manage", preflight
        )

    def _arm(
        self,
        transaction: Transaction,
        benchmark: PolicyBenchmark,
        execution: BenchmarkExecution,
        arm_id: str,
        seed: int,
        side: Literal["baseline", "challenger"],
        source: SourceManifest,
    ) -> None:
        research = benchmark.research_contract
        self.create_campaign(
            transaction,
            CreateCampaign(
                project_id=research.project_id,
                project_revision_id=research.project_revision_id,
                name=f"Policy benchmark {benchmark.request.benchmark_id[:8]} {seed} {side}",
                primary_metric=research.primary_metric,
                input_artifacts=research.input_artifacts,
                limits=benchmark.request.per_arm_limits,
                search=SearchConfiguration(
                    policy="external", version="external-v1", max_parallel_trials=1
                ),
                stopping=StoppingRule(max_trials=2),
            ),
            arm_id,
        )
        self.seal_campaign(transaction, arm_id)
        row = self.start_campaign(transaction, arm_id)
        campaign = CampaignData.model_validate(row["data"])
        if (
            campaign.contract.protocol_digest != research.protocol_digest
            or campaign.contract_digest is None
        ):
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Benchmark arm protocol changed"
            )
        candidate = propose_candidate(
            transaction,
            ProposeCandidate(
                campaign_id=arm_id,
                source_artifact=benchmark.initial_source,
                hypothesis="Frozen benchmark starting comparator",
            ),
            source,
        )
        validation = validate_candidate(transaction, candidate["id"])
        if not validation["data"]["valid"]:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Benchmark initial source is invalid"
            )
        arm = BenchmarkArm(
            benchmark_id=benchmark.request.benchmark_id,
            benchmark_digest=benchmark.digest,
            campaign_id=arm_id,
            campaign_contract_digest=campaign.contract_digest,
            side=side,
            seed=seed,
            policy_digest=benchmark.baseline_policy.digest
            if side == "baseline"
            else execution.learned_policy.digest,
            initial_candidate_id=candidate["id"],
            initial_validation_id=validation["id"],
            comparator_trial_id=new_id(),
            candidate_trial_id=new_id(),
            proposal_request_id=new_id(),
            learning_plan_id=new_id(),
            proposer_id=execution.request.proposer_id,
        )
        transaction.insert(
            "benchmark_arm",
            arm_id,
            arm.model_dump(mode="json"),
            state="immutable",
            campaign_id=arm_id,
            parent_id=benchmark.request.benchmark_id,
        )
        for role, trial_id in (
            ("comparator", arm.comparator_trial_id),
            ("candidate", arm.candidate_trial_id),
        ):
            slot = BenchmarkTrialSlot.model_validate(
                {
                    "benchmark_id": benchmark.request.benchmark_id,
                    "arm_id": arm_id,
                    "trial_id": trial_id,
                    "role": role,
                }
            )
            transaction.insert(
                "benchmark_trial_slot",
                trial_id,
                slot.model_dump(mode="json"),
                state="reserved",
                campaign_id=arm_id,
                parent_id=arm_id,
            )
        self.submit_trial(
            transaction,
            SubmitTrial(
                campaign_id=arm_id,
                candidate_id=candidate["id"],
                validation_id=validation["id"],
                seed=seed,
            ),
            BenchmarkTrialAuthority(arm_id, "comparator", execution_digest(execution)),
        )
