"""Freeze fresh seed cases and one future fitted policy before training admission."""

from research_swarm.control.benchmark_guard import benchmark_plan_key, benchmark_seed_key
from research_swarm.control.benchmark_models import (
    BenchmarkReservation,
    BenchmarkSeed,
    FreezePolicyBenchmark,
    PolicyBenchmark,
)
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_inputs import require_prepared_inputs
from research_swarm.control.learning_models import LearningPlan, QualifiedTrajectoryDataset
from research_swarm.control.learning_profiles import require_training_profile
from research_swarm.control.models import CampaignData, CandidateData, ProjectRevisionData
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Database, JsonObject, Transaction


class BenchmarkControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def freeze(self, context: CommandContext, intent: FreezePolicyBenchmark) -> JsonObject:
        context.principal.require("read")
        payload = intent.model_dump(mode="json")

        def commit(transaction: Transaction) -> JsonObject:
            previous = optional_record(transaction, "policy_benchmark", intent.benchmark_id)
            if previous is not None:
                if previous["data"]["request"] != payload:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Policy benchmark is immutable"
                    )
                return previous
            inputs = require_prepared_inputs(transaction, intent.inputs_id)
            profile = require_training_profile(transaction, intent.profile_id)
            qualified = QualifiedTrajectoryDataset.model_validate(
                transaction.get("learning_dataset", inputs.request.dataset_id)["data"]
            )
            plan = LearningPlan.model_validate(
                transaction.get("learning_plan", qualified.plan_id)["data"]
            )
            episodes = tuple(episode for episode in plan.episodes if episode.split == "development")
            first = episodes[0]
            contract = CampaignData.model_validate(
                transaction.get("campaign", first.campaign_id)["data"]
            ).contract
            initial = CandidateData.model_validate(
                transaction.get("candidate", first.comparator_candidate_id)["data"]
            )
            for episode in episodes:
                other = CampaignData.model_validate(
                    transaction.get("campaign", episode.campaign_id)["data"]
                ).contract
                candidate = CandidateData.model_validate(
                    transaction.get("candidate", episode.comparator_candidate_id)["data"]
                )
                if (
                    other.project_revision_id != contract.project_revision_id
                    or other.protocol_digest != contract.protocol_digest
                    or other.input_artifacts != contract.input_artifacts
                    or other.primary_metric != contract.primary_metric
                    or episode.comparison_digest != first.comparison_digest
                    or candidate.source.tree_digest != initial.source.tree_digest
                    or candidate.source_artifact != initial.source_artifact
                ):
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE,
                        "Seed benchmark requires one frozen research family and starting source",
                    )
            revision = ProjectRevisionData.model_validate(
                transaction.get("project_revision", contract.project_revision_id)["data"]
            )
            attempts = next(
                item.limit for item in intent.per_arm_limits if item.dimension == "attempts"
            )
            if attempts < 2 * len(revision.resolved.manifest.stages):
                raise ResearchError(
                    ErrorCode.BUDGET_EXHAUSTED, "Arm cap cannot cover both declared trial graphs"
                )
            prior_fit = transaction.connection.execute(
                "SELECT a.id FROM records a JOIN records i ON i.workspace_id=a.workspace_id "
                "AND i.kind='learning_inputs' "
                "AND i.id=json_extract(a.payload,'$.request.inputs_id') "
                "WHERE a.workspace_id=? AND a.kind='learning_admission' "
                "AND json_extract(i.payload,'$.plan_digest')=? LIMIT 1",
                (transaction.workspace_id, inputs.plan_digest),
            ).fetchone()
            ordinary_fit = transaction.connection.execute(
                "SELECT t.id FROM records i JOIN records c "
                "ON c.workspace_id=i.workspace_id AND c.kind='campaign' "
                "AND json_extract(c.payload,'$.contract.input_artifacts.behavior.digest')="
                "json_extract(i.payload,'$.behavior.digest') "
                "AND json_extract(c.payload,'$.contract.input_artifacts.behavior.size_bytes')="
                "json_extract(i.payload,'$.behavior.size_bytes') "
                "AND json_extract(c.payload,'$.contract.input_artifacts.trajectories.digest')="
                "json_extract(i.payload,'$.trajectories.digest') "
                "AND json_extract(c.payload,'$.contract.input_artifacts.trajectories.size_bytes')="
                "json_extract(i.payload,'$.trajectories.size_bytes') "
                "JOIN records t ON t.workspace_id=c.workspace_id "
                "AND t.kind='trial' AND t.campaign_id=c.id "
                "WHERE i.workspace_id=? AND i.kind='learning_inputs' "
                "AND json_extract(i.payload,'$.plan_digest')=? LIMIT 1",
                (transaction.workspace_id, inputs.plan_digest),
            ).fetchone()
            if (
                prior_fit is not None
                or ordinary_fit is not None
                or optional_record(transaction, "learning_admission", intent.training_admission_id)
                is not None
            ):
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE,
                    "Benchmark must precede fitting this learning plan",
                )
            reservations = (
                ("benchmark_plan", benchmark_plan_key(inputs.plan_digest)),
                ("benchmark_training", intent.training_admission_id),
                *(
                    ("benchmark_seed", benchmark_seed_key(contract.protocol_digest, seed))
                    for seed in intent.gate.seeds
                ),
            )
            if any(
                optional_record(transaction, kind, identifier) is not None
                for kind, identifier in reservations
            ):
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE,
                    "Learning plan, admission or seed panel is already reserved",
                )
            placeholders = ",".join("?" for _ in intent.gate.seeds)
            exposed = transaction.connection.execute(
                "SELECT id FROM records WHERE workspace_id=? AND kind='trial' "
                "AND json_extract(payload,'$.spec.protocol_digest')=? "
                f"AND json_extract(payload,'$.spec.seed') IN ({placeholders}) LIMIT 1",
                (transaction.workspace_id, contract.protocol_digest, *intent.gate.seeds),
            ).fetchone()
            if exposed is not None or any(
                episode.seed in intent.gate.seeds for episode in episodes
            ):
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE,
                    "Held-out seed was already admitted or used in development",
                )
            watermark = transaction.connection.execute(
                "SELECT next_sequence-1 FROM workspaces WHERE id=?", (transaction.workspace_id,)
            ).one()[0]
            benchmark = PolicyBenchmark(
                request=intent,
                learning_plan_digest=inputs.plan_digest,
                inputs_digest=record_digest(
                    "research.prepared-policy-inputs.v1", inputs.model_dump(mode="json")
                ),
                dataset_digest=inputs.dataset_digest,
                training_profile_digest=profile.digest,
                research_contract=contract,
                initial_source=initial.source_artifact,
                comparison_digest=first.comparison_digest,
                baseline_policy=plan.request.policy,
                reward_spec=plan.request.reward_spec,
                evidence_through=watermark,
                actor_id=context.principal.id,
                recorded_at=utc_now(),
            )
            for purpose, reference in (
                ("initial_source", initial.source_artifact),
                *(("input", reference) for reference in contract.input_artifacts.values()),
            ):
                ArtifactCatalog.available(transaction, reference)
                ArtifactCatalog.retain(
                    transaction,
                    ArtifactRetention(
                        artifact_id=reference.id,
                        owner_kind="policy_benchmark",
                        owner_id=intent.benchmark_id,
                        purpose=purpose,
                    ),
                )
            row = transaction.insert(
                "policy_benchmark",
                intent.benchmark_id,
                benchmark.model_dump(mode="json"),
                state="immutable",
                parent_id=intent.training_admission_id,
            )
            reservation = BenchmarkReservation(
                benchmark_id=intent.benchmark_id, benchmark_digest=benchmark.digest
            )
            for kind, identifier in reservations[:2]:
                transaction.insert(
                    kind,
                    identifier,
                    reservation.model_dump(mode="json"),
                    state="reserved",
                    parent_id=intent.benchmark_id,
                )
            for seed in intent.gate.seeds:
                transaction.insert(
                    "benchmark_seed",
                    benchmark_seed_key(contract.protocol_digest, seed),
                    BenchmarkSeed(
                        **reservation.model_dump(),
                        protocol_digest=contract.protocol_digest,
                        seed=seed,
                    ).model_dump(mode="json"),
                    state="reserved",
                    parent_id=intent.benchmark_id,
                )
            transaction.event(
                "learning.benchmark_frozen", row, {"case_count": len(intent.gate.seeds)}
            )
            return row

        return self.database.execute(
            context, "learning.freeze_benchmark", payload, "learning.manage", commit
        )
