"""Explicitly approve an immutable ordinary project as a policy-training profile."""

from __future__ import annotations

from zipfile import ZipFile

from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_training_models import (
    PolicyTrainingConfiguration,
    PolicyTrainingProfile,
    RegisterPolicyTrainingProfile,
)
from research_swarm.control.models import ProjectRevisionData
from research_swarm.control.preflight import verified_source
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.canonical import content_digest, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import Direction
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService
from research_swarm.state.database import Database, JsonObject, Transaction

MAX_CONFIGURATION_BYTES = 64 * 1024


def profile_revision_digest(revision: ProjectRevisionData) -> str:
    return record_digest("research.learning-project-revision.v1", revision.model_dump(mode="json"))


def check_profile_contract(
    revision: ProjectRevisionData,
    intent: RegisterPolicyTrainingProfile,
    configuration: PolicyTrainingConfiguration,
) -> None:
    manifest = revision.resolved.manifest
    stages = {stage.id: stage for stage in manifest.stages}
    trainer = stages.get(intent.trainer_stage_id)
    checker = stages.get(intent.checker_stage_id)
    metric = next(
        (item for item in manifest.evaluation.metrics if item.name == "update_error"), None
    )
    entries = {entry.path for entry in revision.resolved.source.entries}
    if (
        len(stages) != 2
        or trainer is None
        or checker is None
        or trainer.kind not in {"train", "policy_train"}
        or checker.kind != "evaluate"
        or trainer.dependencies
        or checker.dependencies != (trainer.id,)
        or len(trainer.argv) != 2
        or len(checker.argv) != 2
        or trainer.argv[0] != "python"
        or checker.argv[0] != "python"
        or trainer.argv[1] not in entries
        or checker.argv[1] not in entries
        or any(character in trainer.argv[1] for character in "*?[")
        or manifest.mutation.allow != (trainer.argv[1],)
        or trainer.argv[1] in revision.resolved.protected_paths
        or checker.argv[1] not in manifest.evaluation.protected_paths
        or set(trainer.required_artifacts) != {"behavior", "trajectories"}
        or set(trainer.output_names) != {"policy", "update"}
        or set(checker.required_artifacts) != {"behavior", "trajectories", "policy", "update"}
        or manifest.evaluation.measurement_trust not in {"authenticated", "protected"}
        or manifest.evaluation.expected_members != frozenset({configuration.panel})
        or metric is None
        or metric.direction != Direction.MINIMIZE
        or metric.unit != "absolute_parameter_error"
        or metric.aggregation != "maximum_absolute_residual"
        or manifest.data.manifest_path not in revision.resolved.protected_paths
    ):
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Project does not match the training profile"
        )


def require_training_profile(transaction: Transaction, identifier: str) -> PolicyTrainingProfile:
    row = transaction.get("learning_profile", identifier)
    profile = PolicyTrainingProfile.model_validate(row["data"])
    revision = ProjectRevisionData.model_validate(
        transaction.get("project_revision", profile.request.project_revision_id)["data"]
    )
    if (
        row["state"] != "immutable"
        or row["parent_id"] != profile.request.project_revision_id
        or profile.request.profile_id != identifier
        or revision.project_id != profile.project_id
        or revision.source_artifact != profile.source_artifact
        or profile_revision_digest(revision) != profile.project_revision_digest
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Approved training profile changed")
    check_profile_contract(revision, profile.request, profile.configuration)
    ArtifactCatalog.available(transaction, profile.source_artifact)
    return profile


class LearningProfileControl:
    def __init__(self, database: Database, artifacts: ArtifactService) -> None:
        self.database = database
        self.artifacts = artifacts

    def register(
        self, context: CommandContext, intent: RegisterPolicyTrainingProfile
    ) -> JsonObject:
        context.principal.require("read")
        operation, payload = "learning.register_training_profile", intent.model_dump(mode="json")

        def existing(transaction: Transaction) -> JsonObject | None:
            row = optional_record(transaction, "learning_profile", intent.profile_id)
            if row is not None and row["data"]["request"] != payload:
                raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Training profile is immutable")
            return row

        def snapshot(transaction: Transaction) -> ProjectRevisionData | None:
            if existing(transaction) is not None:
                return None
            return ProjectRevisionData.model_validate(
                transaction.get("project_revision", intent.project_revision_id)["data"]
            )

        def preflight() -> JsonObject:
            revision = self.database.store.read(lambda sql: snapshot(Transaction(sql, context)))
            if revision is None:
                return self.database.execute(
                    context,
                    operation,
                    payload,
                    "learning.profile.manage",
                    lambda tx: existing(tx) or self._missing(),
                )
            with verified_source(self.artifacts, context, revision.source_artifact) as (
                bundle,
                source,
            ):
                path = revision.resolved.manifest.data.manifest_path
                if source != revision.resolved.source:
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "Training project source changed"
                    )
                entry = next((entry for entry in source.entries if entry.path == path), None)
                if entry is None or entry.size_bytes > MAX_CONFIGURATION_BYTES:
                    raise ResearchError(
                        ErrorCode.PROJECT_CONTRACT_INVALID,
                        "Training configuration exceeds its bound",
                    )
                with ZipFile(bundle) as archive, archive.open(path) as stream:
                    content = stream.read(MAX_CONFIGURATION_BYTES + 1)
                if len(content) != entry.size_bytes or content_digest(content) != entry.digest:
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "Training configuration bytes differ"
                    )
                try:
                    configuration = PolicyTrainingConfiguration.model_validate(
                        strict_json_loads(content)
                    )
                except ValueError as error:
                    raise ResearchError(
                        ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid training configuration"
                    ) from error
                check_profile_contract(revision, intent, configuration)

                def commit(transaction: Transaction) -> JsonObject:
                    previous = existing(transaction)
                    if previous is not None:
                        return previous
                    current = ProjectRevisionData.model_validate(
                        transaction.get("project_revision", intent.project_revision_id)["data"]
                    )
                    if current != revision:
                        raise ResearchError(
                            ErrorCode.STALE_FENCE, "Training project changed during approval"
                        )
                    ArtifactCatalog.available(transaction, revision.source_artifact)
                    ArtifactCatalog.retain(
                        transaction,
                        ArtifactRetention(
                            artifact_id=revision.source_artifact.id,
                            owner_kind="learning_profile",
                            owner_id=intent.profile_id,
                            purpose="approved_source",
                        ),
                    )
                    data = PolicyTrainingProfile(
                        request=intent,
                        configuration=configuration,
                        project_id=revision.project_id,
                        project_revision_digest=profile_revision_digest(revision),
                        source_artifact=revision.source_artifact,
                        approved_by=context.principal.id,
                        recorded_at=utc_now(),
                    )
                    row = transaction.insert(
                        "learning_profile",
                        intent.profile_id,
                        data.model_dump(mode="json"),
                        state="immutable",
                        parent_id=intent.project_revision_id,
                    )
                    transaction.event(
                        "learning.training_profile_registered",
                        row,
                        {"project_revision_id": intent.project_revision_id},
                    )
                    return row

                return self.database.execute(
                    context, operation, payload, "learning.profile.manage", commit
                )

        return self.database.with_preflight(
            context, operation, payload, "learning.profile.manage", preflight
        )

    @staticmethod
    def _missing() -> JsonObject:
        raise ResearchError(ErrorCode.STALE_FENCE, "Training profile disappeared")
