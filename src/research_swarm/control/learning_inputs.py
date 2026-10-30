"""Materialize qualified records as bounded immutable training-input artifacts."""

from __future__ import annotations

from research_swarm.control.learning_datasets import MAX_DATASET_BYTES, require_current_dataset
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_models import LearningPlan, QualifiedTrajectoryDataset
from research_swarm.control.learning_training_models import (
    PreparedPolicyInputs,
    PreparePolicyInputs,
)
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService
from research_swarm.state.database import Database, JsonObject, Transaction


def prepared_inputs(transaction: Transaction, intent: PreparePolicyInputs) -> JsonObject | None:
    previous = optional_record(transaction, "learning_inputs", intent.binding_id)
    if previous is not None and previous["data"]["request"] != intent.model_dump(mode="json"):
        raise ResearchError(
            ErrorCode.IDEMPOTENCY_CONFLICT, "Prepared inputs belong to another dataset"
        )
    return previous


def input_bytes(
    transaction: Transaction, qualified: QualifiedTrajectoryDataset
) -> tuple[bytes, bytes]:
    plan = LearningPlan.model_validate(transaction.get("learning_plan", qualified.plan_id)["data"])
    return (
        canonical_bytes(plan.request.policy.model_dump(mode="json")),
        canonical_bytes(qualified.dataset.model_dump(mode="json")),
    )


def require_prepared_inputs(transaction: Transaction, identifier: str) -> PreparedPolicyInputs:
    row = transaction.get("learning_inputs", identifier)
    data = PreparedPolicyInputs.model_validate(row["data"])
    qualified = require_current_dataset(transaction, data.request.dataset_id)
    behavior, trajectories = input_bytes(transaction, qualified)
    if (
        row["state"] != "immutable"
        or row["id"] != data.request.binding_id
        or row["parent_id"] != data.request.dataset_id
        or data.dataset_digest != qualified.dataset.digest
        or data.plan_digest != qualified.plan_digest
        or data.behavior_policy_digest != qualified.dataset.behavior_policy_digest
        or data.behavior.digest != content_digest(behavior)
        or data.behavior.size_bytes != len(behavior)
        or data.trajectories.digest != content_digest(trajectories)
        or data.trajectories.size_bytes != len(trajectories)
    ):
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Prepared training inputs changed")
    ArtifactCatalog.available(transaction, data.behavior)
    ArtifactCatalog.available(transaction, data.trajectories)
    return data


class LearningInputControl:
    def __init__(self, database: Database, artifacts: ArtifactService) -> None:
        self.database = database
        self.artifacts = artifacts

    def prepare(self, context: CommandContext, intent: PreparePolicyInputs) -> JsonObject:
        for scope in ("learning.manage", "artifact.upload", "read"):
            context.principal.require(scope)
        operation, payload = "learning.prepare_training_inputs", intent.model_dump(mode="json")

        def snapshot(transaction: Transaction) -> tuple[bytes, bytes] | None:
            if prepared_inputs(transaction, intent) is not None:
                return None
            return input_bytes(transaction, require_current_dataset(transaction, intent.dataset_id))

        def preflight() -> JsonObject:
            contents = self.database.store.read(lambda sql: snapshot(Transaction(sql, context)))
            references: tuple[ArtifactRef, ArtifactRef] | None = None
            if contents is not None:
                references = (
                    self.artifacts.publish_bytes(context, contents[0], purpose="learning-behavior"),
                    self.artifacts.publish_bytes(
                        context,
                        contents[1],
                        purpose="learning-trajectories",
                        max_bytes=MAX_DATASET_BYTES,
                    ),
                )

            def commit(transaction: Transaction) -> JsonObject:
                previous = prepared_inputs(transaction, intent)
                if previous is not None:
                    return previous
                if contents is None or references is None:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Prepared input identity changed")
                current = require_current_dataset(transaction, intent.dataset_id)
                if input_bytes(transaction, current) != contents:
                    raise ResearchError(
                        ErrorCode.EVIDENCE_INVALIDATED,
                        "Learning input evidence changed during publication",
                    )
                behavior, trajectories = references
                if any(
                    reference.digest != content_digest(content)
                    or reference.size_bytes != len(content)
                    for reference, content in zip(references, contents, strict=True)
                ):
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "Prepared input bytes differ"
                    )
                for reference, purpose in ((behavior, "behavior"), (trajectories, "trajectories")):
                    ArtifactCatalog.available(transaction, reference)
                    ArtifactCatalog.retain(
                        transaction,
                        ArtifactRetention(
                            artifact_id=reference.id,
                            owner_kind="learning_inputs",
                            owner_id=intent.binding_id,
                            purpose=purpose,
                        ),
                    )
                data = PreparedPolicyInputs(
                    request=intent,
                    dataset_digest=current.dataset.digest,
                    plan_digest=current.plan_digest,
                    behavior_policy_digest=current.dataset.behavior_policy_digest,
                    behavior=behavior,
                    trajectories=trajectories,
                    actor_id=context.principal.id,
                    recorded_at=utc_now(),
                )
                row = transaction.insert(
                    "learning_inputs",
                    intent.binding_id,
                    data.model_dump(mode="json"),
                    state="immutable",
                    parent_id=intent.dataset_id,
                )
                transaction.event(
                    "learning.inputs_prepared", row, {"dataset_id": intent.dataset_id}
                )
                return row

            return self.database.execute(context, operation, payload, "learning.manage", commit)

        return self.database.with_preflight(
            context, operation, payload, "learning.manage", preflight
        )
