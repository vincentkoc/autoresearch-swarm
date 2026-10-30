"""Publish one checked policy checkpoint without adopting it or launching more work."""

from contextlib import ExitStack

from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_training_evidence import TrainingEvidence, training_evidence
from research_swarm.control.learning_training_models import (
    PublishedPolicyTraining,
    PublishPolicyTraining,
)
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.canonical import content_digest, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.learning import CategoricalPolicy, PolicyUpdate
from research_swarm.domain.types import utc_now
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService
from research_swarm.state.database import Database, JsonObject, Transaction

MAX_TRAINING_OUTPUT_BYTES = 64 * 1024


def require_current_training(
    transaction: Transaction, admission_id: str
) -> PublishedPolicyTraining:
    row = transaction.get("learning_publication", admission_id)
    published = PublishedPolicyTraining.model_validate(row["data"])
    current = training_evidence(transaction, admission_id)
    if (
        row["state"] != "immutable"
        or row["parent_id"] != admission_id
        or row["campaign_id"] != current.training.campaign_id
        or published.request.admission_id != admission_id
        or published.training != current.training
        or published.bindings_digest != current.digest
        or published.checker_job_id != current.checker_job_id
        or published.checker_attempt_id != current.checker_attempt_id
        or published.update_error != current.update_error
        or published.policy.actions != current.behavior.actions
        or published.policy.parent_digest != current.behavior.digest
    ):
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Published training evidence changed")
    return published


def check_output_lineage(
    evidence: TrainingEvidence, policy: CategoricalPolicy, update: PolicyUpdate
) -> None:
    if (
        update.policy != policy
        or update.behavior_policy_digest != evidence.behavior.digest
        or update.dataset_digest != evidence.training.dataset_digest
        or update.reward_spec_digest != evidence.reward_spec_digest
        or update.algorithm != evidence.configuration.algorithm
        or update.learning_rate != evidence.configuration.learning_rate
        or update.epochs != evidence.configuration.epochs
        or update.sample_count != evidence.sample_count
        or policy.actions != evidence.behavior.actions
        or policy.parent_digest != evidence.behavior.digest
    ):
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Training output provenance differs")


class LearningPublicationControl:
    def __init__(self, database: Database, artifacts: ArtifactService) -> None:
        self.database = database
        self.artifacts = artifacts

    def publish(self, context: CommandContext, intent: PublishPolicyTraining) -> JsonObject:
        context.principal.require("read")
        operation, payload = "learning.publish_training", intent.model_dump(mode="json")

        def snapshot(transaction: Transaction) -> TrainingEvidence | JsonObject:
            previous = optional_record(transaction, "learning_publication", intent.admission_id)
            return (
                previous
                if previous is not None
                else training_evidence(transaction, intent.admission_id)
            )

        def preflight() -> JsonObject:
            evidence = self.database.store.read(lambda sql: snapshot(Transaction(sql, context)))
            if isinstance(evidence, dict):
                return self.database.execute(
                    context,
                    operation,
                    payload,
                    "learning.manage",
                    lambda tx: tx.get("learning_publication", intent.admission_id),
                )
            references = (evidence.training.policy_artifact, evidence.training.update_report)
            if any(reference.size_bytes > MAX_TRAINING_OUTPUT_BYTES for reference in references):
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Training output exceeds its size bound"
                )
            with ExitStack() as stack:
                contents = []
                for reference in references:
                    stream = stack.enter_context(self.artifacts.reader(context, reference))
                    content = stream.read(MAX_TRAINING_OUTPUT_BYTES + 1)
                    if (
                        len(content) != reference.size_bytes
                        or content_digest(content) != reference.digest
                    ):
                        raise ResearchError(
                            ErrorCode.ARTIFACT_UNVERIFIED, "Training output bytes differ"
                        )
                    contents.append(content)
                try:
                    policy = CategoricalPolicy.model_validate(strict_json_loads(contents[0]))
                    update = PolicyUpdate.model_validate(strict_json_loads(contents[1]))
                except ValueError as error:
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "Invalid training output JSON"
                    ) from error
                check_output_lineage(evidence, policy, update)

                def commit(transaction: Transaction) -> JsonObject:
                    current = snapshot(transaction)
                    if isinstance(current, dict):
                        return current
                    if current != evidence:
                        raise ResearchError(
                            ErrorCode.EVIDENCE_INVALIDATED,
                            "Training changed during output verification",
                        )
                    for purpose, reference in zip(("policy", "update"), references, strict=True):
                        ArtifactCatalog.retain(
                            transaction,
                            ArtifactRetention(
                                artifact_id=reference.id,
                                owner_kind="learning_publication",
                                owner_id=intent.admission_id,
                                purpose=purpose,
                            ),
                        )
                    data = PublishedPolicyTraining(
                        request=intent,
                        training=evidence.training,
                        bindings_digest=evidence.digest,
                        policy=policy,
                        update_digest=record_digest(
                            "research.policy-update.v1", update.model_dump(mode="json")
                        ),
                        checker_job_id=evidence.checker_job_id,
                        checker_attempt_id=evidence.checker_attempt_id,
                        update_error=evidence.update_error,
                        actor_id=context.principal.id,
                        recorded_at=utc_now(),
                    )
                    row = transaction.insert(
                        "learning_publication",
                        intent.admission_id,
                        data.model_dump(mode="json"),
                        state="immutable",
                        campaign_id=evidence.training.campaign_id,
                        parent_id=intent.admission_id,
                    )
                    transaction.event(
                        "learning.training_published",
                        row,
                        {"trial_id": evidence.training.trial_id, "policy_digest": policy.digest},
                    )
                    return row

                return self.database.execute(context, operation, payload, "learning.manage", commit)

        return self.database.with_preflight(
            context, operation, payload, "learning.manage", preflight
        )
