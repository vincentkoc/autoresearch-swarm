"""Adopt a currently qualified learned policy with a separate atomic binding CAS."""

from contextvars import ContextVar
from dataclasses import dataclass, field

from research_swarm.control.benchmark_assessment import require_current_assessment
from research_swarm.control.benchmark_models import PolicyBenchmark
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_models import LearningPlan, QualifiedTrajectoryDataset
from research_swarm.control.learning_publication import require_current_training
from research_swarm.control.learning_training_models import PreparedPolicyInputs
from research_swarm.control.policy_guard import pinned_binding, policy_family
from research_swarm.control.policy_models import (
    AdoptPolicy,
    CreatePolicyBinding,
    FrozenPolicyBinding,
    PolicyAdoption,
    PolicyBinding,
    PolicyBindingRef,
)
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.sql import next_counter


@dataclass
class _ProofWalk:
    transaction: Transaction
    verified: dict[str, PolicyAdoption] = field(default_factory=dict)
    active: set[str] = field(default_factory=set)


_proof_walk: ContextVar[_ProofWalk | None] = ContextVar("policy_adoption_proof", default=None)
MAX_ADOPTION_DEPTH = 16


def require_current_adoption(transaction: Transaction, identifier: str) -> PolicyAdoption:
    walk = _proof_walk.get()
    token = None
    if walk is None or walk.transaction is not transaction:
        walk = _ProofWalk(transaction)
        token = _proof_walk.set(walk)
    try:
        if identifier in walk.verified:
            return walk.verified[identifier]
        if identifier in walk.active:
            raise ResearchError(
                ErrorCode.EVIDENCE_INVALIDATED, "Policy adoption ancestry contains a cycle"
            )
        if len(walk.active) >= MAX_ADOPTION_DEPTH:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "Policy adoption ancestry exceeds this profile's depth bound",
            )
        walk.active.add(identifier)
        try:
            result = _verify_current_adoption(transaction, identifier)
            walk.verified[identifier] = result
            return result
        finally:
            walk.active.remove(identifier)
    finally:
        if token is not None:
            _proof_walk.reset(token)


def snapshot_binding(row: JsonObject, binding: PolicyBinding) -> FrozenPolicyBinding:
    return FrozenPolicyBinding(
        reference=PolicyBindingRef(
            binding_id=row["id"],
            revision=row["revision"],
            policy_digest=binding.policy.digest,
        ),
        binding_digest=binding.digest,
        adoption_id=binding.adoption_id,
    )


def retain_binding(transaction: Transaction, binding: PolicyBinding) -> None:
    for purpose, artifact in (
        ("policy", binding.policy_artifact),
        *(("action", artifact) for artifact in binding.action_artifacts.values()),
    ):
        ArtifactCatalog.available(transaction, artifact)
        ArtifactCatalog.retain(
            transaction,
            ArtifactRetention(
                artifact_id=artifact.id,
                owner_kind="policy_binding",
                owner_id=binding.request.binding_id,
                purpose=purpose,
            ),
        )


def _verify_current_adoption(transaction: Transaction, identifier: str) -> PolicyAdoption:
    row = transaction.get("policy_adoption", identifier)
    adoption = PolicyAdoption.model_validate(row["data"])
    assessment = require_current_assessment(transaction, adoption.request.benchmark_id)
    benchmark = PolicyBenchmark.model_validate(
        transaction.get("policy_benchmark", adoption.request.benchmark_id)["data"]
    )
    training = require_current_training(transaction, benchmark.request.training_admission_id)
    binding = adoption.binding
    previous = pinned_binding(transaction, adoption.before_binding)
    if (
        row["state"] != "immutable"
        or row["parent_id"] != adoption.request.binding_id
        or adoption.before_binding.reference.binding_id != adoption.request.binding_id
        or adoption.before_binding.reference.revision != adoption.before_revision
        or adoption.before_policy_digest != previous.policy.digest
        or adoption.after_revision != next_counter(adoption.before_revision)
        or binding.request != previous.request
        or binding.family != previous.family
        or binding.family != policy_family(benchmark)
        or binding.action_artifacts != previous.action_artifacts
        or binding.policy != training.policy
        or binding.policy_artifact != training.training.policy_artifact
        or binding.adoption_id != identifier
        or assessment.gate.outcome != "promote"
        or assessment.baseline_policy_digest != previous.policy.digest
        or assessment.challenger_policy_digest != binding.policy.digest
        or adoption.assessment_digest != assessment.digest
        or adoption.training_publication_digest
        != record_digest("research.training-publication.v1", training.model_dump(mode="json"))
    ):
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Policy adoption evidence changed")
    ArtifactCatalog.available(transaction, binding.policy_artifact)
    return adoption


class PolicyAdoptionControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def create(self, context: CommandContext, intent: CreatePolicyBinding) -> JsonObject:
        context.principal.require("read")

        def commit(transaction: Transaction) -> JsonObject:
            previous = optional_record(transaction, "policy_binding_origin", intent.binding_id)
            if previous is not None:
                if previous["data"]["request"] != intent.model_dump(mode="json"):
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Policy binding origin is immutable"
                    )
                return previous
            benchmark = PolicyBenchmark.model_validate(
                transaction.get("policy_benchmark", intent.benchmark_id)["data"]
            )
            inputs = PreparedPolicyInputs.model_validate(
                transaction.get("learning_inputs", benchmark.request.inputs_id)["data"]
            )
            dataset = QualifiedTrajectoryDataset.model_validate(
                transaction.get("learning_dataset", inputs.request.dataset_id)["data"]
            )
            plan = LearningPlan.model_validate(
                transaction.get("learning_plan", dataset.plan_id)["data"]
            )
            if (
                plan.digest != benchmark.learning_plan_digest
                or plan.request.policy != benchmark.baseline_policy
                or inputs.plan_digest != benchmark.learning_plan_digest
                or inputs.dataset_digest != benchmark.dataset_digest
                or inputs.behavior_policy_digest != benchmark.baseline_policy.digest
                or record_digest(
                    "research.prepared-policy-inputs.v1", inputs.model_dump(mode="json")
                )
                != benchmark.inputs_digest
            ):
                raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Policy binding origin differs")
            binding = PolicyBinding(
                request=intent,
                family=policy_family(benchmark),
                policy=benchmark.baseline_policy,
                policy_artifact=inputs.behavior,
                action_artifacts=plan.request.action_artifacts,
                initialized_by=context.principal.id,
                initialized_at=utc_now(),
            )
            retain_binding(transaction, binding)
            transaction.insert(
                "policy_binding", intent.binding_id, binding.model_dump(mode="json"), state="active"
            )
            origin = transaction.insert(
                "policy_binding_origin",
                intent.binding_id,
                binding.model_dump(mode="json"),
                state="immutable",
            )
            transaction.event(
                "learning.policy_bound", origin, {"policy_digest": binding.policy.digest}
            )
            return origin

        return self.database.execute(
            context,
            "learning.create_policy_binding",
            intent.model_dump(mode="json"),
            "learning.manage",
            commit,
        )

    def adopt(self, context: CommandContext, intent: AdoptPolicy) -> JsonObject:
        context.principal.require("read")

        def commit(transaction: Transaction) -> JsonObject:
            row = transaction.get("policy_binding", intent.binding_id)
            binding = PolicyBinding.model_validate(row["data"])
            if row["state"] != "active" or context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Policy binding revision changed")
            before = snapshot_binding(row, binding)
            if pinned_binding(transaction, before) != binding:
                raise ResearchError(
                    ErrorCode.EVIDENCE_INVALIDATED, "Current policy binding differs"
                )
            assessment = require_current_assessment(transaction, intent.benchmark_id)
            benchmark = PolicyBenchmark.model_validate(
                transaction.get("policy_benchmark", intent.benchmark_id)["data"]
            )
            training = require_current_training(
                transaction, benchmark.request.training_admission_id
            )
            if (
                assessment.gate.outcome != "promote"
                or assessment.baseline_policy_digest != binding.policy.digest
                or assessment.challenger_policy_digest != training.policy.digest
                or binding.family != policy_family(benchmark)
                or training.policy.actions != binding.policy.actions
                or training.policy.parent_digest != binding.policy.digest
            ):
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE,
                    "Policy adoption requires a passing comparable challenger "
                    "to the current baseline",
                )
            identifier = new_id()
            updated_binding = binding.model_copy(
                update={
                    "policy": training.policy,
                    "policy_artifact": training.training.policy_artifact,
                    "adoption_id": identifier,
                }
            )
            retain_binding(transaction, updated_binding)
            adoption = PolicyAdoption(
                request=intent,
                before_revision=row["revision"],
                after_revision=next_counter(row["revision"]),
                before_policy_digest=binding.policy.digest,
                before_binding=before,
                binding=updated_binding,
                assessment_digest=assessment.digest,
                training_publication_digest=record_digest(
                    "research.training-publication.v1", training.model_dump(mode="json")
                ),
                actor_id=context.principal.id,
                recorded_at=utc_now(),
            )
            result = transaction.insert(
                "policy_adoption",
                identifier,
                adoption.model_dump(mode="json"),
                state="immutable",
                parent_id=intent.binding_id,
            )
            # The new chain must itself be usable before it becomes the active pointer.
            require_current_adoption(transaction, identifier)
            transaction.update(
                "policy_binding",
                intent.binding_id,
                expected_revision=row["revision"],
                data=updated_binding.model_dump(mode="json"),
                state="active",
            )
            transaction.event(
                "learning.policy_adopted",
                result,
                {
                    "binding_id": intent.binding_id,
                    "policy_digest": training.policy.digest,
                    "binding_revision": adoption.after_revision,
                },
            )
            return result

        return self.database.execute(
            context,
            "learning.adopt_policy",
            intent.model_dump(mode="json"),
            "learning.adopt",
            commit,
        )
