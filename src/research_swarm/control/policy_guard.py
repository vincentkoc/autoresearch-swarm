"""Pin adoption authority; old plans never resolve a mutable latest-policy pointer."""

from typing import TYPE_CHECKING

from research_swarm.control.benchmark_models import PolicyBenchmark
from research_swarm.control.models import CampaignData, CandidateData
from research_swarm.control.policy_models import (
    FrozenPolicyBinding,
    PolicyAdoption,
    PolicyBinding,
    PolicyFamily,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction

if TYPE_CHECKING:
    from research_swarm.control.learning_models import (
        FreezeLearningPlan,
        FrozenEpisode,
        LearningPlan,
    )


def policy_family(benchmark: PolicyBenchmark) -> PolicyFamily:
    return PolicyFamily(
        protocol_digest=benchmark.research_contract.protocol_digest,
        comparison_digest=benchmark.comparison_digest,
        reward_spec=benchmark.reward_spec,
        actions=benchmark.baseline_policy.actions,
        initial_source_digest=benchmark.initial_source.digest,
    )


def pinned_binding(transaction: Transaction, snapshot: FrozenPolicyBinding) -> PolicyBinding:
    reference = snapshot.reference
    if snapshot.adoption_id is None:
        row = transaction.get("policy_binding_origin", reference.binding_id)
        binding = PolicyBinding.model_validate(row["data"])
        revision = 1
    else:
        # Benchmark planning uses LearningControl; defer this reverse evidence dependency.
        from research_swarm.control.policy_adoption import require_current_adoption

        row = transaction.get("policy_adoption", snapshot.adoption_id)
        adoption: PolicyAdoption = require_current_adoption(transaction, snapshot.adoption_id)
        binding, revision = adoption.binding, adoption.after_revision
    if (
        row["state"] != "immutable"
        or binding.request.binding_id != reference.binding_id
        or binding.digest != snapshot.binding_digest
        or binding.policy.digest != reference.policy_digest
        or binding.adoption_id != snapshot.adoption_id
        or revision != reference.revision
    ):
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Pinned policy binding differs")
    return binding


def check_family(
    transaction: Transaction,
    intent: "FreezeLearningPlan",
    episodes: tuple["FrozenEpisode", ...],
    binding: PolicyBinding,
) -> None:
    if (
        intent.policy != binding.policy
        or intent.reward_spec != binding.family.reward_spec
        or intent.action_artifacts != binding.action_artifacts
    ):
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Learning policy family differs")
    for episode in episodes:
        campaign = CampaignData.model_validate(
            transaction.get("campaign", episode.campaign_id)["data"]
        )
        comparator = CandidateData.model_validate(
            transaction.get("candidate", episode.comparator_candidate_id)["data"]
        )
        if (
            campaign.contract.protocol_digest != binding.family.protocol_digest
            or episode.comparison_digest != binding.family.comparison_digest
            or comparator.source_artifact.digest != binding.family.initial_source_digest
        ):
            raise ResearchError(
                ErrorCode.COMPARISON_INELIGIBLE, "Learning episode is outside the policy family"
            )


def freeze_binding(
    transaction: Transaction,
    intent: "FreezeLearningPlan",
    episodes: tuple["FrozenEpisode", ...],
) -> FrozenPolicyBinding | None:
    reference = intent.policy_binding
    if reference is None:
        return None
    row = transaction.get("policy_binding", reference.binding_id)
    binding = PolicyBinding.model_validate(row["data"])
    if (
        row["state"] != "active"
        or row["revision"] != reference.revision
        or binding.policy.digest != reference.policy_digest
    ):
        raise ResearchError(ErrorCode.REVISION_CONFLICT, "Policy binding revision changed")
    snapshot = FrozenPolicyBinding(
        reference=reference,
        binding_digest=binding.digest,
        adoption_id=binding.adoption_id,
    )
    if pinned_binding(transaction, snapshot) != binding:
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Policy binding provenance changed")
    check_family(transaction, intent, episodes, binding)
    return snapshot


def require_plan_binding(transaction: Transaction, plan: "LearningPlan") -> None:
    if plan.request.policy_binding is None and plan.policy_binding is None:
        return
    if plan.policy_binding is None or plan.request.policy_binding != plan.policy_binding.reference:
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Learning plan lost its policy binding")
    check_family(
        transaction, plan.request, plan.episodes, pinned_binding(transaction, plan.policy_binding)
    )
