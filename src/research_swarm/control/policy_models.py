"""Explicit policy adoption and immutable references used by future learning plans."""

from __future__ import annotations

from pydantic import Field

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.learning import CategoricalPolicy, PolicyAction, RewardSpec
from research_swarm.domain.types import ID, Digest, Name, Revision, Timestamp, VersionedRecord


class CreatePolicyBinding(VersionedRecord):
    binding_id: ID
    benchmark_id: ID


class AdoptPolicy(VersionedRecord):
    binding_id: ID
    benchmark_id: ID


class PolicyFamily(VersionedRecord):
    protocol_digest: Digest
    comparison_digest: Digest
    reward_spec: RewardSpec
    actions: tuple[PolicyAction, ...] = Field(min_length=2, max_length=32)
    initial_source_digest: Digest


class PolicyBinding(VersionedRecord):
    request: CreatePolicyBinding
    family: PolicyFamily
    policy: CategoricalPolicy
    policy_artifact: ArtifactRef
    action_artifacts: dict[Name, ArtifactRef] = Field(min_length=2, max_length=32)
    adoption_id: ID | None = None
    initialized_by: ID
    initialized_at: Timestamp

    @property
    def digest(self) -> str:
        return record_digest("research.policy-binding.v1", self.model_dump(mode="json"))


class PolicyAdoption(VersionedRecord):
    request: AdoptPolicy
    before_revision: Revision
    after_revision: Revision
    before_policy_digest: Digest
    before_binding: FrozenPolicyBinding
    binding: PolicyBinding
    assessment_digest: Digest
    training_publication_digest: Digest
    actor_id: ID
    recorded_at: Timestamp

    @property
    def digest(self) -> str:
        return record_digest("research.policy-adoption.v1", self.model_dump(mode="json"))


class PolicyBindingRef(VersionedRecord):
    binding_id: ID
    revision: Revision
    policy_digest: Digest


class FrozenPolicyBinding(VersionedRecord):
    reference: PolicyBindingRef
    binding_digest: Digest
    adoption_id: ID | None
