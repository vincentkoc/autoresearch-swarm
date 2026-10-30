"""Immutable input preparation for ordinary policy-training campaigns."""

from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import ID, Digest, Timestamp, VersionedRecord


class PreparePolicyInputs(VersionedRecord):
    binding_id: ID
    dataset_id: ID


class PreparedPolicyInputs(VersionedRecord):
    request: PreparePolicyInputs
    dataset_digest: Digest
    plan_digest: Digest
    behavior_policy_digest: Digest
    behavior: ArtifactRef
    trajectories: ArtifactRef
    actor_id: ID
    recorded_at: Timestamp
