"""An explicit research archive preserves alternatives without declaring winners."""

from typing import Literal, Self

from pydantic import Field, model_validator

from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import ID, Digest, Name, Revision, Timestamp, VersionedRecord


class RetainPopulationMember(VersionedRecord):
    campaign_id: ID
    candidate_id: ID
    validation_id: ID
    evaluation_id: ID
    reason: str = Field(min_length=1, max_length=2048)
    labels: tuple[Name, ...] = Field(default=(), max_length=8)

    @model_validator(mode="after")
    def distinct_labels(self) -> Self:
        if len(set(self.labels)) != len(self.labels):
            raise ValueError("Archive labels must be distinct")
        return self


class RetirePopulationMember(VersionedRecord):
    member_id: ID
    reason: str = Field(min_length=1, max_length=2048)


class PopulationMember(VersionedRecord):
    request: RetainPopulationMember
    source_artifact: ArtifactRef
    source_tree_digest: Digest
    campaign_contract_digest: Digest
    comparison_digest: Digest
    actor_id: ID
    recorded_at: Timestamp


class PopulationChange(VersionedRecord):
    member_id: ID
    before_revision: Revision
    after_revision: Revision
    state: Literal["active", "retired"]
    member: PopulationMember
    actor_id: ID
    reason: str = Field(min_length=1, max_length=2048)
    recorded_at: Timestamp


class PopulationParentRef(VersionedRecord):
    member_id: ID
    revision: int = Field(strict=True, ge=1, le=9_007_199_254_740_991)


class FrozenPopulationParent(VersionedRecord):
    reference: PopulationParentRef
    change_id: ID
    member: PopulationMember
