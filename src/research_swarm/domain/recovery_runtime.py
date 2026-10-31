"""A protected target runtime plan contains identities, never credential values."""

from typing import Self

from pydantic import Field, model_validator

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.identity import Principal
from research_swarm.domain.time_witnesses import WitnessAuthorityScope, WitnessSource
from research_swarm.domain.types import ID, Digest, VersionedRecord


class RecoveryRuntimeCredential(VersionedRecord):
    id: ID
    principal: Principal
    token_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class RecoveryRuntimePlan(VersionedRecord):
    restore_id: ID
    deployment_digest: Digest
    configuration_digest: Digest
    authority: WitnessAuthorityScope
    source: WitnessSource
    credentials: tuple[RecoveryRuntimeCredential, ...] = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def unique_roster(self) -> Self:
        if (
            len({item.id for item in self.credentials}) != len(self.credentials)
            or len({item.token_digest for item in self.credentials}) != len(self.credentials)
            or any(
                item.principal.workspace_id != self.authority.workspace_id
                for item in self.credentials
            )
        ):
            raise ValueError("Recovery runtime credential roster differs")
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.recovery-runtime-plan.v1", self.model_dump(mode="json"))
