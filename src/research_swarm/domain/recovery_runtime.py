"""A protected target runtime plan contains identities, never credential values."""

from typing import Literal, Self

from pydantic import Field, model_validator

from research_swarm.domain.assignments import SignatureHex
from research_swarm.domain.backup import ActivateRecovery
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.identity import Principal
from research_swarm.domain.time_providers import TimeProviderData
from research_swarm.domain.time_witnesses import (
    SignedTimeWitness,
    WitnessAuthorityScope,
    WitnessSource,
)
from research_swarm.domain.types import ID, Digest, Revision, Timestamp, VersionedRecord


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


class HostedActivationRuntime(VersionedRecord):
    """Reconstructed from the elected plan and its immutable preparation receipt."""

    plan: RecoveryRuntimePlan
    preparation_digest: Digest
    provider_revision: Revision
    provider: TimeProviderData

    @model_validator(mode="after")
    def bound(self) -> Self:
        if (
            self.provider.scope.authority != self.plan.authority
            or self.provider.source != self.plan.source
            or self.provider.source_epoch is None
            or self.provider_revision < 1
        ):
            raise ValueError("Prepared activation runtime differs")
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.hosted-activation-runtime.v1", self.model_dump(mode="json"))


class RuntimePreparationReceipt(VersionedRecord):
    plan_digest: Digest
    restoration_digest: Digest
    admission_digest: Digest
    provider_revision: Revision
    provider: TimeProviderData
    prepared_at: Timestamp

    @property
    def digest(self) -> str:
        return record_digest(
            "research.runtime-preparation-envelope.v1", self.model_dump(mode="json")
        )


class RequalifyRecoveryRuntime(VersionedRecord):
    """Explicit request; target authority and source stay in protected configuration."""

    request_id: ID
    expected_provider_revision: Revision = Field(ge=1)
    previous_qualification_digest: Digest
    reason: str = Field(min_length=1, max_length=4096)


class RuntimeRequalificationReceipt(VersionedRecord):
    """Immutable successor, stored by its new provider generation with no latest pointer."""

    request: RequalifyRecoveryRuntime
    plan_digest: Digest
    restoration_digest: Digest
    admission_digest: Digest
    previous_generation: ID
    fence_digest: Digest
    provider_revision: Revision
    provider: TimeProviderData
    qualified_at: Timestamp

    @model_validator(mode="after")
    def successor(self) -> Self:
        if (
            self.provider_revision != self.request.expected_provider_revision + 1
            or self.provider.source_epoch is None
            or self.provider.scope.provider_generation == self.previous_generation
        ):
            raise ValueError("Runtime requalification successor differs")
        return self

    @property
    def digest(self) -> str:
        return record_digest(
            "research.runtime-requalification-envelope.v1", self.model_dump(mode="json")
        )


class HostedRecoveryActivation(VersionedRecord):
    profile: Literal["hosted-recovery-activation-v1"] = "hosted-recovery-activation-v1"
    request_id: ID
    command_id: ID
    request: ActivateRecovery
    workspace_id: ID
    controller_id: ID
    restoration_digest: Digest
    admission_digest: Digest
    logical_election_digest: Digest
    deployment_digest: Digest
    runtime_digest: Digest
    view_digest: Digest
    artifact_digest: Digest
    retired_credential_ids: tuple[ID, ...] = Field(max_length=8192)
    final_witness: SignedTimeWitness
    evidence_through: Revision
    gate_revision: Revision
    operator_id: ID
    activated_at: Timestamp
    target_signature: SignatureHex

    @model_validator(mode="after")
    def ordered_retirement(self) -> Self:
        if tuple(sorted(set(self.retired_credential_ids))) != self.retired_credential_ids:
            raise ValueError("Credential retirement inventory must be sorted and unique")
        return self

    @property
    def proof_digest(self) -> str:
        return record_digest(
            "research.hosted-recovery-activation.v1",
            self.model_dump(mode="json", exclude={"target_signature"}),
        )

    @property
    def digest(self) -> str:
        return record_digest(
            "research.hosted-recovery-activation-envelope.v1", self.model_dump(mode="json")
        )
