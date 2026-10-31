"""Durable witness-source lifecycle records, separate from runtime qualification."""

from pydantic import Field, TypeAdapter, model_validator

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.time_witnesses import SignedTimeWitness, WitnessScope, WitnessSource
from research_swarm.domain.types import ID, Digest, Revision, Timestamp, VersionedRecord


class TimeProviderData(VersionedRecord):
    scope: WitnessScope
    source: WitnessSource
    source_epoch: ID | None = None


class TimeProviderGeneration(VersionedRecord):
    provider: TimeProviderData
    admitted_by: ID
    recorded_at: Timestamp
    qualification: SignedTimeWitness | None = None


class RequalifyTimeProvider(VersionedRecord):
    request_id: ID
    expected_provider_revision: Revision = Field(ge=1)
    expected_provider_generation: ID
    reason: str = Field(min_length=1, max_length=4096)


class TimeProviderRequalification(VersionedRecord):
    request: RequalifyTimeProvider
    configuration_digest: Digest
    previous: TimeProviderData
    provider: TimeProviderData
    provider_revision: Revision
    qualified_at: Timestamp

    @model_validator(mode="after")
    def successor(self) -> "TimeProviderRequalification":
        if (
            self.previous.scope.provider_generation != self.request.expected_provider_generation
            or self.previous.scope.authority != self.provider.scope.authority
            or self.previous.source != self.provider.source
            or self.previous.source_epoch is None
            or self.provider.source_epoch is None
            or self.previous.source_epoch == self.provider.source_epoch
            or self.previous.scope.provider_generation == self.provider.scope.provider_generation
            or self.provider_revision != self.request.expected_provider_revision + 1
        ):
            raise ValueError("Time provider requalification must advance the fenced origin")
        return self


class TimeSourceRetirement(VersionedRecord):
    provider: TimeProviderData
    provider_revision: Revision
    observed: SignedTimeWitness
    retired_at: Timestamp

    @property
    def origin_id(self) -> str:
        if self.provider.source_epoch is None:
            raise ValueError("An unobserved source has no origin to retire")
        return source_origin_id(self.provider.source, self.provider.source_epoch)


def source_origin_id(source: WitnessSource, epoch: str) -> str:
    # Policy, activation and generation changes must not resurrect an old origin.
    source = WitnessSource.model_validate(source)
    TypeAdapter(ID).validate_python(epoch)
    return record_digest(
        "research.time-source-origin.v1",
        {"source_id": source.id, "source_key_id": source.key_id, "source_epoch": epoch},
    )


class TimeProviderFence(VersionedRecord):
    provider: TimeProviderData
    provider_revision: Revision
    origin_id: Digest
    observation_digest: Digest
