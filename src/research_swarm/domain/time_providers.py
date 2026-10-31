"""Durable witness-source lifecycle records, separate from runtime qualification."""

from pydantic import TypeAdapter

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
