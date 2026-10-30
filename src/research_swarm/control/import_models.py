"""Historical records retain provenance without acquiring current execution authority."""

from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, model_validator

from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import ID, Digest, Name, Timestamp, VersionedRecord

Finding = Annotated[str, StringConstraints(min_length=1, max_length=1024)]


class HistoricalObservation(VersionedRecord):
    name: Name
    unit: Name
    value: float | None = Field(default=None, strict=True, allow_inf_nan=False)
    panel: Name | None = None
    missing_reason: Finding | None = None

    @model_validator(mode="after")
    def missing_value(self) -> Self:
        if (self.value is None) != (self.missing_reason is not None):
            raise ValueError("A missing historical measurement requires its reason")
        return self


class ImportHistoricalEvidence(VersionedRecord):
    project_id: ID
    source_system: Name
    source_schema: Name
    source_record_id: Annotated[str, StringConstraints(min_length=1, max_length=512)]
    raw_artifact: ArtifactRef
    normalizer: Name
    normalizer_digest: Digest | None = None
    source_status: Name = "unknown"
    source_evidence_class: Name = "unspecified"
    comparison_identifiers: dict[Name, Name] = Field(default_factory=dict, max_length=32)
    observations: tuple[HistoricalObservation, ...] = Field(default=(), max_length=64)
    provenance_findings: tuple[Finding, ...] = Field(default=(), max_length=32)

    @model_validator(mode="after")
    def bounded_metadata(self) -> Self:
        if self.raw_artifact.size_bytes > 1024 * 1024:
            raise ValueError("Historical metadata artifact exceeds 1 MiB")
        if len(canonical_bytes(self.model_dump(mode="json"))) > 64 * 1024:
            raise ValueError("Historical import manifest exceeds 64 KiB")
        return self

    def identity(self) -> dict[str, str]:
        return {
            "project_id": self.project_id,
            "source_system": self.source_system,
            "source_schema": self.source_schema,
            "source_record_id": self.source_record_id,
        }

    def digest(self) -> str:
        return record_digest("research.historical-import-v1", self.model_dump(mode="json"))


class ImportedEvidence(VersionedRecord):
    manifest: ImportHistoricalEvidence
    import_manifest_digest: Digest
    imported: Literal[True] = True
    promotion_eligible: Literal[False] = False
    source_authenticity: Literal["unverified"] = "unverified"
    execution_authority: Literal["none"] = "none"
    budget_authority: Literal["none"] = "none"
    imported_by: ID
    imported_at: Timestamp
