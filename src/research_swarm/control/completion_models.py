"""Scientific completion is independent of operational resource closure."""

from pydantic import Field

from research_swarm.control.commands import CampaignTarget, CompleteCampaign
from research_swarm.domain.types import ID, Digest, Revision, Timestamp, VersionedRecord


class CaptureCampaignObligations(CampaignTarget):
    review_id: ID


class CompleteScientificCampaign(CompleteCampaign):
    obligations_id: ID


class CloseCampaignResources(CompleteCampaign):
    obligations_id: ID


class ObligationReview(VersionedRecord):
    campaign_id: ID
    campaign_revision: Revision
    obligations_digest: Digest
    chunk_ids: tuple[ID, ...] = Field(max_length=128)
    entry_count: Revision
    counts: dict[str, int]
    request_digest: Digest
    captured_by: ID
    captured_at: Timestamp


class ScientificCompletion(VersionedRecord):
    campaign_id: ID
    campaign_revision: Revision
    obligations_id: ID
    obligations_digest: Digest
    request_digest: Digest
    final_report_id: ID
    incumbent_candidate_id: ID | None
    incumbent_revision: Revision
    incumbent_promotion_id: ID | None
    incumbent_eligibility: str
    stopping_basis: str
    reason: str
    evidence_through: Revision
    completed_by: ID
    completed_at: Timestamp


class CampaignResourceClosure(VersionedRecord):
    campaign_id: ID
    scientific_completion_id: ID
    obligations_id: ID
    obligations_digest: Digest
    request_digest: Digest
    final_report_id: ID
    funding_settlement_id: ID | None
    reason: str
    closed_by: ID
    closed_at: Timestamp
