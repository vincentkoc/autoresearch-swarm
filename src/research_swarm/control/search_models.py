"""Durable proposal contracts keep external agent output separate from research decisions."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import Field, model_validator

from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import (
    ID,
    Amount,
    Digest,
    Dimension,
    Name,
    Revision,
    Timestamp,
    VersionedRecord,
)
from research_swarm.state.database import JsonObject


class SearchState(VersionedRecord):
    campaign_id: ID
    campaign_contract_digest: Digest
    policy: Literal["greedy", "external", "portfolio"]
    policy_version: Name
    generation: Revision = 0
    pending_request_ids: tuple[ID, ...] = Field(default=(), max_length=1024)
    active_trial_ids: tuple[ID, ...] = Field(default=(), max_length=1024)
    request_count: Revision = 0
    selection_trial_id: ID | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def selection_owner(self) -> Self:
        if self.selection_trial_id is not None and (
            self.policy != "portfolio" or self.selection_trial_id not in self.active_trial_ids
        ):
            raise ValueError("Portfolio selection must belong to an active policy trial")
        return self


class RequestProposal(VersionedRecord):
    campaign_id: ID
    proposer_id: ID
    seed: int = Field(strict=True, ge=0, le=9_007_199_254_740_991)
    expected_incumbent_revision: Revision
    expected_controller_id: ID
    expected_control_epoch: Revision
    instructions: str = Field(default="", max_length=8192)
    theme_id: Name | None = Field(default=None, exclude_if=lambda value: value is None)


class ProposalRequest(VersionedRecord):
    campaign_id: ID
    proposer_id: ID
    controller_id: ID
    control_epoch: Revision
    policy_revision: Revision
    policy_version: Name
    campaign_contract_digest: Digest
    incumbent_revision: Revision
    parent_candidate_id: ID
    parent_source: ArtifactRef
    seed: int = Field(strict=True, ge=0)
    instructions: str
    context: JsonObject
    context_digest: Digest
    context_watermark: Revision
    requested_at: Timestamp
    budget_reservation_id: ID
    theme_id: Name | None = Field(default=None, exclude_if=lambda value: value is None)


class ReportedAgentUsage(VersionedRecord):
    provider: Name | None = None
    model: Name | None = None
    prompt_digest: Digest | None = None
    amounts: dict[Dimension, Amount] = Field(default_factory=dict, max_length=16)
    authority: Literal["agent_reported"] = "agent_reported"


class RespondToProposal(VersionedRecord):
    request_id: ID
    outcome: Literal["candidate", "failed"]
    source_artifact: ArtifactRef | None = None
    hypothesis: str = Field(default="", max_length=8192)
    finding: str = Field(default="", max_length=8192)
    response_artifact: ArtifactRef | None = None
    usage: ReportedAgentUsage = Field(default_factory=ReportedAgentUsage)

    @model_validator(mode="after")
    def matching_outcome(self) -> Self:
        if self.outcome == "candidate" and (self.source_artifact is None or not self.hypothesis):
            raise ValueError("A candidate response requires source and a hypothesis")
        if self.outcome == "failed" and (self.source_artifact is not None or not self.finding):
            raise ValueError("A failed response requires a finding and no candidate source")
        return self


class ProposalResponse(VersionedRecord):
    request: RespondToProposal
    response_digest: Digest
    campaign_id: ID
    responder_id: ID
    recorded_at: Timestamp
    late_at_recording: bool


class CancelProposal(VersionedRecord):
    campaign_id: ID
    request_id: ID
    reason: str = Field(min_length=1, max_length=2048)


class ProposalCancellation(VersionedRecord):
    request_id: ID
    actor_id: ID
    reason: str = Field(min_length=1, max_length=2048)
    recorded_at: Timestamp


class ProposalRead(VersionedRecord):
    request_id: ID


class AdmitProposalResponse(VersionedRecord):
    campaign_id: ID
    request_id: ID
    expected_incumbent_revision: Revision
    expected_controller_id: ID
    expected_control_epoch: Revision


class ProposalAdmission(VersionedRecord):
    transition_kind: Literal["proposal_admission"] = "proposal_admission"
    campaign_id: ID
    request_id: ID
    response_digest: Digest
    policy_version: Name
    before_revision: Revision
    after_revision: Revision
    outcome: Literal["admitted", "invalid", "duplicate", "agent_failed"]
    candidate_id: ID | None = None
    validation_id: ID | None = None
    trial_id: ID | None = None
    finding: str = Field(default="", max_length=8192)
    committed_by: ID
    committed_at: Timestamp
