"""Typed policy observations separate measurements from operational outcomes."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import Field, model_validator

from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.errors import ErrorCode
from research_swarm.domain.experiments import Metric
from research_swarm.domain.types import ID, Digest, Revision, Timestamp, VersionedRecord

ObservationOutcome = Literal[
    "measurement",
    "execution_failed",
    "not_started",
    "cancelled",
    "execution_unknown",
    "closed_unaccepted",
    "evaluation_invalid",
    "evaluation_incomplete",
    "evidence_invalidated",
    "unrankable",
]


class ObserveTrial(VersionedRecord):
    campaign_id: ID
    trial_id: ID


class TrialFacts(VersionedRecord):
    campaign_id: ID
    trial_id: ID
    trial_revision: Revision
    candidate_id: ID
    spec_digest: Digest
    trial_state: str
    execution_outcome: str
    evaluation_status: str
    resource_status: str
    outcome: ObservationOutcome
    evaluation_id: ID | None = None
    metric: Metric | None = None
    comparison_digest: Digest | None = None
    attempt_outcomes: dict[ID, str] = Field(default_factory=dict, max_length=1024)
    evidence: tuple[EvidenceReference, ...] = Field(default=(), max_length=1024)
    reason_code: ErrorCode | None = None

    @model_validator(mode="after")
    def score_authority(self) -> Self:
        scientific = self.outcome == "measurement"
        if scientific != (self.metric is not None):
            raise ValueError("Only measurement observations carry a score")
        if scientific and (self.evaluation_id is None or self.comparison_digest is None):
            raise ValueError("Measurements require authenticated comparable evaluation")
        return self


class TrialObservation(VersionedRecord):
    facts: TrialFacts
    facts_digest: Digest
    incumbent_revision: Revision
    comparator_id: ID | None
    comparator_evaluation_id: ID | None
    observed_by: ID
    observed_at: Timestamp
    watermark: Revision


class ResolveTrialObservation(VersionedRecord):
    campaign_id: ID
    observation_id: ID
    decision_id: ID | None = None
    expected_controller_id: ID
    expected_control_epoch: Revision


class TrialResolution(VersionedRecord):
    transition_kind: Literal["trial_resolution"] = "trial_resolution"
    campaign_id: ID
    trial_id: ID
    ordered_observation_ids: tuple[ID, ...] = Field(min_length=1, max_length=1)
    decision_id: ID | None
    outcome: Literal["promoted", "rejected", "inconclusive", "non_scientific"]
    before_revision: Revision
    after_revision: Revision
    generation: Revision
    committed_by: ID
    committed_at: Timestamp


class ObservationConsumption(VersionedRecord):
    observation_id: ID
    transition_id: ID
    policy_revision: Revision
    ordinal: Revision
