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


class AbandonObservationSelection(VersionedRecord):
    campaign_id: ID
    observation_id: ID
    selection_id: ID
    reason: str = Field(min_length=1, max_length=2048)
    expected_controller_id: ID
    expected_control_epoch: Revision


class SelectionAbandonment(VersionedRecord):
    selection_id: ID
    original_observation_id: ID
    basis: Literal["trial_evidence_changed", "comparator_invalidated"]
    invalidated_dependencies: tuple[EvidenceReference, ...] = Field(default=(), max_length=1024)
    reason: str = Field(min_length=1, max_length=2048)


class TrialResolution(VersionedRecord):
    transition_kind: Literal["trial_resolution"] = "trial_resolution"
    abandonment: SelectionAbandonment | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
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


class SelectTrialObservation(VersionedRecord):
    campaign_id: ID
    observation_id: ID
    expected_controller_id: ID
    expected_control_epoch: Revision


class ObservationSelection(VersionedRecord):
    transition_kind: Literal["observation_selected"] = "observation_selected"
    campaign_id: ID
    trial_id: ID
    observation_id: ID
    observation_digest: Digest
    facts_digest: Digest
    incumbent_revision: Revision
    comparator_id: ID | None
    comparator_evaluation_id: ID | None
    policy_version: str
    before_revision: Revision
    after_revision: Revision
    committed_by: ID
    committed_at: Timestamp


def observation_selection_id(workspace_id: str, campaign_id: str, trial_id: str) -> str:
    from uuid import NAMESPACE_URL, uuid5

    from research_swarm.domain.canonical import record_digest

    identity = record_digest(
        "research.observation-selection-identity",
        {
            "workspace_id": workspace_id,
            "campaign_id": campaign_id,
            "trial_id": trial_id,
        },
    )
    return str(uuid5(NAMESPACE_URL, "research-swarm:observation-selection:" + identity))
