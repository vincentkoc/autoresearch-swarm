"""Immutable confirmation contracts and separate exposure accounting records."""

from typing import Literal

from pydantic import Field

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.types import ID, Digest, Name, Revision, VersionedRecord
from research_swarm.evaluation.statistics import FixedPairedPolicy


class PlanBinding(VersionedRecord):
    trial_id: ID
    spec_digest: Digest
    candidate_id: ID
    source_tree_digest: Digest
    seed: int = Field(strict=True, ge=0)
    side: Literal["candidate", "comparator"]
    scoring_job_id: ID
    exposure_id: ID


class ConfirmationPlan(VersionedRecord):
    id: ID
    campaign_id: ID
    campaign_contract_digest: Digest
    candidate_id: ID
    comparator_id: ID
    expected_incumbent_revision: Revision
    evaluator_id: ID
    policy: FixedPairedPolicy
    execution_fingerprints: dict[Name, Digest]
    bindings: tuple[PlanBinding, ...]
    plan_digest: Digest | None = None

    def sealed(self) -> "ConfirmationPlan":
        checked = ConfirmationPlan.model_validate(self)
        digest = record_digest(
            "research.confirmation-plan", checked.model_dump(mode="json", exclude={"plan_digest"})
        )
        if self.plan_digest is not None and self.plan_digest != digest:
            raise ValueError("Confirmation plan digest differs")
        return checked.model_copy(update={"plan_digest": digest})

    def verify(self) -> None:
        if self.plan_digest is None:
            raise ValueError("Confirmation plan is not sealed")
        self.sealed()


class ConfirmationData(VersionedRecord):
    plan: ConfirmationPlan
    decision_id: ID | None = None


class ExposureData(VersionedRecord):
    plan_id: ID
    job_id: ID
    reservation_id: ID
    attempt_id: ID | None = None
