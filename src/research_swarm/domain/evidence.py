"""Measurements are descriptive until an authorized evaluation supports them."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, JsonValue, model_validator

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.experiments import Metric, MetricDefinition
from research_swarm.domain.types import (
    ID,
    Amount,
    Digest,
    Dimension,
    Milliseconds,
    Name,
    Revision,
    StringSet,
    Timestamp,
    VersionedRecord,
    new_id,
    utc_now,
)


class ArtifactRef(VersionedRecord):
    id: ID
    digest: Digest
    size_bytes: int = Field(strict=True, ge=0)
    media_type: str = "application/octet-stream"


class RunResult(VersionedRecord):
    campaign_id: ID
    trial_id: ID
    job_id: ID
    attempt_id: ID
    worker_id: ID
    worker_session_id: ID
    spec_digest: Digest
    control_epoch: Revision
    fencing_generation: Revision
    status: Literal["succeeded", "failed", "cancelled"]
    exit_code: int = Field(strict=True)
    started_at: Timestamp
    finished_at: Timestamp
    elapsed_ms: Milliseconds
    observed_seed: int | None = Field(default=None, strict=True, ge=0)
    process_closure_ref: ArtifactRef
    artifacts: tuple[ArtifactRef, ...] = ()
    measurements: tuple[Metric, ...] = ()
    consumed: dict[Dimension, Amount] = Field(default_factory=dict)
    phase_timings_ms: dict[str, Milliseconds] = Field(default_factory=dict)
    execution_fingerprint: Digest
    result_digest: Digest | None = None

    @model_validator(mode="after")
    def consistent_exit(self) -> RunResult:
        if self.status == "succeeded" and self.exit_code != 0:
            raise ValueError("Successful execution requires exit code zero")
        return self

    def sealed(self) -> RunResult:
        validated = RunResult.model_validate(self)
        digest = record_digest(
            "research.run-result", validated.model_dump(mode="json", exclude={"result_digest"})
        )
        if self.result_digest is not None and digest != self.result_digest:
            raise ValueError("Run result digest mismatch")
        return validated.model_copy(update={"result_digest": digest})

    def verify(self) -> None:
        if self.result_digest is None:
            raise ValueError("Run result is not sealed")
        self.sealed()


class EvaluationRequest(VersionedRecord):
    id: ID = Field(default_factory=new_id)
    workspace_id: ID
    campaign_id: ID
    candidate_id: ID
    trial_id: ID
    input_artifact: ArtifactRef
    evaluator_id: ID
    evaluator_contract_digest: Digest
    panel_digest: Digest
    comparison_class: Name
    metric_schema: tuple[MetricDefinition, ...] = Field(min_length=1)
    expected_seed: int = Field(strict=True, ge=0)
    expected_members: StringSet = frozenset()
    exposure_class: Literal["exploratory", "confirmation", "protected", "official"]
    exposure_authorization_id: ID | None = None
    measurement_trust: Literal["self_reported", "authenticated", "protected"] = "authenticated"

    @model_validator(mode="after")
    def protected_authority(self) -> EvaluationRequest:
        if len({metric.name for metric in self.metric_schema}) != len(self.metric_schema):
            raise ValueError("Evaluation metric names must be unique")
        if self.exposure_class in {"protected", "official"}:
            if self.exposure_authorization_id is None:
                raise ValueError("Protected evaluation requires exposure authorization")
            if self.measurement_trust != "protected":
                raise ValueError("Protected evaluation requires a protected evaluator profile")
        return self


class EvaluationReceipt(VersionedRecord):
    id: ID = Field(default_factory=new_id)
    request_id: ID
    evaluator_id: ID
    input_digest: Digest
    evaluator_contract_digest: Digest
    panel_digest: Digest
    observed_seed: int = Field(strict=True, ge=0)
    observed_members: StringSet = frozenset()
    metrics: tuple[Metric, ...]
    validity: Literal["valid", "incomplete", "invalid"]
    findings: tuple[str, ...] = ()
    execution_fingerprint: Digest
    recorded_at: Timestamp = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def metric_panel(self) -> EvaluationReceipt:
        if self.validity == "valid" and not self.metrics:
            raise ValueError("A valid receipt requires measurements")
        if len({metric.name for metric in self.metrics}) != len(self.metrics):
            raise ValueError("Receipt metric names must be unique")
        return self


class PromotionDecision(VersionedRecord):
    id: ID = Field(default_factory=new_id)
    campaign_id: ID
    candidate_id: ID
    comparator_id: ID | None
    expected_incumbent_revision: Revision
    evaluation_ids: tuple[ID, ...] = Field(min_length=1)
    comparison_class: Name
    policy_version: Name
    outcome: Literal["promote", "reject", "replicate", "inconclusive"]
    improvement: float | None = None
    reason_codes: tuple[str, ...]
    explanation: str = ""


class Event(VersionedRecord):
    id: ID = Field(default_factory=new_id)
    workspace_id: ID
    sequence: int = Field(strict=True, ge=1)
    aggregate_type: Name
    aggregate_id: ID
    aggregate_revision: Revision
    type: Name
    occurred_at: Timestamp
    recorded_at: Timestamp
    actor_id: ID
    command_id: ID
    campaign_id: ID | None = None
    payload: dict[str, JsonValue] = Field(default_factory=dict)
