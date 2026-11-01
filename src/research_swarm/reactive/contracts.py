"""Provider-neutral records for cooperative decisions within one training attempt.

Steps count completed updates. An application at step N changes controls before
update N + 1. These records are training diagnostics, never evaluator receipts.
"""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, model_validator

from research_swarm.domain.canonical import MAX_SAFE_INTEGER, canonical_bytes, record_digest
from research_swarm.domain.types import ID, Digest, Name, VersionedRecord

ControlName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")]
Step = Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]
Scalar = Annotated[float, Field(strict=True)]


class NumericValue(VersionedRecord):
    name: ControlName
    value: Scalar


def ordered(values: tuple[NumericValue, ...]) -> tuple[NumericValue, ...]:
    if len({value.name for value in values}) != len(values):
        raise ValueError("Numeric names must be unique")
    return tuple(sorted(values, key=lambda value: value.name))


class NumericControl(VersionedRecord):
    name: ControlName
    minimum: Scalar
    maximum: Scalar
    initial: Scalar

    @model_validator(mode="after")
    def valid_bounds(self) -> Self:
        if not self.minimum <= self.initial <= self.maximum:
            raise ValueError("Control initial value must be within its declared bounds")
        return self


class DiagnosticDefinition(VersionedRecord):
    name: ControlName
    source: Literal["train", "development"]
    unit: Name = "scalar"


class ReactiveContract(VersionedRecord):
    policy_name: Name
    policy_digest: Digest
    controls: tuple[NumericControl, ...] = Field(min_length=1, max_length=32)
    diagnostics: tuple[DiagnosticDefinition, ...] = Field(min_length=1, max_length=64)
    interval_steps: int = Field(default=100, strict=True, ge=1, le=1_000_000)
    max_lag_steps: int = Field(default=100, strict=True, ge=0, le=1_000_000)
    max_decisions: int = Field(default=100, strict=True, ge=1, le=10_000)
    decision_timeout_ms: int = Field(default=30_000, strict=True, ge=1, le=120_000)
    max_prompt_bytes: int = Field(default=65_536, strict=True, ge=1024, le=65_536)
    max_response_bytes: int = Field(default=8192, strict=True, ge=256, le=16_384)
    max_output_tokens: int = Field(default=1024, strict=True, ge=1, le=16_384)

    @model_validator(mode="after")
    def unique_definitions(self) -> Self:
        for definitions in (self.controls, self.diagnostics):
            if len({definition.name for definition in definitions}) != len(definitions):
                raise ValueError("Control and diagnostic definitions must each have unique names")
        if tuple(item.name for item in self.controls) != tuple(
            sorted(item.name for item in self.controls)
        ) or tuple(item.name for item in self.diagnostics) != tuple(
            sorted(item.name for item in self.diagnostics)
        ):
            raise ValueError("Control and diagnostic definitions must be ordered by name")
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.reactive-contract", self.model_dump(mode="json"))

    def initial_controls(self) -> tuple[NumericValue, ...]:
        return tuple(NumericValue(name=item.name, value=item.initial) for item in self.controls)


class RunBinding(VersionedRecord):
    trial_id: ID
    attempt_id: ID
    stage_id: Name
    spec_digest: Digest
    context_digest: Digest


class Observation(VersionedRecord):
    step: Step
    diagnostics: tuple[NumericValue, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def unique_diagnostics(self) -> Self:
        if ordered(self.diagnostics) != self.diagnostics:
            raise ValueError("Diagnostic values must be ordered by name")
        return self


class DecisionRequest(VersionedRecord):
    id: ID
    sequence: int = Field(strict=True, ge=1, le=MAX_SAFE_INTEGER)
    binding: RunBinding
    contract_digest: Digest
    policy_digest: Digest
    observation: Observation
    requested_step: Step
    control_revision: Step
    current_controls: tuple[NumericValue, ...] = Field(min_length=1, max_length=32)
    history: tuple[Observation, ...] = Field(default=(), max_length=32)

    @model_validator(mode="after")
    def ordered_context(self) -> Self:
        if ordered(self.current_controls) != self.current_controls:
            raise ValueError("Current controls must be ordered by name")
        steps = [item.step for item in self.history] + [self.observation.step]
        if any(left >= right for left, right in zip(steps, steps[1:], strict=False)):
            raise ValueError("Observation history must have strictly increasing steps")
        if self.requested_step < self.observation.step:
            raise ValueError("A decision cannot be requested before its observation")
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.reactive-request", self.model_dump(mode="json"))


class ReportedUsage(VersionedRecord):
    """Unverified provider or agent telemetry, not billing or budget authority."""

    source: Literal["provider_reported", "agent_reported"]
    input_tokens: Step | None = None
    output_tokens: Step | None = None


class DecisionProposal(VersionedRecord):
    request_id: ID
    request_digest: Digest
    base_revision: Step
    kind: Literal["keep", "set"]
    values: tuple[NumericValue, ...] = Field(default=(), max_length=32)
    rationale: str = Field(default="", max_length=2048)
    usage: ReportedUsage | None = None

    @model_validator(mode="after")
    def coherent_action(self) -> Self:
        if (self.kind == "keep") != (len(self.values) == 0):
            raise ValueError("Keep requires no values; set requires at least one value")
        if ordered(self.values) != self.values:
            raise ValueError("Proposed controls must be ordered by name")
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.reactive-proposal", self.model_dump(mode="json"))


class ApplicationReceipt(VersionedRecord):
    """Workload observation of an application, separate from the model's proposal."""

    binding: RunBinding
    request_id: ID
    request_digest: Digest
    decision_digest: Digest
    observed_step: Step
    requested_step: Step
    applied_step: Step
    base_revision: Step
    new_revision: Step | None
    outcome: Literal["applied", "kept", "uncertain"]
    before: tuple[NumericValue, ...] = Field(min_length=1, max_length=32)
    after: tuple[NumericValue, ...] | None = Field(max_length=32)

    @model_validator(mode="after")
    def coherent_outcome(self) -> Self:
        if not self.observed_step <= self.requested_step <= self.applied_step:
            raise ValueError("Application steps must follow observation and request")
        if ordered(self.before) != self.before or (
            self.after is not None and ordered(self.after) != self.after
        ):
            raise ValueError("Receipt controls must be ordered by name")
        if self.outcome == "uncertain":
            if self.after is not None or self.new_revision is not None:
                raise ValueError("Uncertain application cannot assert state or revision")
        elif self.new_revision != self.base_revision + 1 or self.after is None:
            raise ValueError("A completed application advances exactly one revision")
        elif tuple(item.name for item in self.before) != tuple(item.name for item in self.after):
            raise ValueError("Application cannot add or remove controls")
        elif self.outcome == "kept" and self.after != self.before:
            raise ValueError("A kept decision cannot change controls")
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.reactive-application", self.model_dump(mode="json"))


def validate_request(contract: ReactiveContract, request: DecisionRequest) -> None:
    """Check the immutable request against the declared intervention surface."""
    contract = ReactiveContract.model_validate(contract)
    request = DecisionRequest.model_validate(request)
    if (
        request.contract_digest != contract.digest
        or request.policy_digest != contract.policy_digest
    ):
        raise ValueError("Decision request differs from its reactive contract or policy")
    if request.sequence > contract.max_decisions:
        raise ValueError("Decision request exceeds the declared decision count")
    definitions = {item.name: item for item in contract.controls}
    if {value.name for value in request.current_controls} != definitions.keys():
        raise ValueError("Current controls differ from the declared control names")
    for value in request.current_controls:
        control = definitions[value.name]
        if not control.minimum <= value.value <= control.maximum:
            raise ValueError("Current control is outside its declared bounds")
    allowed = {item.name for item in contract.diagnostics}
    for observation in (*request.history, request.observation):
        if {item.name for item in observation.diagnostics} - allowed:
            raise ValueError("Observation contains an undeclared diagnostic")
    if request.requested_step - request.observation.step > contract.max_lag_steps:
        raise ValueError("Decision observation is already stale")
    if len(canonical_bytes(request.model_dump(mode="json"))) > contract.max_prompt_bytes:
        raise ValueError("Decision request exceeds the prompt byte limit")


def proposed_controls(
    contract: ReactiveContract, request: DecisionRequest, proposal: DecisionProposal, *, step: int
) -> tuple[NumericValue, ...]:
    """Validate a proposal and derive its complete target state without applying it."""
    contract = ReactiveContract.model_validate(contract)
    request = DecisionRequest.model_validate(request)
    validate_request(contract, request)
    proposal = DecisionProposal.model_validate(proposal)
    if (
        proposal.request_id != request.id
        or proposal.request_digest != request.digest
        or proposal.base_revision != request.control_revision
    ):
        raise ValueError("Decision proposal differs from its request or control revision")
    if type(step) is not int or not request.requested_step <= step <= MAX_SAFE_INTEGER:
        raise ValueError("Application requires an integer step at or after the request")
    if step - request.observation.step > contract.max_lag_steps:
        raise ValueError("Decision proposal is stale at this application step")
    if len(canonical_bytes(proposal.model_dump(mode="json"))) > contract.max_response_bytes:
        raise ValueError("Decision proposal exceeds the response byte limit")
    definitions = {item.name: item for item in contract.controls}
    values = {item.name: item for item in request.current_controls}
    for value in proposal.values:
        definition = definitions.get(value.name)
        if definition is None or not definition.minimum <= value.value <= definition.maximum:
            raise ValueError("Decision proposes an unknown control or an out-of-bounds value")
        values[value.name] = value
    return ordered(tuple(values.values()))
