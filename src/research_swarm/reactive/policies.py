"""Small policies for the same bounded decision protocol used by external agents."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.types import VersionedRecord
from research_swarm.reactive.contracts import (
    ControlName,
    DecisionProposal,
    DecisionRequest,
    NumericValue,
    ReactiveContract,
    Scalar,
)


class DecisionPolicy(Protocol):
    @property
    def digest(self) -> str: ...

    async def decide(
        self, request: DecisionRequest, contract: ReactiveContract
    ) -> DecisionProposal: ...


@dataclass(frozen=True)
class CallbackPolicy:
    """The caller declares the version fingerprint of its cooperative async callback."""

    digest: str
    callback: Callable[[DecisionRequest, ReactiveContract], Awaitable[DecisionProposal]]

    async def decide(
        self, request: DecisionRequest, contract: ReactiveContract
    ) -> DecisionProposal:
        return await self.callback(request, contract)


class ThresholdPolicy(VersionedRecord):
    diagnostic: ControlName
    above: Scalar
    control: ControlName
    value: Scalar

    @property
    def digest(self) -> str:
        return record_digest("research.reactive-threshold-policy", self.model_dump(mode="json"))

    async def decide(
        self, request: DecisionRequest, contract: ReactiveContract
    ) -> DecisionProposal:
        values = {item.name: item.value for item in request.observation.diagnostics}
        change = values[self.diagnostic] > self.above
        return DecisionProposal(
            request_id=request.id,
            request_digest=request.digest,
            base_revision=request.control_revision,
            kind="set" if change else "keep",
            values=(NumericValue(name=self.control, value=self.value),) if change else (),
            rationale="Diagnostic exceeded threshold" if change else "Threshold not exceeded",
        )
