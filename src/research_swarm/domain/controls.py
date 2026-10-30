"""Transport-independent cancellation identity and durable receipt acknowledgment."""

from typing import Literal

from pydantic import Field

from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.domain.workers import WorkerBinding


class ControlAcknowledgment(VersionedRecord):
    attempt_id: ID
    control_sequence: int = Field(strict=True, ge=1)
    dispatch_digest: Digest


class CancellationDirective(ControlAcknowledgment):
    binding: WorkerBinding
    control_sequence: int = Field(default=1, strict=True, ge=1, le=1)
    action: Literal["cancel"] = "cancel"

    def acknowledgment(self) -> ControlAcknowledgment:
        return ControlAcknowledgment(
            attempt_id=self.attempt_id,
            control_sequence=self.control_sequence,
            dispatch_digest=self.dispatch_digest,
        )
