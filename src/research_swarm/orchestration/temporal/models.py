"""Small history-safe DTOs. No credentials, source, logs, or research payloads."""

from dataclasses import dataclass, field

from research_swarm.orchestration.delivery_models import (
    BindingReference as BindingReference,
)
from research_swarm.orchestration.delivery_models import (
    PendingWork as PendingWork,
)
from research_swarm.orchestration.delivery_models import (
    StartupRequest as StartupRequest,
)
from research_swarm.orchestration.delivery_models import (
    WorkOutcome as WorkOutcome,
)
from research_swarm.orchestration.delivery_models import (
    WorkReference as WorkReference,
)
from research_swarm.orchestration.delivery_models import (
    delivery_id,
)


@dataclass(frozen=True)
class WakeSignal:
    campaign_id: str
    epoch: int
    actor_id: str
    wait_id: str


@dataclass(frozen=True)
class WorkflowInput:
    binding: BindingReference
    poll_seconds: float = 5
    rotate_after: int = 128
    activity_timeout_seconds: float = 120
    pending: WorkReference | None = None
    schema_version: str = field(default="1.0", init=False)

    def __post_init__(self) -> None:
        if not 0.05 <= self.poll_seconds <= 60:
            raise ValueError("Temporal polling must be between 0.05 and 60 seconds")
        if type(self.rotate_after) is not int or not 2 <= self.rotate_after <= 1000:
            raise ValueError("Temporal histories rotate after 2 to 1000 iterations")
        if not 0.1 <= self.activity_timeout_seconds <= 300:
            raise ValueError("Temporal activity timeout must be between 0.1 and 300 seconds")
        if self.pending is not None and self.pending.binding != self.binding:
            raise ValueError("Continued work must belong to the same binding")


def workflow_id(reference: BindingReference) -> str:
    return delivery_id(reference)
