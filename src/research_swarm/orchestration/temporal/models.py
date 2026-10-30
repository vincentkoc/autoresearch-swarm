"""Small history-safe DTOs. No credentials, source, logs, or research payloads."""

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class BindingReference:
    workspace_id: str
    campaign_id: str
    epoch: int
    configuration_digest: str
    actors: dict[str, str]


@dataclass(frozen=True)
class StartupRequest:
    binding: BindingReference
    recovery_id: str


@dataclass(frozen=True)
class WorkReference:
    binding: BindingReference
    kind: Literal["turn", "wait"]
    record_id: str
    actor_id: str


@dataclass(frozen=True)
class PendingWork:
    state: Literal["idle", "work", "retired", "terminal"]
    work: WorkReference | None = None


@dataclass(frozen=True)
class WorkOutcome:
    state: Literal["settled", "pending"]
    record_id: str
    disposition: str = ""
    evidence_id: str = ""
    watermark: int = 0


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
    return f"research/{reference.workspace_id}/{reference.campaign_id}/{reference.epoch}"
