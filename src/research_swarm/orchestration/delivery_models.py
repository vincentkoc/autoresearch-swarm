"""Provider-neutral bounded references for delivery of persisted research work."""

from dataclasses import dataclass
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


def delivery_id(reference: BindingReference) -> str:
    return f"research/{reference.workspace_id}/{reference.campaign_id}/{reference.epoch}"
