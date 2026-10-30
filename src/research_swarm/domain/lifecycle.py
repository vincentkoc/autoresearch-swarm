"""Explicit campaign and attempt transitions."""

from __future__ import annotations

from enum import StrEnum

from research_swarm.domain.errors import ErrorCode, ResearchError


class CampaignState(StrEnum):
    DRAFT = "draft"
    READY = "ready"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    AWAITING_INPUT = "awaiting_input"
    STOPPING = "stopping"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    BUDGET_EXHAUSTED = "budget_exhausted"
    FAILED = "failed"


CAMPAIGN_TRANSITIONS: dict[CampaignState, frozenset[CampaignState]] = {
    CampaignState.DRAFT: frozenset({CampaignState.READY, CampaignState.CANCELLED}),
    CampaignState.READY: frozenset({CampaignState.RUNNING, CampaignState.CANCELLED}),
    CampaignState.RUNNING: frozenset(
        {
            CampaignState.PAUSING,
            CampaignState.AWAITING_INPUT,
            CampaignState.STOPPING,
            CampaignState.COMPLETED,
            CampaignState.BUDGET_EXHAUSTED,
            CampaignState.FAILED,
        }
    ),
    CampaignState.PAUSING: frozenset(
        {CampaignState.PAUSED, CampaignState.STOPPING, CampaignState.FAILED}
    ),
    CampaignState.PAUSED: frozenset({CampaignState.RUNNING, CampaignState.STOPPING}),
    CampaignState.AWAITING_INPUT: frozenset({CampaignState.RUNNING, CampaignState.STOPPING}),
    CampaignState.STOPPING: frozenset({CampaignState.CANCELLED, CampaignState.FAILED}),
    CampaignState.COMPLETED: frozenset(),
    CampaignState.CANCELLED: frozenset(),
    CampaignState.BUDGET_EXHAUSTED: frozenset(),
    CampaignState.FAILED: frozenset(),
}

TERMINAL_CAMPAIGNS = frozenset(
    state for state, targets in CAMPAIGN_TRANSITIONS.items() if not targets
)


class AttemptState(StrEnum):
    RESERVED = "reserved"
    STARTING = "starting"
    RUNNING = "running"
    PUBLISHING = "publishing"
    CANCEL_REQUESTED = "cancel_requested"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXECUTION_UNKNOWN = "execution_unknown"
    CLOSED_UNACCEPTED = "closed_unaccepted"


TERMINAL_ATTEMPTS = frozenset(
    {
        AttemptState.SUCCEEDED,
        AttemptState.FAILED,
        AttemptState.CANCELLED,
        AttemptState.CLOSED_UNACCEPTED,
    }
)

ATTEMPT_TRANSITIONS: dict[AttemptState, frozenset[AttemptState]] = {
    AttemptState.RESERVED: frozenset(
        {
            AttemptState.STARTING,
            AttemptState.FAILED,
            AttemptState.CANCEL_REQUESTED,
            AttemptState.EXECUTION_UNKNOWN,
        }
    ),
    AttemptState.STARTING: frozenset(
        {
            AttemptState.RUNNING,
            AttemptState.FAILED,
            AttemptState.CANCEL_REQUESTED,
            AttemptState.EXECUTION_UNKNOWN,
        }
    ),
    AttemptState.RUNNING: frozenset(
        {
            AttemptState.PUBLISHING,
            AttemptState.FAILED,
            AttemptState.CANCEL_REQUESTED,
            AttemptState.EXECUTION_UNKNOWN,
        }
    ),
    AttemptState.PUBLISHING: frozenset(
        {
            AttemptState.SUCCEEDED,
            AttemptState.FAILED,
            AttemptState.CANCEL_REQUESTED,
            AttemptState.EXECUTION_UNKNOWN,
        }
    ),
    AttemptState.CANCEL_REQUESTED: frozenset(
        {AttemptState.CANCELLED, AttemptState.EXECUTION_UNKNOWN}
    ),
    AttemptState.EXECUTION_UNKNOWN: frozenset(),
    AttemptState.SUCCEEDED: frozenset(),
    AttemptState.FAILED: frozenset(),
    AttemptState.CANCELLED: frozenset(),
    AttemptState.CLOSED_UNACCEPTED: frozenset(),
}


def campaign_transition(current: CampaignState, target: CampaignState) -> None:
    if target not in CAMPAIGN_TRANSITIONS[current]:
        raise ResearchError(
            ErrorCode.INVALID_TRANSITION,
            f"Campaign cannot transition from {current} to {target}",
        )


def attempt_transition(
    current: AttemptState, target: AttemptState, *, reconciliation: bool = False
) -> None:
    if (
        reconciliation
        and target == AttemptState.CLOSED_UNACCEPTED
        and current not in TERMINAL_ATTEMPTS
    ):
        return
    if reconciliation and current == AttemptState.EXECUTION_UNKNOWN and target in TERMINAL_ATTEMPTS:
        return
    if target not in ATTEMPT_TRANSITIONS[current]:
        raise ResearchError(
            ErrorCode.INVALID_TRANSITION,
            f"Attempt cannot transition from {current} to {target}",
        )
