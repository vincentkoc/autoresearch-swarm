"""Records and ports shared by the swarm scheduler, workers and search plugins."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol

Direction = Literal["minimize", "maximize"]
Resource = Literal["cpu", "gpu", "llm"]
Outcome = Literal["ok", "failed", "lost"]
FailureClass = Literal[
    "backend_setup",
    "command_exit",
    "timeout",
    "invalid_output",
    "remote_dispatch",
    "cancelled",
    "heartbeat_expired",
]


@dataclass(frozen=True, kw_only=True)
class Candidate:
    """An operator's JSON-serializable input to a trial, with explicit lineage."""

    candidate_id: str
    params: dict[str, Any] = field(default_factory=dict)
    parents: tuple[str, ...] = ()
    operator: str = "seed"
    family: str = "default"
    text: str | None = None
    diff: str | None = None
    source_revision: str | None = None
    # Per-proposal dispatch hints allow the same candidate to run other seeds or rungs.
    seed: int | None = None
    fidelity: float | None = None


@dataclass(frozen=True, kw_only=True)
class CampaignSpec:
    """One immutable campaign definition. Commands are argv, relative to task_dir."""

    campaign_id: str
    metric: str
    direction: Direction
    command: tuple[str, ...]
    max_trials: int
    max_workers: int
    project_id: str = "local"
    task_dir: str = "."
    resource: Resource = "cpu"
    candidates: tuple[Candidate, ...] = ()
    search_space: dict[str, list[Any]] = field(default_factory=dict)
    proposer: str | None = None
    selection_policy: str | None = None
    panel_policy: str | None = None
    evaluation_mode: Literal["calibrated", "smoke"] = "calibrated"
    backend: str | None = None
    worker: str | None = None
    options: dict[str, Any] = field(default_factory=dict)
    seed: int = 0
    fidelity: float = 1.0
    top_k: int = 1
    min_replicates: int = 1
    min_delta: float = 0.0
    target: float | None = None
    max_attempts: int = 2
    trial_timeout_seconds: float = 300.0
    proposal_timeout_seconds: float = 10.0
    full_fidelity_work: int | None = None
    max_search_work: int | None = None
    max_cost: float | None = None
    max_hours: float | None = None
    price_per_hour: float | None = None
    price_per_million_input_tokens: float | None = None
    price_per_million_output_tokens: float | None = None
    estimated_input_tokens: int = 0
    estimated_output_tokens: int = 0
    search_panel: str | None = None
    selection_panel: str | None = None
    confirmation_panel: str | None = None

    def __post_init__(self) -> None:
        if self.full_fidelity_work is None and self.max_search_work is None:
            return
        if any(
            type(n) is not int or n <= 0 for n in (self.full_fidelity_work, self.max_search_work)
        ):
            raise ValueError(
                "full_fidelity_work and max_search_work must both be positive integers"
            )
        if self.max_attempts != 1:
            raise ValueError("A search-work budget requires max_attempts=1")


@dataclass(frozen=True, kw_only=True)
class Trial:
    """Re-execution preserves this identity, seed, fidelity and candidate."""

    trial_id: str
    campaign_id: str
    candidate: Candidate
    seed: int
    fidelity: float


@dataclass(frozen=True, kw_only=True)
class Attempt:
    """A single worker claim of a trial. Timestamps are UTC epoch seconds."""

    attempt_id: str
    trial: Trial
    worker_id: str
    number: int
    started_at: float


@dataclass(frozen=True, kw_only=True)
class Result:
    """An attempt observation. Failed and lost attempts remain in the archive."""

    attempt: Attempt
    status: Outcome
    metrics: dict[str, float] = field(default_factory=dict)
    error: str | None = None
    logs: tuple[str, ...] = ()
    cost: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    peak_vram_mib: float | None = None
    failure_class: FailureClass | None = None


class Proposer(Protocol):
    async def ask(
        self, spec: CampaignSpec, archive: Sequence[Result], k: int
    ) -> Sequence[Candidate]:
        """Return at most k candidates. An empty batch ends an idle search."""
        ...

    def tell(self, result: Result) -> None:
        """Observe a completed attempt, including failure."""
        ...


class Operator(Protocol):
    async def propose(
        self, spec: CampaignSpec, parents: Sequence[Candidate], k: int
    ) -> Sequence[Candidate]: ...


class Backend(Protocol):
    def free_slots(self, spec: CampaignSpec) -> int: ...

    async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
        """Run command with SWARM_TRIAL and SWARM_METRICS pointing to JSON files."""
        ...


class Worker(Protocol):
    async def run_once(
        self,
        spec: CampaignSpec,
        *,
        before_execute: Callable[[Attempt], Awaitable[None]] | None = None,
    ) -> bool:
        """Claim, heartbeat, call before_execute for this dispatch, then execute and report."""
        ...


class Evaluator(Protocol):
    def evaluate(
        self, spec: CampaignSpec, attempt: Attempt, metrics_path: Path
    ) -> dict[str, float]:
        """Read measurements produced by the task's evaluation stage."""
        ...


class Meter(Protocol):
    def estimate(self, spec: CampaignSpec, trial: Trial) -> float:
        """Cost to reserve when dispatching this trial, in campaign currency."""
        ...

    def charge(
        self,
        spec: CampaignSpec,
        attempt: Attempt,
        wall_seconds: float,
        input_tokens: int,
        output_tokens: int,
    ) -> float: ...


class WorkerQueue(Protocol):
    def claim(
        self,
        campaign_id: str,
        worker_id: str,
        *,
        reserve_cost: float | Callable[[Trial], float] = 0.0,
    ) -> Attempt | None:
        """Reserve budget and claim one queued trial in the same transaction."""
        ...

    def heartbeat(self, attempt_id: str, *, cost: float = 0.0) -> bool:
        """Refresh a live attempt with its measured cumulative cost so far."""
        ...

    def report(self, result: Result) -> bool:
        """Finish once. Failed or lost work requeues while attempts remain."""
        ...

    def requeue_expired(self, campaign_id: str, before: float) -> int:
        """Record active attempts last seen before this UTC timestamp as lost."""
        ...


class Store(WorkerQueue, Protocol):
    def create_campaign(self, spec: CampaignSpec) -> None: ...

    def deadline(self, campaign_id: str) -> float | None:
        """Return the UTC timestamp at which the campaign stops admitting attempts."""
        ...

    def submit(self, trial: Trial) -> bool:
        """Insert once by trial id. A replay returns False."""
        ...

    def trials(self, campaign_id: str) -> Sequence[Trial]: ...

    def results(self, campaign_id: str, *, after: int = 0) -> Sequence[Result]:
        """Append-ordered observations. Skip the first 'after' observations."""
        ...

    def outstanding(self, campaign_id: str) -> int:
        """Count queued and running trials, including retries."""
        ...

    def queue_depth(self, campaign_id: str) -> tuple[int, int]:
        """Return queued and running trial counts from the same snapshot."""
        ...

    def select(
        self, campaign_id: str, trial_id: str, *, score: float | None = None, replicates: int = 1
    ) -> None:
        """Persist the selected successful trial."""
        ...

    def incumbent(self, campaign_id: str) -> tuple[str, float | None, int] | None:
        """Return the selected trial id, policy score and replicate count."""
        ...
