"""Run a comparison with calibration and held-out selection shared by its arms."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from benchmarks.suite import (
    PANEL_SEEDS,
    Candidate,
    Policy,
    Task,
    Trial,
    comparison_seeds,
    incumbent,
    run_search,
)
from research_swarm.swarm.panels import Calibration, Decision, PanelSplit, PanelStudy, Role


def recipe_key(candidate: Candidate) -> str:
    return json.dumps(candidate, sort_keys=True, separators=(",", ":"))


def open_study(task: Task, path: Path) -> PanelStudy:
    first = task.initial
    mid_rate, high_rate = ("0.005", "0.02") if task.protocol == "v1" else ("0.004", "0.016")
    if task.name == "program":
        partial = {**first, "line1": "lower", "line2": "strip"}
        full = {**partial, "line2": "hyphenate", "line3": "trim-hyphens"}
    elif task.name == "mlp":
        partial = {**first, "learning_rate": mid_rate}
        full = {**partial, "width": "32", "activation": "gelu", "learning_rate": high_rate}
    else:
        partial = {**first, "rate": f"base_lr = {mid_rate}"}
        full = {
            **partial,
            "schedule": "return base_lr * (1 - progress)",
            "rate": f"base_lr = {high_rate}",
        }
    panels = PanelSplit(**{role: task.members(role) for role in PANEL_SEEDS})

    def evaluate(key: str, role: Role, members: tuple[str, ...], seed: int) -> float:
        if members != task.members(role):
            raise ValueError("panel membership does not match the workload")
        return task.score(json.loads(key), seed=seed, panel=role)

    return PanelStudy(
        path,
        panels=panels,
        variants=[recipe_key(c) for c in (first, partial, full)],
        seeds=comparison_seeds(task.protocol, calibration=True),
        evaluate=evaluate,
        direction=task.direction,
        revision=f"{task.name}/{task.metric}/{task.device}/{task.size}/{task.protocol[1:]}",
        look_limits={"search": 120, "selection": 36, "confirmation": 6},
    )


@dataclass(frozen=True)
class Arm:
    trials: list[Trial]
    decision: Decision | None
    selected: Candidate | None
    confirmed_score: float | None


def run_campaign(task: Task, study: PanelStudy, proposer: Policy, *, budget: int, seed: int) -> Arm:
    study.calibrate()
    trials = run_search(
        task,
        proposer,
        budget=budget,
        seed=seed,
        evaluate=lambda c, s: study.search(recipe_key(c), s),
    )
    if not any(trial.score is not None for trial in trials):
        return Arm(trials, None, None, None)
    baseline = study.consider(recipe_key(task.initial), None)
    if not baseline.promoted:
        return Arm(trials, baseline, None, None)
    challenger = incumbent(task, trials)
    decision = study.consider(recipe_key(challenger), None)
    selected = challenger if decision.promoted else task.initial
    return Arm(trials, decision, selected, study.confirmed_score(recipe_key(selected)))


class CampaignInputs(Protocol):
    @property
    def campaign_id(self) -> str: ...
    @property
    def task_dir(self) -> str: ...
    @property
    def command(self) -> tuple[str, ...]: ...
    @property
    def metric(self) -> str: ...
    @property
    def direction(self) -> str: ...


class WorkerPanelPolicy:
    """Translate the swarm loop's serialized candidate content into a recipe key."""

    def __init__(self, study: PanelStudy):
        self.study = study

    @staticmethod
    def _recipe(content: str) -> str:
        return recipe_key(json.loads(content)["params"])

    def calibrate(self) -> Calibration:
        return self.study.calibrate()

    def record_search(self, candidate_key: str, seed: int) -> None:
        self.study.record_search(self._recipe(candidate_key), seed)

    def confirmed_score(self, candidate_key: str) -> float | None:
        return self.study.confirmed_score(self._recipe(candidate_key))

    def consider(self, challenger_key: str, incumbent_key: str | None) -> Decision:
        reference = self._recipe(incumbent_key) if incumbent_key is not None else None
        return self.study.consider(self._recipe(challenger_key), reference)


def worker_policy(spec: CampaignInputs) -> WorkerPanelPolicy:
    """YAML panel_policy factory for commands invoking python -m benchmarks.trial."""
    from benchmarks.trial import parse_task

    if spec.command[1:3] != ("-m", "benchmarks.trial"):
        raise ValueError("panel_policy expects python -m benchmarks.trial TASK")
    task = parse_task(spec.command[3:])
    if spec.metric != task.metric or spec.direction != task.direction:
        raise ValueError("campaign objective differs from workload objective")
    namespace = hashlib.sha256(spec.campaign_id.encode()).hexdigest()
    path = Path(spec.task_dir) / ".research" / "panels" / f"{namespace}.sqlite"
    return WorkerPanelPolicy(open_study(task, path))
