"""A fixed-budget comparison. Each policy sees only previous search results."""

from __future__ import annotations

import math
import random
import re
import statistics
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal

Candidate = dict[str, str]
Direction = Literal["maximize", "minimize"]

# Independent data streams. The trial seed controls initialization and sampling.
PANEL_SEEDS = {
    "train": 17,
    "search": 101,
    "selection": 211,
    "confirmation": 307,
    "calibration_proxy": 401,
    "calibration_target": 503,
}

Score = Callable[[Candidate, int, str, str, str, float], float]


@dataclass(frozen=True)
class Task:
    name: str
    objective: str
    metric: str
    direction: Direction
    space: dict[str, tuple[str, ...]]
    evaluate: Score
    device: str
    size: str

    @property
    def initial(self) -> Candidate:
        return {key: choices[0] for key, choices in self.space.items()}

    def score(
        self, candidate: Candidate, *, seed: int, panel: str = "search", fidelity: float = 1.0
    ) -> float:
        if candidate.keys() != self.space.keys() or any(
            candidate[key] not in choices for key, choices in self.space.items()
        ):
            raise ValueError("candidate falls outside the declared search space")
        if panel not in PANEL_SEEDS or panel == "train":
            raise ValueError("unknown evaluation panel")
        if not 0 < fidelity <= 1:
            raise ValueError("fidelity must be in (0, 1]")
        value = self.evaluate(candidate, seed, panel, self.device, self.size, fidelity)
        if not math.isfinite(value):
            raise ValueError("non-finite metric")
        return value

    def improved(self, value: float, previous: float) -> bool:
        return value > previous if self.direction == "maximize" else value < previous


@dataclass(frozen=True)
class Trial:
    candidate: Candidate | None
    score: float | None
    best: float | None
    seconds: float
    error: str | None = None


Policy = Callable[[Task, Sequence[Trial], random.Random], Candidate]


def random_search(task: Task, history: Sequence[Trial], rng: random.Random) -> Candidate:
    return {key: rng.choice(values) for key, values in task.space.items()}


def incumbent(task: Task, history: Sequence[Trial]) -> Candidate:
    successful = [trial for trial in history if trial.score is not None]
    if not successful:
        return task.initial
    # 'best' is the cumulative best through each trial. Keep the first strict winner.
    best = successful[-1].best
    winner = next(trial for trial in successful if trial.score == best)
    assert winner.candidate is not None
    return winner.candidate.copy()


def greedy_search(task: Task, history: Sequence[Trial], rng: random.Random) -> Candidate:
    parent = incumbent(task, history)
    neighbors = [
        {**parent, key: value}
        for key, choices in task.space.items()
        for value in choices
        if value != parent[key]
    ]
    unseen = [child for child in neighbors if all(t.candidate != child for t in history)]
    return rng.choice(unseen or neighbors)


def run_search(task: Task, propose: Policy, *, budget: int, seed: int) -> list[Trial]:
    if budget < 1:
        raise ValueError("budget must include at least the initial trial")
    rng = random.Random(seed)
    history: list[Trial] = []
    best = None
    for _ in range(budget):
        start = time.perf_counter()
        candidate = None
        score = None
        error = None
        try:
            candidate = task.initial if not history else propose(task, tuple(history), rng)
            score = task.score(candidate, seed=seed)
            if best is None or task.improved(score, best):
                best = score
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        history.append(Trial(candidate, score, best, time.perf_counter() - start, error))
    return history


OPS = ("identity", "strip", "lower", "hyphenate", "trim-hyphens")
PROGRAM_SPACE = {f"line{i}": OPS for i in range(1, 5)}


def _slug_score(
    candidate: Candidate, seed: int, panel: str, device: str, size: str, fidelity: float
) -> float:
    rng = random.Random(PANEL_SEEDS[panel])
    operations: dict[str, Callable[[str], str]] = {
        "identity": lambda s: s,
        "strip": str.strip,
        "lower": str.lower,
        "hyphenate": lambda s: re.sub(r"[\s_-]+", "-", s),
        "trim-hyphens": lambda s: s.strip("-"),
    }
    scores = []
    for index in range(math.ceil(fidelity * (48 if size == "small" else 512))):
        words = rng.sample(("Elm", "ash", "OAK", "gingko", "FIR", "Birch"), k=2)
        # The unique suffix identifies a fixed case across all runs of this panel.
        suffix = f"{PANEL_SEEDS[panel]}x{index}"
        raw = rng.choice(("", " ", "__", "-")) + words[0]
        raw += rng.choice((" ", "__", " - ", "-")) + words[1] + "-" + suffix
        value = raw + rng.choice(("", " ", "  ", "_"))
        for key in PROGRAM_SPACE:
            value = operations[candidate[key]](value)
        expected = "-".join([*(word.lower() for word in words), suffix])
        scores.append(value == expected)
    return statistics.mean(scores)


def get_task(name: str, device: str = "cpu", size: str = "small") -> Task:
    if size not in ("small", "full") or device not in ("cpu", "mps", "cuda"):
        raise ValueError("unknown size or device")
    if name == "program":
        if device != "cpu":
            raise ValueError("the program evaluator runs on CPU")
        return Task(
            name,
            "Normalize labels to lowercase words joined by one hyphen. "
            "Remove leading and trailing separators.",
            "pass_rate",
            "maximize",
            PROGRAM_SPACE,
            _slug_score,
            device,
            size,
        )
    from benchmarks.training import MLP_SPACE, TRAINING_SPACE, mlp_accuracy, nanogpt_loss

    if name == "mlp":
        return Task(
            name,
            "Classify synthetic points by quadrant parity.",
            "accuracy",
            "maximize",
            MLP_SPACE,
            mlp_accuracy,
            device,
            size,
        )
    if name == "nanogpt":
        return Task(
            name,
            "Edit the optimizer and learning-rate schedule in the causal LM training "
            "loop to minimize next-token validation loss.",
            "val_loss",
            "minimize",
            TRAINING_SPACE,
            nanogpt_loss,
            device,
            size,
        )
    raise ValueError(f"unknown task {name!r}")
