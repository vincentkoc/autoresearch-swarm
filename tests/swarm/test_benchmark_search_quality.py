"""Opt-in CPU-small comparison on the benchmark suite's declared trial budget."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from statistics import fmean

import pytest

from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Result, Trial
from research_swarm.swarm.operators.config import SearchOptions


def compare_seed(task, study, seed: int, model: str) -> dict[str, tuple[float, float]]:
    from benchmarks.study import run_campaign
    from benchmarks.suite import greedy_search, random_search

    initial = Candidate(candidate_id="initial", params=task.initial)
    spec = CampaignSpec(
        campaign_id=task.name,
        metric=task.metric,
        direction=task.direction,
        command=("evaluate",),
        candidates=(initial,),
        max_trials=12,
        max_workers=1,
        seed=seed,
        proposal_timeout_seconds=30.0,
    )
    arms = ["param-mutate", "crossover"]
    if task.name != "mlp":
        arms.insert(0, "llm-choice-rewrite")
    policy = SearchOptions.model_validate(
        {
            "space": task.space,
            "model": model,
            "objective": task.objective,
            "operators": arms,
            "replicate": False,
        }
    ).build(spec)
    previous = initial
    row = {}
    with asyncio.Runner() as runner:

        def propose(task_, history, rng):
            nonlocal previous
            last = history[-1]
            trial = Trial(
                trial_id=str(len(history)),
                campaign_id=task.name,
                seed=seed,
                fidelity=1.0,
                candidate=previous,
            )
            attempt = Attempt(
                attempt_id=trial.trial_id, trial=trial, worker_id="cpu", number=1, started_at=0
            )
            policy.tell(
                Result(
                    attempt=attempt,
                    status="ok" if last.score is not None else "failed",
                    metrics={task.metric: last.score} if last.score is not None else {},
                    error=last.error,
                )
            )
            batch = runner.run(policy.ask(spec, [], 1))
            if not batch:
                raise ValueError("proposal batch exhausted")
            previous = batch[0]
            return previous.params

        for arm, fn in [("swarm", propose), ("random", random_search), ("greedy", greedy_search)]:
            run = run_campaign(task, study, fn, budget=12, seed=seed)
            assert len(run.trials) == 12 and not any(t.error for t in run.trials), [
                t.error for t in run.trials
            ]
            curve = [t.best for t in run.trials]
            assert curve == sorted(curve, reverse=task.direction == "minimize")
            assert run.decision is not None
            confirmed_gain = (
                (run.decision.confirmation_gain or 0.0) if run.decision.promoted else 0.0
            )
            row[arm] = (curve[-1], confirmed_gain)
            print(task.name, seed, arm, "search", curve[-1], "held-out", run.decision, flush=True)
    return row


@pytest.mark.skipif(
    "SWARM_BENCHMARK_MODEL" not in os.environ,
    reason="requires a local Ollama model and torch extra",
)
@pytest.mark.parametrize("name", ["program", "mlp", "nanogpt"])
def test_equal_budget_search(name: str, tmp_path: Path) -> None:
    import torch

    torch.set_num_threads(1)
    from benchmarks.study import open_study
    from benchmarks.suite import get_task

    task = get_task(name, device="cpu", size="small")
    study = open_study(task, tmp_path / "study.sqlite")
    rows = [
        compare_seed(task, study, seed, os.environ["SWARM_BENCHMARK_MODEL"])
        for seed in (11, 23, 47)
    ]
    sign = -1 if name == "nanogpt" else 1
    for baseline in ("random", "greedy"):
        for panel in (0, 1):
            deltas = [
                (sign if panel == 0 else 1) * (row["swarm"][panel] - row[baseline][panel])
                for row in rows
            ]
            assert fmean(deltas) > 0, (name, baseline, panel, deltas)
