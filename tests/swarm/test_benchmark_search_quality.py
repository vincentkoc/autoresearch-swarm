"""Opt-in CPU-small comparison on the benchmark suite's declared trial budget."""

from __future__ import annotations

import asyncio
import hashlib
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
            assert len(run.trials) == 12
            failures = [(i, t.error) for i, t in enumerate(run.trials, 1) if t.error]
            curve = [t.best for t in run.trials]
            assert curve == sorted(curve, reverse=task.direction == "minimize")
            assert run.decision is not None
            confirmed_gain = (
                (run.decision.confirmation_gain or 0.0) if run.decision.promoted else 0.0
            )
            row[arm] = (curve[-1], confirmed_gain)
            print(
                task.name,
                seed,
                arm,
                "curve",
                curve,
                "failures",
                failures,
                "held-out",
                run.decision,
                flush=True,
            )
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

    protocol = os.getenv("SWARM_BENCHMARK_PROTOCOL", "v2")
    task = get_task(name, device="cpu", size="small", protocol=protocol)
    study = open_study(task, tmp_path / "study.sqlite")
    seeds = (
        (11, 23, 47)
        if protocol == "v1"
        else tuple(
            int.from_bytes(
                hashlib.sha256(f"ars-bench-v2/search-seed/{i}".encode()).digest()[:4], "big"
            )
            for i in range(3)
        )
    )
    rows = [compare_seed(task, study, seed, os.environ["SWARM_BENCHMARK_MODEL"]) for seed in seeds]
    sign = -1 if name == "nanogpt" else 1
    comparisons = {}
    for baseline in ("random", "greedy"):
        search = [sign * (row["swarm"][0] - row[baseline][0]) for row in rows]
        confirmed = [row["swarm"][1] - row[baseline][1] for row in rows]
        print(name, baseline, "paired search", search, "paired confirmation", confirmed, flush=True)
        comparisons[baseline] = fmean(confirmed)
        if protocol == "v1":
            comparisons[baseline + "_search"] = fmean(search)
    assert all(delta > 0 for delta in comparisons.values()), (name, comparisons)
