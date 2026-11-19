"""Opt-in CPU-small comparison on the benchmark suite's declared trial budget."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from itertools import pairwise
from pathlib import Path
from statistics import fmean
from unittest.mock import AsyncMock, Mock, patch

import httpx
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
            # A proposal error consumes a search slot without creating a worker trial.
            if last.candidate is not None:
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
            # Read token counts from successful HTTP responses without changing proposal inputs.
            tokens = [0, 0]
            lock = threading.Lock()
            send = httpx.AsyncClient.send

            async def metered_send(
                client, request, *, original=send, counts=tokens, guard=lock, **kwargs
            ):
                response = await original(client, request, **kwargs)
                if request.url.path == "/api/generate" and response.is_success:
                    body = response.json()
                    with guard:
                        counts[0] += body.get("prompt_eval_count", 0)
                        counts[1] += body.get("eval_count", 0)
                return response

            start = time.monotonic()
            with patch.object(httpx.AsyncClient, "send", metered_send):
                run = run_campaign(task, study, fn, budget=12, seed=seed)
            elapsed = time.monotonic() - start
            assert len(run.trials) == 12
            failures = [(i, t.error) for i, t in enumerate(run.trials, 1) if t.error]
            curve = [t.best for t in run.trials]
            assert all(
                before is None or (after is not None and not task.improved(before, after))
                for before, after in pairwise(curve)
            )
            assert run.decision is not None
            assert run.confirmed_score is not None
            row[arm] = (curve[-1], run.confirmed_score)
            print(
                "arm",
                json.dumps(
                    {
                        "task": task.name,
                        "seed": seed,
                        "policy": arm,
                        "curve": curve,
                        "failed_attempts": failures,
                        "selected": run.selected,
                        "confirm": run.confirmed_score,
                        "decision": run.decision.reason,
                        "seconds": elapsed,
                        "input_tokens": tokens[0],
                        "output_tokens": tokens[1],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
    return row


def test_failed_proposal_uses_a_slot_without_charging_the_previous_recipe(
    monkeypatch, capsys, tmp_path
):
    from benchmarks.study import open_study
    from benchmarks.suite import Task

    task = Task(
        "toy",
        "score",
        "score",
        "maximize",
        {"x": tuple(map(str, range(12)))},
        lambda *args: 0.0,
        "cpu",
        "small",
    )
    policy = Mock()
    policy.ask = AsyncMock(
        side_effect=[
            [] if i == 2 else [Candidate(candidate_id=str(i), params={"x": str(i)})]
            for i in range(1, 12)
        ]
    )
    monkeypatch.setattr(SearchOptions, "build", lambda self, spec: policy)

    def score(recipe, role, members, seed):
        value = float(json.loads(recipe)["x"])
        if role == "search" and value == 0:
            raise ValueError("initial recipe failed")
        return value

    study = open_study(task, tmp_path / "toy.sqlite")
    monkeypatch.setattr(study, "evaluate", score)
    compare_seed(task, study, 23, "fixture")
    swarm = json.loads(capsys.readouterr().out.splitlines()[0].removeprefix("arm "))
    assert swarm["curve"][:3] == [None, 1, 1]
    assert len(swarm["curve"]) == 12
    assert [slot for slot, _ in swarm["failed_attempts"]] == [1, 3]
    failed = [
        call.args[0] for call in policy.tell.call_args_list if call.args[0].status == "failed"
    ]
    assert [result.attempt.trial.candidate.params for result in failed] == [task.initial]


@pytest.mark.skipif(
    "SWARM_BENCHMARK_MODEL" not in os.environ,
    reason="requires a local Ollama model and torch extra",
)
@pytest.mark.parametrize("name", ["program", "mlp", "nanogpt"])
def test_equal_budget_search(name: str, tmp_path: Path) -> None:
    import torch

    torch.set_num_threads(1)
    from benchmarks.study import open_study
    from benchmarks.suite import comparison_seeds, get_task

    protocol = os.getenv("SWARM_BENCHMARK_PROTOCOL", "v2")
    task = get_task(name, device="cpu", size="small", protocol=protocol)
    study = open_study(task, tmp_path / "study.sqlite")
    seeds = comparison_seeds(protocol)
    rows = [compare_seed(task, study, seed, os.environ["SWARM_BENCHMARK_MODEL"]) for seed in seeds]
    sign = -1 if name == "nanogpt" else 1
    comparisons = {}
    for baseline in ("random", "greedy"):
        search = [sign * (row["swarm"][0] - row[baseline][0]) for row in rows]
        confirmed = [sign * (row["swarm"][1] - row[baseline][1]) for row in rows]
        print(name, baseline, "paired search", search, "paired confirmation", confirmed, flush=True)
        comparisons[baseline] = fmean(confirmed)
        if protocol == "v1":
            comparisons[baseline + "_search"] = fmean(search)
    assert all(delta > 0 for delta in comparisons.values()), (name, comparisons)
