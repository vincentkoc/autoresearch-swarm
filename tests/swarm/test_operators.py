import asyncio
import json
import random
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from research_swarm.swarm.interfaces import (
    Attempt,
    CampaignSpec,
    Candidate,
    Operator,
    Result,
    Trial,
)
from research_swarm.swarm.operators import (
    CodeDiff,
    Crossover,
    EnsembleAdd,
    NewSeed,
    OptunaSampler,
    ParamMutation,
    Range,
    TextRewrite,
)

SPEC = CampaignSpec(
    campaign_id="test",
    metric="loss",
    direction="minimize",
    command=("train",),
    max_workers=4,
    max_trials=10,
)
PARENT = Candidate(
    candidate_id="a", params={"width": 8, "activation": "relu"}, family="mlp", seed=1, fidelity=0.25
)


async def test_lineage_and_recipe_operators() -> None:
    other = replace(PARENT, candidate_id="b", params={"width": 16, "activation": "tanh"})
    third = replace(PARENT, candidate_id="c", params={"width": 32, "activation": "tanh"})
    mutate: Operator = ParamMutation(
        {"width": [8, 16], "activation": ["relu", "tanh"]}, random.Random(4)
    )
    children = await mutate.propose(SPEC, [PARENT], 2)
    assert len(children) == 2
    for candidate in children:
        assert candidate.parents == ("a",) and candidate.family == "mlp"
        assert sum(candidate.params[k] != v for k, v in PARENT.params.items()) == 1
    cross = (await Crossover(random.Random(4)).propose(SPEC, [PARENT, other], 1))[0]
    assert set(cross.parents) == {"a", "b"}
    assert cross.params not in (PARENT.params, other.params)
    ensemble = (await EnsembleAdd().propose(SPEC, [PARENT, other], 1))[0]
    grown = (await EnsembleAdd().propose(SPEC, [ensemble, other, third], 2))[0]
    assert [m["candidate_id"] for m in grown.params["members"]] == ["a", "b", "c"]
    assert grown.params["members"][0]["params"] == PARENT.params
    seeds = await NewSeed(random.Random(12)).propose(SPEC, [PARENT], 5)
    assert len({c.seed for c in seeds} - {1}) == 5
    assert all(c.params == PARENT.params and c.fidelity == 0.25 for c in seeds)


async def test_optuna_updates_actual_trial_and_handles_retry() -> None:
    pytest.importorskip("optuna")
    spec = replace(SPEC, max_attempts=2)
    sampler = OptunaSampler(
        {
            "lr": Range(0.001, 0.1, log=True),
            "width": Range(8, 16, integer=True),
            "activation": ["relu", "tanh"],
        }
    )
    a, b = await sampler.propose(spec, [], 2)
    for candidate in [a, b]:
        assert 0.001 <= candidate.params["lr"] <= 0.1
        assert isinstance(candidate.params["width"], int)
    for candidate, value in [(b, 1.5), (a, 4.0)]:
        trial = Trial(
            trial_id=candidate.candidate_id,
            campaign_id="test",
            candidate=candidate,
            seed=1,
            fidelity=1,
        )
        attempt = Attempt(attempt_id="first", trial=trial, worker_id="test", number=1, started_at=0)
        sampler.tell(Result(attempt=attempt, status="lost"))
        done = Result(attempt=replace(attempt, number=2), status="ok", metrics={"loss": value})
        sampler.tell(done)
        sampler.tell(done)
    assert sampler.studies["test"].best_trial.params == b.params


async def test_parallel_timeout_keeps_fast_results_and_caps_late_work() -> None:
    both_started = asyncio.Event()
    release = asyncio.Event()
    started = 0

    async def fake(prompt: str, schema: dict[str, Any], seed: int) -> str:
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await both_started.wait()
        if seed == 1:
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()
        assert schema["additionalProperties"] is False
        return json.dumps({"content": str(seed), "rationale": "shorten"})

    op = TextRewrite(fake, "make the prompt shorter", concurrency=2)
    spec = replace(SPEC, proposal_timeout_seconds=0.05)
    try:
        batch = await asyncio.wait_for(op.propose(spec, [PARENT], 8), timeout=1)
        assert [c.text for c in batch] == ["2"]
        assert op.timeouts == 1
        batch = await asyncio.wait_for(op.propose(spec, [PARENT], 8), timeout=1)
        assert [c.text for c in batch] == ["3"]
    finally:
        release.set()


async def test_diff_applies_to_pinned_source_and_bad_json_is_dropped(tmp_path: Path) -> None:
    async def fake(prompt: str, schema: dict[str, Any], seed: int) -> str:
        if seed == 1:
            return json.dumps(
                {"before": "baseline\n", "after": "improved", "rationale": "change recipe"}
            )
        return '{"content":42,"extra":true}'

    source = tmp_path / "recipe.txt"
    source.write_text("return baseline\n")
    op = CodeDiff(
        fake, "improve the recipe", path=source.name, source=source.read_text(), revision="base"
    )
    batch = await op.propose(SPEC, [PARENT], 2)
    assert len(batch) == 1 and op.errors == 1
    diff = batch[0].diff
    assert diff and batch[0].source_revision == "base"
    subprocess.run(["git", "apply", "-"], cwd=tmp_path, input=diff, text=True, check=True)
    assert source.read_text() == "return improved"
