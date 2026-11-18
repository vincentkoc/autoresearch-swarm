import asyncio
import random
from dataclasses import replace
from typing import Any

from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Result, Trial
from research_swarm.swarm.operators import ParamMutation, TextRewrite
from research_swarm.swarm.operators.bandit import OperatorBandit
from research_swarm.swarm.operators.portfolio import ReplicatedSelection, SearchPortfolio


def test_bandit_spends_on_realized_gain_per_cost_and_explores() -> None:
    bandit = OperatorBandit(["expensive", "cheap"], exploration=0.4)
    for _ in range(64):
        name = bandit.choose(bandit.arms)
        gain, cost = (8, 100) if name == "expensive" else (1, 1)
        bandit.observe(name, gain, cost)
    assert bandit.arms["cheap"].pulls > bandit.arms["expensive"].pulls >= 1
    bandit.observe("cheap", 0, 100)
    arm = bandit.arms["cheap"]
    assert arm.rate == arm.gain / (arm.pulls + 100)


def observation(candidate: Candidate, score: float) -> Result:
    trial = Trial(
        trial_id=candidate.candidate_id,
        campaign_id="test",
        candidate=replace(candidate, seed=None, fidelity=None),
        seed=candidate.seed if candidate.seed is not None else 0,
        fidelity=candidate.fidelity if candidate.fidelity is not None else 1.0,
    )
    attempt = Attempt(
        attempt_id=candidate.candidate_id, trial=trial, worker_id="worker", number=1, started_at=0
    )
    return Result(attempt=attempt, status="ok", metrics={"loss": score})


async def test_full_fidelity_replicates_precede_incumbent_selection_and_replay() -> None:
    candidates = tuple(Candidate(candidate_id=str(i), params={"x": i}) for i in range(4))
    spec = CampaignSpec(
        campaign_id="test",
        metric="loss",
        direction="minimize",
        command=("score",),
        max_trials=12,
        max_workers=4,
        candidates=candidates,
    )
    selection = ReplicatedSelection(spec, seed_std=0.01)

    def make_policy() -> SearchPortfolio:
        return SearchPortfolio(
            spec,
            {"param-mutate": ParamMutation({"x": [0, 1, 2, 3, 4]}, random.Random(2))},
            fidelities=[0.25, 1],
            reduction=2,
            selection=selection,
        )

    policy = make_policy()
    initial = await policy.ask(spec, [], 4)
    archive = [observation(c, c.params["x"]) for c in initial]
    promotions = await policy.ask(spec, archive, 2)
    assert [(p.params["x"], p.operator, p.fidelity) for p in promotions] == [
        (0, "asha", 1),
        (1, "asha", 1),
    ]
    archive.extend(observation(c, 10 - 5 * c.params["x"]) for c in promotions)
    assert selection(archive) is None
    replicates = await make_policy().ask(spec, archive, 2)
    assert [p.params["x"] for p in replicates] == [1, 0]
    assert all(p.operator == "replicate" and p.seed == 1 and p.fidelity == 1 for p in replicates)
    archive.extend(observation(c, 10 - 5 * c.params["x"]) for c in replicates)
    winner = selection(archive)
    assert winner is not None and winner.result.attempt.trial.candidate.params == {"x": 1}
    assert all(c.operator not in {"replicate", "asha"} for c in await policy.ask(spec, archive, 1))


async def test_mix_of_timeout_and_fast_operators_returns_inside_loop_deadline() -> None:
    async def hang(prompt: str, schema: dict[str, Any], seed: int) -> str:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    base = Candidate(candidate_id="a", params={"width": 1}, text="instruction")
    spec = CampaignSpec(
        campaign_id="test",
        metric="loss",
        direction="minimize",
        command=("score",),
        max_trials=5,
        max_workers=3,
        candidates=(base,),
        proposal_timeout_seconds=0.15,
    )
    model = TextRewrite(hang, "shorten")
    mutation = ParamMutation({"width": [1, 2]}, random.Random(1))
    policy = SearchPortfolio(spec, {model.name: model, mutation.name: mutation})
    async with asyncio.timeout(spec.proposal_timeout_seconds):
        batch = await policy.ask(spec, [], 3)
    assert [c.params["width"] for c in batch] == [1, 2]
    assert model.timeouts == 1
