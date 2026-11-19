import asyncio
import json
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


async def test_model_search_is_independent_of_held_out_worker_metrics() -> None:
    base = Candidate(candidate_id="initial", text="baseline")
    spec = CampaignSpec(
        campaign_id="test",
        metric="loss",
        direction="minimize",
        command=("score",),
        candidates=(base,),
        max_trials=3,
        max_workers=1,
    )
    prompts = []
    proposals = []

    async def generate(prompt: str, schema: dict[str, Any], seed: int) -> str:
        prompts.append(json.loads(prompt))
        history = prompts[-1]["recent_results"]
        content = (
            "fit the held-out score"
            if history[-1]["metrics"].get("confirmation", 0) > 0
            else "use search"
        )
        return json.dumps({"content": content, "rationale": "fixture"})

    for held_out in (-1.0, 1.0):
        editor = TextRewrite(generate, "minimize loss")
        policy = SearchPortfolio(spec, {editor.name: editor})
        failed = replace(
            observation(Candidate(candidate_id="failed", text="invalid"), 0),
            status="failed",
            metrics={"selection": held_out},
            error="invalid recipe",
        )
        metrics = {"loss": 0.0, "selection": -held_out, "confirmation": held_out}
        scored = replace(observation(base, 0), metrics=metrics.copy())
        (proposal,) = await policy.ask(spec, [failed, scored], 1)
        proposals.append(proposal.text)
        assert scored.metrics == metrics

    assert proposals == ["use search", "use search"]
    assert prompts[0] == prompts[1]
    assert [row["metrics"] for row in prompts[0]["recent_results"]] == [{}, {"loss": 0.0}]


async def test_model_repairs_a_terminal_workload_failure_without_a_scored_parent() -> None:
    seed = Candidate(candidate_id="broken", text="sum()")
    spec = CampaignSpec(
        campaign_id="test",
        metric="loss",
        direction="minimize",
        command=("score",),
        max_trials=4,
        max_workers=1,
        max_attempts=1,
        candidates=(seed,),
    )
    calls = []

    async def diagnose(prompt: str, schema: dict[str, Any], sample_seed: int) -> str:
        calls.append(json.loads(prompt))
        if "cause" in schema["properties"]:
            return json.dumps({"cause": "workload", "reason": "sum needs the input sequence"})
        assert calls[-1]["diagnosis"]["cause"] == "workload"
        return json.dumps({"content": "sum(values)", "rationale": "supply the missing argument"})

    model = TextRewrite(diagnose, "minimize loss")
    policy = SearchPortfolio(spec, {model.name: model})
    (initial,) = await policy.ask(spec, [], 1)
    failure = replace(
        observation(initial, 0),
        status="failed",
        metrics={},
        error="exited 1",
        logs=("x" * 2048 + "missing argument",),
    )
    (repair,) = await policy.ask(spec, [failure], 1)
    assert repair.text == "sum(values)" and repair.parents == (seed.candidate_id,)
    assert calls[0]["failure"]["error"] == "exited 1"
    assert calls[0]["failure"]["attempt_number"] == failure.attempt.number
    (tail,) = calls[0]["failure"]["output_tail"]
    assert len(tail) == 1024 and tail.endswith("missing argument")
    replay = TextRewrite(diagnose, "minimize loss")
    same_failure = replace(failure, attempt=replace(failure.attempt, attempt_id="new-storage-id"))
    replay_policy = SearchPortfolio(spec, {replay.name: replay})
    await replay_policy.ask(spec, [], 1)
    await replay_policy.ask(spec, [same_failure], 1)
    assert calls[2:] == calls[:2]

    async def environment(prompt: str, schema: dict[str, Any], sample_seed: int) -> str:
        context = json.loads(prompt)
        calls.append(context)
        if "failure" not in context:
            return json.dumps({"content": seed.text, "rationale": "same input"})
        return json.dumps({"cause": "environment", "reason": "worker unavailable"})

    for outcome in ("lost", "retry", "environment"):
        campaign = replace(spec, max_attempts=2 if outcome == "retry" else 1)
        operator = TextRewrite(environment, "minimize loss")
        fresh = SearchPortfolio(campaign, {operator.name: operator})
        await fresh.ask(campaign, [], 1)
        before = len(calls)
        result = replace(failure, status="lost") if outcome == "lost" else failure
        assert await fresh.ask(campaign, [result], 1) == []
        assert sum("failure" in call for call in calls[before:]) == int(outcome == "environment")
        assert "failure" not in calls[-1]
        if outcome == "environment":
            before = len(calls)
            assert await fresh.ask(campaign, [result], 1) == []
            assert all("failure" not in call for call in calls[before:])


async def test_diagnosis_timeout_returns_the_fast_sibling_proposal() -> None:
    async def silent(prompt: str, schema: dict[str, Any], seed: int) -> str:
        assert "cause" in schema["properties"]
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    base = Candidate(candidate_id="initial", text="instruction", params={"width": 1})
    spec = CampaignSpec(
        campaign_id="test",
        metric="loss",
        direction="minimize",
        command=("score",),
        max_trials=5,
        max_workers=2,
        max_attempts=1,
        candidates=(base,),
        proposal_timeout_seconds=0.25,
    )
    model = TextRewrite(silent, "repair the artifact")
    mutate = ParamMutation({"width": [1, 2]}, random.Random(1))
    policy = SearchPortfolio(spec, {model.name: model, mutate.name: mutate})
    (initial,) = await policy.ask(spec, [], 1)
    failure = replace(observation(initial, 0), status="failed", metrics={}, error="invalid input")
    async with asyncio.timeout(spec.proposal_timeout_seconds):
        batch = await policy.ask(spec, [failure], 2)
    assert [candidate.params["width"] for candidate in batch] == [2]
    assert model.timeouts >= 1


async def test_pending_promotions_and_followup_cost_survive_restart() -> None:
    a = Candidate(
        candidate_id="a", params={"x": 1}, operator="param-mutate", family="a", fidelity=0.25
    )
    b = Candidate(
        candidate_id="b", params={"x": 2}, operator="param-mutate", family="b", fidelity=0.25
    )
    spec = CampaignSpec(
        campaign_id="test",
        metric="loss",
        direction="minimize",
        command=("score",),
        max_workers=2,
        max_trials=8,
    )
    mutate = ParamMutation({"x": [1, 2, 3, 4]}, random.Random(4))

    def make() -> SearchPortfolio:
        return SearchPortfolio(
            spec, {mutate.name: mutate}, fidelities=[0.25, 1], reduction=2, family_trial_limit=2
        )

    original = make()
    archive = [replace(observation(c, float(c.params["x"])), cost=3) for c in (a, b)]
    upgrade = (await original.ask(spec, archive, 1))[0]
    assert upgrade.operator == "asha"
    resumed = make()
    for result in archive:
        resumed.tell(result)
    pending = observation(upgrade, 0).attempt.trial
    resumed.restore_trials([r.attempt.trial for r in archive] + [pending])
    next_batch = await resumed.ask(spec, archive, 1)
    assert all(c.operator != "asha" and c.family != "a" for c in next_batch)
    resumed.tell(replace(observation(upgrade, 0), status="failed", cost=12))
    assert resumed.bandit.arms[mutate.name].cost == 18
