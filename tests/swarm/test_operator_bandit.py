import asyncio
import json
import sqlite3
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Result, Trial
from research_swarm.swarm.operators.bandit import BanditProposer
from research_swarm.swarm.operators.llm import TextRewrite

SPEC = CampaignSpec(
    campaign_id="allocation",
    metric="score",
    direction="maximize",
    command=("evaluate",),
    max_trials=8,
    max_workers=2,
)


def result(name, key, value, cost, *, number=1, fidelity=1.0):
    candidate = Candidate(candidate_id=key, operator=name, params={"label": key})
    trial = Trial(
        trial_id=key, campaign_id=SPEC.campaign_id, candidate=candidate, seed=0, fidelity=fidelity
    )
    attempt = Attempt(
        attempt_id=f"{key}-{number}", trial=trial, worker_id="cpu", number=number, started_at=0
    )
    return Result(
        attempt=attempt,
        status="failed" if value is None else "ok",
        metrics={} if value is None else {"score": value},
        cost=cost,
    )


class Offer:
    def __init__(self, name):
        self.name = name
        self.calls = 0

    async def propose(self, spec, parents, k):
        self.calls += 1
        return [
            Candidate(
                candidate_id=f"{self.name}-{self.calls}",
                operator=self.name,
                params={"new": self.name},
            )
        ]


async def test_gain_per_cost_and_pending_work_survive_replay():
    operators = {name: Offer(name) for name in ("cheap", "expensive")}
    policy = BanditProposer(SPEC, operators)
    baseline = result("seed", "baseline", 0, 0)
    cheap = result("cheap", "one", 1, 1)
    pricey = result("expensive", "two", 3, 100)
    # A lower-fidelity score cannot set the reference for full-fidelity gain.
    coarse = result("seed", "coarse", 500, 1, fidelity=0.25)
    archive = [coarse, baseline, cheap, pricey]
    (proposal,) = await policy.ask(SPEC, archive, 1)
    assert proposal.operator == "cheap"
    assert policy.arms["expensive"].gain == 2
    assert policy.arms["cheap"].gain == 1
    failure = result("cheap", "failed", None, 30)
    policy.tell(failure)
    policy.tell(failure)
    assert policy.arms["cheap"].spend == 31

    fresh = BanditProposer(SPEC, {name: Offer(name) for name in operators})
    for observation in [*archive, failure]:
        fresh.tell(observation)
    pending = replace(cheap.attempt.trial, trial_id="pending", candidate=proposal)
    fresh.restore_trials([*(r.attempt.trial for r in archive), pending])
    (other,) = await fresh.ask(SPEC, archive, 1)
    assert other.operator == "expensive" and other.params != proposal.params
    assert fresh.arms["cheap"].spend == policy.arms["cheap"].spend


async def test_silent_local_model_keeps_fast_sibling_and_does_not_hold_loop():
    async def silent(prompt, schema, seed):
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    model = TextRewrite(silent, "rewrite the text")
    fast = Offer("fast")
    spec = replace(SPEC, proposal_timeout_seconds=0.25)
    policy = BanditProposer(spec, {model.name: model, fast.name: fast})
    async with asyncio.timeout(spec.proposal_timeout_seconds):
        children = await policy.ask(spec, [result("seed", "baseline", 0, 0)], 2)
    assert [c.operator for c in children] == ["fast"]
    assert model.timeouts == 1


@pytest.mark.parametrize("error", [asyncio.CancelledError(), AssertionError("operator defect")])
async def test_proposal_control_and_program_errors_reach_the_caller(error):
    async def interrupted(prompt, schema, seed):
        raise error

    model = TextRewrite(interrupted, "rewrite the text")
    policy = BanditProposer(SPEC, {model.name: model})
    with pytest.raises(type(error)):
        await policy.ask(SPEC, [result("seed", "baseline", 0, 0)], 1)
    assert model.errors == model.timeouts == 0


async def test_custom_operators_control_exhaustion_even_with_a_declared_grid():
    operator = Offer("constant")
    spec = replace(SPEC, search_space={"new": ["constant", "next"]})
    policy = BanditProposer(spec, {operator.name: operator})
    (first,) = await policy.ask(spec, (), 1)
    assert first.params == {"new": "constant"}
    assert await policy.ask(spec, (), 1) == []


def test_installed_bandit_runs_and_resumes_without_repeating_recipes(tmp_path):
    command = (
        "import json, os; from pathlib import Path; "
        "trial=json.loads(Path(os.environ['SWARM_TRIAL']).read_text()); "
        "score=sum(trial['candidate']['params'].values()); "
        "Path(os.environ['SWARM_METRICS']).write_text(json.dumps({'score':score}))"
    )
    document = {
        "campaign_id": "release-bandit",
        "metric": "score",
        "direction": "maximize",
        "evaluation_mode": "smoke",
        "max_trials": 10,
        "max_workers": 1,
        "command": [sys.executable, "-c", command],
        "search_space": {"x": [0, 1, 2], "y": [0, 1, 2]},
        "candidates": [
            {"candidate_id": "low", "params": {"x": 0, "y": 0}, "text": "starter"},
            {"candidate_id": "high", "params": {"x": 1, "y": 1}, "text": "starter"},
        ],
        "proposer": "research_swarm.swarm.operators.bandit:BanditProposer",
    }
    path = tmp_path / "campaign.yaml"
    path.write_text(json.dumps(document))
    args = [str(Path(sys.executable).parent / "research"), "swarm", "run", str(path)]
    first, resumed = [
        json.loads(
            subprocess.run(args, text=True, capture_output=True, check=True, timeout=20).stdout
        )
        for _ in range(2)
    ]
    assert first["trials"] == first["attempts"] == 9 and first["failed"] == 0
    assert first["selected"]["score"] == 4
    assert resumed["attempts"] == first["attempts"] and resumed["selected"] == first["selected"]
    with sqlite3.connect(tmp_path / "swarm.sqlite") as db:
        recipes = [json.loads(row[0]) for row in db.execute("SELECT payload FROM swarm_candidates")]
    assert sum(c["operator"] == "seed" for c in recipes) == 2
    assert any(c["operator"] == "grid" for c in recipes)
    assert all(c["text"] == "starter" for c in recipes)
    assert {(c["params"]["x"], c["params"]["y"]) for c in recipes} == {
        (x, y) for x in range(3) for y in range(3)
    }
