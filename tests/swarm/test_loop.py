import asyncio
import json
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

import pytest

from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Result, Trial
from research_swarm.swarm.loop import SwarmLoop
from research_swarm.swarm.population import Population, Ranked
from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import WorkerLoop


def campaign(**changes: object) -> CampaignSpec:
    return replace(
        CampaignSpec(
            campaign_id="refill",
            metric="score",
            direction="maximize",
            max_workers=2,
            max_trials=3,
            command=("test",),
        ),
        **changes,
    )


class Producer:
    def __init__(self) -> None:
        self.requests: list[int] = []
        self.observed: list[str] = []

    async def ask(
        self, spec: CampaignSpec, archive: Sequence[Result], k: int
    ) -> Sequence[Candidate]:
        self.requests.append(k)
        if len(self.requests) == 1:
            return [Candidate(candidate_id="slow"), Candidate(candidate_id="fast")]
        assert self.observed == ["fast"]
        return [Candidate(candidate_id="child", parents=("fast",), seed=17, fidelity=2)]

    def tell(self, result: Result) -> None:
        self.observed.append(result.attempt.trial.candidate.candidate_id)


class Backend:
    def __init__(self) -> None:
        self.child = asyncio.Event()

    def free_slots(self, spec: CampaignSpec) -> int:
        return spec.max_workers

    async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
        name = attempt.trial.candidate.candidate_id
        if name == "slow":
            await asyncio.wait_for(self.child.wait(), 3)
        if name == "child":
            self.child.set()
        return Result(attempt=attempt, status="ok", metrics={"score": 1.0})


@pytest.mark.asyncio
async def test_refill_on_first_completion_and_tell_lineage(tmp_path: Path) -> None:
    spec = campaign()
    store, backend, producer = SQLiteStore(tmp_path / "queue"), Backend(), Producer()
    loop = SwarmLoop(spec, store, backend, WorkerLoop(store, backend, "worker"), proposer=producer)
    summary = await loop.run()
    assert producer.requests == [2, 1] and sorted(producer.observed) == ["child", "fast", "slow"]
    assert summary.trials == 3 and summary.selected
    trial = summary.selected.result.attempt.trial
    assert trial.seed == 17 and trial.fidelity == 2
    assert loop.population.lineage("child") == ("fast",)
    assert await loop.run() == summary
    assert sorted(producer.observed) == ["child", "fast", "slow"]


@pytest.mark.asyncio
async def test_idle_proposer_timeout_and_no_affordable_claim_finish(tmp_path: Path) -> None:
    class Slow(Producer):
        async def ask(
            self, spec: CampaignSpec, archive: Sequence[Result], k: int
        ) -> Sequence[Candidate]:
            await asyncio.Event().wait()
            return []

    store, backend = SQLiteStore(tmp_path / "queue"), Backend()
    worker = WorkerLoop(store, backend, "worker")
    timed_out = await asyncio.wait_for(
        SwarmLoop(
            campaign(proposal_timeout_seconds=0.01), store, backend, worker, proposer=Slow()
        ).run(),
        2,
    )
    assert timed_out.reason == "proposal_timeout" and timed_out.attempts == 0
    capped = campaign(
        campaign_id="capped",
        candidates=(Candidate(candidate_id="fast"),),
        max_cost=0.0,
        price_per_hour=1.0,
    )
    denied = await asyncio.wait_for(SwarmLoop(capped, store, backend, worker).run(), 2)
    assert denied.reason == "dispatch_unavailable" and denied.selected is None


def test_population_ranks_means_at_comparable_fidelity_by_family() -> None:
    spec = campaign(top_k=2, min_replicates=2)
    pop = Population(spec, min_replicates=2)

    def add(name: str, seed: int, level: float, metric: float, family: str) -> None:
        trial = Trial(
            trial_id=f"{name}-{seed}-{level}",
            campaign_id=spec.campaign_id,
            candidate=Candidate(candidate_id=name, family=family),
            seed=seed,
            fidelity=level,
        )
        attempt = Attempt(
            attempt_id=trial.trial_id, trial=trial, worker_id="w", number=1, started_at=0
        )
        pop.add(Result(attempt=attempt, status="ok", metrics={"score": metric}))

    for seed, score in enumerate((2.0, 4.0)):
        add("a", seed, 1.0, score, "x")
        add("b", seed, 1.0, 2.9, "y")
    add("a", 0, 2.0, 1.0, "x")
    assert [r.score for r in pop.top()] == [3.0, 2.9]
    assert [r.score for r in pop.top(family="y")] == [2.9]
    add("a", 1, 2.0, 1.0, "x")
    assert [r.score for r in pop.top()] == [1.0]


@pytest.mark.asyncio
async def test_calibrate_and_hold_out_decision_before_persisting_winner(tmp_path: Path) -> None:
    events: list[str] = []

    class Decision:
        promoted = False

    class Study:
        def calibrate(self) -> None:
            events.append("calibrated")

        def record_search(self, candidate_key: str, seed: int) -> None:
            assert json.loads(candidate_key)["params"] == {"x": 1}
            events.append("search")

        def consider(self, challenger_key: str, incumbent_key: str | None) -> Decision:
            events.append("consider")
            return Decision()

    spec = campaign(
        candidates=(Candidate(candidate_id="fast", params={"x": 1}),), search_panel="search"
    )
    store, backend = SQLiteStore(tmp_path / "queue"), Backend()
    summary = await SwarmLoop(
        spec, store, backend, WorkerLoop(store, backend, "worker"), panels=Study()
    ).run()
    assert events == ["calibrated", "search", "consider"] and summary.attempts == 1
    assert summary.selected is None and store.incumbent(spec.campaign_id) is None


@pytest.mark.asyncio
async def test_policy_score_drives_target_and_survives_resume(tmp_path: Path) -> None:
    class SeedScore(Backend):
        async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
            return Result(attempt=attempt, status="ok", metrics={"score": attempt.trial.seed})

    def means(results: Sequence[Result]) -> Ranked | None:
        if len(results) < 2:
            return None
        first = min(results, key=lambda r: r.metrics["score"])
        return Ranked(first, sum(r.metrics["score"] for r in results) / len(results), len(results))

    spec = campaign(
        target=15,
        max_trials=2,
        candidates=tuple(Candidate(candidate_id="recipe", seed=seed) for seed in (10, 20)),
    )
    store, backend = SQLiteStore(tmp_path / "queue"), SeedScore()
    worker = WorkerLoop(store, backend, "worker")
    summary = await SwarmLoop(spec, store, backend, worker, selection=means).run()
    assert summary.reason == "target" and summary.selected is not None
    assert summary.selected.result.metrics["score"] == 10
    assert (summary.selected.score, summary.selected.replicates) == (15, 2)
    replay = await SwarmLoop(spec, store, backend, worker, selection=lambda _: None).run()
    assert replay == summary
