import asyncio
import json
import sqlite3
import threading
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Result, Trial
from research_swarm.swarm.loop import SwarmLoop
from research_swarm.swarm.population import Population, Ranked
from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import WorkerLoop


@pytest.mark.asyncio
async def test_wall_time_drains_claimed_work_then_stops_proposing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1000.0
    clock = SimpleNamespace(time=lambda: now)
    monkeypatch.setattr("research_swarm.swarm.loop.time", clock)
    monkeypatch.setattr("research_swarm.swarm.store.time", clock)

    class CrossDeadline(Backend):
        async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
            nonlocal now
            assert attempt.started_at == now
            now += 3600
            return Result(attempt=attempt, status="ok", metrics={"score": 2})

    spec = campaign(max_hours=1, max_workers=1, search_space={"x": [1, 2, 3]})
    store, backend = SQLiteStore(tmp_path / "queue"), CrossDeadline()
    summary = await SwarmLoop(spec, store, backend, WorkerLoop(store, backend, "worker")).run()
    assert summary.reason == "time_limit" and summary.trials == summary.attempts == 1
    assert summary.selected and summary.selected.score == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_trials", "target", "reason"),
    [(1, None, "trial_limit"), (2, 1.0, "target"), (2, None, "exhausted")],
)
async def test_completed_run_keeps_its_reason_after_the_deadline(
    tmp_path: Path, max_trials: int, target: float | None, reason: str
) -> None:
    spec = campaign(
        max_hours=1,
        max_trials=max_trials,
        target=target,
        candidates=(Candidate(candidate_id="first"),),
    )
    path = tmp_path / "queue"
    store, backend = SQLiteStore(path), Backend()
    worker = WorkerLoop(store, backend, "worker")
    first = await SwarmLoop(spec, store, backend, worker).run()
    assert first.reason == reason and first.attempts == 1
    with sqlite3.connect(path) as db:
        db.execute("UPDATE swarm_events SET at=at-7200 WHERE kind='created'")
    assert await SwarmLoop(spec, store, backend, worker).run() == first
    if reason == "exhausted":
        retry = await SwarmLoop(spec, store, backend, worker).run(retry_exhausted=True)
        assert retry.reason == "time_limit" and retry.attempts == 1


def campaign(**changes: object) -> CampaignSpec:
    return replace(
        CampaignSpec(
            campaign_id="refill",
            evaluation_mode="smoke",
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
async def test_serial_greedy_keeps_incumbent_until_strict_improvement(tmp_path: Path) -> None:
    scores = {"baseline": 2.0, "worse": 1.0, "a-tie": 2.0, "better": 3.0, "over-budget": 4.0}
    spec = campaign(
        max_workers=1,
        top_k=1,
        max_trials=4,
        candidates=tuple(Candidate(candidate_id=name, params={"name": name}) for name in scores),
    )
    store = SQLiteStore(tmp_path / "queue")
    dispatched = []

    class ScoringBackend(Backend):
        async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
            assert store.queue_depth(spec.campaign_id) == (0, 1)
            name = attempt.trial.candidate.candidate_id
            dispatched.append(name)
            return Result(attempt=attempt, status="ok", metrics={"score": scores[name]})

    backend = ScoringBackend()
    worker = WorkerLoop(store, backend, "worker")
    result = await SwarmLoop(spec, store, backend, worker).run()
    assert result.reason == "trial_limit" and result.attempts == result.trials == 4
    assert result.selected and result.top == (result.selected,)
    assert result.selected.result.attempt.trial.candidate.candidate_id == "better"
    with sqlite3.connect(tmp_path / "queue") as db:
        selected = [
            row[0]
            for row in db.execute(
                "SELECT trial_id FROM swarm_events WHERE kind='selected' ORDER BY id"
            )
        ]
    assert selected == [
        trial.trial_id
        for trial in store.trials(spec.campaign_id)
        if trial.candidate.candidate_id in {"baseline", "better"}
    ]
    assert await SwarmLoop(spec, store, backend, worker).run() == result
    assert dispatched == ["baseline", "worse", "a-tie", "better"]


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
    assert store.exhausted_after("refill") is None
    capped = campaign(
        campaign_id="capped",
        candidates=(Candidate(candidate_id="fast"),),
        max_cost=0.0,
        price_per_hour=1.0,
    )
    denied = await asyncio.wait_for(SwarmLoop(capped, store, backend, worker).run(), 2)
    assert denied.reason == "dispatch_unavailable" and denied.selected is None


@pytest.mark.asyncio
async def test_exhausted_resume_reuses_history_until_a_retry_or_new_observation(
    tmp_path: Path,
) -> None:
    class Empty(Producer):
        async def ask(
            self, spec: CampaignSpec, archive: Sequence[Result], k: int
        ) -> Sequence[Candidate]:
            self.requests.append(len(archive))
            return []

    spec = campaign(max_workers=1)
    path = tmp_path / "queue"
    producer = Empty()

    def reopen() -> SwarmLoop:
        store, backend = SQLiteStore(path), Backend()
        return SwarmLoop(
            spec, store, backend, WorkerLoop(store, backend, "worker"), proposer=producer
        )

    first = await reopen().run()
    assert first.reason == "exhausted" and producer.requests == [0]
    assert await reopen().run() == first and producer.requests == [0]
    assert await reopen().run(retry_exhausted=True) == first and producer.requests == [0, 0]
    store = SQLiteStore(path)
    store.submit(
        Trial(
            trial_id="external",
            campaign_id=spec.campaign_id,
            candidate=Candidate(candidate_id="external"),
            seed=1,
            fidelity=1.0,
        )
    )
    resumed = await reopen().run()
    assert resumed.attempts == 1 and resumed.selected and producer.requests == [0, 0, 1]
    store.mark_exhausted(spec.campaign_id, 0)
    assert await reopen().run() == resumed and producer.requests == [0, 0, 1]


@pytest.mark.asyncio
@pytest.mark.parametrize("budget,expected_wait", [(None, 60), (5 / 3600, 5)])
async def test_worker_retry_hint_waits_for_capacity_within_campaign_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, budget: float | None, expected_wait: int
) -> None:
    now = 1000.0
    waits: list[float] = []
    clock = SimpleNamespace(time=lambda: now)
    monkeypatch.setattr("research_swarm.swarm.loop.time", clock)
    monkeypatch.setattr("research_swarm.swarm.store.time", clock)

    async def sleep(seconds: float) -> None:
        nonlocal now
        waits.append(seconds)
        now += seconds

    # Advance only the controller's retry wait; worker heartbeats use the real event loop.
    controller_asyncio = SimpleNamespace(**vars(asyncio))
    controller_asyncio.sleep = sleep
    monkeypatch.setattr("research_swarm.swarm.loop.asyncio", controller_asyncio)
    spec = campaign(max_hours=budget, max_trials=1, candidates=(Candidate(candidate_id="fast"),))
    store, backend = SQLiteStore(tmp_path / "queue"), Backend()

    class RateLimited(WorkerLoop):
        def retry_after(self, spec: CampaignSpec) -> float | None:
            return 1060 - now

        async def run_once(self, spec: CampaignSpec, **kwargs: object) -> bool:
            if now < 1060:
                return False
            return await super().run_once(spec)

    summary = await SwarmLoop(spec, store, backend, RateLimited(store, backend, "worker")).run()
    assert waits == [expected_wait]
    if budget is None:
        assert summary.attempts == 1 and summary.selected and summary.reason == "trial_limit"
    else:
        assert summary.attempts == 0 and summary.selected is None and summary.reason == "time_limit"


@pytest.mark.asyncio
async def test_restores_pending_trials_before_first_ask(tmp_path: Path) -> None:
    store, backend = SQLiteStore(tmp_path / "queue"), Backend()
    spec = campaign(max_trials=2)
    restored = Trial(
        trial_id="pending",
        campaign_id=spec.campaign_id,
        candidate=Candidate(candidate_id="fast"),
        seed=11,
        fidelity=1,
    )
    store.create_campaign(spec)
    store.submit(restored)

    class Resumable(Producer):
        def restore_trials(self, trials: Sequence[Trial]) -> None:
            assert list(trials) == [restored]
            self.reserved = trials[0].seed

        async def ask(
            self, spec: CampaignSpec, archive: Sequence[Result], k: int
        ) -> Sequence[Candidate]:
            return [Candidate(candidate_id="replicate", seed=self.reserved + 1)]

    result = await SwarmLoop(
        spec, store, backend, WorkerLoop(store, backend, "worker"), proposer=Resumable()
    ).run()
    assert result.attempts == 2
    assert [t.seed for t in store.trials(spec.campaign_id)] == [11, 12]


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
        candidates=(Candidate(candidate_id="fast", params={"x": 1}),),
        search_panel="search",
        evaluation_mode="calibrated",
    )
    store, backend = SQLiteStore(tmp_path / "queue"), Backend()
    summary = await SwarmLoop(
        spec, store, backend, WorkerLoop(store, backend, "worker"), panels=Study()
    ).run()
    assert events == ["calibrated", "search", "consider"] and summary.attempts == 1
    assert summary.selected is None and store.incumbent(spec.campaign_id) is None

    events.clear()
    expired = replace(spec, campaign_id="expired", max_hours=0)
    store.create_campaign(expired)
    store.submit(
        Trial(
            trial_id="waiting",
            campaign_id="expired",
            candidate=spec.candidates[0],
            seed=0,
            fidelity=1,
        )
    )
    stopped = await SwarmLoop(
        expired, store, backend, WorkerLoop(store, backend, "worker"), panels=Study()
    ).run()
    assert stopped.reason == "time_limit" and stopped.attempts == 0 and events == []


@pytest.mark.asyncio
async def test_calibrated_mode_requires_policy_before_admitting_trials(tmp_path: Path) -> None:
    store, backend = SQLiteStore(tmp_path / "queue"), Backend()
    worker = WorkerLoop(store, backend, "worker")
    with pytest.raises(ValueError, match="panel_policy"):
        await SwarmLoop(campaign(evaluation_mode="calibrated"), store, backend, worker).run()
    with pytest.raises(ValueError, match="cannot declare"):
        await SwarmLoop(campaign(selection_panel="held-out"), store, backend, worker).run()
    assert store.trials("refill") == []


@pytest.mark.asyncio
@pytest.mark.parametrize("custom", [False, True])
async def test_rejected_leader_does_not_hide_valid_challenger(tmp_path: Path, custom: bool) -> None:
    calls: list[int] = []

    class Study:
        def record_search(self, key: str, seed: int) -> None:
            pass

        def calibrate(self) -> None:
            pass

        def consider(self, key: str, previous: str | None) -> object:
            score = json.loads(key)["params"]["score"]
            calls.append(score)
            return SimpleNamespace(promoted=score != 100)

    class Scores(Backend):
        async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
            return Result(attempt=attempt, status="ok", metrics=attempt.trial.candidate.params)

    store, backend = SQLiteStore(tmp_path / "queue"), Scores()
    spec = campaign(
        evaluation_mode="calibrated",
        max_workers=1,
        top_k=1,
        candidates=tuple(
            Candidate(candidate_id=str(score), params={"score": score}) for score in (0, 100, 10)
        ),
    )

    def highest(archive: Sequence[Result]) -> Result:
        return max(archive, key=lambda r: r.metrics["score"])

    summary = await SwarmLoop(
        spec,
        store,
        backend,
        WorkerLoop(store, backend, "worker"),
        panels=Study(),
        selection=highest if custom else None,
    ).run()
    assert calls == [0, 100, 10]
    assert summary.selected and summary.selected.score == 10


@pytest.mark.asyncio
async def test_slow_held_out_scoring_keeps_sibling_worker_heartbeating(tmp_path: Path) -> None:
    evaluating, heartbeat = threading.Event(), threading.Event()

    class Store(SQLiteStore):
        def heartbeat(self, attempt_id: str, *, cost: float = 0) -> bool:
            if evaluating.is_set():
                heartbeat.set()
            return super().heartbeat(attempt_id, cost=cost)

    class Study:
        def calibrate(self) -> None:
            pass

        def record_search(self, key: str, seed: int) -> None:
            pass

        def consider(self, key: str, previous: str | None) -> object:
            evaluating.set()
            assert heartbeat.wait(timeout=2), "panel evaluation stalled the worker heartbeat"
            return SimpleNamespace(promoted=True)

    class Scores(Backend):
        async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
            if attempt.trial.candidate.candidate_id == "slow":
                while not heartbeat.is_set():
                    await asyncio.sleep(0.005)
            return Result(attempt=attempt, status="ok", metrics={"score": 1})

    store, backend = Store(tmp_path / "queue"), Scores()
    spec = campaign(
        evaluation_mode="calibrated",
        candidates=(Candidate(candidate_id="fast"), Candidate(candidate_id="slow")),
    )
    worker = WorkerLoop(store, backend, "worker", heartbeat_seconds=0.01, lost_after_seconds=1)
    summary = await SwarmLoop(spec, store, backend, worker, panels=Study()).run()
    assert summary.attempts == 2 and summary.lost == 0 and heartbeat.is_set()


@pytest.mark.asyncio
async def test_search_looks_count_restored_attempts_and_retries_but_not_denied_claims(
    tmp_path: Path,
) -> None:
    looks: list[int] = []

    class Study:
        def calibrate(self) -> None:
            pass

        def record_search(self, key: str, seed: int) -> None:
            looks.append(seed)

        def consider(self, key: str, previous: str | None) -> object:
            return SimpleNamespace(promoted=True)

    class PreemptedOnce(Backend):
        async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
            # An exposure must have been recorded before this backend can read the panel.
            assert looks == [11] * attempt.number
            if attempt.number == 1:
                return Result(attempt=attempt, status="lost")
            return Result(attempt=attempt, status="ok", metrics={"score": 1})

    store, backend = SQLiteStore(tmp_path / "queue"), PreemptedOnce()
    spec = campaign(evaluation_mode="calibrated", max_trials=1)
    store.create_campaign(spec)
    store.submit(
        Trial(
            trial_id="restored",
            campaign_id=spec.campaign_id,
            candidate=Candidate(candidate_id="recipe"),
            seed=11,
            fidelity=1,
        )
    )
    worker = WorkerLoop(store, backend, "worker")
    result = await SwarmLoop(spec, store, backend, worker, panels=Study()).run()
    assert result.attempts == 2 and result.lost == 1 and result.selected and looks == [11, 11]
    denied = replace(
        spec,
        campaign_id="no-budget",
        max_cost=0,
        price_per_hour=1,
        candidates=(Candidate(candidate_id="expensive"),),
    )
    result = await SwarmLoop(denied, store, backend, worker, panels=Study()).run()
    assert result.attempts == 0 and looks == [11, 11]


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
