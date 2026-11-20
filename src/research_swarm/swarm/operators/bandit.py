"""Cost-aware operator allocation for the campaign proposer's ask/tell interface."""

import asyncio
import math
import random
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from itertools import islice

from research_swarm.swarm.interfaces import CampaignSpec, Candidate, Operator, Result, Trial
from research_swarm.swarm.loop import SeedProposer, candidate_key
from research_swarm.swarm.operators.params import Crossover, ParamMutation, child
from research_swarm.swarm.population import Population

DEFAULT_EXPLORATION = 1.0


@dataclass
class _ArmTotals:
    pulls: int = 0
    gain: float = 0.0


@dataclass
class Arm(_ArmTotals):
    spend: float = 0.0
    effort: float = 0.0

    def rate(self, priced: bool) -> float:
        cost = self.spend if priced else self.effort
        return self.gain / cost if cost else 0.0


@dataclass
class _MeasuredArm(_ArmTotals):
    cost: float = 0.0

    @property
    def rate(self) -> float:
        return self.gain / self.cost if self.cost else 0.0


def _allocate(
    arms: Mapping[str, _ArmTotals], rates: Mapping[str, float], exploration: float
) -> str:
    if not rates:
        raise ValueError("no eligible operators")
    scale = max(rates.values()) or 1.0
    total = sum(arm.pulls for arm in arms.values())

    def priority(name: str) -> float:
        pulls = arms[name].pulls
        if not pulls:
            return math.inf
        return rates[name] / scale + exploration * math.sqrt(math.log1p(total) / pulls)

    chosen = max(rates, key=priority)
    arms[chosen].pulls += 1
    return chosen


class OperatorBandit:
    """Allocate portfolio slots against the measured cost supplied by the search policy."""

    def __init__(self, names: Iterable[str], exploration: float = DEFAULT_EXPLORATION) -> None:
        self.arms = {name: _MeasuredArm() for name in names}
        if not self.arms or not math.isfinite(exploration) or exploration < 0:
            raise ValueError("expected operators and finite nonnegative exploration")
        self.exploration = exploration

    def choose(self, eligible: Iterable[str]) -> str:
        rates = {name: self.arms[name].rate for name in eligible}
        return _allocate(self.arms, rates, self.exploration)

    def observe(self, name: str, gain: float, cost: float) -> None:
        if not math.isfinite(gain) or not math.isfinite(cost) or cost < 0:
            raise ValueError("expected finite gain and finite nonnegative measured cost")
        arm = self.arms[name]
        arm.gain += max(0.0, gain)
        arm.cost += cost


class BanditProposer:
    """Credit improvements over the best result at the same fidelity; retain failed costs."""

    def __init__(self, spec: CampaignSpec, operators: Mapping[str, Operator] | None = None) -> None:
        self.grid: Iterator[Candidate] = iter(())
        if operators is None:
            self.grid = SeedProposer(replace(spec, candidates=()), ()).remaining
            rng = random.Random(spec.seed)
            defaults = (ParamMutation(spec.search_space, rng), Crossover(rng))
            operators = {op.name: op for op in defaults}
        if not operators:
            raise ValueError("at least one candidate operator is required")
        self.spec = spec
        self.operators = dict(operators)
        self.arms = {name: Arm() for name in operators}
        self.population = Population(spec)
        self.best: dict[float, float] = {}
        self.issued: set[tuple[str, int, float]] = set()
        self.attempts: set[str] = set()
        self.jobs: dict[str, asyncio.Task[Sequence[Candidate]]] = {}

    def _remember(self, candidate: Candidate, seed: int | None, fidelity: float | None) -> bool:
        seed = self.spec.seed if seed is None else seed
        fidelity = self.spec.fidelity if fidelity is None else fidelity
        key = (candidate_key(candidate), seed, fidelity)
        if key in self.issued:
            return False
        self.issued.add(key)
        return True

    def restore_trials(self, trials: Sequence[Trial]) -> None:
        for trial in trials:
            arm = self.arms.get(trial.candidate.operator)
            if self._remember(trial.candidate, trial.seed, trial.fidelity) and arm is not None:
                arm.pulls += 1

    def tell(self, result: Result) -> None:
        attempt = result.attempt
        if attempt.attempt_id in self.attempts:
            return
        self.population.add(result)
        self.attempts.add(attempt.attempt_id)
        trial = attempt.trial
        self.restore_trials([trial])
        score = result.metrics.get(self.spec.metric)
        gain = 0.0
        if result.status == "ok" and score is not None and math.isfinite(score):
            value = -score if self.spec.direction == "minimize" else score
            best = self.best.get(trial.fidelity, value)
            gain = max(0.0, value - best)
            self.best[trial.fidelity] = max(best, value)
        if arm := self.arms.get(trial.candidate.operator):
            arm.gain += gain
            arm.spend += result.cost
            arm.effort += trial.fidelity
        for op in self.operators.values():
            if observe := getattr(op, "tell", None):
                observe(result)

    def _choose(self, eligible: Sequence[str]) -> str:
        priced = any(arm.spend for arm in self.arms.values())
        rates = {name: self.arms[name].rate(priced) for name in eligible}
        return _allocate(self.arms, rates, 0.5)

    async def ask(self, spec: CampaignSpec, archive: Sequence[Result], k: int) -> list[Candidate]:
        for result in archive:
            self.tell(result)
        initial = (c for c in spec.candidates if self._remember(c, c.seed, c.fidelity))
        batch = list(islice(initial, max(0, k)))
        ranked = self.population.ranked()[: max(2, spec.top_k)]
        parents = [entry.result.attempt.trial.candidate for entry in ranked] or spec.candidates
        eligible = [name for name in self.arms if name not in self.jobs or self.jobs[name].done()]
        loop = asyncio.get_running_loop()
        end = loop.time() + 0.85 * spec.proposal_timeout_seconds
        inner = replace(spec, proposal_timeout_seconds=0.7 * spec.proposal_timeout_seconds)

        async def propose(name: str, count: int) -> Sequence[Candidate]:
            try:
                return await self.operators[name].propose(inner, parents, count)
            except TimeoutError:
                return ()

        while len(batch) < k and eligible and loop.time() < end:
            counts = Counter(self._choose(eligible) for _ in range(k - len(batch)))
            jobs = {name: asyncio.create_task(propose(name, counts[name])) for name in counts}
            self.jobs.update(jobs)
            try:
                done, _ = await asyncio.wait(jobs.values(), timeout=max(0, end - loop.time()))
                for name, job in jobs.items():
                    eligible.remove(name)
                    if job not in done:
                        continue
                    for candidate in job.result()[: counts[name]]:
                        if self._remember(candidate, candidate.seed, candidate.fidelity):
                            batch.append(replace(candidate, operator=name))
                        elif discard := getattr(self.operators[name], "discard", None):
                            discard(candidate)
            finally:
                for job in jobs.values():
                    job.cancel()
        while len(batch) < k and loop.time() < end and (grid := next(self.grid, None)) is not None:
            if parents:
                grid = child(parents[0], grid.operator, params=parents[0].params | grid.params)
            if self._remember(grid, grid.seed, grid.fidelity):
                batch.append(grid)
        return batch
