"""Cost-aware operator allocation for the campaign proposer's ask/tell interface."""

import asyncio
import math
import random
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from itertools import islice

from research_swarm.swarm.interfaces import CampaignSpec, Candidate, Operator, Result, Trial
from research_swarm.swarm.loop import candidate_key
from research_swarm.swarm.operators.params import Crossover, ParamMutation
from research_swarm.swarm.population import Population


@dataclass
class Arm:
    pulls: int = 0
    gain: float = 0.0
    spend: float = 0.0
    effort: float = 0.0

    def rate(self, priced: bool) -> float:
        cost = self.spend if priced else self.effort
        return self.gain / cost if cost else 0.0


class BanditProposer:
    """Credit improvements over the best result at the same fidelity; retain failed costs."""

    def __init__(self, spec: CampaignSpec, operators: Mapping[str, Operator] | None = None) -> None:
        if operators is None:
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
        self.jobs: dict[asyncio.Task[Sequence[Candidate]], str] = {}

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
        scale = max(self.arms[name].rate(priced) for name in eligible) or 1.0
        total = sum(arm.pulls for arm in self.arms.values())

        def priority(name: str) -> float:
            arm = self.arms[name]
            if not arm.pulls:
                return math.inf
            return arm.rate(priced) / scale + 0.5 * math.sqrt(math.log1p(total) / arm.pulls)

        chosen = max(eligible, key=priority)
        self.arms[chosen].pulls += 1
        return chosen

    def _finished(self, job: asyncio.Task[Sequence[Candidate]]) -> None:
        self.jobs.pop(job, None)
        if not job.cancelled():
            job.exception()

    async def ask(self, spec: CampaignSpec, archive: Sequence[Result], k: int) -> list[Candidate]:
        for result in archive:
            self.tell(result)
        initial = (c for c in spec.candidates if self._remember(c, c.seed, c.fidelity))
        batch = list(islice(initial, max(0, k)))
        ranked = self.population.ranked()[: max(2, spec.top_k)]
        parents = [entry.result.attempt.trial.candidate for entry in ranked] or list(
            spec.candidates
        )
        eligible = [name for name in self.operators if name not in self.jobs.values()]
        loop = asyncio.get_running_loop()
        end = loop.time() + 0.85 * spec.proposal_timeout_seconds
        inner = replace(spec, proposal_timeout_seconds=0.7 * spec.proposal_timeout_seconds)
        while len(batch) < k and eligible and loop.time() < end:
            counts = Counter(self._choose(eligible) for _ in range(k - len(batch)))
            jobs = {
                asyncio.create_task(self.operators[name].propose(inner, parents, count)): name
                for name, count in counts.items()
            }
            self.jobs.update(jobs)
            for job in jobs:
                job.add_done_callback(self._finished)
            try:
                done, _ = await asyncio.wait(jobs, timeout=max(0, end - loop.time()))
                for job, name in jobs.items():
                    eligible.remove(name)
                    if job not in done or job.cancelled() or job.exception() is not None:
                        continue
                    for candidate in job.result()[: counts[name]]:
                        child = replace(candidate, operator=name)
                        if self._remember(child, child.seed, child.fidelity):
                            batch.append(child)
                        elif discard := getattr(self.operators[name], "discard", None):
                            discard(candidate)
            finally:
                for job in jobs:
                    if not job.done():
                        job.cancel()
        return batch
