"""Campaign-scoped search policy implementing the swarm proposer contract."""

from __future__ import annotations

import asyncio
import json
import math
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from statistics import fmean

from research_swarm.swarm.interfaces import CampaignSpec, Candidate, Operator, Result
from research_swarm.swarm.loop import candidate_key
from research_swarm.swarm.operators.bandit import OperatorBandit
from research_swarm.swarm.operators.params import child
from research_swarm.swarm.population import Ranked
from research_swarm.swarm.schedule import ASHA
from research_swarm.swarm.selection import Parent, compare_seeds, island_parents


class ReplicatedSelection:
    """Select a representative full-fidelity result only after comparing seed means."""

    def __init__(
        self,
        spec: CampaignSpec,
        *,
        min_replicates: int = 2,
        seed_std: float = 0.0,
        threshold: float = 1.96,
    ) -> None:
        self.spec = spec
        self.min_replicates = min_replicates
        self.seed_std = seed_std
        self.threshold = threshold
        compare_seeds({}, {}, min_replicates=min_replicates, seed_std=seed_std, threshold=threshold)

    def groups(self, archive: Sequence[Result]) -> dict[str, dict[int, Result]]:
        groups: dict[str, dict[int, Result]] = defaultdict(dict)
        for result in archive:
            trial = result.attempt.trial
            score = result.metrics.get(self.spec.metric)
            if (
                trial.campaign_id == self.spec.campaign_id
                and trial.fidelity == self.spec.fidelity
                and result.status == "ok"
                and score is not None
                and math.isfinite(score)
            ):
                groups[candidate_key(trial.candidate)].setdefault(trial.seed, result)
        return groups

    def __call__(self, archive: Sequence[Result]) -> Ranked | None:
        leader: dict[int, float] = {}
        representative = None
        for replicates in self.groups(archive).values():
            scores = {seed: r.metrics[self.spec.metric] for seed, r in replicates.items()}
            if len(scores) < self.min_replicates:
                continue
            if (
                leader
                and compare_seeds(
                    leader,
                    scores,
                    minimize=self.spec.direction == "minimize",
                    min_replicates=self.min_replicates,
                    seed_std=self.seed_std,
                    threshold=self.threshold,
                ).decision
                != "promote"
            ):
                continue
            mean = fmean(scores.values())
            result = replicates[min(scores, key=lambda seed: abs(scores[seed] - mean))]
            representative = Ranked(result, mean, len(scores))
            leader = scores
        return representative


class SearchPortfolio:
    """Inject into SwarmLoop as its proposer. One instance belongs to one campaign."""

    def __init__(
        self,
        spec: CampaignSpec,
        operators: Mapping[str, Operator],
        *,
        fidelities: Sequence[float] | None = None,
        reduction: int = 3,
        exploration: float = 0.5,
        family_cap: int = 2,
        selection: ReplicatedSelection | None = None,
        cost: Callable[[Result], float] | None = None,
    ) -> None:
        self.spec = spec
        self.operators = dict(operators)
        self.minimize = spec.direction == "minimize"
        self.scheduler = ASHA(
            fidelities if fidelities is not None else [spec.fidelity],
            reduction=reduction,
            minimize=self.minimize,
        )
        if self.scheduler.fidelities[-1] != spec.fidelity or family_cap < 1:
            raise ValueError("highest rung must be spec.fidelity and family_cap must be positive")
        self.bandit = OperatorBandit(operators, exploration)
        self.family_cap = family_cap
        self.selection = selection
        priced = any(
            (
                spec.price_per_hour,
                spec.price_per_million_input_tokens,
                spec.price_per_million_output_tokens,
            )
        )
        self.cost = cost or (lambda r: r.cost if priced else r.attempt.trial.fidelity)
        self._seen: set[str] = set()
        self._archive: list[Result] = []
        self._at_rung: dict[tuple[str, float], float] = {}
        self._by_recipe: dict[tuple[str, float], dict[int, Result]] = defaultdict(dict)
        self._at_seed: dict[str, Candidate] = {}
        self._issued: set[tuple[str, int, float]] = set()
        self._chosen: set[str] = set()
        self._arm_jobs: dict[asyncio.Task[Sequence[Candidate]], str] = {}
        self._failed: set[tuple[str, int, float]] = set()

    def tell(self, result: Result) -> None:
        trial = result.attempt.trial
        if trial.campaign_id != self.spec.campaign_id:
            raise ValueError("result belongs to another campaign")
        if result.attempt.attempt_id in self._seen:
            return
        self._seen.add(result.attempt.attempt_id)
        self._archive.append(result)
        candidate = trial.candidate
        key = candidate_key(candidate)
        self._issued.add((key, trial.seed, trial.fidelity))
        value = result.metrics.get(self.spec.metric)
        ok = result.status == "ok" and value is not None and math.isfinite(value)
        if not ok and (result.status == "ok" or result.attempt.number >= self.spec.max_attempts):
            self._failed.add((key, trial.seed, trial.fidelity))
        gain = 0.0
        if value is not None and ok:
            parent_values = [
                self._at_rung[p, trial.fidelity]
                for p in candidate.parents
                if (p, trial.fidelity) in self._at_rung
            ]
            if not parent_values:
                parent_values = [v for (_, f), v in self._at_rung.items() if f == trial.fidelity]
            if parent_values:
                gain = min(parent_values) - value if self.minimize else value - max(parent_values)
            self._at_rung[candidate.candidate_id, trial.fidelity] = value
            self._by_recipe[key, trial.fidelity].setdefault(trial.seed, result)
            trajectory = json.dumps([key, trial.seed])
            self._at_seed[trajectory] = replace(candidate, seed=trial.seed, fidelity=trial.fidelity)
            if trial.fidelity == self.scheduler.fidelities[0] or candidate.operator == "asha":
                self.scheduler.restore(trajectory, trial.fidelity, value)
        if candidate.operator in self.operators:
            if candidate.candidate_id not in self._chosen:
                self.bandit.arms[candidate.operator].pulls += 1  # restored from the archive
                self._chosen.add(candidate.candidate_id)
            self.bandit.observe(candidate.operator, gain, self.cost(result))
        for op in self.operators.values():
            observe = getattr(op, "tell", None)
            if observe is not None:
                observe(result)

    def _parents(self) -> list[Candidate]:
        pool: list[Parent] = []
        candidates: dict[str, Candidate] = {}
        # Use the highest observed fidelity of each recipe. Rank within each
        # fidelity before combining the pools so metrics from different rungs
        # never enter a numerical comparison.
        represented: set[str] = set()
        for fidelity in reversed(self.scheduler.fidelities):
            rung = []
            for (key, level), seeds in self._by_recipe.items():
                if level != fidelity or key in represented:
                    continue
                result = next(iter(seeds.values()))
                trial = result.attempt.trial
                candidate = replace(trial.candidate, seed=trial.seed, fidelity=trial.fidelity)
                features = frozenset(
                    f"{k}={json.dumps(v, sort_keys=True)}" for k, v in candidate.params.items()
                )
                if candidate.text or candidate.diff:
                    features |= frozenset((candidate.text or candidate.diff or "").split())
                score = fmean(r.metrics[self.spec.metric] for r in seeds.values())
                rung.append(
                    (score, key, Parent(candidate.candidate_id, candidate.family, score, features))
                )
                candidates[candidate.candidate_id] = candidate
                represented.add(key)
            rung.sort(key=lambda row: (row[0] if self.minimize else -row[0], row[1]))
            offset = len(pool)
            pool.extend(replace(row[2], score=float(i)) for i, row in enumerate(rung, offset))
        selected = island_parents(pool, max(2, self.spec.top_k), per_family=self.family_cap)
        return [candidates[p.candidate_id] for p in selected] or list(self.spec.candidates)

    def _reserve(self, candidate: Candidate) -> Candidate | None:
        seed = self.spec.seed if candidate.seed is None else candidate.seed
        fidelity = (
            self.scheduler.fidelities[0] if candidate.fidelity is None else candidate.fidelity
        )
        identity = (candidate_key(candidate), seed, fidelity)
        if identity in self._issued:
            return None
        self._issued.add(identity)
        self._chosen.add(candidate.candidate_id)
        return replace(candidate, seed=seed, fidelity=fidelity)

    def _followups(self, k: int) -> list[Candidate]:
        batch: list[Candidate] = []
        if self.selection is not None:
            groups = self.selection.groups(self._archive)
            ranked = sorted(
                groups.items(),
                key=lambda item: fmean(r.metrics[self.spec.metric] for r in item[1].values()),
                reverse=not self.minimize,
            )
            for key, seeds in ranked:
                if len(batch) == k:
                    return batch
                issued = {
                    s for key_, s, f in self._issued if key_ == key and f == self.spec.fidelity
                }
                viable = sum((key, seed, self.spec.fidelity) not in self._failed for seed in issued)
                if (
                    len(seeds) < self.selection.min_replicates
                    and viable < self.selection.min_replicates
                ):
                    parent = next(iter(seeds.values())).attempt.trial.candidate
                    seed = self.spec.seed
                    while seed in issued:
                        seed += 1
                    replicate = self._reserve(
                        child(parent, "replicate", seed=seed, fidelity=self.spec.fidelity)
                    )
                    if replicate is not None:
                        batch.append(replicate)
        while len(batch) < k and (promotion := self.scheduler.promote()) is not None:
            trajectory, fidelity = promotion
            parent = self._at_seed[trajectory]
            proposal = self._reserve(child(parent, "asha", seed=parent.seed, fidelity=fidelity))
            if proposal is not None:
                batch.append(proposal)
        return batch

    def _finished(self, task: asyncio.Task[Sequence[Candidate]]) -> None:
        self._arm_jobs.pop(task, None)
        if not task.cancelled():
            task.exception()

    async def ask(
        self, spec: CampaignSpec, archive: Sequence[Result], k: int
    ) -> Sequence[Candidate]:
        if spec.campaign_id != self.spec.campaign_id:
            raise ValueError("proposer belongs to another campaign")
        if k <= 0:
            return []
        for result in archive:
            self.tell(result)
        batch = self._followups(k)
        for initial in spec.candidates:
            if len(batch) == k:
                return batch
            if (candidate := self._reserve(initial)) is not None:
                batch.append(candidate)
        parents = self._parents()
        eligible = [name for name in self.operators if name not in self._arm_jobs.values()]
        if len(parents) < 2:
            eligible = [name for name in eligible if name not in {"crossover", "ensemble-add"}]
        if not parents:
            eligible = [
                name
                for name in eligible
                if name not in {"new-seed", "llm-code-diff", "llm-text-rewrite"}
            ]

        # Keep some of the loop's outer deadline for returning fast siblings.
        timeout = spec.proposal_timeout_seconds * 0.85
        inner = replace(spec, proposal_timeout_seconds=timeout * 0.85)
        loop = asyncio.get_running_loop()
        end = loop.time() + timeout
        while eligible and len(batch) < k and loop.time() < end:
            counts = Counter(self.bandit.choose(eligible) for _ in range(k - len(batch)))
            jobs = {
                asyncio.create_task(self.operators[name].propose(inner, parents, count)): name
                for name, count in counts.items()
            }
            self._arm_jobs.update(jobs)
            for job in jobs:
                job.add_done_callback(self._finished)
            try:
                done, _ = await asyncio.wait(jobs, timeout=max(0, end - loop.time()))
                for job, name in jobs.items():
                    if job in done and not job.cancelled() and job.exception() is None:
                        for candidate in job.result()[: counts[name]]:
                            if (proposal := self._reserve(candidate)) is not None:
                                batch.append(proposal)
                    eligible.remove(name)
            finally:
                for job in jobs:
                    if not job.done():
                        job.cancel()
        return batch
