"""Model-guided mutation of a finite categorical or gridded search space."""

from __future__ import annotations

import itertools
import math
import random
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from statistics import fmean
from uuid import uuid4

from research_swarm.swarm.interfaces import CampaignSpec, Candidate, Result
from research_swarm.swarm.operators.params import child


class FiniteMutation:
    """Fit regularized coordinate effects and explore poorly sampled levels.

    Balanced level coverage supplies the initial design. The predictor combines
    effects of each coordinate with effects shared by coordinates with the
    same domain, e.g. repeated stages of a pipeline.
    """

    name = "param-mutate"

    def __init__(self, spec: CampaignSpec, space: Mapping[str, Sequence[str]]) -> None:
        self.spec = spec
        self.keys = tuple(space)
        self.domains = tuple(tuple(space[k]) for k in self.keys)
        if not self.keys or any(
            not values or len(set(values)) != len(values) for values in self.domains
        ):
            raise ValueError("expected distinct levels for every coordinate")
        self.rng = random.Random(spec.seed)
        self.data: dict[tuple[str, ...], list[float]] = defaultdict(list)
        self._issued: set[tuple[str, ...]] = set()
        self._coverage: Counter[tuple[int, str]] = Counter()

    def _remember(self, point: tuple[str, ...]) -> None:
        if point not in self._issued:
            self._issued.add(point)
            self._coverage.update(self._features(point))

    def tell(self, result: Result) -> None:
        trial = result.attempt.trial
        params = trial.candidate.params
        if trial.campaign_id != self.spec.campaign_id or not all(
            params.get(key) in domain for key, domain in zip(self.keys, self.domains, strict=True)
        ):
            return
        point = tuple(params[key] for key in self.keys)
        self._remember(point)
        value = result.metrics.get(self.spec.metric)
        if (
            result.status == "ok"
            and value is not None
            and math.isfinite(value)
            and trial.fidelity == self.spec.fidelity
        ):
            self.data[point].append(-value if self.spec.direction == "minimize" else value)

    def _features(self, point: tuple[str, ...]) -> Counter[tuple[int, str]]:
        features = Counter(enumerate(point))
        for value, domain in zip(point, self.domains, strict=True):
            if self.domains.count(domain) > 1:
                features[-1 - self.domains.index(domain), value] += 1
        return features

    def _fit(self) -> dict[tuple[int, str], float]:
        # Coordinate descent for ridge regression. Center and scale observed
        # scores so the exploration bonus is independent of objective units.
        values = [fmean(v) for v in self.data.values()]
        if not values:
            return {}
        avg = fmean(values)
        spread = max(values) - min(values) or 1.0
        residual = [(v - avg) / spread for v in values]
        design: dict[tuple[int, str], list[tuple[int, int]]] = defaultdict(list)
        for i, point in enumerate(self.data):
            for feature, amount in self._features(point).items():
                design[feature].append((i, amount))
        weights: dict[tuple[int, str], float] = defaultdict(float)
        for _ in range(8):
            for feature, column in design.items():
                delta = (sum(residual[i] * x for i, x in column) - weights[feature]) / (
                    1 + sum(x * x for _, x in column)
                )
                weights[feature] += delta
                for i, x in column:
                    residual[i] -= delta * x
        return weights

    async def propose(
        self, spec: CampaignSpec, parents: Sequence[Candidate], k: int
    ) -> list[Candidate]:
        total = math.prod(map(len, self.domains))
        if total <= 2048:
            pool = list(itertools.product(*self.domains))
        else:
            pool = [tuple(self.rng.choice(domain) for domain in self.domains) for _ in range(512)]
            for parent in parents:
                if not all(key in parent.params for key in self.keys):
                    continue
                point = tuple(parent.params[key] for key in self.keys)
                pool.extend(
                    point[:i] + (v,) + point[i + 1 :]
                    for i, domain in enumerate(self.domains)
                    for v in domain[:64]
                )
        pool = list(dict.fromkeys(p for p in pool if p not in self._issued))
        # Alternate global moves with a local polish once half the evaluation
        # budget has been observed. This disentangles levels whose effects a
        # sparse factorial design could only estimate jointly.
        if len(self.data) >= spec.max_trials / 2 and len(self.data) % 2 == 0:
            incumbent = max(self.data, key=lambda p: fmean(self.data[p]))
            nearby = [
                p for p in pool if sum(a != b for a, b in zip(p, incumbent, strict=True)) == 1
            ]
            if len(nearby) >= k:
                pool = nearby
        self.rng.shuffle(pool)
        weights = self._fit()
        batch = []
        for _ in range(min(k, len(pool))):

            def acquisition(point: tuple[str, ...]) -> float:
                features = self._features(point)
                estimate = sum(weights.get(f, 0.0) * x for f, x in features.items())
                # Diminishing information from repeating the same categorical
                # level within a recipe or across earlier experiments.
                bonus = sum(math.sqrt(x / (1 + self._coverage[f])) for f, x in features.items())
                return estimate + 0.8 * bonus / math.sqrt(len(self.keys))

            best = max(pool, key=acquisition)
            pool.remove(best)
            self._remember(best)
            params = dict(zip(self.keys, best, strict=True))
            if parents:

                def matches(parent: Candidate, mutation: dict[str, str] = params) -> int:
                    return sum(parent.params.get(key) == value for key, value in mutation.items())

                nearest = max(parents, key=matches)
                batch.append(child(nearest, self.name, params={**nearest.params, **params}))
            else:
                batch.append(Candidate(candidate_id=uuid4().hex, operator=self.name, params=params))
        return batch
