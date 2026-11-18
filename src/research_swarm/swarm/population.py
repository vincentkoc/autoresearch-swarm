"""The campaign's attempt history and its default comparable-fidelity selection rule."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from statistics import fmean

from research_swarm.swarm.interfaces import CampaignSpec, Candidate, Result


@dataclass(frozen=True)
class Ranked:
    result: Result
    score: float
    replicates: int


class Population:
    def __init__(self, spec: CampaignSpec, *, min_replicates: int = 1) -> None:
        if min_replicates < 1:
            raise ValueError("min_replicates must be positive")
        self.spec = spec
        self.min_replicates = min_replicates
        self._archive: list[Result] = []
        self._candidates: dict[str, Candidate] = {}
        self._groups: dict[tuple[float, str], dict[int, Result]] = defaultdict(dict)

    @property
    def archive(self) -> Sequence[Result]:
        return tuple(self._archive)

    def add(self, result: Result) -> None:
        trial = result.attempt.trial
        if trial.campaign_id != self.spec.campaign_id:
            raise ValueError("Observation belongs to another campaign")
        self._archive.append(result)
        self._candidates[trial.candidate.candidate_id] = trial.candidate
        score = result.metrics.get(self.spec.metric)
        if result.status == "ok" and score is not None and math.isfinite(score):
            self._groups[trial.fidelity, trial.candidate.candidate_id][trial.seed] = result

    def lineage(self, candidate_id: str) -> tuple[str, ...]:
        return self._candidates[candidate_id].parents

    def measure(self, fidelity: float, candidate_id: str) -> Ranked:
        runs = self._groups[fidelity, candidate_id]
        return Ranked(
            min(runs.values(), key=lambda r: r.attempt.trial.trial_id),
            fmean(r.metrics[self.spec.metric] for r in runs.values()),
            len(runs),
        )

    def ranked(self, *, family: str | None = None) -> list[Ranked]:
        eligible = [
            key
            for key, runs in self._groups.items()
            if len(runs) >= self.min_replicates
            and (family is None or self._candidates[key[1]].family == family)
        ]
        if not eligible:
            return []
        fidelity = max(key[0] for key in eligible)
        ranked = [self.measure(*key) for key in eligible if key[0] == fidelity]
        sign = 1 if self.spec.direction == "minimize" else -1
        ranked.sort(
            key=lambda item: (sign * item.score, item.result.attempt.trial.candidate.candidate_id)
        )
        return ranked

    def top(self, *, family: str | None = None) -> list[Ranked]:
        return self.ranked(family=family)[: self.spec.top_k]
