"""Optional Optuna ask/tell sampler for a declared parameter space."""

from collections.abc import Sequence
from importlib import import_module
from math import isfinite
from typing import Any
from uuid import uuid4

from research_swarm.swarm.interfaces import CampaignSpec, Candidate, Result
from research_swarm.swarm.operators.params import Range, Space, validate_space


class OptunaSampler:
    name = "optuna"

    def __init__(self, space: Space) -> None:
        validate_space(space)
        self.space = space
        # Import only when this operator is selected.
        try:
            self.optuna = import_module("optuna")
        except ModuleNotFoundError as exc:
            if exc.name != "optuna":
                raise
            raise ImportError(
                "install autoresearch-swarm[optuna] to use the Optuna sampler"
            ) from exc
        distributions = self.optuna.distributions
        self._domains = {}
        for key, domain in space.items():
            if not isinstance(domain, Range):
                self._domains[key] = distributions.CategoricalDistribution(tuple(domain))
            elif domain.integer:
                self._domains[key] = distributions.IntDistribution(
                    int(domain.low), int(domain.high), log=domain.log
                )
            else:
                self._domains[key] = distributions.FloatDistribution(
                    domain.low, domain.high, log=domain.log
                )
        self.studies: dict[str, Any] = {}
        self._specs: dict[str, CampaignSpec] = {}
        self._deferred: dict[str, list[Result]] = {}
        self._closed: set[str] = set()
        self._pending: dict[str, tuple[CampaignSpec, int]] = {}

    async def propose(
        self, spec: CampaignSpec, parents: Sequence[Candidate], k: int
    ) -> list[Candidate]:
        if spec.campaign_id not in self.studies:
            self._specs[spec.campaign_id] = spec
            sampler = self.optuna.samplers.TPESampler(seed=spec.seed, n_startup_trials=4)
            self.studies[spec.campaign_id] = self.optuna.create_study(
                direction=spec.direction, sampler=sampler
            )
            for result in self._deferred.pop(spec.campaign_id, []):
                self.tell(result)
        study = self.studies[spec.campaign_id]
        batch = []
        for _ in range(k):
            trial = study.ask(fixed_distributions=self._domains)
            params = trial.params
            candidate = Candidate(candidate_id=uuid4().hex, operator=self.name, params=params)
            self._pending[candidate.candidate_id] = (spec, trial.number)
            batch.append(candidate)
        return batch

    def tell(self, result: Result) -> None:
        trial = result.attempt.trial
        candidate = trial.candidate
        key = candidate.candidate_id
        if candidate.operator != self.name or key in self._closed:
            return
        spec = self._specs.get(trial.campaign_id)
        if spec is None:
            # On resume the loop replays observations before it first calls ask.
            self._deferred.setdefault(trial.campaign_id, []).append(result)
            return
        if result.status != "ok" and result.attempt.number < spec.max_attempts:
            return
        value = result.metrics.get(spec.metric)
        state = self.optuna.trial.TrialState.COMPLETE
        if result.status != "ok" or value is None or not isfinite(value):
            value = None
            state = self.optuna.trial.TrialState.FAIL
        study = self.studies[spec.campaign_id]
        pending = self._pending.pop(key, None)
        if pending is not None:
            study.tell(pending[1], value, state=state)
        else:
            frozen = self.optuna.trial.create_trial(
                params=candidate.params, distributions=self._domains, value=value, state=state
            )
            study.add_trial(frozen)
        self._closed.add(key)

    def discard(self, candidate: Candidate) -> None:
        pending = self._pending.pop(candidate.candidate_id, None)
        if pending is not None:
            spec, number = pending
            self.studies[spec.campaign_id].tell(number, state=self.optuna.trial.TrialState.PRUNED)
            self._closed.add(candidate.candidate_id)
