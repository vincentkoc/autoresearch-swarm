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
        self.studies: dict[str, Any] = {}
        self._pending: dict[str, tuple[CampaignSpec, int]] = {}

    async def propose(
        self, spec: CampaignSpec, parents: Sequence[Candidate], k: int
    ) -> list[Candidate]:
        if spec.campaign_id not in self.studies:
            sampler = self.optuna.samplers.TPESampler(seed=spec.seed, n_startup_trials=4)
            self.studies[spec.campaign_id] = self.optuna.create_study(
                direction=spec.direction, sampler=sampler
            )
        study = self.studies[spec.campaign_id]
        batch = []
        for _ in range(k):
            trial = study.ask()
            params = {}
            for key, domain in self.space.items():
                if not isinstance(domain, Range):
                    params[key] = trial.suggest_categorical(key, list(domain))
                elif domain.integer:
                    params[key] = trial.suggest_int(
                        key, int(domain.low), int(domain.high), log=domain.log
                    )
                else:
                    params[key] = trial.suggest_float(key, domain.low, domain.high, log=domain.log)
            candidate = Candidate(candidate_id=uuid4().hex, operator=self.name, params=params)
            self._pending[candidate.candidate_id] = (spec, trial.number)
            batch.append(candidate)
        return batch

    def tell(self, result: Result) -> None:
        key = result.attempt.trial.candidate.candidate_id
        pending = self._pending.get(key)
        if pending is None:
            return
        spec, number = pending
        if result.status != "ok" and result.attempt.number < spec.max_attempts:
            return  # the queue may retry this same sampling trial
        value = result.metrics.get(spec.metric)
        study = self.studies[spec.campaign_id]
        if result.status == "ok" and value is not None and isfinite(value):
            study.tell(number, value)
        else:
            study.tell(number, state=self.optuna.trial.TrialState.FAIL)
        del self._pending[key]
