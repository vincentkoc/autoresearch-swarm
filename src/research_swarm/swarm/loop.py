"""Fill available worker slots, consume observations as they arrive and select in code."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from itertools import chain, islice, product
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from research_swarm.swarm.interfaces import (
    Backend,
    CampaignSpec,
    Candidate,
    Proposer,
    Result,
    Store,
    Trial,
    Worker,
)
from research_swarm.swarm.population import Population, Ranked


class PanelDecision(Protocol):
    @property
    def promoted(self) -> bool: ...


class PanelPolicy(Protocol):
    def calibrate(self) -> object: ...
    def record_search(self, candidate_key: str, seed: int) -> object: ...
    def consider(self, challenger_key: str, incumbent_key: str | None) -> PanelDecision: ...


def candidate_key(candidate: Candidate) -> str:
    """Held-out evaluators can reconstruct the recipe independently of its trial or lineage."""
    return json.dumps(
        {
            "params": candidate.params,
            "text": candidate.text,
            "diff": candidate.diff,
            "source_revision": candidate.source_revision,
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _trial(spec: CampaignSpec, candidate: Candidate) -> Trial:
    seed = spec.seed if candidate.seed is None else candidate.seed
    fidelity = spec.fidelity if candidate.fidelity is None else candidate.fidelity
    identity = json.dumps([spec.campaign_id, candidate.candidate_id, seed, float(fidelity)])
    return Trial(
        trial_id=uuid5(NAMESPACE_URL, identity).hex,
        campaign_id=spec.campaign_id,
        candidate=replace(candidate, seed=None, fidelity=None),
        seed=seed,
        fidelity=fidelity,
    )


class SeedProposer:
    """Consume a finite YAML candidate list, skipping trials restored from SQLite."""

    def __init__(self, spec: CampaignSpec, previous: Sequence[Trial]) -> None:
        submitted = {t.trial_id for t in previous}
        grid = (
            Candidate(
                candidate_id=f"grid-{i}",
                params=dict(zip(spec.search_space, values, strict=True)),
                operator="grid",
            )
            for i, values in enumerate(product(*spec.search_space.values()))
            if spec.search_space
        )
        self.remaining = (
            c for c in chain(spec.candidates, grid) if _trial(spec, c).trial_id not in submitted
        )

    async def ask(
        self, spec: CampaignSpec, archive: Sequence[Result], k: int
    ) -> Sequence[Candidate]:
        return list(islice(self.remaining, k))

    def tell(self, result: Result) -> None:
        pass


@dataclass(frozen=True)
class RunSummary:
    campaign_id: str
    evaluation_mode: str
    reason: str
    trials: int
    attempts: int
    failed: int
    lost: int
    selected: Ranked | None
    top: tuple[Ranked, ...]


class SwarmLoop:
    def __init__(
        self,
        spec: CampaignSpec,
        store: Store,
        backend: Backend,
        worker: Worker,
        *,
        proposer: Proposer | None = None,
        selection: Callable[[Sequence[Result]], Ranked | Result | None] | None = None,
        panels: PanelPolicy | None = None,
    ) -> None:
        self.spec = spec
        self.store = store
        self.backend = backend
        self.worker = worker
        self.proposer = proposer
        self.selection = selection
        self.panels = panels
        self.population = Population(spec, min_replicates=spec.min_replicates)
        self.incumbent: Ranked | None = None

    def _observe(self, proposer: Proposer, offset: int) -> int:
        observations = self.store.results(self.spec.campaign_id, after=offset)
        for result in observations:
            self.population.add(result)
            proposer.tell(result)
            if result.status == "ok":
                self._select()
        return offset + len(observations)

    def _select(self) -> None:
        if self.selection is None:
            top = self.population.top()
            if not top:
                return
            best = top[0]
        else:
            chosen = self.selection(self.population.archive)
            if chosen is None:
                return
            result = chosen.result if isinstance(chosen, Ranked) else chosen
            if result.status != "ok" or result not in self.population.archive:
                raise ValueError("Selection policy must select a successful archived observation")
            best = (
                chosen
                if isinstance(chosen, Ranked)
                else Ranked(result, result.metrics[self.spec.metric], 1)
            )
            if not math.isfinite(best.score) or best.replicates < 1:
                raise ValueError("Selection requires a finite score and positive replicate count")

        previous = self.incumbent
        changed_candidate = (
            previous is None
            or previous.result.attempt.trial.candidate != best.result.attempt.trial.candidate
        )
        if previous is not None:
            prior = previous.result.attempt.trial
            if self.selection is None:
                previous = self.population.measure(prior.fidelity, prior.candidate.candidate_id)
                if previous != self.incumbent:
                    self._save(previous)
            gain = best.score - previous.score
            if self.spec.direction == "minimize":
                gain = -gain
            if (
                self.selection is None
                and changed_candidate
                and self.spec.min_delta > 0
                and previous.result.attempt.trial.fidelity == best.result.attempt.trial.fidelity
                and gain < self.spec.min_delta
            ):
                return
        if self.panels is not None and changed_candidate:
            key = candidate_key(best.result.attempt.trial.candidate)
            old_key = candidate_key(previous.result.attempt.trial.candidate) if previous else None
            if not self.panels.consider(key, old_key).promoted:
                return
        self._save(best)

    def _save(self, best: Ranked) -> None:
        self.store.select(
            self.spec.campaign_id,
            best.result.attempt.trial.trial_id,
            score=best.score,
            replicates=best.replicates,
        )
        self.incumbent = best

    def _at_target(self) -> bool:
        if self.spec.target is None or self.incumbent is None:
            return False
        if self.spec.direction == "minimize":
            return self.incumbent.score <= self.spec.target
        return self.incumbent.score >= self.spec.target

    async def run(self) -> RunSummary:
        s = self.spec
        if s.evaluation_mode == "calibrated" and self.panels is None:
            raise ValueError("Configure a panel_policy or explicitly set evaluation_mode: smoke")
        if s.evaluation_mode == "smoke" and (
            self.panels is not None
            or any((s.search_panel, s.selection_panel, s.confirmation_panel))
        ):
            raise ValueError(
                "Smoke campaigns use direct metrics and cannot declare evaluation panels"
            )
        self.store.create_campaign(s)
        previous = self.store.trials(s.campaign_id)
        seen = {trial.trial_id for trial in previous}
        proposer = self.proposer or SeedProposer(s, previous)
        if self.panels is not None:
            self.panels.calibrate()
        cursor = len(self.population.archive)
        recorded = self.store.results(s.campaign_id, after=cursor)
        incumbent = self.store.incumbent(s.campaign_id)
        for result in recorded:
            self.population.add(result)
            proposer.tell(result)
            if (
                incumbent
                and result.status == "ok"
                and result.attempt.trial.trial_id == incumbent[0]
            ):
                score = result.metrics[s.metric] if incumbent[1] is None else incumbent[1]
                self.incumbent = Ranked(result, score, incumbent[2])
        if recorded:
            self._select()
        restore = getattr(proposer, "restore_trials", None)
        if restore is not None:
            restore(previous)
        cursor += len(recorded)
        pending: set[asyncio.Task[bool]] = set()
        no_claim = False
        try:
            while True:
                cursor = self._observe(proposer, cursor)
                queued, running = self.store.queue_depth(s.campaign_id)
                slots = min(s.max_workers - len(pending), self.backend.free_slots(s))
                room = min(
                    s.max_trials - len(seen), s.max_workers - queued - running, slots - queued
                )
                reason = "trial_limit" if len(seen) >= s.max_trials else "exhausted"
                if self._at_target():
                    reason = "target"
                elif room > 0:
                    try:
                        async with asyncio.timeout(s.proposal_timeout_seconds):
                            batch = await proposer.ask(s, self.population.archive, room)
                    except TimeoutError:
                        batch = ()
                        reason = "proposal_timeout"
                    if len(batch) > room:
                        raise ValueError("Proposer exceeded requested capacity")
                    for candidate in batch:
                        trial = _trial(s, candidate)
                        if trial.trial_id in seen:
                            self.store.submit(trial)
                            continue
                        if self.panels is not None:
                            self.panels.record_search(candidate_key(candidate), trial.seed)
                        if self.store.submit(trial):
                            queued += 1
                        seen.add(trial.trial_id)

                if no_claim and queued and not pending and not running:
                    reason = "dispatch_unavailable"
                    break
                for _ in range(min(slots, queued)):
                    pending.add(asyncio.create_task(self.worker.run_once(s)))
                if pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    no_claim = not any(await asyncio.gather(*done))
                    continue
                if running:
                    # Another worker may finish or expire. run_once also sweeps old heartbeats.
                    await asyncio.sleep(0.25)
                    await self.worker.run_once(s)
                    continue
                if slots <= 0 and (queued or len(seen) < s.max_trials):
                    reason = "dispatch_unavailable"
                break
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

        self._observe(proposer, cursor)
        archive = self.population.archive
        return RunSummary(
            s.campaign_id,
            s.evaluation_mode,
            reason,
            len(seen),
            len(archive),
            sum(r.status == "failed" for r in archive),
            sum(r.status == "lost" for r in archive),
            self.incumbent,
            tuple(self.population.top()),
        )
