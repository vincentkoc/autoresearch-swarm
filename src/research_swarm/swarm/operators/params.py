"""Mutation, crossover, ensemble growth and seed replication."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from typing import Any
from uuid import uuid4

from research_swarm.swarm.interfaces import CampaignSpec, Candidate

Scalar = str | float | int | bool | None


@dataclass(frozen=True)
class Range:
    low: float
    high: float
    integer: bool = False
    log: bool = False

    def __post_init__(self) -> None:
        if not (math.isfinite(self.low) and math.isfinite(self.high) and self.low <= self.high):
            raise ValueError("invalid numeric range")
        if self.log and self.low <= 0:
            raise ValueError("log range must be positive")
        if self.integer and (self.low != int(self.low) or self.high != int(self.high)):
            raise ValueError("integer bounds must be integers")


Space = Mapping[str, Range | Sequence[Scalar]]


def validate_space(space: Space) -> None:
    if not space:
        raise ValueError("empty search space")
    for domain in space.values():
        if not isinstance(domain, Range) and (isinstance(domain, str) or not domain):
            raise ValueError("categorical domains must be nonempty sequences of choices")


def child(parent: Candidate, operator: str, **changes: Any) -> Candidate:
    return replace(
        parent,
        candidate_id=uuid4().hex,
        operator=operator,
        **({"parents": (parent.candidate_id,), "seed": None, "fidelity": None} | changes),
    )


def _draw(domain: Range | Sequence[Scalar], rng: random.Random, previous: Any) -> Scalar:
    if not isinstance(domain, Range):
        return rng.choice([value for value in domain if value != previous] or list(domain))
    low, high = domain.low, domain.high
    lower, upper = (math.log(low), math.log(high)) if domain.log else (low, high)
    if isinstance(previous, (int, float)) and low <= previous <= high:
        center = math.log(previous) if domain.log else previous
        draw = max(lower, min(upper, rng.gauss(center, (upper - lower) / 4)))
    else:
        draw = rng.uniform(lower, upper)
    value = math.exp(draw) if domain.log else draw
    if domain.integer:
        rounded = round(value)
        if rounded == previous and low != high:
            return rng.choice([x for x in (rounded - 1, rounded + 1) if low <= x <= high])
        return rounded
    return max(low, min(high, value))


class ParamMutation:
    """Mutate a declared coordinate or draw a fresh recipe when there are no parents."""

    name = "param-mutate"

    def __init__(self, space: Space, rng: random.Random) -> None:
        validate_space(space)
        self.space = space
        self.rng = rng

    async def propose(
        self, spec: CampaignSpec, parents: Sequence[Candidate], k: int
    ) -> list[Candidate]:
        batch = []
        for i in range(k):
            parent = parents[i % len(parents)] if parents else None
            params = dict(parent.params) if parent else {}
            missing = [key for key in self.space if key not in params]
            keys = missing or [self.rng.choice(list(self.space))]
            for key in keys:
                params[key] = _draw(self.space[key], self.rng, params.get(key, object()))
            if parent is None:
                batch.append(Candidate(candidate_id=uuid4().hex, params=params, operator=self.name))
            elif params != parent.params:
                batch.append(child(parent, self.name, params=params))
        return batch


class NewSeed:
    name = "new-seed"

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    async def propose(
        self, spec: CampaignSpec, parents: Sequence[Candidate], k: int
    ) -> list[Candidate]:
        batch = []
        used = {spec.seed, *(parent.seed for parent in parents)}
        for i in range(k if parents else 0):
            seed = self.rng.randrange(2**31)
            while seed in used:
                seed = self.rng.randrange(2**31)
            used.add(seed)
            parent = parents[i % len(parents)]
            batch.append(child(parent, self.name, seed=seed, fidelity=parent.fidelity))
        return batch


class Crossover:
    name = "crossover"

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    async def propose(
        self, spec: CampaignSpec, parents: Sequence[Candidate], k: int
    ) -> list[Candidate]:
        batch: list[Candidate] = []
        if len(parents) < 2:
            return batch
        for _ in range(k):
            a, b = self.rng.sample(list(parents), 2)
            if a.source_revision != b.source_revision or a.params.keys() != b.params.keys():
                continue
            params = dict(a.params)
            different = [key for key, value in params.items() if value != b.params[key]]
            if len(different) >= 2:
                for key in self.rng.sample(different, self.rng.randrange(1, len(different))):
                    params[key] = b.params[key]
                batch.append(
                    child(a, self.name, parents=(a.candidate_id, b.candidate_id), params=params)
                )
            elif a.text and b.text and not (a.diff or b.diff):
                first, second = a.text.splitlines(keepends=True), b.text.splitlines(keepends=True)
                if len(first) >= 2 and len(second) >= 2:
                    text = "".join(
                        first[: self.rng.randrange(1, len(first))]
                        + second[self.rng.randrange(1, len(second)) :]
                    )
                    if text not in (a.text, b.text):
                        batch.append(
                            child(a, self.name, parents=(a.candidate_id, b.candidate_id), text=text)
                        )
        return batch


def _ensemble_members(params: Mapping[str, Any]) -> list[dict[str, Any]]:
    members = params.get("members")
    if not isinstance(members, list) or not all(
        isinstance(member, dict)
        and isinstance(member.get("candidate_id"), str)
        and isinstance(member.get("params"), dict)
        and isinstance(member.get("operator"), str)
        for member in members
    ):
        return []

    # Older runs could store a replicated or promoted ensemble as a nested member.
    leaves: dict[str, dict[str, Any]] = {}
    for member in members:
        for leaf in _ensemble_members(member["params"]) or [member]:
            leaves.setdefault(leaf["candidate_id"], leaf)
    return list(leaves.values())


class EnsembleAdd:
    """Expose the member recipes in SWARM_TRIAL for an ensemble-capable workload."""

    name = "ensemble-add"

    async def propose(
        self, spec: CampaignSpec, parents: Sequence[Candidate], k: int
    ) -> list[Candidate]:
        batch: list[Candidate] = []
        if not parents:
            return batch
        anchor = parents[0]
        members = _ensemble_members(anchor.params) or [asdict(anchor)]
        existing = {member["candidate_id"] for member in members}
        for addition in parents[1:]:
            if len(batch) >= k:
                break
            if addition.candidate_id in existing or _ensemble_members(addition.params):
                continue
            existing.add(addition.candidate_id)
            batch.append(
                child(
                    anchor,
                    self.name,
                    parents=(anchor.candidate_id, addition.candidate_id),
                    params={"members": [*members, asdict(addition)]},
                    text=None,
                    diff=None,
                )
            )
        return batch
