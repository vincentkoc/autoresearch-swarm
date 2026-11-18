"""Allocate proposal slots using realized objective gain per unit of cost."""

from collections.abc import Iterable
from dataclasses import dataclass
from math import isfinite, log, sqrt


@dataclass
class Arm:
    pulls: int = 0
    gain: float = 0.0
    cost: float = 0.0

    @property
    def rate(self) -> float:
        return self.gain / self.cost if self.cost else 0.0


class OperatorBandit:
    """UCB allocation with cumulative gain/cost and in-flight pull counts."""

    def __init__(self, names: Iterable[str], exploration: float = 0.5) -> None:
        self.arms = {name: Arm() for name in names}
        if not self.arms or not isfinite(exploration) or exploration < 0:
            raise ValueError("expected operators and finite nonnegative exploration")
        self.exploration = exploration

    def choose(self, eligible: Iterable[str]) -> str:
        options = list(eligible)
        if not options:
            raise ValueError("no eligible operators")
        total = sum(arm.pulls for arm in self.arms.values())
        scale = max((self.arms[name].rate for name in options), default=0.0) or 1.0

        def priority(name: str) -> float:
            arm = self.arms[name]
            if not arm.pulls:
                return float("inf")
            return arm.rate / scale + self.exploration * sqrt(log(total + 1) / arm.pulls)

        name = max(options, key=priority)
        self.arms[name].pulls += 1
        return name

    def observe(self, name: str, gain: float, cost: float) -> None:
        if not isfinite(gain) or not isfinite(cost) or cost < 0:
            raise ValueError("expected finite gain and finite nonnegative measured cost")
        arm = self.arms[name]
        arm.gain += max(0.0, gain)
        arm.cost += cost
