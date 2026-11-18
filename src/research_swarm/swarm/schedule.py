"""Promotion-based asynchronous successive halving for a single campaign."""

from collections.abc import Sequence
from math import isfinite


class ASHA:
    """Report each candidate/seed trajectory at its assigned fidelity.

    Ask for promotions as slots become free. Unpromoted trajectories consume
    no more budget unless they qualify for a later slot.
    """

    def __init__(
        self, fidelities: Sequence[float], *, reduction: int = 3, minimize: bool = True
    ) -> None:
        self.fidelities = tuple(fidelities)
        if not self.fidelities or any(not isfinite(f) or f <= 0 for f in self.fidelities):
            raise ValueError("fidelities must be positive and finite")
        if any(a >= b for a, b in zip(self.fidelities, self.fidelities[1:], strict=False)):
            raise ValueError("fidelities must be strictly increasing")
        if reduction < 2:
            raise ValueError("reduction must be at least two")
        self.reduction = reduction
        self.minimize = minimize
        self._scores: list[dict[str, float]] = [{} for _ in self.fidelities]
        self._promoted: list[set[str]] = [set() for _ in self.fidelities]

    def report(self, key: str, fidelity: float, score: float) -> None:
        if not isfinite(score):
            raise ValueError("score must be finite")
        rung = self.fidelities.index(fidelity)
        if rung and key not in self._promoted[rung - 1]:
            raise ValueError("trajectory was never promoted to this fidelity")
        if self._scores[rung].setdefault(key, score) != score:
            raise ValueError("conflicting score for the same trajectory and rung")

    def promote(self) -> tuple[str, float] | None:
        """Reserve one promotion, favouring the most advanced eligible rung."""
        for rung in range(len(self.fidelities) - 2, -1, -1):
            scores = self._scores[rung]
            quota = len(scores) // self.reduction
            issued = self._promoted[rung]
            if len(issued) >= quota:
                continue
            ranked = sorted(scores, key=lambda k: (scores[k] if self.minimize else -scores[k], k))
            for key in ranked[:quota]:
                if key not in issued:
                    issued.add(key)
                    return key, self.fidelities[rung + 1]
        return None
