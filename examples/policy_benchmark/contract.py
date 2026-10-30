"""A frozen, bounded experiment design; this example does not train a policy."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from typing import Literal

from pydantic import Field, model_validator

from research_swarm.domain.types import VersionedRecord


class Case(VersionedRecord):
    name: str = Field(pattern=r"^[a-z][a-z0-9-]{0,63}$")
    split: Literal["development", "held_out"]
    seed: int = Field(strict=True, ge=0, le=1_000_000)


class Definition(VersionedRecord):
    repeats: int = Field(default=2, strict=True, ge=1, le=4)
    no_op_panels: int = Field(default=3, strict=True, ge=1, le=8)
    cases: tuple[Case, ...] = Field(min_length=2, max_length=8)

    @model_validator(mode="after")
    def independent_cells(self) -> Definition:
        if {case.split for case in self.cases} != {"development", "held_out"}:
            raise ValueError("Declare development and held-out starting states before execution")
        if len({case.name for case in self.cases}) != len(self.cases):
            raise ValueError("Case names must be unique")
        seeds = [
            case.seed + repeat * 2_000_000 + offset
            for case in self.cases
            for repeat in range(self.repeats)
            for offset in range(3)
        ]
        if len(set(seeds)) != len(seeds):
            raise ValueError("Starting-state seed panels overlap")
        return self

    def cells(self) -> list[tuple[Case, int, str]]:
        # Counterbalance policy execution order to reduce systematic order effects.
        return [
            (case, repeat, policy)
            for case in self.cases
            for repeat in range(self.repeats)
            for policy in (("greedy", "portfolio") if repeat % 2 == 0 else ("portfolio", "greedy"))
        ]


def frequency_interval(successes: int, total: int) -> dict:
    """Descriptive Wilson 95% interval; independent Bernoulli panels are an assumption."""
    if type(successes) is not int or type(total) is not int or not 0 <= successes <= total:
        raise ValueError("Invalid frequency counts")
    if total == 0:
        return {"count": successes, "total": total, "rate": None, "interval_95": None}
    z = 1.959963984540054
    rate = successes / total
    denominator = 1 + z * z / total
    center = (rate + z * z / (2 * total)) / denominator
    width = z * math.sqrt(rate * (1 - rate) / total + z * z / (4 * total**2)) / denominator
    return {
        "count": successes,
        "total": total,
        "rate": rate,
        "interval_95": [max(0.0, center - width), min(1.0, center + width)],
    }


def summarize(cells: list[dict], panels: list[dict]) -> dict:
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for cell in cells:
        groups[cell["split"], cell["policy"]].append(cell)
    policies = []
    for (split, policy), group in sorted(groups.items()):
        complete = [cell for cell in group if cell["complete"]]
        # Never present a complete-case average as an all-case policy result.
        quality = (
            sum(cell["improvement"] for cell in complete) / len(group)
            if len(complete) == len(group)
            else None
        )
        policies.append(
            {
                "split": split,
                "policy": policy,
                "planned_cells": len(group),
                "complete_cells": len(complete),
                "mean_observed_improvement": quality,
                "scientific_claim": "descriptive_fixed_proposer_benchmark",
            }
        )
    complete_panels = [panel for panel in panels if panel["complete"]]
    differences = [
        pair["values"]["comparator"] - pair["values"]["candidate"]
        for panel in complete_panels
        for pair in panel.get("pairs", [])
    ]
    paired = defaultdict(dict)
    for cell in cells:
        if "case" in cell:
            paired[(cell["case"], cell["repeat"], cell["split"])][cell["policy"]] = cell
    return {
        "policies": policies,
        "paired_policy_differences": [
            {
                "case": case,
                "repeat": repeat,
                "split": split,
                "portfolio_minus_greedy_improvement": (
                    pair["portfolio"]["improvement"] - pair["greedy"]["improvement"]
                    if set(pair) == {"portfolio", "greedy"}
                    and all(cell["complete"] for cell in pair.values())
                    else None
                ),
            }
            for (case, repeat, split), pair in sorted(paired.items())
        ],
        "no_op": {
            "planned_panels": len(panels),
            "incomplete_panels": len(panels) - len(complete_panels),
            "false_promotion": frequency_interval(
                sum(panel["outcome"] == "promote" for panel in complete_panels),
                len(complete_panels),
            ),
            "claim": "observed frequency; small panels do not certify a 5% error bound",
            "paired_differences": differences,
            "mean_difference": statistics.mean(differences) if differences else None,
            "sample_standard_deviation": statistics.stdev(differences)
            if len(differences) > 1
            else None,
        },
    }
