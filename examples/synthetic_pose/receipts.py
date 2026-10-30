"""Explicit source-format readers. Macro and pooled errors keep their denominators."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from statistics import fmean
from typing import Any


def count(value: Any) -> int:
    if type(value) is not int or not 0 < value <= 9_007_199_254_740_991:
        raise ValueError("Receipt count must be a positive safe integer")
    return value


def nonnegative(value: Any) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError("Receipt statistic must be finite and nonnegative")
    return float(value)


def coverage(people: dict[str, Any], expected: list[str]) -> None:
    if (
        not isinstance(people, dict)
        or not expected
        or len(expected) != len(set(expected))
        or set(people) != set(expected)
    ):
        raise ValueError("Receipt participant coverage changed")


@dataclass(frozen=True)
class Panel:
    name: str
    per_person_mae_deg: dict[str, float]
    macro_mae_deg: float
    pooled_mae_deg: float
    absolute_error_sum_radians: float
    valid_elements: int
    windows: int

    def values(self) -> dict[str, Any]:
        return asdict(self)


def schema2_panel(endpoint: dict[str, Any], expected_people: list[str]) -> Panel:
    if endpoint["units"] != "absolute_error_sum_radians":
        raise ValueError("Schema-2 endpoint has incompatible units")
    people = endpoint["people"]
    coverage(people, expected_people)
    sums = {person: nonnegative(value["error_sum"]) for person, value in people.items()}
    elements = {person: count(value["elements"]) for person, value in people.items()}
    windows = count(sum(count(value["windows"]) for value in people.values()))
    per_person = {
        person: nonnegative(math.degrees(sums[person] / elements[person])) for person in people
    }
    total = nonnegative(sum(sums.values()))
    denominator = count(sum(elements.values()))
    return Panel(
        "full",
        per_person,
        nonnegative(fmean(per_person.values())),
        nonnegative(math.degrees(total / denominator)),
        total,
        denominator,
        windows,
    )


def candidates_panel(name: str, scores: dict[str, Any], expected_people: list[str]) -> Panel:
    people = scores["per_person_mae_deg"]
    coverage(people, expected_people)
    per_person = {person: nonnegative(value) for person, value in people.items()}
    total = nonnegative(scores["absolute_error_radians"])
    denominator = count(scores["valid_joint_frames"])
    windows = count(scores["windows"])
    macro = nonnegative(fmean(per_person.values()))
    pooled = nonnegative(math.degrees(total / denominator))
    if not math.isclose(
        macro, nonnegative(scores["person_mae_deg"]), rel_tol=1e-12, abs_tol=1e-9
    ) or not math.isclose(
        pooled, nonnegative(scores["pooled_mae_deg"]), rel_tol=1e-12, abs_tol=1e-9
    ):
        raise ValueError("Archived aggregate disagrees with its sufficient statistics")
    return Panel(name, per_person, macro, pooled, total, denominator, windows)


def normalize(
    endpoint: dict[str, Any],
    *,
    source_schema: str,
    expected_people: list[str],
    expected_identity: dict[str, str],
) -> dict[str, Panel]:
    if not isinstance(endpoint, dict):
        raise ValueError("Endpoint must be an object")
    for name, value in expected_identity.items():
        if endpoint.get(name) != value:
            raise ValueError("Receipt identity differs: " + name)
    if source_schema == "campaign-schema-2":
        return {"full": schema2_panel(endpoint, expected_people)}
    if source_schema == "candidates-v2":
        if "units" in endpoint and endpoint["units"] != "absolute_error_sum_radians":
            raise ValueError("Archived endpoint has incompatible units")
        if set(endpoint["scores"]) != {"central", "full"}:
            raise ValueError("Archived endpoint must preserve both declared panels")
        return {
            name: candidates_panel(name, scores, expected_people)
            for name, scores in endpoint["scores"].items()
        }
    raise ValueError("Unknown endpoint schema; select a qualified project-local reader")
