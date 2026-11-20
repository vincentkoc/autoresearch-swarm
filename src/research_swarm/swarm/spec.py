"""Read a campaign from YAML using the same record definitions as Python callers."""

from __future__ import annotations

import json
import math
from dataclasses import fields, replace
from pathlib import Path
from typing import Any

import yaml
from pydantic import TypeAdapter

from research_swarm.domain.canonical import MAX_SAFE_INTEGER
from research_swarm.swarm.interfaces import CampaignSpec, Candidate


def search_work(fidelity: float, full_fidelity_work: int) -> int:
    """Number of workload units in a fresh fit at this rung."""
    if not math.isfinite(fidelity) or fidelity <= 0:
        raise ValueError("Fidelity must be positive and finite")
    try:
        return math.ceil(fidelity * full_fidelity_work)
    except OverflowError:
        # Very large finite offers still have a cost and can be declined normally.
        numerator, denominator = fidelity.as_integer_ratio()
        return -(-numerator * full_fidelity_work // denominator)


def validate_seed(seed: int | None) -> None:
    if seed is not None and (type(seed) is not int or abs(seed) > MAX_SAFE_INTEGER):
        raise ValueError(
            f"Trial seed must be an integer between {-MAX_SAFE_INTEGER} and {MAX_SAFE_INTEGER}"
        )


def _check_fields(value: Any, record: type[CampaignSpec] | type[Candidate]) -> None:
    if not isinstance(value, dict):
        raise ValueError(f"Expected a YAML mapping for {record.__name__}")
    unknown = set(value).difference(f.name for f in fields(record))
    if unknown:
        raise ValueError(f"Unknown {record.__name__} field: {next(iter(unknown))}")


def load_spec(path: Path) -> CampaignSpec:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid campaign YAML: {exc}") from exc
    _check_fields(data, CampaignSpec)
    if isinstance(data.get("candidates"), list):
        for candidate in data["candidates"]:
            _check_fields(candidate, Candidate)
    try:
        encoded = json.dumps(data, allow_nan=False)
    except (ValueError, TypeError) as exc:
        raise ValueError("Campaign values must be finite JSON data") from exc
    spec = TypeAdapter(CampaignSpec).validate_json(encoded, strict=True)
    validate_seed(spec.seed)
    if spec.max_hours is not None and spec.max_hours < 0:
        raise ValueError("max_hours must be non-negative")
    if (
        not spec.campaign_id.strip()
        or not spec.metric.strip()
        or not spec.command
        or not spec.command[0]
    ):
        raise ValueError("Specify a campaign_id, metric and non-empty command")
    if (
        min(
            spec.max_trials,
            spec.max_workers,
            spec.max_attempts,
            spec.top_k,
            spec.min_replicates,
            spec.fidelity,
            spec.trial_timeout_seconds,
            spec.proposal_timeout_seconds,
        )
        <= 0
    ):
        raise ValueError("Limits, fidelity and timeouts must be positive")
    if (
        min(
            spec.min_delta,
            spec.price_per_hour or 0.0,
            spec.price_per_million_input_tokens or 0.0,
            spec.price_per_million_output_tokens or 0.0,
            spec.estimated_input_tokens,
            spec.estimated_output_tokens,
            spec.max_cost if spec.max_cost is not None else 0,
        )
        < 0
    ):
        raise ValueError("Improvement, costs and token counts must be non-negative")
    if not (spec.candidates or spec.proposer or spec.search_space):
        raise ValueError("Supply candidates, a search_space or a proposer")
    if any(not values for values in spec.search_space.values()):
        raise ValueError("Search space dimensions require at least one value")
    recipes: dict[str, Candidate] = {}
    for candidate in spec.candidates:
        validate_seed(candidate.seed)
        if not candidate.candidate_id or (
            candidate.fidelity is not None and candidate.fidelity <= 0
        ):
            raise ValueError("Each candidate needs an id and positive fidelity")
        normalized = replace(candidate, seed=None, fidelity=None)
        if recipes.setdefault(candidate.candidate_id, normalized) != normalized:
            raise ValueError(f"Conflicting candidate {candidate.candidate_id}")
        if spec.search_space and candidate.candidate_id.startswith("grid-"):
            raise ValueError("grid- ids are reserved when search_space is configured")
    return replace(spec, task_dir=str((path.parent / spec.task_dir).resolve(strict=True)))
