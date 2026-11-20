"""Wire a YAML campaign to its execution and search plugins."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from research_swarm.domain.canonical import MAX_SAFE_INTEGER
from research_swarm.swarm.backends.local import from_spec as local_backend
from research_swarm.swarm.interfaces import Backend, CampaignSpec, Worker
from research_swarm.swarm.loop import SwarmLoop, candidate_key
from research_swarm.swarm.report import _read
from research_swarm.swarm.spec import load_spec, validate_seed
from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import from_spec


def _factory(name: str) -> Callable[..., Any]:
    module, separator, attribute = name.partition(":")
    if not module or not separator or not attribute.isidentifier():
        raise ValueError(f"Expected a module:factory name, got {name!r}")
    try:
        implementation = getattr(importlib.import_module(module), attribute)
    except (ImportError, AttributeError) as exc:
        raise ValueError(f"Cannot load campaign factory {name!r}") from exc
    if not callable(implementation):
        raise ValueError(f"Campaign factory {name!r} must be callable")
    return cast(Callable[..., Any], implementation)


def campaign_backend(spec: CampaignSpec) -> Backend:
    if spec.backend:
        return cast(Backend, _factory(spec.backend)(spec))
    return local_backend(spec)


def _check_archived_seeds(database: Path, campaign_id: str) -> None:
    if not database.is_file():
        return
    with _read(database) as db:
        if db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='swarm_trials'"
        ).fetchone():
            row = db.execute(
                "SELECT seed FROM swarm_trials WHERE campaign_id=? "
                "AND (typeof(seed) != 'integer' OR seed NOT BETWEEN ? AND ?) LIMIT 1",
                (campaign_id, -MAX_SAFE_INTEGER, MAX_SAFE_INTEGER),
            ).fetchone()
            if row is not None:
                validate_seed(row[0])


def run_campaign(
    path: Path, database: Path, *, retry_exhausted: bool = False
) -> tuple[dict[str, Any], int]:
    spec = load_spec(path)
    _check_archived_seeds(database, spec.campaign_id)
    backend = campaign_backend(spec)
    database.parent.mkdir(parents=True, exist_ok=True)
    store = SQLiteStore(database)
    worker: Worker
    if spec.worker:
        worker = _factory(spec.worker)(spec, store, backend)
    else:
        worker = from_spec(spec, store, backend)
    loop = SwarmLoop(
        spec,
        store,
        backend,
        worker,
        proposer=_factory(spec.proposer)(spec) if spec.proposer else None,
        selection=_factory(spec.selection_policy)(spec) if spec.selection_policy else None,
        panels=_factory(spec.panel_policy)(spec) if spec.panel_policy else None,
    )
    summary = asyncio.run(loop.run(retry_exhausted=retry_exhausted))
    report = asdict(summary)
    read_score = getattr(loop.panels, "confirmed_score", None)
    report["confirmed_score"] = (
        read_score(candidate_key(summary.selected.result.attempt.trial.candidate))
        if summary.selected and read_score is not None
        else None
    )
    return report, 0 if summary.selected and summary.reason != "dispatch_unavailable" else 3
