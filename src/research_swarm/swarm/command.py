"""Wire a YAML campaign to its execution and search plugins."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from research_swarm.swarm.backends.local import from_spec as local_backend
from research_swarm.swarm.interfaces import Backend, CampaignSpec, Worker
from research_swarm.swarm.loop import SwarmLoop
from research_swarm.swarm.spec import load_spec
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


def run_campaign(path: Path, database: Path) -> tuple[dict[str, Any], int]:
    spec = load_spec(path)
    database.parent.mkdir(parents=True, exist_ok=True)
    store = SQLiteStore(database)
    backend = campaign_backend(spec)
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
    summary = asyncio.run(loop.run())
    return asdict(
        summary
    ), 0 if summary.selected and summary.reason != "dispatch_unavailable" else 3
