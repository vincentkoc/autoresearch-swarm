"""Optional execution profiles; project model code stays outside the engine."""

from research_swarm.swarm.engines.models import (
    ENGINE_DEFINITIONS,
    EngineDescriptor,
    ProbeReport,
    engine_definition,
)
from research_swarm.swarm.engines.probe import probe_engine

__all__ = [
    "ENGINE_DEFINITIONS",
    "EngineDescriptor",
    "ProbeReport",
    "engine_definition",
    "probe_engine",
]
