"""Optional execution profiles; project model code stays outside the engine."""

from research_swarm.engines.models import EngineDescriptor, ProbeReport
from research_swarm.engines.probe import probe_engine
from research_swarm.engines.registry import ENGINE_DEFINITIONS, engine_definition

__all__ = [
    "ENGINE_DEFINITIONS",
    "EngineDescriptor",
    "ProbeReport",
    "engine_definition",
    "probe_engine",
]
