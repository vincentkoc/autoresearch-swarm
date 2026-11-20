"""Compatibility imports for registered engine profiles."""

import sys

from research_swarm.swarm.engines import models
from research_swarm.swarm.engines.models import ENGINE_DEFINITIONS as ENGINE_DEFINITIONS
from research_swarm.swarm.engines.models import EngineDefinition as EngineDefinition
from research_swarm.swarm.engines.models import engine_definition as engine_definition

# Descriptor validation and legacy registry overrides use the same definitions.
sys.modules[__name__] = models
