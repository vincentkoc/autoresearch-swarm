"""Compatibility imports for native worker supervision."""

import sys

from research_swarm.swarm.workers import supervision
from research_swarm.swarm.workers.supervision import NativeChild as NativeChild
from research_swarm.swarm.workers.supervision import base_environment as base_environment
from research_swarm.swarm.workers.supervision import run_native as run_native

# Both import paths share the same module-level clock and supervisor bindings.
sys.modules[__name__] = supervision
