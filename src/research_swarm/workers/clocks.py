"""Compatibility imports for native worker clocks."""

import sys

from research_swarm.swarm.workers import clocks
from research_swarm.swarm.workers.clocks import NativeClock as NativeClock
from research_swarm.swarm.workers.clocks import continuous_time as continuous_time
from research_swarm.swarm.workers.clocks import native_clock as native_clock

# Both import paths share the same module-level clock and supervisor bindings.
sys.modules[__name__] = clocks
