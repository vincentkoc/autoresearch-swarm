"""Compatibility imports for campaign engine execution."""

import sys

from research_swarm.swarm.engines import execution as _implementation
from research_swarm.swarm.engines.execution import *  # noqa: F403

sys.modules[__name__] = _implementation
