"""Compatibility imports for campaign engine custody."""

import sys

from research_swarm.swarm.engines import custody as _implementation
from research_swarm.swarm.engines.custody import *  # noqa: F403

sys.modules[__name__] = _implementation
