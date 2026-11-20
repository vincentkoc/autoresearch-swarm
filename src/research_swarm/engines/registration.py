"""Compatibility imports for campaign engine registration."""

import sys

from research_swarm.swarm.engines import registration as _implementation
from research_swarm.swarm.engines.registration import *  # noqa: F403

sys.modules[__name__] = _implementation
