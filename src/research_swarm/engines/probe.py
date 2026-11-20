"""Compatibility imports for campaign engine probe."""

import sys

from research_swarm.swarm.engines import probe as _implementation
from research_swarm.swarm.engines.probe import *  # noqa: F403

sys.modules[__name__] = _implementation
