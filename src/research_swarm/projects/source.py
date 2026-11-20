"""Compatibility alias for source bundles, including shared module-level limits."""

import sys
from typing import TYPE_CHECKING

from research_swarm.swarm.projects import source

if TYPE_CHECKING:
    from research_swarm.swarm.projects.source import *  # noqa: F403

sys.modules[__name__] = source
