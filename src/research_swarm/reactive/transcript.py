"""Compatibility imports for the campaign reactive transcript."""

import sys

from research_swarm.swarm.reactive import transcript
from research_swarm.swarm.reactive.transcript import *  # noqa: F403

sys.modules[__name__] = transcript
