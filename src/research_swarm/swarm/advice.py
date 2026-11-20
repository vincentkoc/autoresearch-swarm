"""Compatibility imports for campaign advice."""

import argparse
from pathlib import Path
from typing import Any

from research_swarm.swarm import agents
from research_swarm.swarm.agents import SCHEMAS, add_arguments, ask_agent

__all__ = ["SCHEMAS", "add_arguments", "advise", "ask_agent"]

_original_provider = ask_agent


async def advise(database: Path, args: argparse.Namespace) -> dict[str, Any]:
    # Existing callers may replace the provider on this legacy module.
    provider = agents.ask_agent if ask_agent is _original_provider else ask_agent
    return await agents._advise(database, args, provider)
