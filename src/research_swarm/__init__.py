"""Local-first orchestration for reproducible, agent-controlled research."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from research_swarm.client import AsyncResearchClient as AsyncResearchClient
    from research_swarm.client import ResearchClient as ResearchClient

__version__ = "0.1.0"

__all__ = ["AsyncResearchClient", "ResearchClient"]


def __getattr__(name: str) -> object:
    if name in __all__:
        from research_swarm import client

        return getattr(client, name)
    raise AttributeError(name)
