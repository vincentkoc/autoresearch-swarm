"""Optional synchronous execution boundary beneath transports and above SQL effects."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Protocol

from research_swarm.domain.errors import ResearchError

if TYPE_CHECKING:
    from research_swarm.state.database import Transaction


class CommandBoundary(Protocol):
    def invoke(
        self,
        transaction: Transaction,
        *,
        name: str,
        payload: Mapping[str, Any],
        perform: Callable[[], dict[str, Any]],
        reject: Callable[[ResearchError], dict[str, Any]],
    ) -> dict[str, Any]:
        """Call perform or reject once; return that exact outcome without I/O.

        perform owns the handler savepoint, domain rejection hook and command
        acknowledgment. A failure from perform must escape, never be rejected again.
        """
        ...

    def rejected(
        self,
        transaction: Transaction,
        *,
        name: str,
        payload: Mapping[str, Any],
        error: ResearchError,
    ) -> None:
        """Apply a conservative fence before the existing rejection hook and acknowledgment."""
        ...
