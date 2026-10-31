"""Optional synchronous execution boundary beneath transports and above SQL effects."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

from research_swarm.domain.errors import ResearchError

if TYPE_CHECKING:
    from research_swarm.state.database import Transaction


T = TypeVar("T")


@dataclass(frozen=True)
class QueryOutcome(Generic[T]):
    value: T | None = None
    error: ResearchError | None = None


def synchronous_value(value: T) -> T:
    if inspect.isawaitable(value) or inspect.isgenerator(value) or inspect.isasyncgen(value):
        if inspect.iscoroutine(value) or inspect.isgenerator(value):
            value.close()
        raise TypeError("Query readers must complete synchronously")
    return value


class ExecutionBoundary(Protocol):
    def invoke(
        self,
        transaction: Transaction,
        *,
        name: str,
        payload: Mapping[str, Any],
        perform: Callable[[], T],
        reject: Callable[[ResearchError], T],
    ) -> T:
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


CommandBoundary = ExecutionBoundary
