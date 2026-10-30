"""Synchronous SQL boundaries shared by native and hosted command authorities."""

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

from research_swarm.domain.errors import ErrorCode, ResearchError

SqlValue = str | int | float | bytes | None
T = TypeVar("T")
MAX_SQL_COUNTER = 2**53 - 1


def next_counter(value: int) -> int:
    """Keep SQL authority counters exact in both native and JavaScript runtimes."""
    if type(value) is not int or not 0 <= value < MAX_SQL_COUNTER:
        raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "SQL authority counter exhausted")
    return value + 1


class IntegrityViolation(Exception):
    """A SQL constraint refused a write; runtime-specific exceptions stay below this port."""


class SqlRow(Protocol):
    def __getitem__(self, key: str | int) -> Any: ...
    def keys(self) -> list[str]: ...


class SqlResult(Protocol):
    def __iter__(self) -> Iterator[SqlRow]: ...
    def fetchone(self) -> SqlRow | None: ...
    def fetchall(self) -> list[SqlRow]: ...
    def one(self) -> SqlRow:
        """Consume exactly one required row, or fail instead of fabricating it."""
        ...


class SqlSession(Protocol):
    def execute(self, statement: str, parameters: Sequence[SqlValue] = ()) -> SqlResult: ...

    def atomic(self, callback: Callable[["SqlSession"], T]) -> T:
        """Run once synchronously; an exception rolls back only this nested callback."""
        ...


@dataclass(frozen=True)
class StoreIdentity:
    backend: str
    location: str


class StateStore(Protocol):
    @property
    def identity(self) -> StoreIdentity: ...

    def read(self, callback: Callable[[SqlSession], T]) -> T:
        """Run once in a consistent read snapshot without awaiting or external I/O."""
        ...

    def write(self, callback: Callable[[SqlSession], T]) -> T:
        """Serialize writes and atomically commit all callback effects on success."""
        ...
