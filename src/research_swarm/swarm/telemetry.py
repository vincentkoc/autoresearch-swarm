"""Deliver a subprocess launch acknowledgement to the owning attempt's queue."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_observer: ContextVar[Callable[[str], None] | None] = ContextVar("workload_start", default=None)


@contextmanager
def observe_starts(observer: Callable[[str], None] | None) -> Iterator[None]:
    token = _observer.set(observer)
    try:
        yield
    finally:
        _observer.reset(token)


def workload_started(attempt_id: str) -> None:
    observer = _observer.get()
    if observer is not None:
        observer(attempt_id)
