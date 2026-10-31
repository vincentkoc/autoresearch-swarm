"""Opaque synchronous command identity; this marker grants no authority."""

from collections.abc import Iterator
from contextlib import contextmanager
from threading import local

# These boundaries are synchronous. ContextVar would let a copied outer context
# restore its token from inside a nested command and borrow the outer witness.
_execution = local()


def current_execution() -> object | None:
    return getattr(_execution, "token", None)


@contextmanager
def command_execution() -> Iterator[None]:
    previous = current_execution()
    _execution.token = object()
    try:
        yield
    finally:
        _execution.token = previous
