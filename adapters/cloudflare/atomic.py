"""Preserve Python values and exceptions across a synchronous JS transaction callback."""

import inspect


def atomic(storage, callback):
    values, failures = [], []
    called = False

    def invoke():
        nonlocal called
        try:
            if called:
                raise RuntimeError("Atomic callback was invoked more than once")
            called = True
            value = callback()
            if inspect.isawaitable(value):
                if inspect.iscoroutine(value):
                    value.close()
                raise TypeError("Atomic callbacks must complete synchronously")
            values.append(value)
        except BaseException as error:
            failures.append(error)
            raise  # The JS storage must observe failure and roll back its savepoint.

    try:
        storage.transactionSync(invoke)
    except BaseException as bridge_error:
        if failures:
            if failures[0] is bridge_error:
                raise
            raise failures[0] from bridge_error
        raise  # A storage/commit failure is not a domain rejection.
    if failures or len(values) != 1:
        raise RuntimeError("Storage did not preserve the synchronous transaction callback")
    return values[0]
