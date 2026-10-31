"""Keep Python outcomes local across a Durable Object exclusive async callback."""

import asyncio

from research_swarm.domain.errors import ResearchError


async def exclusive(state, callback):
    """Preserve domain refusals without resetting the object; unexpected failures reset.

    The callback must finish its owned work before returning. The runtime gate
    excludes other delivered events, not tasks the callback itself creates.
    """
    values, refusals, failures = [], [], []
    called, closed = False, False
    owner, callback_task = asyncio.current_task(), None

    async def invoke():
        nonlocal called, callback_task
        try:
            if closed:
                raise RuntimeError("Exclusive callback outlived its gate")
            if called:
                raise RuntimeError("Exclusive callback was invoked more than once")
            called = True
            callback_task = asyncio.current_task()
            try:
                values.append(await callback())
            except ResearchError as error:
                # An expected refusal must leave the callback normally. Throwing
                # through blockConcurrencyWhile asks the runtime to reset the DO.
                refusals.append(error)
        except BaseException as error:
            failures.append(error)
            raise

    try:
        # Pass the callable, not an already-created coroutine. Capture Python
        # values locally instead of transferring them through JavaScript.
        await state.blockConcurrencyWhile(invoke)
        if failures or len(values) + len(refusals) != 1:
            raise RuntimeError("Runtime did not preserve the exclusive callback")
        if refusals:
            raise refusals[0]
        return values[0]
    except BaseException as bridge_error:
        if failures and failures[0] is not bridge_error:
            raise failures[0] from bridge_error
        raise
    finally:
        closed = True
        if callback_task is not None and callback_task is not owner and not callback_task.done():
            callback_task.cancel()
            await asyncio.gather(callback_task, return_exceptions=True)
