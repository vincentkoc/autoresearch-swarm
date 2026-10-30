"""Keep admitted synchronous API calls owned until they settle."""

import asyncio
from collections.abc import Callable
from contextlib import suppress
from typing import TypeVar

T = TypeVar("T")


async def run_owned_step(function: Callable[[], T]) -> T:
    task = asyncio.create_task(asyncio.to_thread(function))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            with suppress(asyncio.CancelledError, Exception):
                await asyncio.shield(task)
        with suppress(asyncio.CancelledError, Exception):
            task.result()
        raise
