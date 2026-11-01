"""Python bridge contracts; real event exclusion requires the workerd suite."""

import asyncio

import pytest

from adapters.cloudflare.runtime.exclusive import exclusive
from research_swarm.domain.errors import ErrorCode, ResearchError


class Bridge:
    def __init__(self):
        self.active = False
        self.reset = False

    async def blockConcurrencyWhile(self, callback):
        self.active = True
        try:
            await callback()
        except BaseException as error:
            self.reset = True
            raise RuntimeError("Foreign callback wrapper") from error
        finally:
            self.active = False


async def test_python_result_identity_and_task_survive_a_suspension():
    bridge, value = Bridge(), object()

    async def operation():
        owner = asyncio.current_task()
        assert bridge.active
        await asyncio.sleep(0)
        assert bridge.active and asyncio.current_task() is owner
        return value

    assert await exclusive(bridge, operation) is value
    assert not bridge.active and not bridge.reset


async def test_expected_domain_refusal_escapes_only_after_gate_release():
    bridge = Bridge()
    refusal = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Unavailable witness")

    async def operation():
        await asyncio.sleep(0)
        assert bridge.active
        raise refusal

    with pytest.raises(ResearchError) as observed:
        await exclusive(bridge, operation)
    assert observed.value is refusal
    assert not bridge.active and not bridge.reset


@pytest.mark.parametrize("failure", [ValueError("Broken invariant"), asyncio.CancelledError()])
async def test_unexpected_failure_and_cancellation_reach_runtime_reset(failure):
    bridge = Bridge()

    async def operation():
        raise failure

    with pytest.raises(type(failure)) as observed:
        await exclusive(bridge, operation)
    assert observed.value is failure
    assert bridge.reset and not bridge.active


@pytest.mark.parametrize("mode", ["skip", "repeat", "swallow", "after"])
async def test_runtime_cannot_skip_repeat_or_hide_the_callback(mode):
    calls = 0

    class BrokenBridge:
        async def blockConcurrencyWhile(self, callback):
            if mode == "skip":
                return
            if mode == "swallow":
                try:
                    await callback()
                except ValueError:
                    return
            else:
                await callback()
                if mode == "repeat":
                    await callback()
                if mode == "after":
                    raise OSError("Runtime completion failed")

    async def operation():
        nonlocal calls
        calls += 1
        if mode == "swallow":
            raise ValueError("Broken invariant")
        return None

    expected = OSError if mode == "after" else ValueError if mode == "swallow" else RuntimeError
    with pytest.raises(expected):
        await exclusive(BrokenBridge(), operation)
    assert calls == (0 if mode == "skip" else 1)


async def test_none_is_a_valid_result():
    async def operation():
        return None

    assert await exclusive(Bridge(), operation) is None


@pytest.mark.parametrize("reject", [False, True])
async def test_callback_cannot_start_after_bridge_completion(reject):
    calls = 0

    class EarlyBridge:
        async def blockConcurrencyWhile(self, callback):
            self.late = callback
            if reject:
                raise OSError("Bridge rejected before callback entry")

    async def operation():
        nonlocal calls
        calls += 1

    bridge = EarlyBridge()
    with pytest.raises(OSError if reject else RuntimeError):
        await exclusive(bridge, operation)
    with pytest.raises(RuntimeError, match="outlived"):
        await bridge.late()
    assert calls == 0


@pytest.mark.parametrize("cancel_caller", [False, True])
async def test_separately_scheduled_callback_settles_before_caller_exits(cancel_caller):
    started, finished, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    committed = False

    class DetachedBridge:
        async def blockConcurrencyWhile(self, callback):
            self.task = asyncio.create_task(callback())
            await started.wait()
            if cancel_caller:
                await asyncio.shield(self.task)

    async def operation():
        nonlocal committed
        try:
            started.set()
            await release.wait()
            committed = True
        finally:
            finished.set()

    bridge = DetachedBridge()
    caller = asyncio.create_task(exclusive(bridge, operation))
    await asyncio.wait_for(started.wait(), 1)
    if cancel_caller:
        caller.cancel()
    with pytest.raises(asyncio.CancelledError if cancel_caller else RuntimeError):
        await asyncio.wait_for(caller, 1)
    assert bridge.task.done() and finished.is_set() and not committed
    release.set()
