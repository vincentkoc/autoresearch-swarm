from __future__ import annotations

import asyncio
import threading

import pytest

from research_swarm.domain.types import new_id
from research_swarm.orchestration.search import SearchDriver, SearchDriverResult


@pytest.fixture
def driver() -> SearchDriver:
    # Loop tests replace the bounded pass; no transport is used.
    instance = object.__new__(SearchDriver)
    instance.campaign_id = new_id()
    return instance


@pytest.mark.parametrize("interval", [0, -1, 61, float("nan"), float("inf"), True])
async def test_loop_rejects_invalid_interval(driver: SearchDriver, interval: float) -> None:
    with pytest.raises(ValueError, match="interval"):
        await driver.run_async(asyncio.Event(), poll_seconds=interval)


async def test_preset_stop_does_not_read_state(driver: SearchDriver) -> None:
    stop = asyncio.Event()
    stop.set()
    assert (await driver.run_async(stop)).state == "stopped"
    with pytest.raises(ValueError, match="actions"):
        await driver.run_async(stop, max_steps=0)


@pytest.mark.parametrize(
    "state",
    [
        "paused",
        "terminal",
        "needs_review",
        "budget_exhausted",
        "target_reached",
        "delivery_uncertain",
        "transport_unavailable",
    ],
)
async def test_boundaries_return_without_retry(
    driver: SearchDriver, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    calls = []

    def advance(*, max_steps: int) -> SearchDriverResult:
        calls.append(max_steps)
        return SearchDriverResult(campaign_id=driver.campaign_id, state=state, reason="boundary")

    monkeypatch.setattr(driver, "advance", advance)
    assert (await driver.run_async(asyncio.Event(), poll_seconds=0.01)).state == state
    assert calls == [32]


@pytest.mark.parametrize(
    "state",
    [
        "awaiting_baseline",
        "awaiting_proposal",
        "awaiting_execution",
        "awaiting_evaluation",
        "awaiting_confirmation",
        "step_limit",
        "contended",
    ],
)
async def test_wait_states_are_paced_and_stop_is_prompt(
    driver: SearchDriver, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    entered = asyncio.Event()
    loop = asyncio.get_running_loop()
    calls = []

    def advance(*, max_steps: int) -> SearchDriverResult:
        calls.append(max_steps)
        loop.call_soon_threadsafe(entered.set)
        return SearchDriverResult(campaign_id=driver.campaign_id, state=state, reason="waiting")

    monkeypatch.setattr(driver, "advance", advance)
    stop = asyncio.Event()
    task = asyncio.create_task(driver.run_async(stop, poll_seconds=60, max_steps=1))
    await asyncio.wait_for(entered.wait(), 2)
    stop.set()
    assert (await asyncio.wait_for(task, 2)).state == "stopped"
    assert calls == [1]


async def test_loop_continues_after_contention_and_pass_limit(
    driver: SearchDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    states = iter(["contended", "step_limit", "awaiting_proposal", "budget_exhausted"])
    calls = []

    def advance(*, max_steps: int) -> SearchDriverResult:
        calls.append(max_steps)
        return SearchDriverResult(
            campaign_id=driver.campaign_id, state=next(states), reason="bounded pass"
        )

    monkeypatch.setattr(driver, "advance", advance)
    result = await asyncio.wait_for(driver.run_async(asyncio.Event(), poll_seconds=0.01), 2)
    assert result.state == "budget_exhausted"
    assert calls == [32] * 4


@pytest.mark.parametrize("run_loop", [False, True])
async def test_cancellation_drains_inflight_pass(
    driver: SearchDriver, monkeypatch: pytest.MonkeyPatch, run_loop: bool
) -> None:
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def advance(*, max_steps: int) -> SearchDriverResult:
        entered.set()
        assert release.wait(5)
        finished.set()
        return SearchDriverResult(
            campaign_id=driver.campaign_id, state="awaiting_execution", reason="waiting"
        )

    monkeypatch.setattr(driver, "advance", advance)
    operation = driver.run_async(asyncio.Event()) if run_loop else driver.advance_async()
    task = asyncio.create_task(operation)
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        task.cancel()  # Repeated cancellation must not release the transport either.
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert finished.is_set()


@pytest.mark.parametrize("watch", [False, True])
def test_cli_watch_routes_to_continuous_driver(
    monkeypatch: pytest.MonkeyPatch, watch: bool
) -> None:
    from unittest.mock import MagicMock

    from research_swarm.cli import _execute, parser

    campaign_id = new_id()
    client = MagicMock()
    client.get.return_value = {"data": {"contract": {"search": {"policy": "greedy"}}}}
    calls = []
    result = SearchDriverResult(campaign_id=campaign_id, state="terminal", reason="finished")

    def advance(self: SearchDriver, *, max_steps: int) -> SearchDriverResult:
        calls.append(("once", max_steps))
        return result

    async def run(
        self: SearchDriver, stop: asyncio.Event, *, max_steps: int, poll_seconds: float
    ) -> SearchDriverResult:
        assert not stop.is_set()
        calls.append(("watch", max_steps, poll_seconds))
        return result

    monkeypatch.setattr(SearchDriver, "advance", advance)
    monkeypatch.setattr(SearchDriver, "run_async", run)
    arguments = ["search", "advance", campaign_id, "--proposer", new_id(), "--max-steps", "2"]
    if watch:
        arguments += ["--watch", "--poll-seconds", "0.05"]
    assert _execute(client, parser().parse_args(arguments)) == result.model_dump(mode="json")
    assert calls == ([("watch", 2, 0.05)] if watch else [("once", 2)])
