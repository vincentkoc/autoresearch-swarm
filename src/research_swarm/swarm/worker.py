"""A resident worker for the heartbeat-backed trial queue."""

from __future__ import annotations

import asyncio
import contextlib
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Protocol, runtime_checkable
from uuid import uuid4

from research_swarm.swarm.interfaces import Attempt, Backend, CampaignSpec, Meter, Result, Store
from research_swarm.swarm.meter import CostMeter
from research_swarm.swarm.resources import Allocation, Demand, ResourcePool


@runtime_checkable
class _DeviceBackend(Protocol):
    def for_device(self, device: str) -> Backend: ...


class _ExpiredAttempt(Exception):
    pass


def from_spec(spec: CampaignSpec, store: Store, backend: Backend) -> WorkerLoop:
    """Configure the worker from a YAML campaign's options.resources and options.demand."""
    options = getattr(spec, "options", {})
    capacities = options.get("resources")
    amounts = options.get("demand")
    pool_keys = {"cpu", "llm", "tokens_per_minute", "devices_mib", "target"}
    if capacities is not None and (
        not isinstance(capacities, dict) or capacities.keys() - pool_keys
    ):
        raise ValueError("options.resources must be a mapping of slot, rate and device limits")
    try:
        pool = ResourcePool(**capacities) if capacities is not None else None
        demand = Demand(**amounts) if amounts is not None else None
    except (TypeError, AttributeError) as exc:
        raise ValueError(f"invalid resource configuration: {exc}") from exc
    return WorkerLoop(store, backend, f"worker-{uuid4().hex}", pool=pool, demand=demand)


class WorkerLoop:
    def __init__(
        self,
        store: Store,
        backend: Backend,
        worker_id: str,
        *,
        meter: Meter | None = None,
        pool: ResourcePool | None = None,
        demand: Demand | None = None,
        heartbeat_seconds: float = 5.0,
        lost_after_seconds: float = 30.0,
    ) -> None:
        if not worker_id or not 0 < heartbeat_seconds < lost_after_seconds < math.inf:
            raise ValueError("set a worker id and a heartbeat interval below the loss timeout")
        if demand is not None and pool is None:
            raise ValueError("an explicit resource demand requires a pool")
        self.store = store
        self.backend = backend
        self.worker_id = worker_id
        self.meter = meter if meter is not None else CostMeter()
        self.pool = pool
        self.demand = demand
        self.heartbeat_seconds = heartbeat_seconds
        self.lost_after_seconds = lost_after_seconds

    def _demand(self, spec: CampaignSpec) -> Demand:
        if self.demand is not None:
            return self.demand
        if spec.resource == "gpu":
            return Demand(gpu_profile=spec.campaign_id)
        if spec.resource == "llm":
            return Demand(
                llm=1, llm_tokens=spec.estimated_input_tokens + spec.estimated_output_tokens
            )
        return Demand(cpu=1)

    async def run_once(
        self,
        spec: CampaignSpec,
        *,
        before_execute: Callable[[Attempt], Awaitable[None]] | None = None,
    ) -> bool:
        """Reclaim expired work and dispatch at most one attempt. False means no claim."""
        self.store.requeue_expired(spec.campaign_id, time.time() - self.lost_after_seconds)
        backend = self.backend
        if backend.free_slots(spec) <= 0:
            return False
        allocation: Allocation | None = None
        if self.pool is not None:
            allocation = self.pool.acquire(self._demand(spec))
            if allocation is None:
                return False
        attempt = None
        peak = None
        try:
            if allocation is not None and allocation.device is not None:
                if not isinstance(backend, _DeviceBackend):
                    raise ValueError("GPU packing requires a backend with for_device()")
                backend = backend.for_device(allocation.device)
            startup = getattr(backend, "startup_timeout_seconds", 0.0)
            if not isinstance(startup, (int, float)) or not 0 <= startup < math.inf:
                raise ValueError("backend startup timeout must be finite and nonnegative")
            dispatch_spec = replace(
                spec, trial_timeout_seconds=spec.trial_timeout_seconds + startup
            )
            attempt = self.store.claim(
                spec.campaign_id,
                self.worker_id,
                reserve_cost=lambda trial: self.meter.estimate(dispatch_spec, trial),
            )
            if attempt is None:
                return False
            started = time.monotonic()
            try:
                result = await self._supervise(
                    spec,
                    attempt,
                    backend,
                    started,
                    dispatch_spec.trial_timeout_seconds,
                    before_execute,
                )
            except asyncio.CancelledError:
                # The backend has unwound and stopped its command group.
                cost = self.meter.charge(spec, attempt, time.monotonic() - started, 0, 0)
                self.store.report(
                    Result(attempt=attempt, status="lost", error="worker cancelled", cost=cost)
                )
                raise
            if result is not None:
                self.store.report(result)
                if (
                    allocation is not None
                    and allocation.device is not None
                    and result.peak_vram_mib
                ):
                    peak = math.ceil(result.peak_vram_mib)
            return True
        finally:
            if self.pool is not None and allocation is not None:
                self.pool.release(allocation, peak_vram_mib=peak, dispatched=attempt is not None)

    async def _supervise(
        self,
        spec: CampaignSpec,
        attempt: Attempt,
        backend: Backend,
        started: float,
        deadline: float,
        before_execute: Callable[[Attempt], Awaitable[None]] | None,
    ) -> Result | None:
        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(self.heartbeat_seconds)
                cost = self.meter.charge(spec, attempt, time.monotonic() - started, 0, 0)
                if not self.store.heartbeat(attempt.attempt_id, cost=cost):
                    raise _ExpiredAttempt

        result: Result | None = None
        try:
            async with asyncio.TaskGroup() as group:
                pulse = group.create_task(heartbeat())
                cost: float | None = None
                try:
                    # Give the backend its deadline and time to return logs and usage
                    # from teardown. A backend that stalls also loses this attempt.
                    async with asyncio.timeout(deadline + self.heartbeat_seconds):
                        if before_execute is not None:
                            await before_execute(attempt)
                        result = await backend.execute(spec, attempt)
                    if result.attempt != attempt:
                        raise ValueError("backend returned a result for a different claim")
                    cost = self.meter.charge(
                        spec,
                        attempt,
                        time.monotonic() - started,
                        result.input_tokens,
                        result.output_tokens,
                    )
                    if result.status not in ("ok", "failed", "lost") or any(
                        not math.isfinite(value) for value in result.metrics.values()
                    ):
                        raise ValueError("invalid backend outcome or metric")
                    if result.status == "ok" and spec.metric not in result.metrics:
                        raise ValueError(f"missing objective metric: {spec.metric}")
                    if (
                        result.peak_vram_mib is not None
                        and not 0 <= result.peak_vram_mib < math.inf
                    ):
                        raise ValueError("invalid peak VRAM measurement")
                    result = replace(result, cost=cost)
                except asyncio.CancelledError:
                    owner = asyncio.current_task()
                    if owner is None or owner.cancelling():
                        raise
                    cost = self.meter.charge(spec, attempt, time.monotonic() - started, 0, 0)
                    result = Result(
                        attempt=attempt, status="lost", error="backend cancelled", cost=cost
                    )
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                    if cost is None or result is None:
                        cost = self.meter.charge(spec, attempt, time.monotonic() - started, 0, 0)
                        result = Result(attempt=attempt, status="failed", error=error, cost=cost)
                    else:
                        # Invalid measurements do not erase validated usage or command logs.
                        result = replace(
                            result,
                            status="failed",
                            error=error,
                            cost=cost,
                            metrics={},
                            peak_vram_mib=None,
                        )
                finally:
                    pulse.cancel()
        except* _ExpiredAttempt:
            # A replacement owns the trial. TaskGroup cancelled and drained execute().
            result = None
        return result

    async def run(
        self, spec: CampaignSpec, *, stop: asyncio.Event, poll_seconds: float = 0.25
    ) -> None:
        """Fill available slots until stopped, then drain the current attempts."""
        if spec.max_workers <= 0 or not 0 < poll_seconds < math.inf:
            raise ValueError("worker count and poll interval must be positive")

        async def consume() -> None:
            while not stop.is_set():
                if await self.run_once(spec):
                    await asyncio.sleep(0)
                    continue
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=poll_seconds)

        async with asyncio.TaskGroup() as group:
            for _ in range(spec.max_workers):
                group.create_task(consume())
