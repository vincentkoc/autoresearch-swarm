"""Slot admission for a scheduler that owns a disjoint set of devices and quotas."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import Lock


@dataclass(frozen=True)
class Demand:
    """Resources per fit. A profile identifies comparable model/shape/precision runs."""

    cpu: int = 0
    llm: int = 0
    llm_tokens: int = 0
    gpu_profile: str | None = None

    def __post_init__(self) -> None:
        if any(type(n) is not int or n < 0 for n in (self.cpu, self.llm, self.llm_tokens)):
            raise ValueError("slot and token demand must be nonnegative integers")
        if self.llm_tokens and not self.llm:
            raise ValueError("token demand requires an LLM slot")
        if not (self.cpu or self.llm or self.gpu_profile) or self.gpu_profile == "":
            raise ValueError("a fit must request at least one resource")


@dataclass(frozen=True, eq=False)
class Allocation:
    demand: Demand
    device: str | None
    vram_mib: int
    fits: int


class ResourcePool:
    """Pack mixed demands atomically. Share one instance across a backend's dispatchers.

    Start unknown GPU profiles alone. Once a workload reports a measured single-fit
    peak, keep the largest observation for that profile/device. A batch runner can
    request up to max_fits and pass allocation.fits to its vectorized workload.
    """

    def __init__(
        self,
        *,
        cpu: int = 0,
        llm: int = 0,
        tokens_per_minute: int | None = None,
        devices_mib: Mapping[str, int] | None = None,
        target: float = 0.85,
        gpu_utilisation: Callable[[str], float] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        limits = (cpu, llm) if tokens_per_minute is None else (cpu, llm, tokens_per_minute)
        if any(type(n) is not int or n < 0 for n in limits) or not 0 < target <= 1:
            raise ValueError("invalid capacity or GPU utilisation target")
        devices = devices_mib or {}
        if any(not name or type(mib) is not int or mib < 1 for name, mib in devices.items()):
            raise ValueError("devices require a name and positive capacity in MiB")
        self._cpu = cpu
        self._llm = llm
        self._devices = {name: int(mib * target) for name, mib in devices.items()}
        self._target = target
        self._gpu_utilisation = gpu_utilisation
        self._peaks: dict[tuple[str, str], int] = {}
        self._active: set[Allocation] = set()
        self._token_limit = tokens_per_minute
        self._tokens: deque[tuple[float, Allocation]] = deque()
        self._clock = clock
        self._lock = Lock()

    def acquire(self, demand: Demand, *, max_fits: int = 1) -> Allocation | None:
        allocation, _ = self.acquire_or_delay(demand, max_fits=max_fits)
        return allocation

    def acquire_or_delay(
        self, demand: Demand, *, max_fits: int = 1
    ) -> tuple[Allocation | None, float | None]:
        """Try admission and return seconds until enough LLM tokens replenish, if known."""
        if type(max_fits) is not int or max_fits < 1:
            raise ValueError("max_fits must be a positive integer")
        with self._lock:
            fits = max_fits
            if demand.cpu:
                used_cpu = sum(a.demand.cpu * a.fits for a in self._active)
                fits = min(fits, (self._cpu - used_cpu) // demand.cpu)
            if demand.llm:
                used_llm = sum(a.demand.llm * a.fits for a in self._active)
                fits = min(fits, (self._llm - used_llm) // demand.llm)
            now = self._clock()
            while self._tokens and self._tokens[0][0] <= now - 60:
                self._tokens.popleft()
            if self._token_limit is not None and demand.llm:
                if not demand.llm_tokens:
                    raise ValueError("a rate-limited LLM requires a token allowance")
                available = self._token_limit - sum(
                    a.demand.llm_tokens * a.fits for _, a in self._tokens
                )
                if fits and available < demand.llm_tokens <= self._token_limit:
                    needed = demand.llm_tokens - available
                    for at, entry in self._tokens:
                        needed -= entry.demand.llm_tokens * entry.fits
                        if needed <= 0:
                            return None, at + 60 - now
                fits = min(fits, available // demand.llm_tokens)
            if fits == 0:
                return None, None
            device = None
            vram = 0
            if demand.gpu_profile is not None:
                placements = []
                for name, capacity in self._devices.items():
                    if self._gpu_utilisation is not None:
                        utilisation = self._gpu_utilisation(name)
                        if not 0 <= utilisation <= 1:
                            raise ValueError("GPU utilisation must be a fraction from 0 to 1")
                        if utilisation >= self._target:
                            continue
                    # A sibling can report a higher peak while comparable fits are still active.
                    used = sum(
                        max(a.vram_mib, a.fits * self._peaks.get((name, a.demand.gpu_profile), 0))
                        for a in self._active
                        if a.device == name and a.demand.gpu_profile is not None
                    )
                    peak = self._peaks.get((name, demand.gpu_profile), capacity)
                    count = min(fits, (capacity - used) // peak) if peak else 0
                    if count > 0:
                        # Prefer the largest batch, then the tightest fit.
                        placements.append((-count, capacity - used - count * peak, name, peak))
                if not placements:
                    return None, None
                neg_count, _, device, peak = min(placements)
                fits = -neg_count
                vram = fits * peak
            allocation = Allocation(demand, device, vram, fits)
            self._active.add(allocation)
            if self._token_limit is not None and demand.llm_tokens:
                self._tokens.append((now, allocation))
            return allocation, None

    def release(
        self, allocation: Allocation, *, peak_vram_mib: int | None = None, dispatched: bool = True
    ) -> None:
        """Release slots. peak_vram_mib is a measured per-fit peak, even for batch runners."""
        with self._lock:
            if allocation not in self._active:
                raise ValueError("allocation is not active in this pool")
            if peak_vram_mib is not None:
                profile = allocation.demand.gpu_profile
                if (
                    allocation.device is None
                    or profile is None
                    or type(peak_vram_mib) is not int
                    or peak_vram_mib <= 0
                ):
                    raise ValueError("a GPU measurement must be a positive integer in MiB")
                key = allocation.device, profile
                self._peaks[key] = max(self._peaks.get(key, 0), peak_vram_mib)
            if not dispatched and allocation.demand.llm_tokens:
                self._tokens = deque(entry for entry in self._tokens if entry[1] is not allocation)
            self._active.remove(allocation)
