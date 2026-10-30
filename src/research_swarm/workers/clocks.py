"""Native elapsed clocks with explicit suspend semantics, independent of UTC."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache

from research_swarm.domain.errors import ErrorCode, ResearchError

MAX_SAMPLE_SPAN_NS = 50_000_000
MAX_SAMPLE_UNCERTAINTY_NS = 100_000_000
NATIVE_CLOCK_PROFILES = frozenset(
    {"linux-boottime-v1", "darwin-mach-continuous-v1", "windows-qpc-unbiased-v1"}
)


@dataclass(frozen=True)
class ClockReading:
    continuous_ns: int
    awake_ns: int
    wall_ns: int
    read_uncertainty_ns: int


@dataclass(frozen=True)
class NativeClock:
    profile: str
    continuous_ns: Callable[[], int]
    awake_ns: Callable[[], int]
    resolution_ns: int

    def sample(self) -> ClockReading:
        before = self.continuous_ns()
        awake, wall = self.awake_ns(), time.time_ns()
        after = self.continuous_ns()
        if before < 0 or awake < 0 or after < before:
            raise ResearchError(ErrorCode.STALE_FENCE, "Native clock regressed")
        if after - before > MAX_SAMPLE_SPAN_NS:
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock sample interval is too wide")
        return ClockReading(after, awake, wall, after - before + self.resolution_ns)


def require_continuity(
    before: ClockReading, after: ClockReading, *, tolerance_ns: int = 50_000_000
) -> None:
    """Reject suspend, counter reversal, or a wall step before reusing calibration."""
    if type(tolerance_ns) is not int or tolerance_ns < 0:
        raise ValueError("Clock tolerance must be a nonnegative integer")
    if any(
        not 0 <= sample.read_uncertainty_ns <= MAX_SAMPLE_UNCERTAINTY_NS
        for sample in (before, after)
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Clock sample uncertainty exceeds its bound")
    elapsed = after.continuous_ns - before.continuous_ns
    awake = after.awake_ns - before.awake_ns
    wall = after.wall_ns - before.wall_ns
    tolerance = tolerance_ns + before.read_uncertainty_ns + after.read_uncertainty_ns
    if elapsed < 0 or awake < 0:
        raise ResearchError(ErrorCode.STALE_FENCE, "Native clock regressed")
    if abs(elapsed - awake) > tolerance:
        raise ResearchError(ErrorCode.STALE_FENCE, "Clock calibration crossed a suspend boundary")
    if abs(wall - elapsed) > tolerance:
        raise ResearchError(ErrorCode.STALE_FENCE, "Clock calibration crossed a wall-clock step")


def _darwin_clock() -> NativeClock:
    import ctypes

    library = ctypes.CDLL("/usr/lib/libSystem.B.dylib")

    class Timebase(ctypes.Structure):
        _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]

    info = library.mach_timebase_info
    info.argtypes, info.restype = [ctypes.POINTER(Timebase)], ctypes.c_int
    base = Timebase()
    if info(ctypes.byref(base)) != 0 or base.numer == 0 or base.denom == 0:
        raise OSError("Mach timebase is unavailable")
    continuous, awake = library.mach_continuous_time, library.mach_absolute_time
    for function in (continuous, awake):
        function.argtypes, function.restype = [], ctypes.c_uint64
    return NativeClock(
        "darwin-mach-continuous-v1",
        lambda: int(continuous()) * base.numer // base.denom,
        lambda: int(awake()) * base.numer // base.denom,
        max(1, (base.numer + base.denom - 1) // base.denom),
    )


def _windows_clock() -> NativeClock:
    import ctypes

    if sys.platform != "win32":
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Windows clocks are unavailable")
    library = ctypes.WinDLL("kernel32", use_last_error=True)
    counter, frequency = library.QueryPerformanceCounter, library.QueryPerformanceFrequency
    for function in (counter, frequency):
        function.argtypes, function.restype = [ctypes.POINTER(ctypes.c_int64)], ctypes.c_int
    rate = ctypes.c_int64()
    if not frequency(ctypes.byref(rate)) or rate.value <= 0:
        raise OSError("Performance-counter frequency is unavailable")
    unbiased = library.QueryUnbiasedInterruptTime
    unbiased.argtypes, unbiased.restype = [ctypes.POINTER(ctypes.c_uint64)], ctypes.c_int

    def continuous_ns() -> int:
        value = ctypes.c_int64()
        if not counter(ctypes.byref(value)):
            raise OSError("Performance counter is unavailable")
        return value.value * 1_000_000_000 // rate.value

    def awake_ns() -> int:
        value = ctypes.c_uint64()
        if not unbiased(ctypes.byref(value)):
            raise OSError("Unbiased interrupt time is unavailable")
        return value.value * 100

    # The awake companion has timer-tick precision; this is not QPC resolution.
    return NativeClock("windows-qpc-unbiased-v1", continuous_ns, awake_ns, 16_000_000)


@lru_cache(maxsize=1)
def native_clock() -> NativeClock:
    try:
        if sys.platform == "darwin":
            return _darwin_clock()
        if sys.platform == "win32":
            return _windows_clock()
        if sys.platform.startswith("linux"):
            clock = NativeClock(
                "linux-boottime-v1",
                lambda: time.clock_gettime_ns(time.CLOCK_BOOTTIME),
                lambda: time.clock_gettime_ns(time.CLOCK_MONOTONIC),
                max(1, int(time.clock_getres(time.CLOCK_BOOTTIME) * 1_000_000_000)),
            )
            clock.sample()
            return clock
    except (AttributeError, OSError) as error:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Native clock is unavailable"
        ) from error
    raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Suspend-inclusive clock is unsupported")


def continuous_time() -> float:
    return native_clock().continuous_ns() / 1_000_000_000
