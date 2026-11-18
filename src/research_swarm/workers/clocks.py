"""Native elapsed clocks with explicit suspend semantics, independent of UTC."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache

from research_swarm.domain.errors import ErrorCode, ResearchError


@dataclass(frozen=True)
class NativeClock:
    profile: str
    continuous_ns: Callable[[], int]


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
    continuous = library.mach_continuous_time
    continuous.argtypes, continuous.restype = [], ctypes.c_uint64
    return NativeClock(
        "darwin-mach-continuous-v1",
        lambda: int(continuous()) * base.numer // base.denom,
    )


@lru_cache(maxsize=1)
def native_clock() -> NativeClock:
    try:
        if sys.platform == "darwin":
            return _darwin_clock()
        if sys.platform.startswith("linux"):
            clock = NativeClock(
                "linux-boottime-v1",
                lambda: time.clock_gettime_ns(time.CLOCK_BOOTTIME),
            )
            clock.continuous_ns()
            return clock
    except (AttributeError, OSError) as error:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Native clock is unavailable"
        ) from error
    raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Suspend-inclusive clock is unsupported")


def continuous_time() -> float:
    return native_clock().continuous_ns() / 1_000_000_000
