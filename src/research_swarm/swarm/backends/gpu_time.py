"""Sample dedicated local CUDA allocations independently of trials and packed fits."""

from __future__ import annotations

import math
import re
import subprocess
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event
from typing import Any, Protocol
from uuid import uuid4

from research_swarm.swarm.process import run as run_command

GPU_UUID = re.compile(r"GPU-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")
PERIOD_SECONDS = 1.0


class GPUTimeStore(Protocol):
    def record_gpu_time(self, campaign: str, kind: str, sample: dict[str, Any]) -> None: ...


def _busy_fractions() -> dict[str, float]:
    """Read the driver's sampled engine activity, keyed by full physical UUID."""
    try:
        query = run_command(
            ["nvidia-smi", "--query-gpu=uuid,utilization.gpu", "--format=csv,noheader,nounits"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=PERIOD_SECONDS,
            check=True,
        )
        lines = query.stdout.decode().splitlines()
    except (OSError, UnicodeError, subprocess.SubprocessError):
        return {}
    values = {}
    for line in lines:
        device, _, percent = line.partition(",")
        try:
            fraction = float(percent) / 100
        except ValueError:
            continue
        if 0 <= fraction <= 1:
            values[device.strip()] = fraction
    return values


@contextmanager
def sample_allocation(
    store: GPUTimeStore,
    campaign: str,
    worker_id: str,
    devices: tuple[str, ...],
) -> Iterator[None]:
    """Hold this run's dedicated device set from first dispatch through drain."""
    identities = [
        {"allocation_id": uuid4().hex, "device": device, "worker_id": worker_id}
        for device in devices
    ]
    for identity in identities:
        store.record_gpu_time(campaign, "gpu_allocation_started", {**identity, "sequence": 0})
    started = time.monotonic()
    stop = Event()

    def collect() -> None:
        previous = started
        due = started + PERIOD_SECONDS
        sequence = 0
        while True:
            final = stop.wait(max(0, due - time.monotonic()))
            fractions = _busy_fractions()
            now = time.monotonic()
            seconds = now - previous
            sequence += 1
            for identity in identities:
                fraction = fractions.get(identity["device"])
                # The driver averages a recent sample window. For a short allocation the
                # window may predate the owner, and a late reading cannot cover a polling stall.
                busy = (
                    seconds * fraction
                    if fraction is not None
                    and now - started >= PERIOD_SECONDS
                    and seconds <= 2 * PERIOD_SECONDS
                    else None
                )
                store.record_gpu_time(
                    campaign,
                    "gpu_device_interval",
                    {
                        **identity,
                        "sequence": sequence,
                        "allocated_device_seconds": seconds,
                        "busy_device_seconds": busy,
                    },
                )
            previous = now
            if final:
                break
            due = started + (math.floor((now - started) / PERIOD_SECONDS) + 1) * PERIOD_SECONDS
        for identity in identities:
            store.record_gpu_time(
                campaign,
                "gpu_allocation_finished",
                {
                    **identity,
                    "sequence": sequence + 1,
                },
            )

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="gpu-sampler") as executor:
        sampling = executor.submit(collect)
        try:
            yield
        finally:
            stop.set()
            sampling.result()
