"""Run a trial in an isolated POSIX process group using its configured argv."""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from copy import copy
from dataclasses import asdict
from pathlib import Path

from research_swarm.swarm.backends.capacity import Slots
from research_swarm.swarm.backends.gpu_time import GPU_UUID, GPUTimeStore, sample_allocation
from research_swarm.swarm.data_stage import Stage, local_stage, verify_local_stage
from research_swarm.swarm.interfaces import Attempt, CampaignSpec, FailureClass, Result
from research_swarm.swarm.process import run_async
from research_swarm.swarm.telemetry import workload_started


async def _tail(stream: asyncio.StreamReader) -> bytes:
    tail = bytearray()
    while chunk := await stream.read(16384):
        tail.extend(chunk)
        del tail[:-16384]
    return bytes(tail)


def _numbers(path: Path) -> dict[str, float]:
    with path.open("rb") as reader:
        data = reader.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise ValueError(f"{path.name} exceeds one MiB")
    values = json.loads(data)
    if not isinstance(values, dict) or any(
        type(value) not in (int, float) or not math.isfinite(value) for value in values.values()
    ):
        raise ValueError(f"{path.name} must contain a JSON object of finite numbers")
    return {key: float(value) for key, value in values.items()}


def _device_env(device: str) -> dict[str, str]:
    if GPU_UUID.fullmatch(device):
        return {"CUDA_VISIBLE_DEVICES": device, "SWARM_DEVICE": "cuda:0"}
    if device == "cuda":
        return {"SWARM_DEVICE": device}
    if re.fullmatch(r"cuda:[0-9]+", device):
        return {"CUDA_VISIBLE_DEVICES": device.split(":")[1], "SWARM_DEVICE": "cuda:0"}
    if device in {"mps", "cpu"}:
        return {"CUDA_VISIBLE_DEVICES": "", "SWARM_DEVICE": device}
    raise ValueError("device must be cpu, mps, cuda, cuda:N or a full GPU UUID")


class LocalBackend:
    def __init__(
        self,
        slots: int,
        *,
        device: str = "cpu",
        data_dir: Path | None = None,
        sample_gpu_time: bool = False,
    ) -> None:
        self._slots = Slots(slots)
        self._env = _device_env(device)
        if type(sample_gpu_time) is not bool or (
            sample_gpu_time and not GPU_UUID.fullmatch(device)
        ):
            raise ValueError(
                "sample_gpu_time requires a dedicated local CUDA device by full GPU UUID"
            )
        self._sample_gpu_time = sample_gpu_time
        self._stage: Stage | None = None
        if data_dir is not None:
            data = data_dir.resolve(strict=True)
            self._env["SWARM_DATA"] = str(data)
            self._stage = local_stage(data)

    @contextmanager
    def monitor_gpu_allocation(
        self, spec: CampaignSpec, store: GPUTimeStore, worker: object
    ) -> Iterator[None]:
        if not self._sample_gpu_time:
            yield
            return
        device = self._env["CUDA_VISIBLE_DEVICES"]
        devices = tuple(spec.options.get("resources", {}).get("devices_mib", {})) or (device,)
        worker_id = getattr(worker, "worker_id", None)
        if (
            spec.resource != "gpu"
            or device not in devices
            or not all(GPU_UUID.fullmatch(name) for name in devices)
            or not isinstance(worker_id, str)
            or not worker_id
        ):
            raise ValueError(
                "GPU sampling requires a local GPU worker and dedicated devices by full UUID"
            )
        with sample_allocation(store, spec.campaign_id, worker_id, devices):
            yield

    def for_device(self, device: str) -> LocalBackend:
        backend = copy(self)
        backend._env = {**self._env, **_device_env(device)}
        if device == "cuda":
            backend._env.pop("CUDA_VISIBLE_DEVICES", None)
        return backend

    def free_slots(self, spec: CampaignSpec) -> int:
        return self._slots.free(spec)

    async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
        async with self._slots.claim(spec):
            try:
                if self._stage is not None:
                    await asyncio.to_thread(
                        verify_local_stage, Path(self._env["SWARM_DATA"]), self._stage
                    )
                with tempfile.TemporaryDirectory(prefix="swarm-trial-") as temp:
                    return await self._run(spec, attempt, Path(temp))
            except (OSError, ValueError, OverflowError) as error:
                return Result(
                    attempt=attempt,
                    status="failed",
                    error=str(error),
                    failure_class="backend_setup",
                )

    async def _run(self, spec: CampaignSpec, attempt: Attempt, directory: Path) -> Result:
        trial, metrics, usage = (
            directory / name for name in ("trial.json", "metrics.json", "usage.json")
        )
        trial.write_text(json.dumps(asdict(attempt.trial), allow_nan=False), encoding="utf-8")
        env = {
            **os.environ,
            **self._env,
            "SWARM_TRIAL": str(trial),
            "SWARM_ATTEMPT_ID": attempt.attempt_id,
            "SWARM_METRICS": str(metrics),
            "SWARM_USAGE": str(usage),
        }
        error_message = None
        failure_class: FailureClass | None = None
        try:
            result = await run_async(
                spec.command,
                cwd=spec.task_dir,
                env=env,
                timeout=spec.trial_timeout_seconds,
                started=lambda: workload_started(attempt.attempt_id),
                stdout=_tail,
                stderr=_tail,
            )
            output = (result.stdout, result.stderr)
            if result.returncode:
                error_message = f"command exited with status {result.returncode}"
                failure_class = "command_exit"
        except subprocess.TimeoutExpired as exc:
            output = (exc.output or b"", exc.stderr or b"")
            error_message = f"command timed out after {spec.trial_timeout_seconds:g} seconds"
            failure_class = "timeout"
        logs = tuple(item.decode(errors="replace") for item in output if item)

        counters: dict[str, float] = {}
        measurements: dict[str, float] = {}
        try:
            if usage.exists():
                counters = _numbers(usage)
                for field in ("input_tokens", "output_tokens"):
                    value = counters.get(field, 0.0)
                    if value < 0 or not value.is_integer():
                        raise ValueError(f"{field} must be a nonnegative integer")
                if counters.get("peak_vram_mib", 0.0) < 0:
                    raise ValueError("peak_vram_mib must be nonnegative")
        except (ValueError, OSError, OverflowError) as error:
            return Result(
                attempt=attempt,
                status="failed",
                error=str(error),
                logs=logs,
                failure_class="invalid_output",
            )
        if error_message is None:
            try:
                measurements = _numbers(metrics)
                if spec.metric not in measurements:
                    raise ValueError(f"missing objective metric: {spec.metric}")
            except (ValueError, OSError, OverflowError) as error:
                error_message = str(error)
                failure_class = "invalid_output"
                measurements = {}
        return Result(
            attempt=attempt,
            status="failed" if error_message else "ok",
            error=error_message,
            failure_class=failure_class,
            metrics=measurements,
            logs=logs,
            input_tokens=int(counters.get("input_tokens", 0)),
            output_tokens=int(counters.get("output_tokens", 0)),
            peak_vram_mib=counters.get("peak_vram_mib"),
        )


def from_spec(spec: CampaignSpec) -> LocalBackend:
    """Bind a local device and data directory from campaign options.backend.

    Setting sample_gpu_time declares the full UUID device (or devices_mib set)
    dedicated to this controller from first dispatch until drain. Shared devices
    and capacity already allocated outside that window require allocator telemetry.
    """
    options = dict(getattr(spec, "options", {}).get("backend", {}))
    data = options.pop("data_dir", None)
    if spec.resource == "gpu" and options.get("device", "cpu") == "cpu":
        raise ValueError("set options.backend.device to mps or cuda for GPU trials")
    return LocalBackend(
        options.pop("slots", spec.max_workers), data_dir=Path(data) if data else None, **options
    )
