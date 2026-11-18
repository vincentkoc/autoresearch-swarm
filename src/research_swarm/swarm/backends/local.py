"""Run a trial in an isolated POSIX process group using its configured argv."""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import signal
import tempfile
from contextlib import suppress
from copy import copy
from dataclasses import asdict
from pathlib import Path

from research_swarm.swarm.backends.capacity import Slots
from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Result


async def _tail(stream: asyncio.StreamReader) -> str:
    tail = bytearray()
    while chunk := await stream.read(16384):
        tail.extend(chunk)
        del tail[:-16384]
    return tail.decode(errors="replace")


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
    if device == "cuda":
        return {"SWARM_DEVICE": device}
    if re.fullmatch(r"cuda:[0-9]+", device):
        return {"CUDA_VISIBLE_DEVICES": device.split(":")[1], "SWARM_DEVICE": "cuda:0"}
    if device in {"mps", "cpu"}:
        return {"CUDA_VISIBLE_DEVICES": "", "SWARM_DEVICE": device}
    raise ValueError("device must be cpu, mps, cuda or cuda:N")


class LocalBackend:
    def __init__(self, slots: int, *, device: str = "cpu", data_dir: Path | None = None) -> None:
        self._slots = Slots(slots)
        self._env = _device_env(device)
        if data_dir is not None:
            self._env["SWARM_DATA"] = str(data_dir.resolve(strict=True))

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
                with tempfile.TemporaryDirectory(prefix="swarm-trial-") as temp:
                    return await self._run(spec, attempt, Path(temp))
            except (OSError, ValueError, OverflowError) as error:
                return Result(attempt=attempt, status="failed", error=str(error))

    async def _run(self, spec: CampaignSpec, attempt: Attempt, directory: Path) -> Result:
        trial, metrics, usage = (
            directory / name for name in ("trial.json", "metrics.json", "usage.json")
        )
        trial.write_text(json.dumps(asdict(attempt.trial), allow_nan=False), encoding="utf-8")
        env = {
            **os.environ,
            **self._env,
            "SWARM_TRIAL": str(trial),
            "SWARM_METRICS": str(metrics),
            "SWARM_USAGE": str(usage),
        }
        process = await asyncio.create_subprocess_exec(
            *spec.command,
            cwd=spec.task_dir,
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        assert process.stdout and process.stderr
        stdout = asyncio.create_task(_tail(process.stdout))
        stderr = asyncio.create_task(_tail(process.stderr))
        error_message = None
        try:
            async with asyncio.timeout(spec.trial_timeout_seconds):
                code = await process.wait()
                if code:
                    error_message = f"command exited with status {code}"
        except TimeoutError:
            error_message = f"command timed out after {spec.trial_timeout_seconds:g} seconds"
        finally:
            # Descendants may outlive the leader or keep its output pipes open.
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
            logs = tuple(item for item in await asyncio.gather(stdout, stderr) if item)

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
            if error_message is None:
                measurements = _numbers(metrics)
                if spec.metric not in measurements:
                    raise ValueError(f"missing objective metric: {spec.metric}")
        except (ValueError, OSError, OverflowError) as error:
            return Result(attempt=attempt, status="failed", error=str(error), logs=logs)
        return Result(
            attempt=attempt,
            status="failed" if error_message else "ok",
            error=error_message,
            metrics=measurements,
            logs=logs,
            input_tokens=int(counters.get("input_tokens", 0)),
            output_tokens=int(counters.get("output_tokens", 0)),
            peak_vram_mib=counters.get("peak_vram_mib"),
        )


def from_spec(spec: CampaignSpec) -> LocalBackend:
    """Bind a local device and data directory from campaign options.backend."""
    options = dict(getattr(spec, "options", {}).get("backend", {}))
    data = options.pop("data_dir", None)
    if spec.resource == "gpu" and options.get("device", "cpu") == "cpu":
        raise ValueError("set options.backend.device to mps or cuda for GPU trials")
    return LocalBackend(
        options.pop("slots", spec.max_workers), data_dir=Path(data) if data else None, **options
    )
