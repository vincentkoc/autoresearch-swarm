"""Keep a job's deadline and outcome when its heartbeat path is unavailable."""

from __future__ import annotations

import json
import os
import signal
import subprocess
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from research_swarm.swarm import agent_guard, agents


@pytest.mark.parametrize("timeout_owner", ["caller", "guard"])
async def test_missing_heartbeat_at_launch_and_expiry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, timeout_owner: str
) -> None:
    now = 100.0
    heartbeat = tmp_path / "heartbeat.json"
    retained = tmp_path / "retained-pulse.json"
    outcome = tmp_path / "outcome.json"
    expected = {"role": "reporter", "deadline": 1010.0, "status": "timed_out"}

    def elapse(remaining: float) -> None:
        nonlocal now
        assert not heartbeat.exists() and remaining == 8.0
        now = 104.0
        retained.rename(heartbeat)
        os.utime(heartbeat, (1004.0, 1004.0))
        assert json.loads(heartbeat.read_text())["deadline"] == expected["deadline"]
        now = 108.0
        heartbeat.rename(retained)
        now = 110.0
        assert not heartbeat.exists() and not outcome.exists()

    def provider_wait(*, timeout: float) -> None:
        elapse(timeout)
        raise subprocess.TimeoutExpired("fixture", timeout)

    def intercept_expiry(*_: object) -> None:
        assert now == 110.0
        assert json.loads(outcome.read_text()) == expected

    async def launch(*argv: str, **kwargs: object) -> SimpleNamespace:
        nonlocal now
        heartbeat.rename(retained)
        now = 102.0
        assert not heartbeat.exists()

        async def communicate(payload: bytes) -> tuple[bytes, bytes]:
            if timeout_owner == "caller":
                elapse(float(argv[4]) - now)
                raise TimeoutError
            agent_guard.supervise(Path(argv[3]), float(argv[4]))
            return b"", b""

        return SimpleNamespace(
            communicate=communicate, pid=900001, returncode=-signal.SIGKILL, wait=AsyncMock()
        )

    def caller_wait(remaining: float) -> nullcontext[None]:
        assert remaining == 8.0
        return nullcontext()

    clock = SimpleNamespace(monotonic=lambda: now, time=lambda: 900.0 + now)
    process_ops = SimpleNamespace(
        getpid=lambda: 900001, getsid=lambda _: 900001, killpg=intercept_expiry, link=os.link
    )
    monkeypatch.setattr(agents, "time", clock)
    monkeypatch.setattr(agent_guard, "time", clock)
    monkeypatch.setattr(agents.tempfile, "TemporaryDirectory", lambda **kw: nullcontext(tmp_path))
    monkeypatch.setattr(agents.asyncio, "create_subprocess_exec", launch)
    monkeypatch.setattr(agents.asyncio, "timeout", caller_wait)
    monkeypatch.setattr(
        agent_guard.subprocess, "Popen", lambda *a, **kw: SimpleNamespace(communicate=provider_wait)
    )
    monkeypatch.setattr(agent_guard, "os", process_ops)
    monkeypatch.setattr(agents, "os", process_ops)

    reply = await agents.ask_agent("reporter", {}, model=None, jobs=tmp_path, timeout=10)

    assert reply == {"role": "reporter", "status": "timed_out", "timeout_seconds": 10}
    assert not heartbeat.exists() and retained.is_file()
    assert json.loads(outcome.read_text()) == expected
