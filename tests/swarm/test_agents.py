from __future__ import annotations

import asyncio
import io
import json
import signal
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from research_swarm.cli import main
from research_swarm.swarm import agents
from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec, trial


def test_all_three_roles_read_campaign_without_dispatch(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "queue.sqlite"
    queue = SQLiteStore(path)
    queue.create_campaign(spec(search_space={"x": [1, 2]}, max_attempts=1))
    queue.submit(trial(1))
    attempt = queue.claim("test", "local")
    assert attempt
    queue.report(Result(attempt=attempt, status="failed", error="bad recipe"))
    before = path.read_bytes()
    for role, field in [("proposer", "rationale"), ("diagnoser", "cause"), ("reporter", "summary")]:
        argv = ["swarm", "advise", role, "test", "--db", str(path), "--fake"]
        if role == "diagnoser":
            argv += ["--attempt", attempt.attempt_id]
        assert main(argv) == 0
        reply = json.loads(capsys.readouterr().out)
        assert reply["role"] == role and reply["status"] == "ok" and field in reply["advice"]
    assert (
        main(
            [
                "swarm",
                "advise",
                "diagnoser",
                "test",
                "--db",
                str(path),
                "--fake",
                "--attempt",
                "unknown",
            ]
        )
        == 2
    )
    assert (
        main(["swarm", "advise", "proposer", "test", "--db", str(path), "--fake", "--count", "2"])
        == 0
    )
    replies = json.loads(capsys.readouterr().out)["replies"]
    assert [r["advice"]["params"] for r in replies] == [{"x": 1}, {"x": 2}]
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "answer,error",
    [
        ({"params": {"x": False}, "rationale": "different JSON type"}, ValueError),
        ({"params": {"x": 9}, "rationale": "unknown choice"}, ValueError),
        ({"params": {"x": 0}, "rationale": "valid", "dispatch": True}, ValidationError),
    ],
)
async def test_provider_replies_are_advisory_and_in_space(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    answer: dict,
    error: type[Exception],
) -> None:
    heartbeat = tmp_path / "heartbeat.json"
    heartbeat.touch()
    request = {
        "role": "proposer",
        "context": {"search_space": {"x": [0]}},
        "seed": 0,
        "model": None,
    }
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(request)))
    monkeypatch.setattr(agents, "_fake", lambda *args: json.dumps(answer))
    with pytest.raises(error):
        await agents._reply(heartbeat)


@pytest.mark.parametrize("cancel", [False, True])
async def test_wedged_job_is_killed_and_reaped(tmp_path: Path, cancel: bool) -> None:
    launched = asyncio.Event()
    processes = []
    start = asyncio.create_subprocess_exec

    async def hang(*args, **kwargs):
        process = await start(sys.executable, "-c", "import time; time.sleep(60)", **kwargs)
        processes.append(process)
        launched.set()
        return process

    with patch.object(asyncio, "create_subprocess_exec", hang):
        task = asyncio.create_task(
            agents.ask_agent(
                "reporter", {}, model=None, timeout=10 if cancel else 0.3, jobs=tmp_path
            )
        )
        await asyncio.wait_for(launched.wait(), 3)
        (beat,) = tmp_path.glob("reporter-*/heartbeat.json")
        assert json.loads(beat.read_text())["role"] == "reporter"
        if cancel:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            assert (await asyncio.wait_for(task, 3))["status"] == "timed_out"
    assert processes[0].returncode == -signal.SIGKILL
    assert list(tmp_path.iterdir()) == []
