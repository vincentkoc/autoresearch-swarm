from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from research_swarm.cli import main
from research_swarm.swarm import advice, agents
from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec, trial


async def test_legacy_advice_uses_the_same_provider_and_preserves_batch_order(
    tmp_path: Path,
) -> None:
    database = tmp_path / "queue.sqlite"
    SQLiteStore(database).create_campaign(spec(search_space={"x": [1, 2]}))
    parser = argparse.ArgumentParser()
    advice.add_arguments(parser)
    args = parser.parse_args(["proposer", "test", "--fake", "--count", "2", "--seed", "9"])
    replies = [{"status": "ok"}, {"status": "failed"}]
    provider = AsyncMock(side_effect=replies)
    with patch.object(agents, "ask_agent", provider):
        result = await advice.advise(database, args)
    assert result == {"role": "proposer", "status": "incomplete", "replies": replies}
    assert [call.kwargs["seed"] for call in provider.await_args_list] == [9, 10]


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


async def test_live_supervisor_reports_provider_alarm_as_timeout(tmp_path: Path) -> None:
    start = asyncio.create_subprocess_exec

    async def expired(*args, **kwargs):
        return await start(
            sys.executable, "-c", "import signal; signal.raise_signal(signal.SIGALRM)", **kwargs
        )

    with patch.object(asyncio, "create_subprocess_exec", expired):
        result = await agents.ask_agent("reporter", {}, model=None, timeout=10, jobs=tmp_path)
    assert result == {"role": "reporter", "status": "timed_out", "timeout_seconds": 10}


def test_reporter_deadline_survives_supervisor_sigkill(tmp_path: Path) -> None:
    queue = tmp_path / "campaign.sqlite"
    SQLiteStore(queue).create_campaign(spec())
    before = queue.read_bytes()
    command = [
        sys.executable,
        "-m",
        "research_swarm",
        "swarm",
        "advise",
        "reporter",
        "test",
        "--db",
        str(queue),
    ]
    with socket.create_server(("127.0.0.1", 0)) as listener:
        listener.settimeout(10)
        # Only the isolated CLI and its provider load this stalled model.
        (tmp_path / "sitecustomize.py").write_text(
            "import socket\n"
            "from research_swarm.swarm.operators.llm import Ollama\n"
            "async def stall(self, prompt, schema, seed):\n"
            f"    with socket.create_connection({listener.getsockname()!r}) as peer:\n"
            "        peer.sendall(b'R')\n"
            "        peer.recv(1)\n"
            "    raise RuntimeError('test peer released')\n"
            "Ollama.__call__ = stall\n"
        )
        env = os.environ.copy()
        env["PYTHONPATH"] = os.pathsep.join(filter(None, (str(tmp_path), env.get("PYTHONPATH"))))
        with subprocess.Popen([*command, "--model", "stalled", "--timeout", "4"], env=env) as cli:
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(5)
                assert connection.recv(1) == b"R"
                (heartbeat,) = (tmp_path / "campaign.sqlite.agent-jobs").glob(
                    "reporter-*/heartbeat.json"
                )
                deadline = json.loads(heartbeat.read_text())["deadline"]
                assert time.time() < deadline
                cli.kill()
                assert cli.wait(timeout=3) == -signal.SIGKILL
                connection.settimeout(max(0.1, deadline - time.time() + 1))
                assert connection.recv(1) == b""
    recovered = subprocess.run([*command, "--fake"], capture_output=True, check=True, timeout=10)
    assert json.loads(recovered.stdout)["status"] == "ok"
    assert queue.read_bytes() == before
