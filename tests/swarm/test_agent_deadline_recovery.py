"""Exercise advisory deadlines across real process groups without calling a model."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import select
import signal
import socket
import subprocess
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_swarm.swarm import agent_guard
from research_swarm.swarm.agents import ask_agent
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec

# All fixture peers have their own bounded socket timeout if the test exits early.
_PROVIDER = """
import asyncio, json, os, socket, subprocess, sys
from research_swarm.swarm.operators.llm import Ollama

def peer(kind):
    sock = socket.create_connection(('127.0.0.1', int(os.environ['FIXTURE_PORT'])), timeout=12)
    sock.sendall((json.dumps([kind, os.getpid(), os.getpgrp()]) + '\\n').encode())
    return sock

async def respond(self, prompt, schema, seed):
    kind = self.model
    with peer(kind) as sock:
        if kind == 'stalled':
            # Inherit the provider's group. This separate interpreter has no deadline timer.
            subprocess.Popen([sys.executable, '-c',
                              "from sitecustomize import peer; peer('descendant').recv(1)"],
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
            released = sock.recv(1)
        else:
            sock.setblocking(False)
            released = await asyncio.get_running_loop().sock_recv(sock, 1)
        if released != b'G':
            raise RuntimeError('test peer closed')
    return json.dumps({'summary': 'generated healthy result'})
Ollama.__call__ = respond
"""

_SUPERVISOR = """
import asyncio, json, sys
from pathlib import Path
from research_swarm.swarm.agents import ask_agent

async def run():
    jobs = Path(sys.argv[1])
    replies = await asyncio.gather(
        ask_agent('reporter', {}, model='stalled', timeout=5, jobs=jobs),
        ask_agent('reporter', {}, model='healthy', timeout=12, jobs=jobs),
    )
    print(json.dumps(replies), flush=True)
asyncio.run(run())
"""


def _line(sock: socket.socket) -> list:
    with sock.makefile("rb") as stream:
        return json.loads(stream.readline())


def _terminated(*pids: int) -> dict[int, str]:
    # A zombie has terminated even when the host init has yet to reap the orphan.
    until = time.monotonic() + 2
    while True:
        snapshot = subprocess.run(
            ["ps", "-o", "pid=,stat=", "-p", ",".join(map(str, pids))],
            capture_output=True,
            text=True,
            timeout=2,
        )
        assert snapshot.returncode in {0, 1} and not snapshot.stderr, snapshot.stderr
        states = dict.fromkeys(pids, "absent")
        for line in snapshot.stdout.splitlines():
            pid, state = line.split()
            states[int(pid)] = state
        if all(state == "absent" or "Z" in state for state in states.values()):
            return states
        if time.monotonic() >= until:
            pytest.fail(f"Members of the stalled job still running: {states}")
        time.sleep(0.02)


def _receipt(path: Path) -> dict:
    deadline = time.monotonic() + 3
    while not path.is_file():
        if time.monotonic() >= deadline:
            pytest.fail(f"Terminal record missing from {path.parent.name}")
        time.sleep(0.02)
    return json.loads(path.read_text())


@pytest.mark.parametrize(
    "receipt_time,status", [(109.75, "ok"), (110.0, "timed_out"), (110.25, "timed_out")]
)
def test_advice_receipt_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsysbinary: pytest.CaptureFixture[bytes],
    receipt_time: float,
    status: str,
) -> None:
    deadline = 110.0
    heartbeat = tmp_path / "heartbeat.json"
    heartbeat.write_text(json.dumps({"role": "reporter", "deadline": deadline}))
    os.utime(heartbeat, (100, 100))
    now = 109.0
    reply = b'{"summary":"Generated report."}\n'

    def communicate(*, timeout: float) -> tuple[bytes, bytes]:
        nonlocal now
        assert timeout == deadline - now
        now = receipt_time
        return reply, b""

    provider = SimpleNamespace(returncode=0, communicate=communicate)
    monkeypatch.setattr(agent_guard, "time", SimpleNamespace(monotonic=lambda: now))
    monkeypatch.setattr(agent_guard.subprocess, "Popen", lambda *args, **kwargs: provider)
    monkeypatch.setattr(agent_guard.os, "getsid", lambda _: os.getpid())
    monkeypatch.setattr(agent_guard.os, "killpg", lambda *_: None)

    agent_guard.supervise(heartbeat, deadline)

    assert heartbeat.stat().st_mtime == 100
    assert json.loads((tmp_path / "outcome.json").read_text()) == {
        "role": "reporter",
        "deadline": deadline,
        "status": status,
    }
    assert capsysbinary.readouterr().out == (reply if status == "ok" else b"")


@pytest.mark.parametrize("restart", [False, True], ids=["live", "restarted"])
def test_deadline_is_group_scoped_and_healthy_advice_finishes_once(
    tmp_path: Path,
    restart: bool,
) -> None:
    queue = tmp_path / "campaign.sqlite"
    SQLiteStore(queue).create_campaign(spec())
    before = queue.read_bytes()
    jobs = tmp_path / "campaign.sqlite.agent-jobs"
    hook = tmp_path / "sitecustomize.py"
    hook.write_text(_PROVIDER)
    hook_before = hook.read_bytes()
    with ExitStack() as resources:
        listener = resources.enter_context(socket.create_server(("127.0.0.1", 0)))
        listener.settimeout(8)
        env = {
            **os.environ,
            "PYTHONPATH": str(tmp_path),
            "FIXTURE_PORT": str(listener.getsockname()[1]),
        }
        caller = subprocess.Popen(
            [sys.executable, "-c", _SUPERVISOR, str(jobs)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        resources.callback(caller.wait)
        resources.callback(caller.kill)
        sockets = {}
        groups = {}
        pids = {}
        for _ in range(3):
            sock, _ = listener.accept()
            sock = resources.enter_context(sock)
            sock.settimeout(8)
            kind, pid, pgid = _line(sock)
            assert kind not in sockets
            sockets[kind], pids[kind], groups[kind] = sock, pid, pgid
        assert groups["stalled"] == groups["descendant"] != groups["healthy"]
        assert groups["stalled"] != os.getpgrp() and groups["healthy"] != os.getpgrp()
        stalled, healthy = sorted(
            jobs.glob("*/heartbeat.json"), key=lambda p: json.loads(p.read_text())["deadline"]
        )
        deadline = json.loads(stalled.read_text())["deadline"]
        stalled_pulse = stalled.stat().st_mtime_ns
        healthy_pulse = healthy.stat().st_mtime_ns
        assert json.loads(stalled.read_text())["role"] == "reporter" and time.time() < deadline
        if restart:
            caller.kill()
            assert caller.wait(timeout=3) == -signal.SIGKILL
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
                "--fake",
            ]
            recovered = subprocess.run(command, env=env, capture_output=True, timeout=8, check=True)
            assert json.loads(recovered.stdout)["status"] == "ok"
        observed_exit = {}
        for kind in ("stalled", "descendant"):
            sockets[kind].settimeout(max(0.1, deadline + 1 - time.time()))
            try:
                assert sockets[kind].recv(1) == b""
            except TimeoutError:
                pytest.fail(f"{kind} survived its job deadline")
            observed_exit[kind] = time.time()
        assert time.time() >= deadline - 0.1
        terminated = _terminated(groups["stalled"], pids["stalled"], pids["descendant"])
        if restart:
            timeout = _receipt(stalled.with_name("outcome.json"))
            assert timeout == {"status": "timed_out", "role": "reporter", "deadline": deadline}
            assert stalled.stat().st_mtime_ns == stalled_pulse
        if not restart and not healthy.exists():
            out, err = caller.communicate(timeout=3)
            pytest.fail(f"Healthy job disappeared early: {out!r} {err!r}")
        last_pulse = healthy.stat().st_mtime_ns
        assert last_pulse > healthy_pulse
        sockets["healthy"].sendall(b"G")
        if restart:
            success = _receipt(healthy.with_name("outcome.json"))
            assert success == {"status": "ok", **json.loads(healthy.read_text())}
            assert len(list(jobs.glob("*/outcome.json"))) == 2
            receipts = {
                p: (p.read_bytes(), p.stat().st_mtime_ns) for p in jobs.glob("*/outcome.json")
            }
            replay = subprocess.run(command, env=env, capture_output=True, timeout=8, check=True)
            assert json.loads(replay.stdout)["status"] == "ok"
            assert {
                p: (p.read_bytes(), p.stat().st_mtime_ns) for p in jobs.glob("*/outcome.json")
            } == receipts
        else:
            out, err = caller.communicate(timeout=5)
            assert caller.returncode == 0, err
            timeout, success = json.loads(out)
            assert timeout == {"role": "reporter", "status": "timed_out", "timeout_seconds": 5}
            assert (
                success["status"] == "ok"
                and success["advice"]["summary"] == "generated healthy result"
            )
        assert not select.select([listener], [], [], 0)[0], "an original provider started twice"
        assert queue.read_bytes() == before and hook.read_bytes() == hook_before
        # Retain public fixture facts without process arguments or model inputs.
        (tmp_path / "proof.json").write_text(
            json.dumps(
                {
                    "pids": pids,
                    "groups": groups,
                    "deadline": deadline,
                    "caller_restarted": restart,
                    "stalled_outcome": timeout["status"],
                    "healthy_outcome": success["status"],
                    "observed_peer_eof": observed_exit,
                    "stalled_process_states": terminated,
                    "healthy_heartbeat_ns": [healthy_pulse, last_pulse],
                    "same_queue_sha256": hashlib.sha256(before).hexdigest(),
                    "same_provider_fixture_sha256": hashlib.sha256(hook_before).hexdigest(),
                    "retained_terminal_records": len(list(jobs.glob("*/outcome.json"))),
                },
                sort_keys=True,
            )
        )


@pytest.mark.parametrize("exit_code", [-signal.SIGKILL, 1, None], ids=["killed", "failed", "live"])
async def test_concurrent_guard_exit_only_accepts_a_confirmed_sigkill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exit_code: int | None
) -> None:
    class ExitingGuard:
        pid = 123

        async def communicate(self, payload: bytes) -> None:
            raise TimeoutError

        async def wait(self) -> int:
            if exit_code is None:
                await asyncio.Future()  # This guard is still alive.
                raise AssertionError("wait unexpectedly completed")
            return exit_code

    async def launch(*args, **kwargs):
        return ExitingGuard()

    def already_exiting(pid: int, sig: int) -> None:
        assert pid == ExitingGuard.pid and sig == signal.SIGKILL
        raise PermissionError("process group cannot be signalled")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
    monkeypatch.setattr(os, "killpg", already_exiting)
    reply = ask_agent("reporter", {}, model=None, timeout=1, jobs=tmp_path)
    if exit_code == -signal.SIGKILL:
        assert await reply == {"role": "reporter", "status": "timed_out", "timeout_seconds": 1}
    else:
        with pytest.raises(PermissionError):
            await reply
