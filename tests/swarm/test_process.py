"""Exercise group cleanup through both transport and blocking command callers."""

import asyncio
import os
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path

import pytest

from research_swarm.swarm import process
from research_swarm.swarm.backends import remote
from tests.swarm.test_local_backend import assert_child_stopped


def test_blocking_command_exchanges_binary_streams_and_reports_command_status() -> None:
    payload = bytes(range(256)) * 2048
    script = (
        "import sys\n"
        "sys.stderr.buffer.write(b'x' * 131072)\n"
        "sys.stdout.buffer.write(sys.stdin.buffer.read())\n"
        "sys.exit(7)"
    )
    argv = [sys.executable, "-c", script]
    with pytest.raises(subprocess.CalledProcessError) as raised:
        process.run(
            argv,
            input=payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
            check=True,
        )
    assert raised.value.cmd == argv and raised.value.returncode == 7
    assert raised.value.stdout == payload and raised.value.stderr == b"x" * 131072


@pytest.mark.parametrize("caller", ["blocking", "bounded"])
async def test_guard_exit_without_status_stops_its_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caller: str
) -> None:
    marker = tmp_path / "child.pid"

    def guard(fd: int, argv: Sequence[str], observe_closure: bool = False) -> list[str]:
        script = (
            "import os, pathlib, socket, subprocess, sys\n"
            "owner = socket.socket(fileno=int(sys.argv[1]))\n"
            "assert owner.recv(1) == b'1'\n"
            "child = subprocess.Popen(sys.argv[3:])\n"
            "pathlib.Path(sys.argv[2]).write_text(str(child.pid))\n"
            "owner.sendall(b'0\\n')\n"
            "os._exit(1)"
        )
        return [sys.executable, "-c", script, str(fd), str(marker), *argv]

    monkeypatch.setattr(process, "_guard", guard)
    argv = [sys.executable, "-c", "import time; time.sleep(6)"]
    with pytest.raises(OSError, match="without reporting command status"):
        if caller == "blocking":
            await asyncio.to_thread(
                process.run, argv, input=bytes(1024 * 1024), stdout=subprocess.PIPE, timeout=2
            )
        else:
            await asyncio.to_thread(
                process.read_bounded,
                argv,
                env=dict(os.environ),
                limit=32,
                deadline=time.monotonic() + 2,
            )
    await assert_child_stopped(int(marker.read_text()))


@pytest.mark.parametrize("caller", ["transport", "blocking", "advisory"])
@pytest.mark.parametrize("root", ["exits", "timeout"])
async def test_descendant_cannot_hold_command_pipes_open(tmp_path: Path, caller: str, root: str):
    marker = tmp_path / "child.pid"
    argv = [
        sys.executable,
        "-c",
        "import pathlib, subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(15)'])\n"
        "pathlib.Path(sys.argv[1]).write_text(str(child.pid))\n"
        "print('ready', flush=True)\n"
        "if sys.argv[2] == 'timeout': time.sleep(15)",
        str(marker),
        root,
    ]
    timeout = 0.8 if root == "timeout" else 5
    async with asyncio.timeout(7):
        if caller == "transport":
            read = remote.command(argv, timeout=timeout)
            if root == "timeout":
                with pytest.raises(TimeoutError):
                    await read
            else:
                assert await read == "ready\n"
        elif caller == "blocking":
            call = asyncio.to_thread(process.run, argv, stdout=subprocess.PIPE, timeout=timeout)
            if root == "timeout":
                with pytest.raises(subprocess.TimeoutExpired) as raised:
                    await call
                assert raised.value.cmd == argv and raised.value.timeout == timeout
                assert raised.value.stdout == b"ready\n"
            else:
                assert (await call).stdout == b"ready\n"
        else:
            script = (
                "import sys, time\n"
                "from research_swarm.swarm._process_guard import supervise\n"
                "def report(status, out, err): print(status, out.decode().strip(), flush=True)\n"
                "deadline = time.monotonic() + float(sys.argv[1])\n"
                "supervise(sys.argv[2:], deadline=deadline, report=report)"
            )
            async with process.guard_session(
                [sys.executable, "-c", script, str(timeout), *argv]
            ) as guard:
                output, errors = await guard.communicate()
            assert errors == b""
            assert output.strip() == (b"timed_out" if root == "timeout" else b"ok ready")
        await assert_child_stopped(int(marker.read_text()))


async def test_owner_exit_after_release_closes_the_command(tmp_path: Path):
    marker = tmp_path / "running.pid"
    caller = [
        sys.executable,
        "-c",
        "import os, sys, threading, time\n"
        "from pathlib import Path\n"
        "from research_swarm.swarm.process import run\n"
        "marker = Path(sys.argv[1])\n"
        "def leave():\n"
        " while not marker.exists(): time.sleep(.01)\n"
        " os._exit(77)\n"
        "threading.Thread(target=leave, daemon=True).start()\n"
        "run([sys.executable, '-c', sys.argv[2], str(marker)])",
        str(marker),
        "from pathlib import Path; import os, sys, time; "
        "Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(15)",
    ]
    result = await asyncio.to_thread(subprocess.run, caller, capture_output=True, timeout=5)
    assert result.returncode == 77 and not result.stderr
    await assert_child_stopped(int(marker.read_text()))
