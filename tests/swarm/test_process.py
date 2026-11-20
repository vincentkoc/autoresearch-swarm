"""Exercise group cleanup through both transport and blocking command callers."""

import asyncio
import subprocess
import sys
from pathlib import Path

import pytest

from research_swarm.swarm import process
from research_swarm.swarm.backends import remote
from tests.swarm.test_local_backend import assert_child_stopped


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
                with pytest.raises(subprocess.TimeoutExpired):
                    await call
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
