"""Run POSIX commands with a private process group and an owner socket."""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import IO, BinaryIO


def _guard(fd: int, argv: Sequence[str], observe_closure: bool = False) -> list[str]:
    return [
        sys.executable,
        "-I",
        "-S",
        str(Path(__file__).with_name("_process_guard.py")),
        str(fd),
        "observe" if observe_closure else "close",
        *argv,
    ]


def _status(line: bytes) -> int:
    if not line:
        raise OSError("Process guard exited without reporting command status")
    return int(line)


def _started(line: bytes, executable: str) -> None:
    if error := _status(line):
        raise OSError(error, os.strerror(error), executable)


def stop_group(child: subprocess.Popen[bytes], *, force: bool = True) -> None:
    """Signal an owned, unreaped leader. Its PID cannot have been reused."""
    if child.returncode is None:
        with suppress(ProcessLookupError):
            os.killpg(child.pid, signal.SIGKILL if force else signal.SIGTERM)


def start_group(
    argv: Sequence[str],
    *,
    cwd: str | Path | None = None,
    env: dict[str, str] | None = None,
    stdin: int | None = None,
    stdout: int | BinaryIO | None = None,
    stderr: int | BinaryIO | None = None,
    observe_closure: bool = False,
) -> tuple[subprocess.Popen[bytes], socket.socket]:
    """Prepare a linked group; the owner's release byte authorizes the first exec."""
    owner, endpoint = socket.socketpair()
    try:
        with endpoint:
            fd = endpoint.fileno()
            child = subprocess.Popen(
                _guard(fd, argv, observe_closure),
                pass_fds=(fd,),
                start_new_session=True,
                cwd=cwd,
                env=env,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
            )
        return child, owner
    except BaseException:
        owner.close()
        raise


def run(
    argv: Sequence[str],
    *,
    input: bytes | None = None,
    cwd: str | Path | None = None,
    env: dict[str, str] | None = None,
    stdout: int | BinaryIO | None = None,
    stderr: int | BinaryIO | None = None,
    check: bool = False,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[bytes]:
    """Preserve the command's exit code, streams and launch errors across the guard."""
    child, owner = start_group(
        argv,
        cwd=cwd,
        env=env,
        stdin=subprocess.PIPE if input is not None else None,
        stdout=stdout,
        stderr=stderr,
    )
    with owner:
        try:
            owner.sendall(b"1")
            try:
                out, err = child.communicate(input, timeout=timeout)
            except subprocess.TimeoutExpired as exc:
                raise subprocess.TimeoutExpired(argv, exc.timeout, exc.output, exc.stderr) from exc
            with owner.makefile("rb") as control:
                _started(control.readline(), argv[0])
                code = _status(control.readline())
        except BaseException:
            stop_group(child)
            raise
        finally:
            owner.close()
            child.wait()
            for pipe in (child.stdin, child.stdout, child.stderr):
                if pipe is not None:
                    pipe.close()
    result = subprocess.CompletedProcess(argv, code, out, err)
    if check:
        result.check_returncode()
    return result


def read_bounded(argv: Sequence[str], *, env: dict[str, str], limit: int, deadline: float) -> bytes:
    """Read at most limit bytes while enforcing a single deadline across the entire invocation."""
    import psutil

    with tempfile.TemporaryFile() as output:
        child, owner = start_group(argv, env=env, stdout=output, stderr=subprocess.DEVNULL)
        descendants: dict[int, psutil.Process] = {}
        try:
            owner.sendall(b"1")
            while child.poll() is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(argv, 0)
                if os.fstat(output.fileno()).st_size > limit:
                    raise ValueError("Command output exceeds its declared source bound")
                with suppress(psutil.NoSuchProcess):
                    for member in psutil.Process(child.pid).children(recursive=True):
                        descendants[member.pid] = member
                with suppress(subprocess.TimeoutExpired):
                    child.wait(timeout=min(0.02, remaining))
            with owner.makefile("rb") as control:
                _started(control.readline(), argv[0])
                code = _status(control.readline())
            if code:
                raise subprocess.CalledProcessError(code, argv)
            output.seek(0)
            content = output.read(limit + 1)
            if len(content) > limit:
                raise ValueError("Command output exceeds its declared source bound")
            return content
        except BaseException:
            stop_group(child)
            raise
        finally:
            owner.close()
            child.wait()
            # Source tools may have spawned a detached helper. psutil verifies
            # the identity of previously observed descendants before signalling.
            for member in descendants.values():
                with suppress(psutil.NoSuchProcess):
                    member.kill()


@asynccontextmanager
async def guard_session(argv: Sequence[str]) -> AsyncIterator[asyncio.subprocess.Process]:
    """Launch an independent deadline guard. Explicit cancellation closes its owned group."""
    process = await asyncio.create_subprocess_exec(
        *argv,
        start_new_session=True,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        yield process
    except BaseException:
        if process.returncode is not None:
            # asyncio has already reaped this leader. Never signal a reused PID.
            raise
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except PermissionError as error:
            # Darwin can deny a signal to a group already closing under its own guard.
            try:
                exit_code = await asyncio.wait_for(process.wait(), timeout=1)
            except TimeoutError:
                raise error from None
            if exit_code != -signal.SIGKILL:
                raise
        await process.wait()
        raise


async def read_all(stream: asyncio.StreamReader) -> bytes:
    return await stream.read()


async def _collect(
    pipe: IO[bytes], read: Callable[[asyncio.StreamReader], Awaitable[bytes]]
) -> bytes:
    stream = asyncio.StreamReader()
    transport, _ = await asyncio.get_running_loop().connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(stream), pipe
    )
    try:
        return await read(stream)
    finally:
        transport.close()


def _feed(pipe: IO[bytes], payload: bytes) -> None:
    with pipe, suppress(BrokenPipeError, ConnectionResetError):
        pipe.write(payload)
        pipe.flush()


async def run_async(
    argv: Sequence[str],
    *,
    timeout: float,
    input: bytes | None = None,
    cwd: str | Path | None = None,
    env: dict[str, str] | None = None,
    started: Callable[[], None] | None = None,
    stdout: Callable[[asyncio.StreamReader], Awaitable[bytes]] = read_all,
    stderr: Callable[[asyncio.StreamReader], Awaitable[bytes]] = read_all,
) -> subprocess.CompletedProcess[bytes]:
    child, owner = start_group(
        argv,
        cwd=cwd,
        env=env,
        stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert child.stdout and child.stderr
    output = asyncio.gather(_collect(child.stdout, stdout), _collect(child.stderr, stderr))
    feed = None
    timed_out = False
    try:
        control, writer = await asyncio.open_connection(sock=owner)
        try:
            async with asyncio.timeout(timeout):
                writer.write(b"1")
                _started(await control.readline(), argv[0])
                if started is not None:
                    started()
                if child.stdin is not None:
                    assert input is not None
                    feed = asyncio.create_task(asyncio.to_thread(_feed, child.stdin, input))
                code = _status(await control.readline())
        finally:
            writer.close()
    except TimeoutError:
        timed_out = True
        stop_group(child)
    except BaseException:
        stop_group(child)
        raise
    finally:
        # No asyncio child watcher reaps this PID before failure cleanup.
        owner.close()
        await asyncio.to_thread(child.wait)
        if feed is not None:
            await feed
        elif child.stdin is not None:
            child.stdin.close()
        out, err = await output
    if timed_out:
        raise subprocess.TimeoutExpired(argv, timeout, out, err)
    return subprocess.CompletedProcess(argv, code, out, err)
