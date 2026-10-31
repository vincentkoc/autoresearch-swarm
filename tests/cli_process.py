"""Identify actual fixture CLI processes behind venv redirectors."""

from __future__ import annotations

import faulthandler
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import psutil


@dataclass
class OwnedCliProcess:
    launcher: subprocess.Popen[bytes]
    process: psutil.Process

    def kill_and_wait(self) -> None:
        self.close(force=True)

    def close(self, *, force: bool = False) -> None:
        try:
            if self.process.is_running():
                if force:
                    self.process.kill()
                else:
                    self.process.terminate()
                try:
                    self.process.wait(timeout=5 if force else 10)
                except psutil.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
        except psutil.NoSuchProcess:
            pass
        assert not self.process.is_running(), "Owned interpreter is still alive"
        try:
            self.launcher.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            self.launcher.kill()
            self.launcher.communicate(timeout=5)


def require_ancestry(process: psutil.Process, owner: psutil.Process) -> None:
    created, owner_created = process.create_time(), owner.create_time()
    for current in (process, owner):
        if not current.is_running():
            raise psutil.NoSuchProcess(current.pid)
    assert any(
        ancestor.pid == owner.pid and ancestor.create_time() == owner_created
        for ancestor in (process, *process.parents())
    ), "Fixture process is outside the pinned launcher's ancestry"
    for current in (process, owner):
        if not current.is_running():
            raise psutil.NoSuchProcess(current.pid)
    assert process.create_time() == created and owner.create_time() == owner_created


def require_driver(launcher: psutil.Process, witness: Path, nonce: str) -> psutil.Process:
    identity = json.loads(witness.read_text(encoding="utf-8"))
    assert set(identity) == {"nonce", "pid", "created_at_hex"} and identity["nonce"] == nonce
    assert type(identity["pid"]) is int and identity["pid"] > 0
    process = psutil.Process(identity["pid"])
    assert process.create_time().hex() == identity["created_at_hex"]
    require_ancestry(process, launcher)
    return process


def acknowledge_driver(launcher: psutil.Process, witness: Path, nonce: str) -> psutil.Process:
    process = require_driver(launcher, witness, nonce)
    publish(
        witness.with_suffix(".ready"),
        {
            "nonce": nonce,
            "pid": process.pid,
            "created_at_hex": process.create_time().hex(),
        },
    )
    return process


def publish(path: Path, value: dict[str, str | int]) -> None:
    pending = path.with_suffix(".pending")
    pending.write_text(json.dumps(value), encoding="utf-8")
    pending.replace(path)


def main() -> int:
    witness, nonce, lock_path = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
    identity = {
        "nonce": nonce,
        "pid": os.getpid(),
        "created_at_hex": psutil.Process().create_time().hex(),
    }
    publish(witness, identity)
    mode = sys.argv[4]
    assert mode in {"", "ack", "serve"}
    if mode in {"ack", "serve"}:
        until = time.monotonic() + 10
        while True:
            if time.monotonic() >= until:
                raise TimeoutError("Fixture driver identity was not acknowledged")
            try:
                acknowledged = json.loads(witness.with_suffix(".ready").read_text(encoding="utf-8"))
            except FileNotFoundError:
                time.sleep(0.01)
            else:
                if acknowledged != identity:
                    raise ValueError("Fixture driver acknowledgment has another identity")
                break
    trace = None
    if mode == "serve":
        import uvicorn

        original_startup = uvicorn.Server.startup
        trace = witness.with_suffix(".trace").open("w", encoding="utf-8")

        async def startup(server, sockets=None):
            await original_startup(server, sockets=sockets)
            if server.started:
                faulthandler.dump_traceback_later(8, repeat=True, file=trace)
                addresses = [
                    listener.getsockname()
                    for instance in server.servers
                    for listener in instance.sockets
                ]
                assert len(addresses) == 1 and addresses[0][0] == "127.0.0.1"
                publish(
                    witness.with_suffix(".listening"),
                    {
                        **identity,
                        "host": addresses[0][0],
                        "port": addresses[0][1],
                    },
                )

        uvicorn.Server.startup = startup
    if lock_path:
        expected = Path(lock_path).resolve()
        trace = witness.with_suffix(".trace").open("w", encoding="utf-8")
        faulthandler.dump_traceback_later(8, file=trace)

        def phase(name: str) -> None:
            witness.with_suffix(".phase").write_text(name, encoding="utf-8")

        phase("importing-worker")
        from research_swarm.workers import local

        phase("worker-imported")

        class ObservedLock(local.FileLock):
            def acquire(self, *args, **kwargs):
                observed = Path(self.lock_file).resolve() == expected
                if observed:
                    phase("requesting-attempt-lock")
                acquired = super().acquire(*args, **kwargs)
                try:
                    if observed:
                        phase("acquired-attempt-lock")
                        publish(
                            witness.with_suffix(".acquired"),
                            {"attempt_id": expected.stem, **identity},
                        )
                        faulthandler.cancel_dump_traceback_later()
                except BaseException:
                    self.release()
                    raise
                return acquired

        local.FileLock = ObservedLock
        phase("entering-worker-cli")
    try:
        from research_swarm.cli import main as cli_main

        return cli_main(sys.argv[5:])
    finally:
        if trace is not None:
            faulthandler.cancel_dump_traceback_later()
            trace.close()


if __name__ == "__main__":
    raise SystemExit(main())
