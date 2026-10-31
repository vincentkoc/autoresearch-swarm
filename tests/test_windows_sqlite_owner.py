from __future__ import annotations

import ctypes
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier

import pytest

from research_swarm.domain.errors import ResearchError
from research_swarm.runtime import LocalRuntime
from research_swarm.state import windows_privacy as privacy
from research_swarm.state.database import Database

pytestmark = pytest.mark.skipif(
    sys.platform != "win32", reason="Native Windows token qualification"
)


@contextmanager
def token(*, thread: bool):
    api, handle = privacy._api(), privacy.HANDLE()
    opened = (
        api["OpenThreadToken"](api["GetCurrentThread"](), 0x0008, True, ctypes.byref(handle))
        if thread
        else api["OpenProcessToken"](api["GetCurrentProcess"](), 0x0008, ctypes.byref(handle))
    )
    if not opened:
        assert thread and ctypes.get_last_error() == 1008
        yield None
        return
    try:
        yield handle
    finally:
        assert api["CloseHandle"](handle)


def token_snapshot(*, thread: bool):
    with token(thread=thread) as handle:
        if handle is None:
            return None
        owner = privacy._token_information(handle, 4)
        return {
            "id": bytes(privacy._token_information(handle, 10))[:8],
            "owner": privacy._sid(privacy.TokenOwner.from_buffer(owner).owner),
            "privileges": bytes(privacy._token_information(handle, 3)),
        }


def test_sqlite_restores_exact_prior_token_across_nested_connections_and_exceptions(tmp_path):
    process_before = token_snapshot(thread=False)
    thread_before = token_snapshot(thread=True)
    with privacy.private_directory(tmp_path / "state"):
        database = Database(tmp_path / "state" / "research.sqlite")
        assert token_snapshot(thread=True) == thread_before
        with privacy.sqlite_creation_owner():
            prior = token_snapshot(thread=True)
            assert prior["owner"] == privacy.current_user_sid()
            with database.connect() as first:
                first_token = token_snapshot(thread=True)
                assert first_token["id"] != prior["id"]
                with pytest.raises(ValueError, match="owned test failure"), database.connect():
                    second_token = token_snapshot(thread=True)
                    assert second_token["id"] != first_token["id"]
                    raise ValueError("owned test failure")
                assert token_snapshot(thread=True) == first_token
                assert first.execute("SELECT 1").one()[0] == 1
            assert token_snapshot(thread=True) == prior
        assert token_snapshot(thread=True) == thread_before
    assert token_snapshot(thread=False) == process_before


def test_sqlite_owner_scope_is_thread_local():
    process_before, caller_before = token_snapshot(thread=False), token_snapshot(thread=True)
    barrier = Barrier(3, timeout=10)

    def worker():
        before = token_snapshot(thread=True)
        with privacy.sqlite_creation_owner():
            inside = token_snapshot(thread=True)
            barrier.wait()
            barrier.wait()
            assert token_snapshot(thread=True) == inside
        assert token_snapshot(thread=True) == before
        return inside

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(worker) for _ in range(2)]
        barrier.wait()
        assert token_snapshot(thread=True) == caller_before
        assert token_snapshot(thread=False) == process_before
        barrier.wait()
        snapshots = [future.result(timeout=10) for future in futures]
    assert snapshots[0]["id"] != snapshots[1]["id"]
    assert all(item["owner"] == privacy.current_user_sid() for item in snapshots)


def test_crashed_sqlite_sidecars_and_backup_have_private_user_ownership(tmp_path):
    root = tmp_path / "crashed-state"
    LocalRuntime.open(root)
    script = """
import os, sys
from pathlib import Path
from research_swarm.runtime import LocalRuntime
runtime = LocalRuntime.open(Path(sys.argv[1]))
with runtime.database.connect() as session:
    session.execute('CREATE TABLE owner_probe(value TEXT)')
    session.execute("INSERT INTO owner_probe VALUES ('committed')")
    session.execute('BEGIN IMMEDIATE')
    session.execute("INSERT INTO owner_probe VALUES ('uncommitted')")
    os._exit(73)
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(root)], capture_output=True, text=True, timeout=20
    )
    assert completed.returncode == 73, completed.stderr
    for name in ("research.sqlite", "research.sqlite-wal", "research.sqlite-shm"):
        assert (root / name).is_file()
        with privacy.verified_path(root / name, directory=False, protected=False):
            pass
    reopened = LocalRuntime.open(root)
    with reopened.database.connect() as session:
        assert [row[0] for row in session.execute("SELECT value FROM owner_probe")] == ["committed"]
    destination = root / "backup.sqlite"
    reopened.database.backup(destination)
    with privacy.verified_path(destination, directory=False, protected=False):
        pass
    backup = Database(destination)
    with backup.connect() as session:
        assert [row[0] for row in session.execute("SELECT value FROM owner_probe")] == ["committed"]
    destination.unlink()  # Both backup connections must already be closed.


def test_effective_identity_mismatch_is_rejected_before_creation(monkeypatch, tmp_path):
    # Exercise the identity predicate without provisioning another Windows account.
    before = token_snapshot(thread=True)
    monkeypatch.setattr(privacy, "current_user_sid", lambda: "S-1-0-0")
    with pytest.raises(ResearchError, match="effective identity"), privacy.sqlite_creation_owner():
        (tmp_path / "must-not-exist").touch()
    assert not (tmp_path / "must-not-exist").exists()
    assert token_snapshot(thread=True) == before


def test_identification_token_is_rejected_and_exact_prior_identity_retained():
    privacy.current_user_sid.cache_clear()
    api, source, restricted = privacy._api(), privacy.HANDLE(), privacy.HANDLE()
    before = token_snapshot(thread=True)
    # The normal native test process must not already be impersonating another caller.
    assert before is None
    assert api["OpenProcessToken"](api["GetCurrentProcess"](), 0x000A, ctypes.byref(source))
    try:
        assert api["DuplicateTokenEx"](source, 0x000E, None, 1, 2, ctypes.byref(restricted))
        assert api["SetThreadToken"](None, restricted)
        try:
            low_level = token_snapshot(thread=True)
            with (
                pytest.raises(ResearchError, match="impersonation or delegation"),
                privacy.sqlite_creation_owner(),
            ):
                pytest.fail("Identification-only token was admitted")
            assert token_snapshot(thread=True) == low_level
        finally:
            if not api["SetThreadToken"](None, None):
                os._exit(124)
    finally:
        if restricted.value:
            assert api["CloseHandle"](restricted)
        assert api["CloseHandle"](source)
    assert token_snapshot(thread=True) == before


def test_owner_installation_failure_never_enters_sqlite_scope(monkeypatch):
    api, before = privacy._api(), token_snapshot(thread=True)
    original, calls = api["SetThreadToken"], []

    def fail_install(thread, selected):
        calls.append(bool(selected))
        if len(calls) == 1:
            ctypes.set_last_error(5)
            return False
        return original(thread, selected)

    monkeypatch.setitem(api, "SetThreadToken", fail_install)
    with pytest.raises(OSError), privacy.sqlite_creation_owner():
        pytest.fail("Failed installation entered SQLite scope")
    assert len(calls) == 2
    assert token_snapshot(thread=True) == before


def test_failed_restore_terminates_only_the_owned_test_process(tmp_path):
    escaped = tmp_path / "escaped"
    script = """
import ctypes, sys
from pathlib import Path
from research_swarm.state import windows_privacy as privacy
api = privacy._api()
original = api['SetThreadToken']
calls = 0
def fail_restore(thread, selected):
    global calls
    calls += 1
    if calls == 2:
        ctypes.set_last_error(5)
        return False
    return original(thread, selected)
api['SetThreadToken'] = fail_restore
try:
    with privacy.sqlite_creation_owner():
        pass
except BaseException:
    Path(sys.argv[1]).touch()
Path(sys.argv[1]).touch()
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(escaped)], capture_output=True, text=True, timeout=20
    )
    assert completed.returncode == 125, completed.stderr
    assert not escaped.exists()


def test_existing_broad_sidecar_remains_rejected_with_filename_diagnostic(tmp_path):
    root = tmp_path / "unsafe-sidecar"
    runtime = LocalRuntime.open(root)
    with runtime.database.connect() as session:
        session.execute("SELECT COUNT(*) FROM records").one()
        sidecar = root / "research.sqlite-wal"
        assert sidecar.exists()
        subprocess.run(
            ["icacls", str(sidecar), "/grant", "*S-1-1-0:(R)"], check=True, capture_output=True
        )
        with pytest.raises(ResearchError, match="another account") as failure:
            LocalRuntime.open(root)
        assert failure.value.details["state_file"] == sidecar.name
        observed = subprocess.run(["icacls", str(sidecar)], check=True, capture_output=True)
        assert b"(R)" in observed.stdout
