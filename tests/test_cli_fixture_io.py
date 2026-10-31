"""Fixture diagnostics must not block the process being observed."""

import errno
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tests.cli_process import PipeTail, read_acknowledgment


def test_stderr_drain_prevents_pipe_backpressure_and_bounds_retention():
    process = subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stderr.buffer.write(b'x'*262144+b'last-line\\n')"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    assert process.stderr is not None
    captured = PipeTail(process.stderr)
    process.stderr = None
    try:
        assert process.wait(timeout=5) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        captured.close()
    assert captured.tail().endswith(b"last-line\n")
    assert len(captured.tail()) <= 65536


@pytest.mark.parametrize("winerror", [5, 32, 33, None])
def test_only_windows_sharing_denials_can_be_retried(monkeypatch, tmp_path, winerror):
    failure = PermissionError("Synthetic publication sharing denial")
    if winerror is not None:
        failure.winerror = winerror

    def denied(*args, **kwargs):
        raise failure

    monkeypatch.setattr(Path, "read_text", denied)
    if winerror is None:
        with pytest.raises(PermissionError) as observed:
            read_acknowledgment(tmp_path / "driver.ready")
        assert observed.value is failure
    else:
        assert read_acknowledgment(tmp_path / "driver.ready") is None


def test_missing_readiness_is_retryable_but_invalid_json_is_not(tmp_path):
    ready = tmp_path / "driver.ready"
    assert read_acknowledgment(ready) is None
    ready.write_text("{", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        read_acknowledgment(ready)
    ready.write_text("null", encoding="utf-8")
    with pytest.raises(ValueError, match="identity object"):
        read_acknowledgment(ready)
    identity = {"nonce": "public-fixture", "pid": 1, "created_at_hex": "0x1.0p+0"}
    ready.write_text(json.dumps(identity), encoding="utf-8")
    assert read_acknowledgment(ready) == identity


@pytest.mark.parametrize("platform", ["win32", "darwin"])
def test_errno_only_access_denial_is_retryable_only_on_windows(monkeypatch, tmp_path, platform):
    failure = PermissionError(errno.EACCES, "Synthetic CRT publication denial")

    def denied(*args, **kwargs):
        raise failure

    monkeypatch.setattr(Path, "read_text", denied)
    monkeypatch.setattr(sys, "platform", platform)
    if platform == "win32":
        assert read_acknowledgment(tmp_path / "driver.ready") is None
    else:
        with pytest.raises(PermissionError) as observed:
            read_acknowledgment(tmp_path / "driver.ready")
        assert observed.value is failure
