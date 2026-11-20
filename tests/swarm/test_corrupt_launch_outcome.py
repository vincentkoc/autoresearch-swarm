from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_swarm.swarm import _process_guard, agent_guard


@pytest.mark.parametrize(
    "primary",
    [
        b'{"role":"reporter","deadline":1700000110.0',
        b"\xff",
        b"null",
        b'{"role":"diagnoser"}',
        b'{"role":"unknown","deadline":1700000110}',
        b'{"role":"reporter","deadline":true}',
        b'{"role":"reporter","deadline":1e999}',
    ],
)
def test_expiry_records_unknown_launch_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, primary: bytes
) -> None:
    job = tmp_path / "job.json"
    heartbeat = tmp_path / "heartbeat.json"
    outcome = tmp_path / "outcome.json"
    job.write_bytes(primary)
    heartbeat.write_text('{"role":"proposer","deadline":1700003710}')
    expected = {
        "role": None,
        "deadline": None,
        "status": "timed_out",
        "error": "invalid_launch_metadata",
    }

    def observe_expiry(*_: object) -> None:
        assert json.loads(outcome.read_bytes()) == expected

    def refuse_dispatch(*args: object, **kwargs: object) -> None:
        pytest.fail("expired job dispatched a provider")

    monkeypatch.setattr(_process_guard, "time", SimpleNamespace(monotonic=lambda: 110.0))
    monkeypatch.setattr(
        _process_guard,
        "os",
        SimpleNamespace(
            getpid=lambda: 900001, getsid=lambda _: 900001, killpg=observe_expiry, link=os.link
        ),
    )
    monkeypatch.setattr(_process_guard.subprocess, "Popen", refuse_dispatch)
    agent_guard.supervise(heartbeat, 110.0)

    # A second publication by the caller must preserve the first record.
    published = outcome.stat()
    agent_guard.record_outcome(heartbeat, "timed_out")
    assert outcome.stat().st_ino == published.st_ino
    assert json.loads(outcome.read_bytes()) == expected
    assert job.read_bytes() == primary


@pytest.mark.parametrize(
    ("legacy_heartbeat", "diagnostic"),
    [
        (None, "missing_launch_metadata"),
        (b'{"role":"reporter","deadline":1700000110.0', "invalid_launch_metadata"),
    ],
)
def test_legacy_timeout_without_usable_metadata(
    tmp_path: Path, legacy_heartbeat: bytes | None, diagnostic: str
) -> None:
    heartbeat = tmp_path / "heartbeat.json"
    outcome = tmp_path / "outcome.json"
    if legacy_heartbeat is not None:
        heartbeat.write_bytes(legacy_heartbeat)

    with pytest.raises(FileNotFoundError if legacy_heartbeat is None else json.JSONDecodeError):
        agent_guard.record_outcome(heartbeat, "ok")
    assert not outcome.exists()

    agent_guard.record_outcome(heartbeat, "timed_out")

    assert json.loads(outcome.read_bytes()) == {
        "role": None,
        "deadline": None,
        "status": "timed_out",
        "error": diagnostic,
    }
