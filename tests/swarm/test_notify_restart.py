"""Crash a generated watcher at the journal boundary while an independent CPU worker runs."""

from __future__ import annotations

import asyncio
import json
import signal
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from research_swarm.swarm.command import campaign_backend
from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.spec import load_spec
from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import WorkerLoop
from tests.swarm.test_store import trial

# Exercise the CLI in a fresh interpreter. Only the fixture controls the poll clock
# and crash point. SQLite transactions and stdout writes use the real implementation.
_WATCH = """
import os, signal, sqlite3, sys
from pathlib import Path
from types import SimpleNamespace
from research_swarm import cli
from research_swarm.swarm import notify

root, phase, boundary = Path(sys.argv[1]), sys.argv[2], float(sys.argv[3])
first_poll = True
def clock():
    global first_poll
    if phase == 'restart':
        return boundary
    if first_poll:
        first_poll = False
        return boundary - 1
    if sys.stdin.readline() != 'cross\\n':
        raise RuntimeError('missing boundary barrier')
    return boundary
notify.time = SimpleNamespace(time=clock)
notify._POLL_SECONDS = 0.01

connect = sqlite3.connect
def journal_connect(database, *args, **kwargs):
    connection = connect(database, *args, **kwargs)
    if phase == 'before_commit' and str(database).endswith('.notifications.sqlite'):
        def trace(sql):
            if sql.strip().upper() == 'COMMIT' and (root / 'warm').exists():
                os.kill(os.getpid(), signal.SIGKILL)
        connection.set_trace_callback(trace)
    return connection
sqlite3.connect = journal_connect

emit = cli._emit_json
def deliver(item):
    if phase == 'before_stdout' and item['kind'] == 'repeated_failure':
        os.kill(os.getpid(), signal.SIGKILL)
    if phase == 'after_alert' and item['kind'] == 'hourly_digest' and (root / 'warm').exists():
        os.kill(os.getpid(), signal.SIGKILL)
    emit(item)
    if item['kind'] == 'hourly_digest':
        (root / 'warm').touch()
cli._emit_json = deliver
args = ['swarm', 'notify', 'test', '--db', str(root / 'queue.sqlite'),
        '--timezone', 'Asia/Singapore']
sys.exit(cli.main(args + (['--once'] if phase == 'restart' else [])))
"""

_COORDINATOR = """
import os, signal, sys
from pathlib import Path
from research_swarm import cli
from research_swarm.swarm.store import SQLiteStore
root, mode = Path(sys.argv[1]), sys.argv[2]
depth = SQLiteStore.queue_depth
def observe(self, campaign):
    result = depth(self, campaign)
    if result[1]:
        if mode == 'crash':
            os.kill(os.getpid(), signal.SIGKILL)
        (root / 'coordinator-running').touch()
    return result
SQLiteStore.queue_depth = observe
sys.exit(cli.main(['swarm','run',str(root / 'campaign.yaml'),'--db',str(root / 'queue.sqlite')]))
"""

_WORKLOAD = """
import json, os, sys, time
from pathlib import Path
root = Path(sys.argv[1])
(root / 'healthy-running').touch()
deadline = time.monotonic() + 30
while not (root / 'finish-healthy').exists():
    if time.monotonic() > deadline:
        raise RuntimeError('fixture was not released')
    time.sleep(0.02)
Path(os.environ['SWARM_METRICS']).write_text(json.dumps({'loss': 1}))
"""


def _wait(path: Path) -> None:
    deadline = time.monotonic() + 10
    while not path.exists():
        if time.monotonic() > deadline:
            pytest.fail(f"Timed out waiting for {path.name}")
        time.sleep(0.02)


def _journal(path: Path) -> tuple[list[dict], tuple[int, int]]:
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        rows = db.execute("SELECT payload FROM notifications WHERE campaign='test' ORDER BY rowid")
        records = [json.loads(row[0]) for row in rows]
        cursor = db.execute(
            "SELECT last_event,consecutive FROM failure_cursor WHERE campaign='test'"
        ).fetchone()
        return records, cursor


@pytest.mark.parametrize("phase", ["before_commit", "before_stdout", "after_alert"])
def test_observed_hour_boundary_and_open_incident_survive_process_restart(
    tmp_path: Path, phase: str
) -> None:
    database = tmp_path / "queue.sqlite"
    journal = tmp_path / "queue.sqlite.notifications.sqlite"
    sink = tmp_path / "stdout.jsonl"
    campaign_file = tmp_path / "campaign.yaml"
    campaign_file.write_text(
        json.dumps(
            {
                "campaign_id": "test",
                "metric": "loss",
                "direction": "minimize",
                "command": [sys.executable, "-c", _WORKLOAD, str(tmp_path)],
                "task_dir": str(tmp_path),
                "evaluation_mode": "smoke",
                "max_workers": 2,
                "max_attempts": 1,
                "max_trials": 3,
                "candidates": [{"candidate_id": "seed"}],
            }
        )
    )
    spec = load_spec(campaign_file)
    store = SQLiteStore(database)
    store.create_campaign(spec)
    for n in (1, 2, 3):
        store.submit(trial(n))
    first = store.claim("test", "fixture")
    assert first and store.report(Result(attempt=first, status="failed"))
    second = store.claim("test", "fixture")
    assert second
    worker = WorkerLoop(store, campaign_backend(spec), "healthy")
    zone = ZoneInfo("Asia/Singapore")
    boundary = datetime.now(zone).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    watch = [sys.executable, "-c", _WATCH, str(tmp_path)]
    coordinator = [sys.executable, "-c", _COORDINATOR, str(tmp_path)]

    with (
        ThreadPoolExecutor(1) as pool,
        ExitStack() as cleanup,
        sink.open("ab", buffering=0) as stream,
    ):
        healthy = pool.submit(asyncio.run, worker.run_once(spec))
        cleanup.callback((tmp_path / "finish-healthy").touch)
        _wait(tmp_path / "healthy-running")
        crashed = subprocess.run([*coordinator, "crash"], capture_output=True, timeout=10)
        assert crashed.returncode == -signal.SIGKILL, crashed.stderr
        resumed = subprocess.Popen(
            [*coordinator, "resume"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        cleanup.callback(resumed.wait)
        cleanup.callback(resumed.kill)
        _wait(tmp_path / "coordinator-running")
        watcher = subprocess.Popen(
            [*watch, phase, str(boundary.timestamp())],
            stdin=subprocess.PIPE,
            stdout=stream,
            stderr=subprocess.PIPE,
        )
        cleanup.callback(watcher.wait)
        cleanup.callback(watcher.kill)
        _wait(tmp_path / "warm")
        warm, first_cursor = _journal(journal)
        assert len(warm) == 1 and warm[0]["kind"] == "hourly_digest" and first_cursor[1] == 1
        assert store.report(Result(attempt=second, status="lost"))
        _, err = watcher.communicate(b"cross\n", timeout=10)
        assert watcher.returncode == -signal.SIGKILL, err
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as queue:
            threshold = queue.execute(
                "SELECT id FROM swarm_events WHERE kind='finished' AND attempt_id=?",
                (second.attempt_id,),
            ).fetchone()[0]
            assert queue.execute(
                "SELECT status FROM swarm_trials WHERE trial_id='test-3'"
            ).fetchone() == ("running",)
        recovered, cursor = _journal(journal)
        if phase == "before_commit":
            assert (recovered, cursor) == (warm, first_cursor)
        else:
            assert len(recovered) == 3 and cursor == (threshold, 2)
        delivered_before_restart = [json.loads(s) for s in sink.read_bytes().splitlines()]
        assert delivered_before_restart == (recovered[:2] if phase == "after_alert" else warm)

        def restart_watch() -> None:
            subprocess.run(
                [*watch, "restart", str(boundary.timestamp())],
                stdout=stream,
                stderr=subprocess.PIPE,
                check=True,
                timeout=10,
            )

        restart_watch()
        rows, cursor = _journal(journal)
        prior_digest, alert, digest = rows
        assert prior_digest == warm[0]
        assert prior_digest["at"] == (boundary - timedelta(seconds=1)).isoformat()
        assert (digest["kind"], digest["at"]) == ("hourly_digest", boundary.isoformat())
        assert digest["key"] != prior_digest["key"]
        assert (alert["kind"], alert["event_id"], alert["attempt_id"]) == (
            "repeated_failure",
            threshold,
            second.attempt_id,
        )
        assert alert["at"] == boundary.isoformat() and cursor == (threshold, 2)
        if phase != "before_commit":
            assert rows == recovered
        delivered = [json.loads(s) for s in sink.read_bytes().splitlines()]
        assert delivered == (rows if phase == "before_commit" else delivered_before_restart)

        (tmp_path / "finish-healthy").touch()
        assert healthy.result(timeout=10)
        stdout, stderr = resumed.communicate(timeout=10)
        assert resumed.returncode == 0, stderr
        summary = json.loads(stdout)
        assert summary["attempts"] == summary["trials"] == 3
        assert summary["selected"]["result"]["attempt"]["worker_id"] == "healthy"
        restart_watch()
        assert _journal(journal)[0] == rows and _journal(journal)[1][1] == 0
        assert [json.loads(s) for s in sink.read_bytes().splitlines()] == delivered
