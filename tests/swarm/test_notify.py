from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from research_swarm.cli import main
from research_swarm.swarm import notify
from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.notify import poll
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec, trial


def test_repeated_failures_are_journaled_once_per_streak(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "queue.sqlite"
    queue = SQLiteStore(database)
    now = 1000.0
    monkeypatch.setattr("research_swarm.swarm.store.time", SimpleNamespace(time=lambda: now))
    queue.create_campaign(spec(max_attempts=1))
    queue.create_campaign(spec(campaign_id="other"))
    zone = ZoneInfo("Asia/Kathmandu")

    def check() -> list[dict]:
        return poll(database, "test", zone, now=now)

    def finish(n: int, status: str, campaign: str = "test") -> None:
        queue.submit(trial(n, campaign_id=campaign))
        attempt = queue.claim(campaign, "worker")
        assert attempt and queue.report(Result(attempt=attempt, status=status))

    assert [msg["kind"] for msg in check()] == ["hourly_digest"]
    finish(1, "failed")
    assert check() == []
    finish(1, "ok", campaign="other")
    queue.submit(trial(2))
    stale = queue.claim("test", "stale")
    assert stale
    now += 2
    queue.submit(trial(3))
    healthy = queue.claim("test", "healthy")
    assert healthy and queue.requeue_expired("test", now - 1) == 1
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as reader:
        version = reader.execute("PRAGMA data_version").fetchone()
        with ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(check) for _ in range(2)]
            alerts = [item for future in futures for item in future.result()]
        assert reader.execute("PRAGMA data_version").fetchone() == version
        (alert,) = alerts
        assert alert["kind"] == "repeated_failure" and alert["consecutive_failures"] == 2
        assert alert["attempt_id"] == stale.attempt_id and alert["trial_id"] == stale.trial.trial_id
        assert alert["status"] == "lost" and alert["failure_class"] == "heartbeat_expired"
        assert (
            alert["event_id"]
            == reader.execute(
                "SELECT id FROM swarm_events WHERE kind='finished' AND attempt_id=?",
                (stale.attempt_id,),
            ).fetchone()[0]
        )
        assert check() == []
    finish(4, "failed")
    assert check() == []
    assert queue.report(Result(attempt=healthy, status="ok", metrics={"loss": 1}))
    finish(5, "lost")
    assert check() == []
    finish(6, "failed")
    (second,) = check()
    assert second["trial_id"] == "test-6" and second["event_id"] > alert["event_id"]
    assert check() == []
    with closing(sqlite3.connect(str(database) + ".notifications.sqlite")) as journal:
        assert journal.execute("SELECT count(*) FROM notifications").fetchone() == (3,)


async def test_stalled_hourly_agent_reports_timeout_while_alert_and_healthy_work_finish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "queue.sqlite"
    queue = SQLiteStore(database)
    queue.create_campaign(spec(max_attempts=1))
    launched, alerted, digested = asyncio.Event(), asyncio.Event(), asyncio.Event()
    children = []
    start = asyncio.create_subprocess_exec

    async def stall(*args, **kwargs):
        process = await start(sys.executable, "-c", "import time; time.sleep(60)", **kwargs)
        children.append(process)
        launched.set()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", stall)
    monkeypatch.setattr(notify, "_POLL_SECONDS", 0.02)
    monkeypatch.setattr(notify, "time", SimpleNamespace(time=lambda: 1000))
    messages = []

    def emit(item: dict) -> None:
        messages.append(item)
        (alerted if item["kind"] == "repeated_failure" else digested).set()

    args = argparse.Namespace(
        campaign="test",
        once=False,
        score_target=None,
        min_trials_per_hour=None,
        reporter_model=None,
        fake_reporter=True,
        reporter_timeout=1.5,
    )
    watcher = asyncio.create_task(notify._watch(database, args, ZoneInfo("UTC"), emit))
    try:
        await asyncio.wait_for(launched.wait(), 3)
        (beat,) = tmp_path.glob("*.agent-jobs/reporter-*/heartbeat.json")
        heartbeat = beat.stat().st_mtime_ns
        for i, status in enumerate(("failed", "lost", "ok"), start=1):
            queue.submit(trial(i))
            attempt = queue.claim("test", "working")
            assert attempt and queue.report(Result(attempt=attempt, status=status))
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as reader:
            version = reader.execute("PRAGMA data_version").fetchone()
            await asyncio.wait_for(alerted.wait(), 3)
            assert not digested.is_set() and children[0].returncode is None
            assert beat.stat().st_mtime_ns == heartbeat
            await asyncio.wait_for(digested.wait(), 4)
            alert, digest = messages
            assert alert["trial_id"] == "test-2" and digest["reporter"]["status"] == "timed_out"
            assert children[0].returncode is not None
            assert reader.execute(
                "SELECT status FROM swarm_trials WHERE trial_id='test-3'"
            ).fetchone() == ("ok",)
            assert reader.execute("PRAGMA data_version").fetchone() == version
    finally:
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)


@pytest.mark.parametrize("direction,target", [("minimize", 1.0), ("maximize", 3.0)])
def test_target_uses_recorded_selections_including_older_than_report_window(
    tmp_path: Path,
    direction: str,
    target: float,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "queue.sqlite"
    queue = SQLiteStore(database)
    queue.create_campaign(spec(direction=direction))
    queue.submit(trial(1))
    attempt = queue.claim("test", "local")
    assert attempt
    queue.report(Result(attempt=attempt, status="ok", metrics={"loss": target}, cost=1))
    args = [
        "swarm",
        "notify",
        "test",
        "--db",
        str(database),
        "--score-target",
        str(target),
        "--once",
    ]
    assert main(args) == 0
    digest = json.loads(capsys.readouterr().out)
    assert digest["kind"] == "hourly_digest" and digest["selected_score"] is None
    assert digest["completed_trials_last_hour"] == 1
    queue.select("test", "test-1", score=None)
    queue.select("test", "test-1", score=target)
    for n in range(1, 55):
        queue.select("test", "test-1", score=2, replicates=n)
    assert main(args) == 0
    alert = json.loads(capsys.readouterr().out)
    assert (alert["kind"], alert["score"], alert["trial_id"]) == ("score_target", target, "test-1")
    assert main(args) == 0
    assert capsys.readouterr().out == ""
    with closing(sqlite3.connect(database)) as db:
        assert db.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone() == (
            6,
        )


@pytest.mark.parametrize(
    "zone_name,first_at,second_at",
    [
        ("America/New_York", "2024-11-03T01:15:00-04:00", "2024-11-03T01:15:00-05:00"),
        ("Australia/Lord_Howe", "2024-04-07T01:45:00+11:00", "2024-04-07T01:45:00+10:30"),
    ],
)
def test_concurrent_watchers_restart_and_dst_fold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    zone_name: str,
    first_at: str,
    second_at: str,
) -> None:
    database = tmp_path / "queue.sqlite"
    first = datetime.fromisoformat(first_at).timestamp()
    second = datetime.fromisoformat(second_at).timestamp()
    monkeypatch.setattr("research_swarm.swarm.store.time.time", lambda: first - 7200)
    SQLiteStore(database).create_campaign(spec())
    zone = ZoneInfo(zone_name)

    def check(now: float) -> list[dict]:
        return poll(database, "test", zone, now=now, min_trials=1)

    with ThreadPoolExecutor(2) as pool:
        records = [item for batch in pool.map(check, [first, first]) for item in batch]
    assert [r["kind"] for r in records] == ["hourly_digest", "low_trial_rate"]
    assert records[0]["at"] == first_at
    assert check(first + 600) == []
    repeated = check(second)
    assert [item["kind"] for item in repeated] == ["hourly_digest", "low_trial_rate"]
    assert repeated[0]["at"] == second_at
    assert check(second + 600) == []
    with closing(sqlite3.connect(str(database) + ".notifications.sqlite")) as journal:
        assert journal.execute("SELECT count(*) FROM notifications").fetchone() == (4,)


def test_restart_in_spring_forward_partial_hour(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    before_jump = "2024-10-06T01:59:59+10:30"
    after_jump = "2024-10-06T02:30:00+11:00"
    next_hour = "2024-10-06T03:00:00+11:00"
    now = datetime.fromisoformat(before_jump).timestamp()
    clock = SimpleNamespace(time=lambda: now)
    monkeypatch.setattr(notify, "time", clock)
    monkeypatch.setattr("research_swarm.swarm.store.time", clock)
    database = tmp_path / "queue.sqlite"
    SQLiteStore(database).create_campaign(spec())
    parser = argparse.ArgumentParser()
    notify.add_arguments(parser)
    args = parser.parse_args(["test", "--timezone", "Australia/Lord_Howe", "--once"])

    delivered: list[dict] = []
    for at, expected_total in (
        (before_jump, 1),
        (after_jump, 2),
        ("2024-10-06T02:59:59+11:00", 2),
        (next_hour, 3),
    ):
        now = datetime.fromisoformat(at).timestamp()
        notify.watch(database, args, delivered.append)
        assert len(delivered) == expected_total

    assert [(item["kind"], item["at"]) for item in delivered] == [
        ("hourly_digest", before_jump),
        ("hourly_digest", after_jump),
        ("hourly_digest", next_hour),
    ]
    with closing(sqlite3.connect(str(database) + ".notifications.sqlite")) as journal:
        rows = journal.execute("SELECT payload FROM notifications ORDER BY rowid")
        assert [json.loads(payload) for (payload,) in rows] == delivered


def test_restart_honors_legacy_digest_in_second_partial_hour(tmp_path: Path) -> None:
    database = tmp_path / "queue.sqlite"
    SQLiteStore(database).create_campaign(spec())
    zone = ZoneInfo("Australia/Lord_Howe")
    when = datetime.fromisoformat("2024-04-07T01:45:00+10:30").astimezone(zone)
    (digest,) = poll(database, "test", zone, now=when.timestamp())
    # The earlier release used the first hour boundary even for a poll in the later fold.
    old_key = f"digest:{int(when.replace(minute=0).timestamp())}"
    with closing(sqlite3.connect(str(database) + ".notifications.sqlite")) as journal, journal:
        journal.execute(
            "UPDATE notifications SET key=?,payload=? WHERE key=?",
            (old_key, json.dumps({**digest, "key": old_key}), digest["key"]),
        )
    assert poll(database, "test", zone, now=when.timestamp() + 60) == []


@pytest.mark.parametrize("target,resume_target,score", [("2", "2", 1.5), ("-0", "0", 0.0)])
def test_restart_recovers_alert_after_stdout_pipe_breaks(
    tmp_path: Path, target: str, resume_target: str, score: float
) -> None:
    database = tmp_path / "queue.sqlite"
    queue = SQLiteStore(database)
    queue.create_campaign(spec())
    queue.submit(trial(1))
    attempt = queue.claim("test", "local")
    assert attempt and queue.report(Result(attempt=attempt, status="ok", metrics={"loss": score}))
    queue.select("test", "test-1", score=score)
    cmd = [
        sys.executable,
        "-u",
        "-m",
        "research_swarm",
        "swarm",
        "notify",
        "test",
        "--db",
        str(database),
        "--once",
        "--score-target",
    ]
    with closing(sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True)) as campaign:
        version = campaign.execute("PRAGMA data_version").fetchone()
        (event_id,) = campaign.execute(
            "SELECT id FROM swarm_events WHERE kind='selected'"
        ).fetchone()
        read_fd, write_fd = os.pipe()
        os.close(read_fd)  # The first write must fail, regardless of scheduling.
        with os.fdopen(write_fd, "wb") as pipe:
            failed = subprocess.run([*cmd, target], stdout=pipe, stderr=subprocess.PIPE, timeout=15)
        assert failed.returncode != 0 and b"BrokenPipeError" in failed.stderr

        journal_uri = (tmp_path / "queue.sqlite.notifications.sqlite").as_uri() + "?mode=ro"
        with closing(sqlite3.connect(journal_uri, uri=True)) as journal:
            query = "SELECT payload FROM notifications WHERE campaign='test' ORDER BY rowid"
            original = [json.loads(row[0]) for row in journal.execute(query)]
            digest, alert = original
            assert (digest["kind"], digest["selected_score"]) == ("hourly_digest", score)
            assert (alert["kind"], alert["trial_id"], alert["score"], alert["event_id"]) == (
                "score_target",
                "test-1",
                score,
                event_id,
            )

            restarted = subprocess.run(
                [*cmd, resume_target], capture_output=True, check=True, timeout=15
            )
            emitted = [json.loads(line) for line in restarted.stdout.splitlines()]
            recovered = [json.loads(row[0]) for row in journal.execute(query)]
            # A restart across the clock hour may legitimately emit a new digest.
            assert all(
                item["kind"] == "hourly_digest" and item["key"] != digest["key"] for item in emitted
            )
            assert recovered == original + emitted
        assert campaign.execute("PRAGMA data_version").fetchone() == version


def test_invalid_input_and_warmup(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    database = tmp_path / "queue.sqlite"
    SQLiteStore(database).create_campaign(spec())
    args = ["swarm", "notify", "test", "--db", str(database), "--once"]
    for invalid in (
        ["--timezone", "invalid/zone"],
        ["--score-target", "nan"],
        ["--min-trials-per-hour", "0"],
    ):
        assert main(args + invalid) == 2
    assert main(args + ["--min-trials-per-hour", "10"]) == 0
    assert json.loads(capsys.readouterr().out)["kind"] == "hourly_digest"


@pytest.mark.parametrize("timeout,status", [("10", "ok"), ("0.001", "timed_out")])
def test_hourly_reporter_preserves_numeric_digest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], timeout: str, status: str
) -> None:
    db = tmp_path / "queue.sqlite"
    SQLiteStore(db).create_campaign(spec())
    args = [
        "swarm",
        "notify",
        "test",
        "--db",
        str(db),
        "--once",
        "--fake-reporter",
        "--reporter-timeout",
        timeout,
    ]
    assert main(args) == 0
    item = json.loads(capsys.readouterr().out)
    assert item["completed_trials_last_hour"] == 0 and item["reporter"]["status"] == status
    with closing(sqlite3.connect(str(db) + ".notifications.sqlite")) as out:
        assert json.loads(out.execute("SELECT payload FROM notifications").fetchone()[0]) == item
    assert main(args) == 0 and capsys.readouterr().out == ""
