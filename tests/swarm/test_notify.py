from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from research_swarm.cli import main
from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.notify import poll
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec, trial


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


def test_concurrent_watchers_restart_and_dst_fold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "queue.sqlite"
    first = datetime(2024, 11, 3, 5, 15, tzinfo=UTC).timestamp()
    monkeypatch.setattr("research_swarm.swarm.store.time.time", lambda: first - 7200)
    SQLiteStore(database).create_campaign(spec())
    zone = ZoneInfo("America/New_York")

    def check(now: float) -> list[dict]:
        return poll(database, "test", zone, now=now, min_trials=1)

    with ThreadPoolExecutor(2) as pool:
        records = [item for batch in pool.map(check, [first, first]) for item in batch]
    assert [r["kind"] for r in records] == ["hourly_digest", "low_trial_rate"]
    assert records[0]["at"].endswith("01:15:00-04:00")
    assert check(first + 600) == []
    assert check(first + 3600)[0]["at"].endswith("01:15:00-05:00")
    with closing(sqlite3.connect(str(database) + ".notifications.sqlite")) as journal:
        assert journal.execute("SELECT count(*) FROM notifications").fetchone() == (4,)


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
