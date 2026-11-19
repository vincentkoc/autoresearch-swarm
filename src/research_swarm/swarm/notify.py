"""Observe a local campaign and journal hourly digests and metric alerts."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from research_swarm.swarm.report import _read, _usage

_POLL_SECONDS = 10


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("campaign")
    parser.add_argument("--db", type=Path, help="Local swarm campaign database")
    parser.add_argument("--timezone", default="UTC", help="IANA timezone for hourly digests")
    parser.add_argument(
        "--score-target", type=float, help="Alert at the first selected target score"
    )
    parser.add_argument("--min-trials-per-hour", type=int, help="Alert on low hourly completions")
    reporter = parser.add_mutually_exclusive_group()
    reporter.add_argument("--reporter-model", help="Summarize each hourly digest with local Ollama")
    reporter.add_argument("--fake-reporter", action="store_true", help="Use the offline reporter")
    parser.add_argument(
        "--reporter-timeout", type=float, default=10, help="Reporter deadline in seconds"
    )
    parser.add_argument("--once", action="store_true", help="Poll once and exit")


def _repeated_failures(
    queue: sqlite3.Connection, journal: sqlite3.Connection, scope: tuple[str, float]
) -> list[sqlite3.Row]:
    """Advance in finished-event order within the same transaction as alert creation."""
    journal.execute(
        "CREATE TABLE IF NOT EXISTS failure_cursor "
        "(campaign TEXT, created REAL, last_event INTEGER, consecutive INTEGER, "
        "PRIMARY KEY(campaign,created))"
    )
    state = journal.execute(
        "SELECT last_event,consecutive FROM failure_cursor WHERE campaign=? AND created=?", scope
    ).fetchone()
    last_event, consecutive = state or (0, 0)
    alerts = []
    for event in queue.execute(
        "SELECT e.id,e.trial_id,e.attempt_id,json_extract(a.observation,'$.status') AS status, "
        "json_extract(a.observation,'$.failure_class') AS failure_class "
        "FROM swarm_events e JOIN swarm_attempts a USING(attempt_id) "
        "WHERE e.campaign_id=? AND e.kind='finished' AND e.id>? ORDER BY e.id",
        (scope[0], last_event),
    ):
        last_event = event["id"]
        consecutive = consecutive + 1 if event["status"] in ("failed", "lost") else 0
        if consecutive == 2:
            alerts.append(event)
    if last_event and (state is None or state[0] != last_event):
        journal.execute(
            "INSERT INTO failure_cursor VALUES(?,?,?,?) ON CONFLICT(campaign,created) DO UPDATE "
            "SET last_event=excluded.last_event, consecutive=excluded.consecutive",
            (*scope, last_event, consecutive),
        )
    return alerts


def poll(
    database: Path,
    campaign: str,
    zone: ZoneInfo,
    *,
    now: float,
    target: float | None = None,
    min_trials: int | None = None,
) -> list[dict[str, Any]]:
    if target is not None and not math.isfinite(target):
        raise ValueError("Score target must be finite")
    if min_trials is not None and min_trials <= 0:
        raise ValueError("Minimum trials per hour must be positive")
    database = database.resolve()
    when = datetime.fromtimestamp(now, zone)
    # Subtract wall-clock minutes using the observed offset. A DST rollback can
    # repeat part of an hour without repeating its 00-minute boundary.
    hour = int(when.replace(microsecond=0).timestamp()) - when.minute * 60 - when.second
    legacy_hour = int(when.replace(minute=0, second=0, microsecond=0).timestamp())
    records: list[dict[str, Any]] = []
    with _read(database) as queue:
        row = queue.execute(
            "SELECT c.spec, e.at FROM swarm_campaigns c JOIN swarm_events e USING(campaign_id) "
            "WHERE c.campaign_id=? AND e.kind='created'",
            (campaign,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown campaign {campaign}")
        spec = json.loads(row["spec"])
        journal = database.with_name(database.name + ".notifications.sqlite")
        if journal.exists() and journal.samefile(database):
            raise ValueError("Notification journal must be separate from the campaign queue")
        os.close(os.open(journal, os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW, 0o600))
        with closing(sqlite3.connect(journal)) as out, out:
            out.execute("BEGIN IMMEDIATE")
            out.execute(
                "CREATE TABLE IF NOT EXISTS notifications "
                "(campaign TEXT, created REAL, key TEXT, payload TEXT, "
                "PRIMARY KEY(campaign,created,key))"
            )
            scope = (campaign, row["at"])

            def hourly_key(kind: str, suffix: str = "") -> str:
                key = f"{kind}:{hour}{suffix}"
                if hour == legacy_hour:
                    return key
                old_key = f"{kind}:{legacy_hour}{suffix}"
                prior = out.execute(
                    "SELECT payload FROM notifications WHERE campaign=? AND created=? AND key=?",
                    (*scope, old_key),
                ).fetchone()
                if prior:
                    previous = datetime.fromisoformat(json.loads(prior[0])["at"]).astimezone(zone)
                    if (previous.date(), previous.hour, previous.utcoffset()) == (
                        when.date(),
                        when.hour,
                        when.utcoffset(),
                    ):
                        return old_key
                return key

            def missing(key: str) -> bool:
                return (
                    out.execute(
                        "SELECT 1 FROM notifications WHERE campaign=? AND created=? AND key=?",
                        (*scope, key),
                    ).fetchone()
                    is None
                )

            def record(key: str, kind: str, **fields: Any) -> None:
                item = dict(
                    kind=kind,
                    campaign=campaign,
                    at=when.isoformat(),
                    campaign_created_at=row["at"],
                    key=key,
                    **fields,
                )
                out.execute(
                    "INSERT INTO notifications VALUES (?,?,?,?)",
                    (*scope, key, json.dumps(item, allow_nan=False)),
                )
                records.append(item)

            # Emit incident alerts before an optional hourly reporter can delay delivery.
            for failure in _repeated_failures(queue, out, scope):
                record(
                    f"repeated_failure:{failure['id']}",
                    "repeated_failure",
                    event_id=failure["id"],
                    trial_id=failure["trial_id"],
                    attempt_id=failure["attempt_id"],
                    status=failure["status"],
                    failure_class=failure["failure_class"],
                    consecutive_failures=2,
                )

            digest_key = hourly_key("digest")
            rate_key = hourly_key("low_rate", f":{min_trials}")
            due = missing(digest_key)
            check_rate = min_trials is not None and now - row["at"] >= 3600 and missing(rate_key)
            if due or check_rate:
                completed = queue.execute(
                    "SELECT count(*) FROM swarm_events e JOIN swarm_attempts a USING(attempt_id) "
                    "WHERE e.campaign_id=? AND e.kind='finished' AND e.at>? AND e.at<=? "
                    "AND (json_extract(a.observation,'$.status')='ok' OR a.number>=?)",
                    (campaign, now - 3600, now, spec["max_attempts"]),
                ).fetchone()[0]
                if due:
                    selected = queue.execute(
                        "SELECT * FROM swarm_events WHERE campaign_id=? AND kind='selected' "
                        "ORDER BY id DESC LIMIT 1",
                        (campaign,),
                    ).fetchone()
                    record(
                        digest_key,
                        "hourly_digest",
                        metric=spec["metric"],
                        selected_score=dict(selected).get("score") if selected else None,
                        completed_trials_last_hour=completed,
                        meter=_usage(queue, campaign, spec),
                    )
                if check_rate and min_trials is not None and completed < min_trials:
                    record(
                        rate_key,
                        "low_trial_rate",
                        completed_trials_last_hour=completed,
                        minimum=min_trials,
                    )
            if (
                target is not None
                and not out.execute(
                    "SELECT 1 FROM notifications WHERE campaign=? AND created=? "
                    "AND key GLOB 'score_target:*' AND json_extract(payload,'$.target')=? LIMIT 1",
                    (*scope, target),
                ).fetchone()
            ):
                compare = "<=" if spec["direction"] == "minimize" else ">="
                columns = {r["name"] for r in queue.execute("PRAGMA table_info(swarm_events)")}
                if "score" in columns:
                    hit = queue.execute(
                        f"SELECT * FROM swarm_events WHERE campaign_id=? AND kind='selected' "
                        f"AND score {compare} ? ORDER BY id LIMIT 1",
                        (campaign, target),
                    ).fetchone()
                    if hit is not None:
                        record(
                            f"score_target:{target}",
                            "score_target",
                            metric=spec["metric"],
                            target=target,
                            score=hit["score"],
                            trial_id=hit["trial_id"],
                            event_id=hit["id"],
                            selected_at=datetime.fromtimestamp(hit["at"], zone).isoformat(),
                        )
    return records


def watch(database: Path, args: argparse.Namespace, emit: Callable[[Any], None]) -> None:
    database = database.resolve()
    if not math.isfinite(args.reporter_timeout) or args.reporter_timeout <= 0:
        raise ValueError("Reporter timeout must be positive and finite")
    if args.reporter_model is not None and not args.reporter_model.strip():
        raise ValueError("Reporter model name must be nonempty")
    try:
        zone = ZoneInfo(args.timezone)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"Unknown timezone {args.timezone}") from error
    asyncio.run(_watch(database, args, zone, emit))


async def _watch(
    database: Path, args: argparse.Namespace, zone: ZoneInfo, emit: Callable[[Any], None]
) -> None:
    from research_swarm.swarm.agents import ask_agent

    async def report(item: dict[str, Any]) -> None:
        item["reporter"] = await ask_agent(
            "reporter",
            {**item, "purpose": "hourly digest"},
            model=args.reporter_model,
            timeout=args.reporter_timeout,
            jobs=database.with_name(database.name + ".agent-jobs"),
        )
        journal = database.with_name(database.name + ".notifications.sqlite")
        with closing(sqlite3.connect(journal)) as out, out:
            out.execute(
                "UPDATE notifications SET payload=? WHERE campaign=? AND created=? AND key=?",
                (
                    json.dumps(item, allow_nan=False),
                    item["campaign"],
                    item["campaign_created_at"],
                    item["key"],
                ),
            )
        emit(item)

    reporters: set[asyncio.Task[None]] = set()
    try:
        while True:
            for item in poll(
                database,
                args.campaign,
                zone,
                now=time.time(),
                target=args.score_target,
                min_trials=args.min_trials_per_hour,
            ):
                if item["kind"] == "hourly_digest" and (args.reporter_model or args.fake_reporter):
                    reporters.add(asyncio.create_task(report(item)))
                else:
                    emit(item)
            if args.once:
                await asyncio.gather(*reporters)
                return
            await asyncio.sleep(_POLL_SECONDS)
            done = {task for task in reporters if task.done()}
            for task in done:
                task.result()
            reporters -= done
    finally:
        for task in reporters:
            task.cancel()
        await asyncio.gather(*reporters, return_exceptions=True)
