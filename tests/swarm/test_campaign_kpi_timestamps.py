from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.reader import CampaignReader
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec, trial


@pytest.mark.parametrize(
    ("created_at", "launches", "captured_at", "expected_latency", "expected_trials"),
    [
        pytest.param(None, (100,), 100, None, 1, id="missing-creation"),
        pytest.param(100, (100,), 100, 0, 1, id="zero-elapsed-with-completion"),
        pytest.param(100, (7305, 7300), 11000, 7200, 0, id="late-launches-out-of-order"),
    ],
)
def test_campaign_timing_kpis(
    tmp_path: Path,
    created_at: int | None,
    launches: tuple[int, ...],
    captured_at: int,
    expected_latency: int | None,
    expected_trials: int,
) -> None:
    database = tmp_path / "queue.sqlite"
    queue = SQLiteStore(database)
    campaign = spec()
    # A retained queue can have a campaign row without its creation event.
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("INSERT INTO swarm_projects(project_id) VALUES (?)", (campaign.project_id,))
        db.execute(
            "INSERT INTO swarm_campaigns(campaign_id,project_id,spec) VALUES (?,?,?)",
            (campaign.campaign_id, campaign.project_id, json.dumps(asdict(campaign))),
        )
        if created_at is not None:
            db.execute(
                "INSERT INTO swarm_events(campaign_id,kind,at) VALUES (?,'created',?)",
                (campaign.campaign_id, created_at),
            )

    for index, launched_at in enumerate(launches, start=1):
        with patch("research_swarm.swarm.store.time.time", return_value=launched_at):
            queue.submit(trial(index))
            attempt = queue.claim(campaign.campaign_id, f"fixture-{index}")
            assert attempt is not None
            queue.workload_started(attempt.attempt_id)
            assert queue.report(Result(attempt=attempt, status="ok"))

    with patch("research_swarm.swarm.report.datetime", wraps=datetime) as clock:
        clock.now.return_value = datetime.fromtimestamp(captured_at, UTC)
        kpis = CampaignReader(database).snapshot(campaign.campaign_id)["kpis"]

    assert kpis["time_to_first_trial"]["value"] == expected_latency
    assert kpis["trials_per_hour"]["value"] == expected_trials
