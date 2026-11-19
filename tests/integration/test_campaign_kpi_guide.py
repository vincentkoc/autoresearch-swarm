"""Exercise the operator's read-only commands on two campaigns in one queue."""

import json
import os
import re
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.store import SQLiteStore
from tests.paths import REPO_ROOT
from tests.swarm.test_store import spec, trial


def test_operator_inspects_only_the_requested_campaign(tmp_path: Path) -> None:
    campaign = "program-normalization"
    database = tmp_path / ".research" / "program.sqlite"
    database.parent.mkdir()
    queue = SQLiteStore(database)
    for name in (campaign, "another-campaign"):
        queue.create_campaign(spec(campaign_id=name, evaluation_mode="smoke", max_attempts=2))
        queue.submit(trial(1, campaign_id=name))
        first = queue.claim(name, "local")
        assert first
        if name == campaign:
            assert queue.report(Result(attempt=first, status="failed", error="fixture failure"))
            first = queue.claim(name, "retry")
            assert first
        assert queue.report(Result(attempt=first, status="ok", metrics={"loss": 2}))
        queue.select(name, first.trial.trial_id, score=2)

    guide = (REPO_ROOT / "docs/operator-guide.md").read_text()
    blocks = re.findall(r"^~~~sh\n(.*?)^~~~$", guide, re.M | re.S)

    def run_block(marker: str) -> str:
        (example,) = (block for block in blocks if marker in block)
        result = subprocess.run(
            ["sh", "-ec", example],
            cwd=tmp_path,
            env={
                **os.environ,
                "UV_PYTHON": sys.executable,
                "UV_OFFLINE": "1",
                "VIRTUAL_ENV": sys.prefix,
            },
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert result.returncode == 0, result.stderr
        return result.stdout

    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as db:
        version = db.execute("PRAGMA data_version").fetchone()
        rows = run_block("FROM swarm_trials")
        assert "another-campaign" not in rows
        assert f"{campaign} ok 1" in rows
        assert f"{campaign}-1 1 local failed fixture failure" in rows
        assert f"{campaign}-1 2 retry ok" in rows
        run_block("research report view")
        html = (database.parent / "program-kpis.html").read_text()
        assert "Campaign KPIs" in html and campaign in html and "another-campaign" not in html
        metrics = json.loads(run_block('snapshot["kpis"]'))
        section = guide.split("## Read campaign KPIs", 1)[1].split("\n## ", 1)[0]
        documented = set(re.findall(r"^\| `(\w+)` \|", section, re.M))
        assert len(documented) == 8 and documented == metrics.keys()
        assert all({"value", "unit", "detail"} <= metric.keys() for metric in metrics.values())
        assert metrics["trials_per_hour"]["value"] == 1
        assert metrics["failed_attempts_by_class"]["value"] == {"unclassified": 50}
        for key in documented - {"trials_per_hour", "failed_attempts_by_class"}:
            assert metrics[key]["value"] is None and metrics[key]["detail"]
        assert db.execute("PRAGMA data_version").fetchone() == version
