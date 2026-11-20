"""Recover an interrupted scheduler transaction through the campaign and result APIs."""

import asyncio
import os
import signal
import sqlite3
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import psutil
import pytest
from pydantic import TypeAdapter

from research_swarm.swarm import store as store_module
from research_swarm.swarm.interfaces import Candidate, Result
from research_swarm.swarm.loop import SwarmLoop, candidate_key
from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import WorkerLoop
from tests.benchmarks.test_panels import measure, study
from tests.swarm.test_loop import Backend, campaign

CANDIDATE = Candidate(candidate_id="baseline")
SPEC = campaign(evaluation_mode="calibrated", max_trials=1, max_workers=1, candidates=(CANDIDATE,))


class _LargeReportBackend(Backend):
    async def execute(self, spec, attempt):
        result = await super().execute(spec, attempt)
        return replace(result, logs=("x" * (256 * 1024),))


def _campaign(root, backend=None):
    queue = SQLiteStore(root / "queue.sqlite")
    panels = study(root, measure, variants=(candidate_key(CANDIDATE), "one", "two"))
    backend = backend or Backend()
    worker = WorkerLoop(queue, backend, "fixture-worker")
    return queue, panels, SwarmLoop(SPEC, queue, backend, worker, panels=panels)


def _interrupt_scheduler(root, fault):
    queue, _, loop = _campaign(root, _LargeReportBackend() if fault == "disk_full" else Backend())
    event = store_module._event
    finish = queue._finish

    def interrupt_event(db, campaign_id, kind, *args, **kwargs):
        if kind == {"kill_claim": "claimed", "kill_report": "finished"}.get(fault):
            assert db.in_transaction
            assert db.execute("SELECT count(*) FROM swarm_attempts").fetchone()[0] == 1
            expected = "running" if kind == "claimed" else "ok"
            assert db.execute("SELECT status FROM swarm_trials").fetchone()[0] == expected
            assert not psutil.Process().children(recursive=True)
            print(kind, flush=True)
            os.kill(os.getpid(), signal.SIGKILL)
        return event(db, campaign_id, kind, *args, **kwargs)

    def capture_report(db, row, result, max_attempts):
        if fault == "disk_full":
            # Cap only this fixture connection. A real observation larger than the
            # queue file must allocate another page when the scheduler persists it.
            pages = db.execute("PRAGMA page_count").fetchone()[0]
            size = db.execute("PRAGMA page_size").fetchone()[0]
            assert len(result.logs[0]) > pages * size
            assert db.execute(f"PRAGMA max_page_count={pages}").fetchone()[0] == pages
        # Preserve the sender's exact request so it can retry a missing acknowledgment.
        print(TypeAdapter(Result).dump_json(result).decode(), flush=True)
        finish(db, row, result, max_attempts)

    with (
        patch.object(store_module, "_event", interrupt_event),
        patch.object(queue, "_finish", capture_report),
    ):
        try:
            asyncio.run(loop.run())
        except sqlite3.OperationalError as exc:
            assert fault == "disk_full" and exc.sqlite_errorcode == sqlite3.SQLITE_FULL
            assert not psutil.Process().children(recursive=True)
            print(exc.sqlite_errorname, flush=True)
            return
    raise AssertionError(f"scheduled fault {fault} did not occur")


@pytest.mark.parametrize("fault", ["kill_claim", "kill_report", "disk_full"])
def test_scheduler_transaction_recovery(tmp_path: Path, fault: str) -> None:
    if fault.startswith("kill") and os.name != "posix":
        pytest.skip("scheduler crash uses POSIX SIGKILL")
    scheduler = subprocess.run(
        [sys.executable, "-m", "tests.chaos.test_scheduler_transactions", str(tmp_path), fault],
        capture_output=True,
        text=True,
        timeout=25,
    )
    assert scheduler.returncode == (0 if fault == "disk_full" else -signal.SIGKILL), (
        scheduler.stderr
    )
    messages = scheduler.stdout.splitlines()
    assert (
        messages[-1]
        == {"kill_claim": "claimed", "kill_report": "finished", "disk_full": "SQLITE_FULL"}[fault]
    )

    queue, panels, loop = _campaign(tmp_path)
    looks = {"calibration_proxy": 6, "calibration_target": 6}
    assert queue.results(SPEC.campaign_id) == []
    if fault == "kill_claim":
        assert queue.queue_depth(SPEC.campaign_id) == (1, 0)
        assert panels.look_counts() == looks
    else:
        assert queue.queue_depth(SPEC.campaign_id) == (0, 1)
        assert panels.look_counts() == {**looks, "search": 1}
        request = TypeAdapter(Result).validate_json(messages[0])
        assert queue.report(request) and not queue.report(request)
        assert queue.results(SPEC.campaign_id) == [request]

    summary = asyncio.run(loop.run())
    assert (summary.trials, summary.attempts, summary.failed, summary.lost) == (1, 1, 0, 0)
    (result,) = queue.results(SPEC.campaign_id)
    assert result.status == "ok" and result.attempt.number == 1
    assert summary.selected and summary.selected.result == result
    assert not queue.report(result)
    assert queue.queue_depth(SPEC.campaign_id) == (0, 0)
    final_looks = {**looks, "search": 1, "confirmation": 1}
    assert panels.look_counts() == final_looks
    reopened, resumed_panels, resumed_loop = _campaign(tmp_path)
    assert asyncio.run(resumed_loop.run()) == summary
    assert reopened.results(SPEC.campaign_id) == [result]
    assert resumed_panels.look_counts() == final_looks


if __name__ == "__main__":
    _interrupt_scheduler(Path(sys.argv[1]), sys.argv[2])
