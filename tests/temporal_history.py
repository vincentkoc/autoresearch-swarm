"""Frozen provider histories; capture is an explicit, non-overwriting maintainer task."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import re
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from importlib.metadata import version
from pathlib import Path
from typing import Any

from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from research_swarm.orchestration.temporal.runtime import DATA_CONVERTER
from research_swarm.orchestration.temporal.workflow import CampaignWorkflow

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "temporal" / "delivery-v1.json"
IDENTITY = "research-history-fixture"
MAX_HISTORY_BYTES = 512 * 1024
MAX_BUNDLE_BYTES = 2 * 1024 * 1024
SCENARIOS = {
    "command-retry": (
        "test_real_activity_retry_recovers_one_saved_http_command_without_replanning[False]"
    ),
    "continued-wait": "test_real_wait_survives_worker_restart_signals_and_continue_as_new",
    "transferred-checkpoint": (
        "test_real_temporal_successor_recovers_checkpoint_and_preserves_paused_campaign"
        "[lost_creation_reply]"
    ),
}


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def check_privacy(value: Any) -> None:
    """Inspect provider metadata and decoded JSON payloads without rewriting history."""
    if isinstance(value, str):
        if re.search(r"/(?:Users|home)/|[A-Za-z]:\\Users\\|op://|rsw_[A-Za-z0-9_-]{43}", value):
            raise ValueError("History contains private path or credential material")
        if str(Path.home()) in value or socket.gethostname() in value:
            raise ValueError("History contains local machine identity")
    elif isinstance(value, list):
        for item in value:
            check_privacy(item)
    elif isinstance(value, dict):
        if "identity" in value and value["identity"] != IDENTITY:
            raise ValueError("History was not generated with the fixed public identity")
        if "metadata" in value and "data" in value:
            metadata = value["metadata"]
            if metadata.get("encoding") != base64.b64encode(b"json/plain").decode():
                raise ValueError("Uninspected provider payload encoding")
            check_privacy(json.loads(base64.b64decode(value["data"], validate=True)))
        for key, item in value.items():
            check_privacy(key)
            check_privacy(item)


def load_fixture(path: Path = FIXTURE) -> dict[str, Any]:
    with path.open("rb") as stream:
        raw = stream.read(MAX_BUNDLE_BYTES + 1)
    if len(raw) > MAX_BUNDLE_BYTES:
        raise ValueError("History bundle exceeds its byte bound")
    return validate_bundle(json.loads(raw))


def validate_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    if bundle["schema_version"] != 1 or bundle["workflow_type"] != "research.campaign-delivery-v1":
        raise ValueError("Unknown history fixture contract")
    if not re.fullmatch(r"[0-9a-f]{40}", bundle["source_revision"]):
        raise ValueError("History source revision is absent")
    if not re.fullmatch(r"[0-9a-f]{64}", bundle["workflow_source_sha256"]):
        raise ValueError("History workflow source digest is absent")
    if set(bundle["scenarios"]) != set(SCENARIOS):
        raise ValueError("History scenarios are incomplete")
    check_privacy(bundle)
    for name, scenario in bundle["scenarios"].items():
        if scenario["source_test"] != SCENARIOS[name] or not 1 <= len(scenario["runs"]) <= 8:
            raise ValueError("History scenario identity or run bound differs")
        seen: set[str] = set()
        first_run = expected = scenario["runs"][0]["run_id"]
        previous = ""
        for run in scenario["runs"]:
            data = encoded(run["history"])
            if (
                len(data) > MAX_HISTORY_BYTES
                or len(data) != run["size_bytes"]
                or hashlib.sha256(data).hexdigest() != run["sha256"]
                or run["run_id"] != expected
                or run["run_id"] in seen
            ):
                raise ValueError("History bytes or continuation chain differs")
            seen.add(run["run_id"])
            history = WorkflowHistory.from_json(scenario["workflow_id"], run["history"])
            if not 1 <= len(history.events) <= 1024 or history.run_id != run["run_id"]:
                raise ValueError("History event bound or run identity differs")
            started = history.events[0].workflow_execution_started_event_attributes
            if (
                started.first_execution_run_id != first_run
                or started.continued_execution_run_id != previous
                or started.workflow_id != scenario["workflow_id"]
                or started.workflow_type.name != bundle["workflow_type"]
            ):
                raise ValueError("History workflow identity or predecessor differs")
            previous = run["run_id"]
            if [event.event_id for event in history.events] != list(
                range(1, len(history.events) + 1)
            ):
                raise ValueError("History event sequence is incomplete")
            last = history.events[-1]
            if last.HasField("workflow_execution_continued_as_new_event_attributes"):
                expected = (
                    last.workflow_execution_continued_as_new_event_attributes.new_execution_run_id
                )
            elif last.HasField("workflow_execution_completed_event_attributes"):
                expected = ""
            else:
                raise ValueError("History is not closed")
        if expected:
            raise ValueError("History continuation is missing")
    return bundle


def require_source(revision: str) -> None:
    current = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if current != revision:
        raise ValueError("Source revision changed during capture")
    subprocess.run(
        ["git", "diff", "--exit-code", revision, "--", "src", "tests/test_temporal.py"],
        cwd=ROOT,
        check=True,
    )
    if subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "--", "src"], cwd=ROOT
    ):
        raise ValueError("Untracked runtime source cannot be captured")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", type=Path, required=True, help="Existing qualified Temporal CLI")
    parser.add_argument(
        "--output", type=Path, required=True, help="New fixture path; never overwritten"
    )
    args = parser.parse_args()
    if args.output.exists() or not args.cli.is_file():
        parser.error("Use an existing CLI and a new output path")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    require_source(revision)
    workflow_digest = hashlib.sha256(
        (ROOT / "src/research_swarm/orchestration/temporal/workflow.py").read_bytes()
    ).hexdigest()
    cli_version = subprocess.check_output(
        [str(args.cli.resolve()), "--version"], text=True, timeout=10
    ).strip()
    captured: dict[str, Any] = {}

    import pytest
    from temporalio.testing import WorkflowEnvironment

    class Capture:
        scenario = ""

        def pytest_runtest_setup(self, item: Any) -> None:
            self.scenario = next(
                name for name, test in SCENARIOS.items() if item.nodeid.endswith("::" + test)
            )

        async def replay_chain(self, env: Any, handle: Any) -> list[str]:
            runs, texts, seen = [], [], set()
            run_id = handle.first_execution_run_id
            async with asyncio.timeout(15):
                with ThreadPoolExecutor(max_workers=2) as executor:
                    replayer = Replayer(
                        workflows=[CampaignWorkflow],
                        data_converter=DATA_CONVERTER,
                        workflow_task_executor=executor,
                    )
                    while run_id:
                        if len(runs) >= 8 or run_id in seen:
                            raise ValueError("Capture exceeds bounded continuation chain")
                        seen.add(run_id)
                        events = []
                        iterator = env.client.get_workflow_handle(
                            handle.id, run_id=run_id
                        ).fetch_history_events(page_size=128, rpc_timeout=timedelta(seconds=3))
                        async for event in iterator:
                            events.append(event)
                            if len(events) > 1024:
                                raise ValueError("Capture exceeds event bound")
                        history = WorkflowHistory(handle.id, events)
                        data = history.to_json_dict()
                        check_privacy(data)
                        raw = encoded(data)
                        if len(raw) > MAX_HISTORY_BYTES:
                            raise ValueError("Capture exceeds history byte bound")
                        await replayer.replay_workflow(history)
                        runs.append(
                            {
                                "run_id": run_id,
                                "size_bytes": len(raw),
                                "sha256": hashlib.sha256(raw).hexdigest(),
                                "history": data,
                            }
                        )
                        texts.append(history.to_json())
                        run_id = events[
                            -1
                        ].workflow_execution_continued_as_new_event_attributes.new_execution_run_id
            captured[self.scenario] = {
                "source_test": SCENARIOS[self.scenario],
                "workflow_id": handle.id,
                "runs": runs,
            }
            return texts

    capture = Capture()
    original_start = WorkflowEnvironment.start_local

    async def start_local(**kwargs: Any) -> Any:
        kwargs.update(
            identity=IDENTITY,
            ip="127.0.0.1",
            dev_server_existing_path=str(args.cli.resolve()),
            dev_server_startup_timeout=timedelta(seconds=10),
        )
        return await original_start(**kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("RESEARCH_TEMPORAL_TESTS", "1")
        from tests import test_temporal

        patch.setattr(WorkflowEnvironment, "start_local", start_local)
        patch.setattr(test_temporal, "replay_chain", capture.replay_chain)
        result = pytest.main(
            [
                "-q",
                "--tb=short",
                *["tests/test_temporal.py::" + test for test in SCENARIOS.values()],
            ],
            plugins=[capture],
        )
    if result:
        return int(result)
    bundle = {
        "schema_version": 1,
        "workflow_type": "research.campaign-delivery-v1",
        "source_revision": revision,
        "workflow_source_sha256": workflow_digest,
        "sdk_version": version("temporalio"),
        "cli_version": cli_version,
        "identity": IDENTITY,
        "scenarios": captured,
    }
    raw = (json.dumps(bundle, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    if len(raw) > MAX_BUNDLE_BYTES or set(captured) != set(SCENARIOS):
        raise ValueError("Capture is incomplete or exceeds the bundle bound")
    validate_bundle(bundle)
    require_source(revision)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as stream:
        stream.write(raw)
    print(f"Captured {len(captured)} scenarios; {len(raw)} bytes; source {revision}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
