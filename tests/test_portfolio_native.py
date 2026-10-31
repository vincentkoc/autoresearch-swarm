"""Real registered CPU processes prove portfolio progress without a generation barrier."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
from collections.abc import Callable
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any

import psutil
import pytest

from research_swarm.api.http import create_app
from research_swarm.client import ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.control.search_models import RespondToProposal
from research_swarm.control.worker_lifecycle import SetWorkerAdmission
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.engines.registration import register_environment
from research_swarm.orchestration.evaluation import EvaluationDriver
from research_swarm.orchestration.scoped import ScopedWorkerDriver
from research_swarm.orchestration.search import SearchDriver
from research_swarm.search.portfolio import PortfolioDriverConfiguration
from research_swarm.workers.contracts import NativeIdentity
from research_swarm.workers.journal import JournalState
from research_swarm.workers.remote_enrollment import (
    connect_worker,
    describe_worker,
    provision_worker,
)
from tests.test_evaluation_driver import role
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_portfolio_control import portfolio_options


def require_payload_scope(native: NativeIdentity, payload: psutil.Process) -> None:
    if native.backend != "windows_job":
        assert native.pid == payload.pid
        return
    # A Windows venv redirector is the native root; Python may run in its child.
    root = psutil.Process(native.pid)
    root_created, payload_created = root.create_time(), payload.create_time()
    assert native.start_token == f"native-created:{root_created.hex()}"
    assert any(
        process.pid == root.pid and process.create_time() == root_created
        for process in (payload, *payload.parents())
    ), "Payload is not in the observed native root's process ancestry"
    assert root.is_running() and payload.is_running()
    assert root.create_time() == root_created and payload.create_time() == payload_created


def options() -> dict[str, Any]:
    result = portfolio_options(max_trials=4)
    result["source_train_timeout_ms"] = 120_000
    result["limits"] = tuple(
        BudgetLimit(dimension="execution_ms", limit=1_000_000, unit="ms")
        if item.dimension == "execution_ms"
        else item
        for item in result["limits"]
    )
    return result


@contextmanager
def registered_pair(pipeline: Pipeline, tmp_path: Path, url: str) -> Any:
    if (os.cpu_count() or 1) < 2:
        pytest.skip("Native overlap requires two observed CPU slots")
    registered = register_environment(
        tmp_path / "environment" / "engine.json",
        profile="python-cpu",
        executable=Path(sys.executable),
        environment_contracts=(pipeline.trial["data"]["spec"]["environment_digest"],),
        cpu_slots=2,
        probe_directory=tmp_path / "probe-state",
    )
    registry = tmp_path / "shared-machine" / "resources.sqlite"
    with ExitStack() as stack:
        connected = []
        for name in ("worker-a", "worker-b"):
            root = tmp_path / name
            description = describe_worker(
                root, cpu_slots=2, machine_registry=registry, registration=registered
            )
            bundle = root / "bundle.json"
            provision_worker(
                pipeline.client,
                description,
                url=url,
                project_ids=(pipeline.campaign["data"]["contract"]["project_id"],),
                destination=bundle,
                name=name,
            )
            connected.append(stack.enter_context(connect_worker(root, bundle_path=bundle)))
        assert connected[0].worker.resources.path == connected[1].worker.resources.path
        assert connected[0].worker.resources.availability() == {"cpu": 2}
        assert connected[0].bundle.worker.binding != connected[1].bundle.worker.binding
        yield tuple(connected)


def gated_source(
    source: Path, destination: Path, rate: float, marker: Path | None, release: Path | None
) -> Path:
    shutil.copytree(source, destination)
    train = destination / "train.py"
    text = train.read_text(encoding="utf-8")
    assert text.count("LEARNING_RATE = 0.0") == 1
    text = text.replace("LEARNING_RATE = 0.0", f"LEARNING_RATE = {rate}")
    if marker is not None and release is not None:
        # The workload itself records entry before its active training clock starts.
        gate = f"""
    import os
    marker = Path({str(marker)!r})
    pending = marker.with_suffix(".pending")
    pending.write_text(json.dumps({{
        "pid": os.getpid(), "attempt_id": request["attempt_id"],
        "trial_id": request["trial_id"], "seed": request["seed"],
    }}), encoding="utf-8")
    os.replace(pending, marker)
    release = Path({str(release)!r})
    deadline = time.monotonic() + 90
    while not release.exists():
        if time.monotonic() > deadline:
            raise RuntimeError("Native overlap fixture did not release its gate")
        time.sleep(0.05)
"""
        assert text.count("    request = context()\n") == 1
        text = text.replace("    request = context()\n", "    request = context()\n" + gate)
    train.write_text(text, encoding="utf-8")
    return destination


@pytest.mark.parametrize("pipeline", [options()], indirect=True)
async def test_two_registered_workers_refill_a_slot_while_original_native_scope_is_running(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    _, _, controller_token = role(pipeline, "*")
    evaluator, _, evaluator_token = role(pipeline, "read", "evaluation.publish")
    marker_a, marker_c = tmp_path / "native-a.json", tmp_path / "native-c.json"
    release_a, release_c = tmp_path / "release-a", tmp_path / "release-c"
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, controller_token) as controller,
        ResearchClient.connect(url, evaluator_token) as publisher,
        registered_pair(pipeline, tmp_path, url) as (worker_a, worker_b),
    ):
        search = SearchDriver(
            controller,
            pipeline.campaign["id"],
            PortfolioDriverConfiguration(baseline_trial_id=pipeline.trial["id"]),
        )
        evaluation = EvaluationDriver(controller, publisher, pipeline.campaign["id"], evaluator.id)
        # All compared trials use the same immutable registered execution fingerprint.
        assert (await ScopedWorkerDriver(worker_b).run(once=True)).observation_count == 1
        assert [item.operation for item in (await evaluation.advance_async()).actions] == [
            "evaluation.authorize"
        ]
        assert (await ScopedWorkerDriver(worker_b).run(once=True)).observation_count == 1
        assert [item.operation for item in (await evaluation.advance_async()).actions] == [
            "evaluation.complete"
        ]
        assert (await search.advance_async()).state == "awaiting_proposal"
        campaign_id = pipeline.campaign["id"]
        initial = controller.get("search_state", campaign_id)["data"]
        requests = [
            controller.get("proposal_request", identifier)
            for identifier in initial["pending_request_ids"]
        ]
        assert len(requests) == 2

        async def respond(
            request: dict[str, Any],
            label: str,
            rate: float,
            marker: Path | None = None,
            release: Path | None = None,
        ) -> None:
            source = gated_source(
                pipeline.source, tmp_path / f"candidate-{label}", rate, marker, release
            )
            artifact = await asyncio.to_thread(
                controller.capture_project, source, idempotency_key=f"capture-{label}"
            )
            actor = Principal(
                id=request["data"]["proposer_id"],
                workspace_id=pipeline.runtime.identity.workspace_id,
                scopes=frozenset({"proposal.respond"}),
            )
            credential = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), actor)
            with ResearchClient.connect(url, credential.token) as agent:
                await asyncio.to_thread(
                    agent.command,
                    "search.respond",
                    RespondToProposal(
                        request_id=request["id"],
                        outcome="candidate",
                        source_artifact=artifact,
                        hypothesis=f"Evaluate independent optimizer hypothesis {label}",
                    ),
                    idempotency_key=f"response-{label}",
                )

        latest_progress: dict[str, Any] = {}

        def diagnostics() -> str:
            def read(call: Callable[[], Any]) -> Any:
                try:
                    return call()
                except Exception as error:
                    return {
                        "error_type": type(error).__name__,
                        "error_code": error.code if isinstance(error, ResearchError) else None,
                    }

            records = {
                kind: read(
                    lambda kind=kind: controller.list(
                        RecordQuery(kind=kind, campaign_id=campaign_id, limit=12)
                    )["items"]
                )
                for kind in ("proposal_request", "policy_transition", "trial", "job", "attempt")
            }
            native = []
            attempts = records["attempt"] if isinstance(records["attempt"], list) else []
            for attempt in attempts:
                for connected in (worker_a, worker_b):
                    entry = read(
                        lambda connected=connected, attempt=attempt: connected.worker.journal.get(
                            attempt["id"]
                        )
                    )
                    if isinstance(entry, dict):
                        if entry["error_code"] == ErrorCode.NOT_FOUND:
                            continue
                        native.append({"attempt_id": attempt["id"], **entry})
                        continue
                    native.append(
                        {
                            "worker_id": connected.bundle.worker.binding.worker_id,
                            "attempt_id": attempt["id"],
                            "journal_state": entry.state,
                            "finding": entry.finding,
                            "reason": entry.outcome.reason if entry.outcome else None,
                            "exit_code": entry.outcome.exit_code if entry.outcome else None,
                            "closure": entry.outcome.closure.model_dump(mode="json")
                            if entry.outcome
                            else None,
                        }
                    )
            return json.dumps(
                {
                    "progress": latest_progress,
                    "search_state": read(lambda: controller.get("search_state", campaign_id)),
                    "budget": read(lambda: controller.budget(campaign_id)),
                    "records": records,
                    "native": native,
                    "workers": [
                        {
                            "worker_id": driver.worker.worker.binding.worker_id,
                            "admission": driver._admission,
                            "control_error": driver._control_error.code
                            if driver._control_error
                            else None,
                            "pump_failure": driver._pump_failure.code
                            if driver._pump_failure
                            else None,
                            "availability": read(driver.worker.resources.availability),
                        }
                        for driver in drivers
                    ],
                }
            )

        async def progress() -> None:
            scored = await evaluation.advance_async()
            latest_progress["evaluation"] = scored.model_dump(mode="json")
            if scored.state not in {"idle", "advanced"} or scored.findings or scored.continuation:
                pytest.fail(f"Evaluation could not progress: {diagnostics()}")
            result = await search.advance_async()
            latest_progress["search"] = result.model_dump(mode="json")
            assert result.state in {
                "awaiting_proposal",
                "awaiting_execution",
                "awaiting_evaluation",
                "budget_exhausted",
                "contended",
            }, f"Search could not progress: {diagnostics()}"

        async def until(
            predicate: Any, *, advance: bool = False, request_id: str | None = None
        ) -> Any:
            deadline = asyncio.get_running_loop().time() + 80
            while True:
                for task in tasks:
                    if task.done():
                        outcome = task.result()
                        pytest.fail(
                            f"Continuous worker exited before drain: {outcome}; {diagnostics()}"
                        )
                if advance:
                    await progress()
                value = predicate()
                if value:
                    return value
                if request_id is not None:
                    transitions = controller.list(
                        RecordQuery(kind="policy_transition", campaign_id=campaign_id, limit=12)
                    )["items"]
                    admission = next(
                        (row for row in transitions if row["data"].get("request_id") == request_id),
                        None,
                    )
                    if admission is not None:
                        assert admission["data"]["outcome"] == "admitted", (
                            f"Replacement proposal was not admitted: {diagnostics()}"
                        )
                        trial = controller.get("trial", admission["data"]["trial_id"])
                        assert trial["data"]["execution_outcome"] == "pending", (
                            f"Replacement trial finished before its marker: {diagnostics()}"
                        )
                    else:
                        assert latest_progress["search"]["state"] != "budget_exhausted", (
                            f"Replacement admission exhausted its budget: {diagnostics()}"
                        )
                assert asyncio.get_running_loop().time() < deadline, (
                    f"Native portfolio progress exceeded the fixture bound: {diagnostics()}"
                )
                await asyncio.sleep(0.05)

        def active_trials() -> list[dict[str, Any]]:
            return [
                controller.get("trial", identifier)
                for identifier in controller.get("search_state", campaign_id)["data"][
                    "active_trial_ids"
                ]
            ]

        drivers = [
            ScopedWorkerDriver(worker, poll_ms=50, maximum_backoff_ms=500)
            for worker in (worker_a, worker_b)
        ]
        tasks: list[asyncio.Task[Any]] = []
        try:
            await respond(requests[0], "a", 0.05, marker_a, release_a)
            assert (await search.advance_async()).state == "awaiting_execution"
            trial_a = active_trials()[0]
            tasks.append(asyncio.create_task(drivers[0].run()))
            await until(marker_a.exists)
            identity_a = json.loads(marker_a.read_text(encoding="utf-8"))
            entry_a = worker_a.worker.journal.get(identity_a["attempt_id"])
            process_a = psutil.Process(identity_a["pid"])
            assert entry_a.state == JournalState.RUNNING and entry_a.native is not None
            original_a_native = entry_a.native
            require_payload_scope(original_a_native, process_a)
            assert identity_a["trial_id"] == trial_a["id"]
            controller.command(
                "worker.set_admission",
                SetWorkerAdmission(
                    binding=worker_a.bundle.worker.binding,
                    admission="draining",
                    reason="Retain native A while the other worker fills available capacity",
                ),
                expected_revision=0,
                idempotency_key="drain-native-a",
            )

            await respond(requests[1], "b", 0.35)
            await search.advance_async()
            trial_b = next(item for item in active_trials() if item["id"] != trial_a["id"])
            tasks.append(asyncio.create_task(drivers[1].run()))

            def next_request() -> Any:
                state = controller.get("search_state", campaign_id)["data"]
                return (
                    controller.get("proposal_request", state["pending_request_ids"][0])
                    if state["pending_request_ids"]
                    else None
                )

            request_c = await until(next_request, advance=True)
            assert request_c["data"]["parent_candidate_id"] == trial_b["data"]["candidate_id"]
            assert request_c["data"]["incumbent_revision"] == 2
            await respond(request_c, "c", 0.20, marker_c, release_c)
            await until(marker_c.exists, advance=True, request_id=request_c["id"])
            identity_c = json.loads(marker_c.read_text(encoding="utf-8"))
            process_c = psutil.Process(identity_c["pid"])
            entry_c = worker_b.worker.journal.get(identity_c["attempt_id"])
            entry_a = worker_a.worker.journal.get(identity_a["attempt_id"])
            assert entry_a.state == entry_c.state == JournalState.RUNNING
            assert entry_a.native == original_a_native
            assert entry_a.outcome is None and entry_c.outcome is None
            assert entry_c.native is not None
            require_payload_scope(original_a_native, process_a)
            require_payload_scope(entry_c.native, process_c)
            assert process_a.is_running() and process_c.is_running()
            assert not release_a.exists() and not release_c.exists()
            assert worker_b.worker.resources.availability() == {"cpu": 0}
            state = controller.get("search_state", campaign_id)["data"]
            assert set(state["active_trial_ids"]) == {trial_a["id"], identity_c["trial_id"]}
            assert len(controller.list(RecordQuery(kind="observation_consumption"))["items"]) == 1
            assert (
                controller.get("campaign", campaign_id)["data"]["incumbent_candidate_id"]
                == trial_b["data"]["candidate_id"]
            )
            promotion = controller.list(
                RecordQuery(kind="promotion", parent_id=trial_b["data"]["candidate_id"])
            )["items"]
            assert len(promotion) == 1 and promotion[0]["state"] == "committed"
            assert promotion[0]["data"]["evidence_level"] == "observed"
            evaluated_b = controller.get("trial", trial_b["id"])
            assert evaluated_b["data"]["resource_status"] == "closed"
            assert evaluated_b["data"]["evaluation_status"] == "valid"
            eval_job = controller.get("job", trial_b["data"]["job_ids"][1])
            authorization = controller.get(
                "evaluation_request", eval_job["data"]["evaluation_request_id"]
            )
            receipt = controller.get("evaluation_receipt", authorization["data"]["receipt_id"])
            assert receipt["data"]["receipt"]["evaluator_id"] == evaluator.id
            assert evaluator.id not in {
                worker_a.bundle.worker.principal_id,
                worker_b.bundle.worker.principal_id,
            }
            trial_c = controller.get("trial", identity_c["trial_id"])
            candidates = [
                controller.get("candidate", trial["data"]["candidate_id"])
                for trial in (trial_a, trial_b, trial_c)
            ]
            assert len({item["id"] for item in candidates}) == 3
            assert len({item["data"]["source"]["tree_digest"] for item in candidates}) == 3
            for trial, expected_worker in (
                (trial_a, worker_a),
                (trial_b, worker_b),
                (trial_c, worker_b),
            ):
                attempts = controller.list(
                    RecordQuery(kind="attempt", parent_id=trial["data"]["job_ids"][0])
                )["items"]
                assert len(attempts) == 1
                assert (
                    attempts[0]["data"]["worker_id"]
                    == expected_worker.bundle.worker.binding.worker_id
                )
            release_a.touch()
            release_c.touch()
            await until(
                lambda: not controller.get("search_state", campaign_id)["data"]["active_trial_ids"],
                advance=True,
            )
            assert len(controller.list(RecordQuery(kind="observation_consumption"))["items"]) == 3
            assert len(controller.list(RecordQuery(kind="attempt"))["items"]) == 8
            assert worker_b.worker.resources.availability() == {"cpu": 2}
            for worker, identifier in (
                (worker_a, identity_a["attempt_id"]),
                (worker_b, identity_c["attempt_id"]),
            ):
                entry = worker.worker.journal.get(identifier)
                assert entry.state == JournalState.CLOSED and entry.outcome is not None
                assert entry.outcome.closure.scope_closed
                assert (
                    controller.get("attempt", identifier)["data"]["publication_status"]
                    == "accepted"
                )
        finally:
            release_a.touch(exist_ok=True)
            release_c.touch(exist_ok=True)
            for running in drivers:
                running.stop.set()
            outcomes = await asyncio.gather(*tasks, return_exceptions=True)
            for outcome in outcomes:
                assert not isinstance(outcome, BaseException), outcome
