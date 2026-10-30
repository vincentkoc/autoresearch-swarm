"""Bounded controller measurements and native CPU throughput with replayable cells."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import math
import os
import platform
import sqlite3
import statistics
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from threading import Barrier
from time import perf_counter_ns

from filelock import FileLock
from pydantic import Field, model_validator

import research_swarm
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import SubmitTrial
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import ReportCampaign
from research_swarm.control.worker_lifecycle import WorkerHeartbeat
from research_swarm.control.workers import EngineCapabilities, EnrollWorker, WorkerData
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.canonical import (
    canonical_bytes,
    content_digest,
    record_digest,
    strict_json_loads,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import VersionedRecord, new_id
from research_swarm.orchestration.bootstrap import RunOptions, prepare_run
from research_swarm.orchestration.intents import CommandIntents
from research_swarm.orchestration.local import LocalDriver
from research_swarm.runtime import (
    LocalRuntime,
    private_state_directory,
    read_private,
    write_private,
)
from research_swarm.state.database import Database, Transaction
from research_swarm.state.ledger import BudgetLedger
from research_swarm.workers.local_enrollment import ensure_local_enrollment

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / "cpu_bigram"


def workload_digest():
    return record_digest(
        "research.controller-benchmark-workload",
        {
            path.name: content_digest(path.read_bytes())
            for path in sorted(SOURCE.iterdir())
            if path.is_file()
        },
    )


class Definition(VersionedRecord):
    connections: tuple[int, ...] = (1, 2, 4, 8)
    commands: int = Field(default=64, strict=True, ge=8, le=128)
    trials: int = Field(default=8, strict=True, ge=2, le=16)
    repeats: int = Field(default=3, strict=True, ge=1, le=5)

    @model_validator(mode="after")
    def bounded_connections(self):
        if not self.connections or tuple(sorted(set(self.connections))) != self.connections:
            raise ValueError("Connection counts must be unique and increasing")
        if any(type(value) is not int or value not in (1, 2, 4, 8) for value in self.connections):
            raise ValueError("Supported connection counts are 1, 2, 4, 8")
        return self

    def cells(self):
        return (
            [(f"traffic-{count}", "traffic", count, 0) for count in self.connections]
            + [(f"budget-{count}", "budget", count, 0) for count in self.connections if count > 1]
            + [("restart-replay", "recovery", 1, 0)]
            + [
                (f"cpu-{repeat}-{lanes}", "cpu", lanes, repeat)
                for repeat in range(self.repeats)
                for lanes in ((1, 2) if repeat % 2 == 0 else (2, 1))
            ]
        )


def latency(samples):
    values = sorted(item["elapsed_ns"] / 1_000_000 for item in samples)
    return {
        "count": len(values),
        "failures": sum(not item["success"] for item in samples),
        **{
            f"p{percent}_ms": values[max(0, math.ceil(len(values) * percent / 100) - 1)]
            if values
            else None
            for percent in (50, 95, 99)
        },
    }


def measured(operation, function):
    started = perf_counter_ns()
    try:
        value = function()
        sample = {"operation": operation, "success": True}
    except Exception as error:
        value = None
        sample = {
            "operation": operation,
            "success": False,
            "error": error.code.value if isinstance(error, ResearchError) else type(error).__name__,
        }
    return value, {**sample, "elapsed_ns": perf_counter_ns() - started}


def enroll(client, ordinal):
    request = EnrollWorker(
        worker_id=new_id(),
        session_id=new_id(),
        principal_id=client.transport.resolve_principal().id,
        name=f"Synthetic control connection {ordinal}",
        max_concurrency=1,
        profiles=(
            EngineCapabilities(
                name="python-cpu",
                backend="python",
                cpu_slots=1,
                execution_fingerprint=content_digest(b"synthetic-control-only"),
            ),
        ),
    )
    return WorkerData.model_validate(
        client.command("worker.enroll", request, idempotency_key=new_id())["result"]["data"]
    )


def traffic(root, count, commands):
    clients = [ResearchClient.local(root) for _ in range(count)]
    workers = [enroll(client, ordinal) for ordinal, client in enumerate(clients)]
    barrier = Barrier(count + 1)

    def connection(index):
        client, worker = clients[index], workers[index]
        samples, instance = [], new_id()
        barrier.wait(timeout=30)
        for sequence in range(1, commands + 1):
            request = WorkerHeartbeat(
                binding=worker.binding, driver_instance_id=instance, sequence=sequence
            )
            key = new_id()

            def call(request=request, key=key):
                return client.command("worker.heartbeat", request, idempotency_key=key)

            result, sample = measured("heartbeat", call)
            samples.append(sample)
            if sequence % 4 == 0:
                _, sample = measured(
                    "query", lambda: client.get("worker_presence", worker.binding.worker_id)
                )
                samples.append(sample)
            if sequence % 8 == 0:
                replay, sample = measured("replay", call)
                sample["success"] = sample["success"] and replay == result
                samples.append(sample)
        final = client.get("worker_presence", worker.binding.worker_id)["data"][
            "heartbeat_sequence"
        ]
        return {"connection": index, "final_sequence": final, "samples": samples}

    with ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(connection, index) for index in range(count)]
        started = perf_counter_ns()
        barrier.wait(timeout=30)
        results = [future.result() for future in futures]
        elapsed = perf_counter_ns() - started
    samples = [sample for result in results for sample in result["samples"]]
    summaries = {
        operation: latency([sample for sample in samples if sample["operation"] == operation])
        for operation in ("heartbeat", "query", "replay")
    }
    return {
        "connections": count,
        "elapsed_ns": elapsed,
        "connections_evidence": results,
        "latency": summaries,
        "accepted_commands_per_second": sum(
            sample["success"] and sample["operation"] == "heartbeat" for sample in samples
        )
        * 1e9
        / elapsed,
        "valid": all(result["final_sequence"] == commands for result in results)
        and all(sample["success"] for sample in samples),
        "scope": "in-process API with independent SQLite connections; synthetic control traffic",
    }


def budget_race(root, count):
    runtime = LocalRuntime.open(root)
    actor, campaign_id = runtime.principal(), new_id()

    def context():
        return CommandContext(principal=actor, idempotency_key=new_id())

    def initialize(transaction):
        transaction.insert(
            "campaign",
            campaign_id,
            {"synthetic_ledger_benchmark": True},
            state="running",
            campaign_id=campaign_id,
        )
        BudgetLedger.create(
            transaction, campaign_id, (BudgetLimit(dimension="worker_ms", limit=10, unit="ms"),)
        )
        return {}

    runtime.database.execute(context(), "benchmark.initialize", {}, "write", initialize)
    databases = [Database(runtime.database.path) for _ in range(count)]
    barrier = Barrier(count)

    def reserve(database):
        owner = new_id()
        barrier.wait(timeout=30)
        return measured(
            "reserve",
            lambda: database.execute(
                context(),
                "benchmark.reserve",
                {"owner": owner},
                "write",
                lambda transaction: BudgetLedger.reserve(
                    transaction, campaign_id, owner, {"worker_ms": Decimal(8)}
                ).model_dump(mode="json"),
            ),
        )

    with ThreadPoolExecutor(max_workers=count) as pool:
        attempts = list(pool.map(reserve, databases))
    winners = [result["result"] for result, _ in attempts if result is not None]
    settlement_id = new_id()
    if len(winners) == 1:

        def settle(transaction):
            return BudgetLedger.settle(
                transaction, winners[0]["id"], settlement_id, {"worker_ms": Decimal(3)}
            )

        first = runtime.database.execute(context(), "benchmark.settle", {}, "write", settle)
        replay = runtime.database.execute(context(), "benchmark.settle", {}, "write", settle)
        assert first["result"] == replay["result"]
    account = runtime.database.store.read(
        lambda connection: BudgetLedger.account(
            Transaction(connection, context()),
            campaign_id,
            "worker_ms",
        ).model_dump(mode="json")
    )
    samples = [sample for _, sample in attempts]
    return {
        "connections": count,
        "winners": len(winners),
        "samples": samples,
        "latency": latency(samples),
        "account": account,
        "valid": len(winners) == 1
        and all(
            sample["success"] or sample.get("error") == "BUDGET_EXHAUSTED" for sample in samples
        )
        and Decimal(account["reserved"]) == 0
        and Decimal(account["consumed"]) == 3,
        "scope": "native ledger transactions, not public command throughput",
    }


def restart_replay(root):
    client = ResearchClient.local(root)
    worker = enroll(client, 0)
    request = WorkerHeartbeat(binding=worker.binding, driver_instance_id=new_id(), sequence=1)
    key = new_id()
    accepted = client.command("worker.heartbeat", request, idempotency_key=key)
    client.close()  # Discard the acknowledgment at the caller boundary.
    started = perf_counter_ns()
    with ResearchClient.local(root) as reopened:
        replay = reopened.command("worker.heartbeat", request, idempotency_key=key)
        elapsed = perf_counter_ns() - started
        sequence = reopened.get("worker_presence", worker.binding.worker_id)["data"][
            "heartbeat_sequence"
        ]
    return {
        "elapsed_ns": elapsed,
        "same_command": replay == accepted,
        "final_sequence": sequence,
        "valid": replay == accepted and sequence == 1,
        "scope": (
            "runtime reopen and exact replay after discarded acknowledgment; "
            "no process-crash latency claim"
        ),
    }


def overlap(stages):
    points = []
    for stage in stages:
        if stage["stage"] == "train" and "started_at" in stage:
            points.extend(
                (
                    (datetime.fromisoformat(stage["started_at"]), 1),
                    (datetime.fromisoformat(stage["finished_at"]), -1),
                )
            )
    active = maximum = 0
    for _, delta in sorted(points):
        active += delta
        maximum = max(maximum, active)
    return maximum


async def cpu_cell(root, definition, lanes, repeat, *, resumed=False, expected_workload=None):
    if expected_workload is not None and workload_digest() != expected_workload:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Frozen workload changed before capture")
    setup_started = perf_counter_ns()
    with private_state_directory(root):
        pass
    runtime = LocalRuntime.open(root.parent / "native-runtime")
    client = ResearchClient(LocalTransport(runtime))
    enrollment = ensure_local_enrollment(
        runtime, cpu_slots=2, machine_registry=root.parent / "machine" / "resources.sqlite"
    )
    setup_ns = perf_counter_ns() - setup_started
    prepare_started = perf_counter_ns()
    prepared = prepare_run(
        client,
        SOURCE,
        key=root.name,
        options=RunOptions(
            seed=1000 + repeat * 100, max_trials=definition.trials, parallelism=definition.trials
        ),
        profile=enrollment.worker.profile,
    )
    plan = prepared.plan
    if expected_workload is not None and workload_digest() != expected_workload:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Frozen workload changed during capture")
    first = client.get("trial", plan.trial_id)
    intents = CommandIntents(root / "benchmark-commands", {"controller": client})
    trial_ids = [first["id"]]
    for ordinal in range(1, definition.trials):
        trial = intents.command(
            str(ordinal),
            "controller",
            "trial.submit",
            SubmitTrial(
                campaign_id=plan.campaign_id,
                candidate_id=first["data"]["candidate_id"],
                validation_id=first["data"]["validation_id"],
                seed=1000 + repeat * 100 + ordinal,
            ),
            revision=client.get("campaign", plan.campaign_id)["revision"],
        )["result"]
        trial_ids.append(trial["id"])
    preparation_ns = perf_counter_ns() - prepare_started
    started = perf_counter_ns()
    driven = await LocalDriver(client, enrollment, concurrency=lanes).drain(plan.campaign_id)
    elapsed = perf_counter_ns() - started
    trials, stages = [], []
    for identifier in trial_ids:
        trial = client.get("trial", identifier)
        trials.append(
            {
                "id": identifier,
                "state": trial["state"],
                "seed": trial["data"]["spec"]["seed"],
                "execution_outcome": trial["data"]["execution_outcome"],
                "evaluation_status": trial["data"]["evaluation_status"],
            }
        )
        for job_id in trial["data"]["job_ids"]:
            job = client.get("job", job_id)
            attempt_id = job["data"]["current_attempt_id"]
            if attempt_id is not None:
                try:
                    result = client.get("run_result", attempt_id)["data"]
                except ResearchError as error:
                    if error.code != ErrorCode.NOT_FOUND:
                        raise
                    stages.append(
                        {
                            "stage": job["data"]["stage"]["id"],
                            "attempt_id": attempt_id,
                            "status": "result_missing",
                        }
                    )
                    continue
                stages.append(
                    {
                        "stage": job["data"]["stage"]["id"],
                        "attempt_id": attempt_id,
                        **{
                            key: result[key]
                            for key in (
                                "status",
                                "started_at",
                                "finished_at",
                                "elapsed_ms",
                                "phase_timings_ms",
                                "result_digest",
                                "execution_fingerprint",
                            )
                        },
                    }
                )
            model = job["data"]["outputs"].get("model")
            if model is not None:
                context = CommandContext(principal=runtime.principal(), idempotency_key=new_id())
                with runtime.artifacts.reader(context, ArtifactRef.model_validate(model)) as stream:
                    trials[-1]["optimizer_steps"] = strict_json_loads(stream.read())["train_steps"]
    snapshot = client.command(
        "report.create",
        ReportCampaign(campaign_id=plan.campaign_id, recent_limit=50),
        idempotency_key=new_id(),
    )["result"]["data"]["snapshot"]
    receipts = snapshot["recent_evaluations"]
    eligible = sum(item["eligible"] for item in receipts)
    attempts = client.list(RecordQuery(kind="attempt", campaign_id=plan.campaign_id, limit=100))
    complete = (
        eligible == definition.trials
        and len(stages) == definition.trials * 2
        and len(attempts["items"]) == len(stages)
        and attempts["next_cursor"] is None
        and all(
            Decimal(account["reserved"]) == Decimal(account["uncertain"]) == 0
            for account in snapshot["budget"]
        )
    )
    return {
        "lanes": lanes,
        "repeat": repeat,
        "resumed": resumed,
        "setup_ns": None if resumed else setup_ns,
        "preparation_ns": None if resumed else preparation_ns,
        "elapsed_ns": None if resumed else elapsed,
        "eligible_trials_per_hour": eligible * 3.6e12 / elapsed if not resumed else None,
        "eligible_trials": eligible,
        "planned_trials": definition.trials,
        "valid": complete,
        "driver_state": driven.state,
        "trials": trials,
        "stages": stages,
        "receipts": receipts,
        "budget": snapshot["budget"],
        "maximum_train_interval_overlap": overlap(stages),
        "execution_fingerprint": enrollment.worker.profile.execution_fingerprint,
        "comparison_identity": {
            "source_tree_digest": plan.source_tree_digest,
            **{
                key: first["data"]["spec"][key]
                for key in (
                    "protocol_digest",
                    "environment_digest",
                    "evaluator_contract_digest",
                    "data_manifest_digest",
                    "stages",
                )
            },
            "seeds": [trial["seed"] for trial in trials],
        },
        "scope": "single-host two-slot CPU registration; only driver concurrency differs",
    }


def summarize(cells):
    pairs = []
    repeats = sorted({cell["repeat"] for cell in cells if cell["kind"] == "cpu"})
    for repeat in repeats:
        group = {
            cell["count"]: cell
            for cell in cells
            if cell["kind"] == "cpu" and cell["repeat"] == repeat
        }
        ready = set(group) == {1, 2} and all(
            cell["status"] == "complete"
            and cell["result"]["valid"]
            and cell["result"]["eligible_trials_per_hour"]
            for cell in group.values()
        )
        comparable = (
            ready
            and group[1]["result"]["execution_fingerprint"]
            == group[2]["result"]["execution_fingerprint"]
            and group[1]["result"]["comparison_identity"]
            == group[2]["result"]["comparison_identity"]
        )
        ratio = (
            group[2]["result"]["eligible_trials_per_hour"]
            / group[1]["result"]["eligible_trials_per_hour"]
            if comparable
            else None
        )
        pairs.append(
            {
                "repeat": repeat,
                "speedup": ratio,
                "parallel_efficiency": ratio / 2 if ratio is not None else None,
            }
        )
    return {
        "planned_cells": len(cells),
        "complete_cells": sum(cell["status"] == "complete" for cell in cells),
        "censored_cells": sum(
            cell["status"] == "censored" or cell.get("result", {}).get("resumed", False)
            for cell in cells
        ),
        "invalid_cells": sum(
            cell["status"] == "complete" and not cell["result"].get("valid", False)
            for cell in cells
        ),
        "pairs": pairs,
        "median_speedup": statistics.median(pair["speedup"] for pair in pairs)
        if pairs and all(pair["speedup"] is not None for pair in pairs)
        else None,
    }


class Benchmark:
    def __init__(self, root, definition):
        self.root, self.definition = root, definition
        with private_state_directory(root):
            pass
        package = Path(inspect.getfile(research_swarm)).parent
        self.frozen = {
            "schema_version": "1.0",
            "definition": definition.model_dump(mode="json"),
            "system": {
                "os": platform.system(),
                "release": platform.release(),
                "architecture": platform.machine(),
                "logical_cpus": os.cpu_count(),
                "python": platform.python_version(),
                "sqlite": sqlite3.sqlite_version,
            },
            "implementation_digest": record_digest(
                "research.controller-benchmark-code",
                {
                    str(path.relative_to(package)): content_digest(path.read_bytes())
                    for path in sorted(package.rglob("*.py"))
                },
            ),
            "runner_digest": content_digest(Path(__file__).read_bytes()),
            "workload_digest": workload_digest(),
            "cache_policy": (
                "OS caches uncontrolled; per-cell environment setup and admission reported "
                "separately; cells run in declared order"
            ),
        }
        encoded = canonical_bytes(self.frozen)
        path = root / "definition.json"
        if path.exists():
            if read_private(root, path.name, limit=1024 * 1024) != encoded:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Frozen benchmark inputs differ"
                )
        else:
            if any(root.glob("cell-*.json")):
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Benchmark definition is missing"
                )
            write_private(path, encoded)

    async def run(self):
        cells = []
        halt_native = False
        for name, kind, count, repeat in self.definition.cells():
            path = self.root / ("cell-" + name + ".json")
            cell = (
                strict_json_loads(read_private(self.root, path.name, limit=4 * 1024 * 1024))
                if path.exists()
                else None
            )
            resumed = cell is not None
            if cell is None and kind == "cpu" and halt_native:
                cell = {
                    "name": name,
                    "kind": kind,
                    "count": count,
                    "repeat": repeat,
                    "status": "not_run",
                    "reason": "prior native cell did not close cleanly",
                }
                write_private(path, canonical_bytes(cell))
            if cell is None or cell["status"] == "running":
                cell = {
                    "name": name,
                    "kind": kind,
                    "count": count,
                    "repeat": repeat,
                    "status": "running",
                }
                write_private(path, canonical_bytes(cell))
                if resumed and kind != "cpu":
                    cell["status"] = "censored"
                else:
                    try:
                        root = self.root / name
                        result = (
                            await cpu_cell(
                                root,
                                self.definition,
                                count,
                                repeat,
                                resumed=resumed,
                                expected_workload=self.frozen["workload_digest"],
                            )
                            if kind == "cpu"
                            else traffic(root, count, self.definition.commands)
                            if kind == "traffic"
                            else budget_race(root, count)
                            if kind == "budget"
                            else restart_replay(root)
                        )
                        cell.update(status="complete", result=result)
                    except Exception as error:
                        cell.update(
                            status="failed",
                            error=error.code.value
                            if isinstance(error, ResearchError)
                            else type(error).__name__,
                        )
                write_private(path, canonical_bytes(cell))
            cells.append(cell)
            if kind == "cpu" and (cell["status"] != "complete" or not cell["result"]["valid"]):
                halt_native = True
        report = {**self.frozen, "cells": cells, "summary": summarize(cells)}
        write_private(self.root / "result.json", canonical_bytes(report))
        return report


def markdown(report):
    lines = [
        "# Local controller and CPU benchmark",
        "",
        "Descriptive measurements from one host. "
        "No multi-host, accelerator or scientific-improvement claim.",
        "",
        "| Cell | Status | Observation |",
        "| --- | --- | --- |",
    ]
    for cell in report["cells"]:
        result = cell.get("result", {})
        value = (
            f"{result['accepted_commands_per_second']:.1f} commands/s"
            if "accepted_commands_per_second" in result
            else (
                f"{result['eligible_trials_per_hour']:.1f} eligible trials/hour; "
                f"overlap {result['maximum_train_interval_overlap']}"
            )
            if result.get("eligible_trials_per_hour") is not None
            else f"{result['winners']} budget winner(s)"
            if "winners" in result
            else f"{result['elapsed_ns'] / 1e6:.2f} ms reopen/replay"
            if result.get("elapsed_ns")
            else "unmeasured"
        )
        status = cell["status"]
        if status == "complete" and not result["valid"]:
            status = "invalid"
        if result.get("resumed"):
            status += "; timing censored"
        if "eligible_trials" in result:
            value += f"; eligible {result['eligible_trials']}/{result['planned_trials']}"
        if cell["kind"] == "traffic" and "latency" in result:
            value += f"; failures {sum(item['failures'] for item in result['latency'].values())}"
        lines.append(f"| {cell['name']} | {status} | {value} |")
    lines += ["", "Paired CPU ratios:", ""]
    for pair in report["summary"]["pairs"]:
        lines.append(
            f"- repeat {pair['repeat']}: speedup {pair['speedup']}; "
            f"efficiency {pair['parallel_efficiency']}"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--definition", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    definition = (
        Definition.model_validate_json(args.definition.read_bytes())
        if args.definition
        else Definition()
    )
    with private_state_directory(args.state), FileLock(args.state / "benchmark.lock", timeout=0):
        report = asyncio.run(Benchmark(args.state, definition).run())
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "result.json").write_bytes(canonical_bytes(report))
    (args.output / "report.md").write_text(markdown(report))
    print(canonical_bytes(report["summary"]).decode())
    return (
        0
        if not report["summary"]["censored_cells"]
        and all(
            cell["status"] == "complete" and cell["result"]["valid"] for cell in report["cells"]
        )
        else 2
    )


if __name__ == "__main__":
    raise SystemExit(main())
