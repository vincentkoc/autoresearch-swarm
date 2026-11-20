"""Run the preregistered equal-work campaign comparison on generated CPU workloads."""

import argparse
import json
import math
import os
import platform
import sqlite3
import subprocess
import sys
import time
from dataclasses import fields, replace
from datetime import UTC, datetime
from pathlib import Path

from benchmarks.cli_acceptance import MODEL, MODEL_DIGEST, Baseline, full_rung
from benchmarks.model_meter import measure_requests, verify_model
from benchmarks.study import WorkerPanelPolicy, open_study, recipe_key
from benchmarks.suite import comparison_seeds, get_task
from research_swarm.swarm.interfaces import CampaignSpec
from research_swarm.swarm.operators.config import SearchOptions
from research_swarm.swarm.store import SQLiteStore

MODULE = "benchmarks.equal_work"
FULL_WORK = {"program": 48, "mlp": 24, "nanogpt": 12}
ROOT = Path(__file__).resolve().parents[1]


def append(path, row):
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(row, allow_nan=False, separators=(",", ":")) + "\n")


def workload(name):
    if name != "program":
        import torch

        torch.set_num_threads(1)
    return get_task(name, protocol="v3")


def measure(name, ledger, campaign, trial_id, candidate, seed, panel, fidelity):
    """Count completed evaluator operations, including partial work on exceptions."""
    completed = 0

    def increment():
        nonlocal completed
        completed += 1

    scorer = get_task(name, protocol="v3", on_work=increment)
    started = datetime.now(UTC).isoformat()
    value = None
    try:
        value = scorer.score(candidate, seed=seed, panel=panel, fidelity=fidelity)
        return value, completed
    finally:
        append(
            ledger,
            {
                "campaign": campaign,
                "trial_id": trial_id,
                "attempt_id": os.environ.get("SWARM_ATTEMPT_ID"),
                "recipe": candidate,
                "seed": seed,
                "role": panel,
                "fidelity": fidelity,
                "started": started,
                "finished": datetime.now(UTC).isoformat(),
                "work": completed,
                "score": value,
            },
        )


def score(name):
    task = workload(name)
    envelope = json.loads(Path(os.environ["SWARM_TRIAL"]).read_text())
    value, work = measure(
        name,
        Path(os.environ["BENCH_WORK_LEDGER"]),
        envelope["campaign_id"],
        envelope["trial_id"],
        envelope["candidate"]["params"],
        envelope["seed"],
        "search",
        envelope["fidelity"],
    )
    Path(os.environ["SWARM_METRICS"]).write_text(
        json.dumps({task.metric: value, "completed_work": work}, allow_nan=False)
    )


def panel_policy(spec):
    name = spec.command[-1]
    task = workload(name)

    def evaluate(candidate, seed, panel, device, size, fidelity):
        value, _ = measure(
            name,
            Path(spec.options["ledger"]),
            spec.campaign_id,
            None,
            candidate,
            seed,
            panel,
            fidelity,
        )
        return value

    return WorkerPanelPolicy(
        open_study(replace(task, evaluate=evaluate), Path(spec.options["study"]))
    )


def baseline(spec):
    return Baseline(spec, task=workload(spec.command[-1]))


def final_selection(spec):
    def nominate(results):
        if sum(r.metrics.get("completed_work", 0) for r in results) == spec.max_search_work:
            winner = full_rung(results, spec.metric, spec.direction)
            if winner is not None:
                return winner
        return next(
            (
                r
                for r in results
                if r.status == "ok"
                and r.attempt.trial.candidate.params == spec.candidates[0].params
            ),
            None,
        )

    return nominate


def specification(task, seed, arm, folder, port):
    operators = ["param-mutate", "crossover"]
    if task.name != "mlp":
        operators.insert(0, "llm-choice-rewrite")
    return {
        "campaign_id": f"{task.name}-{seed}-{arm}",
        "metric": task.metric,
        "direction": task.direction,
        "command": [sys.executable, "-m", MODULE, "score", task.name],
        "task_dir": str(ROOT),
        "seed": seed,
        "fidelity": 1.0,
        "max_trials": 24,
        "full_fidelity_work": FULL_WORK[task.name],
        "max_search_work": 12 * FULL_WORK[task.name],
        "max_workers": 1,
        "max_attempts": 1,
        "trial_timeout_seconds": 90,
        "proposal_timeout_seconds": 30,
        "evaluation_mode": "calibrated",
        "panel_policy": f"{MODULE}:panel_policy",
        "selection_policy": f"{MODULE}:final_selection",
        "search_panel": "search",
        "selection_panel": "selection",
        "confirmation_panel": "confirmation",
        "proposer": "research_swarm.swarm.operators.config:from_spec"
        if arm == "swarm"
        else f"{MODULE}:baseline",
        "candidates": [{"candidate_id": "initial", "params": task.initial}],
        "search_space": task.space if arm == "swarm" else {},
        "options": {
            "arm": arm,
            "ledger": str(folder / "fits.jsonl"),
            "study": str(folder.parent / f"{task.name}-panels.sqlite"),
            "search": {
                "operators": operators,
                "replicate": False,
                "fidelities": [0.5, 1.0],
                "family_cap": 2,
                "reduction": 3,
                "exploration": SearchOptions().exploration,
                "model": MODEL,
                "objective": task.objective,
                "port": port,
            },
        },
    }


def observations(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.is_file() else []


def run_arm(task, seed, arm, directory, remote_port):
    campaign = f"{task.name}-{seed}-{arm}"
    folder = directory / campaign
    folder.mkdir()
    queue = folder / "queue.sqlite"
    with measure_requests(remote_port, folder / "model.jsonl") as port:
        spec = specification(task, seed, arm, folder, port)
        path = folder / "campaign.yaml"
        path.write_text(json.dumps(spec, indent=2))
        start = time.monotonic()
        env = {
            **os.environ,
            "BENCH_WORK_LEDGER": spec["options"]["ledger"],
            "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "src")]),
        }
        with (
            (folder / "summary.json").open("x") as stdout,
            (folder / "stderr.log").open("x") as stderr,
        ):
            process = subprocess.run(
                [Path(sys.executable).with_name("research"), "swarm", "run", path, "--db", queue],
                env=env,
                stdout=stdout,
                stderr=stderr,
            )
        seconds = time.monotonic() - start
    if process.returncode:
        raise RuntimeError(f"{campaign}: CLI exited {process.returncode}; see stderr.log")
    summary = json.loads((folder / "summary.json").read_text())
    results = SQLiteStore(queue).results(campaign)
    looks = observations(folder / "fits.jsonl")
    searches = [x for x in looks if x["role"] == "search"]
    by_attempt = {x["attempt_id"]: x for x in searches}
    if len(searches) != len(by_attempt) or not (
        summary["trials"] == summary["attempts"] == len(results) == len(searches)
    ):
        raise ValueError("Search observations and queue admissions differ")
    best = {}
    records = []
    for result in results:
        item = result.attempt.trial
        fit = by_attempt[result.attempt.attempt_id]
        value = result.metrics.get(task.metric)
        if (
            result.status != "ok"
            or fit["score"] != value
            or fit["recipe"] != item.candidate.params
            or fit["trial_id"] != item.trial_id
            or fit["seed"] != item.seed
            or fit["fidelity"] != item.fidelity
            or fit["work"] != math.ceil(FULL_WORK[task.name] * item.fidelity)
            or fit["work"] != result.metrics.get("completed_work")
        ):
            raise ValueError(f"Search work or score mismatch on {item.trial_id}")
        if item.fidelity not in best or task.improved(value, best[item.fidelity]):
            best[item.fidelity] = value
        records.append(
            {
                "trial_id": item.trial_id,
                "attempt_id": result.attempt.attempt_id,
                "fidelity": item.fidelity,
                "work": fit["work"],
                "recipe": fit["recipe"],
                "operator": item.candidate.operator,
                "score": value,
                "best_at_rung": best[item.fidelity],
            }
        )
    spent = sum(x["work"] for x in searches)
    if spent != spec["max_search_work"]:
        raise ValueError(f"Spent {spent} of {spec['max_search_work']} search units")
    winner = full_rung(results, task.metric, task.direction)
    chosen = summary["selected"]["result"]["attempt"]["trial"]["candidate"]["params"]
    study = open_study(task, Path(spec["options"]["study"]))
    confirmed = study.confirmed_score(recipe_key(chosen))
    if not winner or confirmed is None or any(x["score"] is None for x in looks):
        raise ValueError("Missing finalist or measured score")
    nominee = winner.attempt.trial.candidate.params
    reference = None if nominee == task.initial else recipe_key(task.initial)
    key = study.decision_key(recipe_key(nominee), reference)
    uri = Path(spec["options"]["study"]).as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as db:
        saved = db.execute("SELECT value FROM panel_state WHERE key=?", (key,)).fetchone()
    if saved is None:
        raise ValueError("Missing recorded finalist decision")
    decision = json.loads(saved[0])
    expected = nominee if decision["promoted"] else task.initial
    if decision.get("error") or chosen != expected:
        raise ValueError("Recorded decision disagrees with selected recipe or failed evaluation")
    roles = {
        role: sum(x["work"] for x in looks if x["role"] == role)
        for role in (
            "search",
            "calibration_proxy",
            "calibration_target",
            "selection",
            "confirmation",
        )
    }
    calls = observations(folder / "model.jsonl")
    if any(call.get("status") == 200 and call.get("resident_cpu") is not True for call in calls):
        raise ValueError("Successful generation missing CPU residency evidence")
    return {
        "task": task.name,
        "seed": seed,
        "arm": arm,
        "stop": summary["reason"],
        "seconds": seconds,
        "work": roles,
        "attempts": records,
        "nominee": nominee,
        "decision": decision,
        "full_rung_best": winner.metrics[task.metric],
        "selected": chosen,
        "confirmed": confirmed,
        "model_requests": calls,
    }


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "score":
        score(sys.argv[2])
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="new directory for all raw records")
    parser.add_argument("--port", type=int, required=True, help="local CPU Ollama port")
    args = parser.parse_args()
    if SearchOptions().exploration != 1.0:
        parser.error("the registered production exploration setting is 1.0")
    if not {"full_fidelity_work", "max_search_work"}.issubset(f.name for f in fields(CampaignSpec)):
        parser.error("the installed campaign CLI lacks search-work admission")
    verify_model(args.port, MODEL, MODEL_DIGEST)
    directory = args.directory.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    import torch

    import research_swarm

    print(
        "runtime", platform.python_version(), torch.__version__, research_swarm.__file__, flush=True
    )
    for name in FULL_WORK:
        task = workload(name)
        for seed in comparison_seeds("v3"):
            for arm in ("swarm", "random", "greedy"):
                try:
                    row = run_arm(task, seed, arm, directory, args.port)
                except (OSError, KeyError, TypeError, ValueError, RuntimeError) as exc:
                    row = {"task": name, "seed": seed, "arm": arm, "error": str(exc)}
                append(directory / "arms.jsonl", row)
                print(name, seed, arm, row.get("error", row.get("confirmed")), flush=True)


if __name__ == "__main__":
    main()
