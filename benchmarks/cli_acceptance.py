"""Run the frozen v1 comparison through the installed operator-portfolio campaign CLI."""

import argparse
import hashlib
import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path

from benchmarks import trial
from benchmarks.study import WorkerPanelPolicy, open_study, recipe_key
from benchmarks.suite import PANEL_SEEDS, greedy_search, random_search
from benchmarks.suite import Trial as SearchTrial
from research_swarm.swarm.interfaces import Candidate
from research_swarm.swarm.store import SQLiteStore

SEEDS = (11, 23, 47)
BUDGET = 12
MODEL = "qwen2.5-coder:0.5b"
MODEL_DIGEST = "6ff36c41f6f588c457e538a5a42d24f9730cc3be7caa391d95d8b8e14f92d103"


def fresh_streams():
    # The frozen functions read this map in each isolated CLI/worker process.
    for role in PANEL_SEEDS:
        if role != "train":
            label = f"ars-cli-acceptance-v1/{role}".encode()
            PANEL_SEEDS[role] = int.from_bytes(hashlib.sha256(label).digest()[:4], "big")


def workload(name):
    fresh_streams()
    return trial.parse_task([name, "--protocol", "v1", "--size", "small", "--device", "cpu"])


def panel_policy(spec):
    task = workload(spec.command[-1])
    return WorkerPanelPolicy(open_study(task, Path(spec.options["study"])))


def full_rung(results, metric, direction):
    eligible = [r for r in results if r.status == "ok" and r.attempt.trial.fidelity == 1.0]
    choose = min if direction == "minimize" else max
    return choose(eligible, key=lambda r: r.metrics[metric]) if eligible else None


def final_selection(spec):
    # Register the common reference. Consult held-out panels for one nominee only
    # after the final search attempt. If it is rejected, the loop filters it out.
    def nominate(results):
        if len(results) == spec.max_trials:
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


class Baseline:
    def __init__(self, spec):
        self.task = workload(spec.command[-1])
        self.propose = {"random": random_search, "greedy": greedy_search}[spec.options["arm"]]
        self.rng = random.Random(spec.seed)
        self.history = []
        self.best = None

    def tell(self, result):
        score = result.metrics.get(self.task.metric) if result.status == "ok" else None
        if score is not None and (self.best is None or self.task.improved(score, self.best)):
            self.best = score
        self.history.append(
            SearchTrial(result.attempt.trial.candidate.params, score, self.best, 0.0, result.error)
        )

    async def ask(self, spec, archive, k):
        if k != 1:
            raise ValueError("The paired comparison requires serial proposals")
        params = (
            self.task.initial
            if not self.history
            else self.propose(self.task, self.history, self.rng)
        )
        # Frozen random sampling is with replacement. Each draw owns a trial slot.
        return [Candidate(candidate_id=f"draw-{len(self.history)}", params=params)]


def run_arm(task, arm, seed, port, directory, study_path):
    campaign_id = f"{task.name}-{arm}-{seed}"
    operators = ["param-mutate", "crossover"]
    if task.name != "mlp":
        operators.insert(0, "llm-choice-rewrite")
    spec = {
        "campaign_id": campaign_id,
        "metric": task.metric,
        "direction": task.direction,
        "command": [sys.executable, "-m", "cli_acceptance", "score", task.name],
        "task_dir": str(Path.cwd()),
        "seed": seed,
        "fidelity": 1.0,
        "max_trials": BUDGET,
        "max_workers": 1,
        "max_attempts": 1,
        "trial_timeout_seconds": 90,
        "proposal_timeout_seconds": 30,
        "evaluation_mode": "calibrated",
        "panel_policy": "cli_acceptance:panel_policy",
        "selection_policy": "cli_acceptance:final_selection",
        "search_panel": "search",
        "selection_panel": "selection",
        "confirmation_panel": "confirmation",
        "proposer": "research_swarm.swarm.operators.config:from_spec"
        if arm == "swarm"
        else "cli_acceptance:Baseline",
        "candidates": [{"candidate_id": "initial", "params": task.initial}],
        "search_space": task.space if arm == "swarm" else {},
        "options": {
            "arm": arm,
            "study": str(study_path),
            "search": {
                "operators": operators,
                "replicate": False,
                "fidelities": [0.5, 1.0],
                "reduction": 3,
                "model": MODEL,
                "objective": task.objective,
                "port": port,
            },
        },
    }
    path = directory / f"{campaign_id}.yaml"
    database = path.with_suffix(".sqlite")
    path.write_text(json.dumps(spec))
    start = time.monotonic()
    cli = Path(sys.executable).with_name("research")
    process = subprocess.run(
        [cli, "swarm", "run", path, "--db", database], capture_output=True, text=True
    )
    seconds = time.monotonic() - start
    if process.returncode:
        raise RuntimeError(f"{campaign_id}: {process.stdout[-1500:]} {process.stderr[-1500:]}")
    summary = json.loads(process.stdout)
    results = SQLiteStore(database).results(campaign_id)
    if len(results) != summary["attempts"] or summary["trials"] != len(results):
        raise ValueError("Inconsistent terminal attempt count")
    winner = full_rung(results, task.metric, task.direction)
    chosen = summary["selected"]["result"]["attempt"]["trial"]["candidate"]["params"]
    study = open_study(task, study_path)
    confirmation = study.confirmed_score(recipe_key(chosen))
    if confirmation is None:
        raise ValueError("Selected recipe has no recorded confirmation")
    records = []
    best = {}
    for result in results:
        attempted = result.attempt.trial
        score = result.metrics.get(task.metric) if result.status == "ok" else None
        rung = attempted.fidelity
        if score is not None and (rung not in best or task.improved(score, best[rung])):
            best[rung] = score
        records.append(
            {
                "recipe": attempted.candidate.params,
                "operator": attempted.candidate.operator,
                "fidelity": rung,
                "score": score,
                "best_at_rung": best.get(rung),
                "status": result.status,
            }
        )
    return {
        "task": task.name,
        "arm": arm,
        "seed": seed,
        "reason": summary["reason"],
        "budget": BUDGET,
        "attempts": records,
        "seconds": seconds,
        "full_rung_best": winner.metrics[task.metric] if winner else None,
        "nominee": winner.attempt.trial.candidate.params if winner else None,
        "selected": chosen,
        "confirmed": confirmation,
    }


def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "score":
        fresh_streams()
        del sys.argv[1]
        trial.main()
        return
    from urllib.request import urlopen

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directory", type=Path, help="new directory for all campaign and panel databases"
    )
    parser.add_argument("--port", type=int, default=11434, help="local Ollama port")
    args = parser.parse_args()
    with urlopen(f"http://127.0.0.1:{args.port}/api/tags", timeout=5) as response:
        tags = json.load(response)["models"]
    if not any(item["name"] == MODEL and item["digest"] == MODEL_DIGEST for item in tags):
        raise ValueError("The frozen local model is unavailable")
    directory = args.directory.resolve()
    directory.mkdir(exist_ok=False)
    # The docs checkout owns this driver. The candidate checkout owns the CLI and scorers.
    os.environ["PYTHONPATH"] = os.pathsep.join(
        [str(Path(__file__).resolve().parent), str(Path.cwd())]
    )
    with (directory / "arms.jsonl").open("x") as out:
        for name in ("program", "mlp", "nanogpt"):
            task = workload(name)
            study_path = directory / f"{name}-panels.sqlite"
            for seed in SEEDS:
                for arm in ("swarm", "random", "greedy"):
                    row = run_arm(task, arm, seed, args.port, directory, study_path)
                    out.write(json.dumps(row, allow_nan=False, separators=(",", ":")) + "\n")
                    out.flush()
                    print(name, seed, arm, len(row["attempts"]), row["confirmed"], flush=True)
            study = open_study(task, study_path)
            print(name, "looks", study.look_counts(), flush=True)


if __name__ == "__main__":
    main()
