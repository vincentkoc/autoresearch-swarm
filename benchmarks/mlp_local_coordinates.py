"""Run the frozen, search-only coordinate comparison from a fresh local directory."""

import argparse
import hashlib
import itertools
import json
import math
import os
import platform
import random
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import torch

from benchmarks import trial
from benchmarks.cli_acceptance import Baseline
from benchmarks.suite import PANEL_SEEDS, greedy_search, panel_size, random_search
from research_swarm.swarm.operators.config import from_spec
from research_swarm.swarm.operators.factorial import FiniteMutation
from research_swarm.swarm.operators.params import child
from research_swarm.swarm.store import SQLiteStore

CONTROL = "158d29f8af81790ef697473e90cca180a18dd610"
PROTOCOL = "d55bdd42cbf1e936a277568d71bfa5687ca138f40720c6d98240e34799ff8818"
SEEDS = (3884184493, 237053528, 4205356979)
SEARCH = 3413209931
ARMS = ("control", "challenger", "random", "greedy")
MODULE = "benchmarks.mlp_local_coordinates"


def task():
    PANEL_SEEDS["search"] = SEARCH
    return trial.parse_task(["mlp", "--protocol", "v1", "--size", "small", "--device", "cpu"])


class FrozenBaseline(Baseline):
    """Reuse the frozen proposal/tell methods with the prospective task's search stream."""

    def __init__(self, spec):
        self.task = task()
        self.propose = {"random": random_search, "greedy": greedy_search}[spec.options["arm"]]
        self.rng = random.Random(spec.seed)
        self.history = []
        self.best = None


class LocalCoverage(FiniteMutation):
    """Trace control moves and change only the challenger's scheduled local choice."""

    def __init__(self, spec, space):
        super().__init__(spec, space)
        self.counts = defaultdict(Counter)

    async def propose(self, spec, parents, k):
        assert k == 1
        observed = len(self._observed_trials)
        if not self.data or observed < spec.max_trials / 2 or observed % 2:
            return await super().propose(spec, parents, k)
        incumbent = max(self.data, key=lambda p: statistics.fmean(self.data[p]))
        coordinates = {}
        for point in itertools.product(*self.domains):
            changes = [i for i, (a, b) in enumerate(zip(point, incumbent, strict=True)) if a != b]
            if len(changes) == 1 and point not in self._issued:
                coordinates[point] = self.keys[changes[0]]
        if not coordinates:
            return await super().propose(spec, parents, k)
        previous = self.counts[incumbent]
        if spec.options["arm"] == "control":
            result = await super().propose(spec, parents, k)
            best = tuple(result[0].params[key] for key in self.keys)
        else:
            # Shuffle the complete control neighborhood once before filtering ties.
            neighbors = list(coordinates)
            self.rng.shuffle(neighbors)
            weights = self._fit()
            least = min(previous[key] for key in coordinates.values())

            def acquisition(point):
                features = self._features(point)
                estimate = sum(weights.get(f, 0.0) * x for f, x in features.items())
                bonus = sum(math.sqrt(x / (1 + self._coverage[f])) for f, x in features.items())
                return estimate + 0.8 * bonus / math.sqrt(len(self.keys))

            best = max((p for p in neighbors if previous[coordinates[p]] == least), key=acquisition)
            self._remember(best)
            params = dict(zip(self.keys, best, strict=True))
            nearest = max(
                parents, key=lambda p: sum(p.params.get(k) == v for k, v in params.items())
            )
            result = [child(nearest, self.name, params={**nearest.params, **params})]
        record = {
            "after_trials": observed,
            "incumbent": dict(zip(self.keys, incumbent, strict=True)),
            "eligible_coordinates": sorted(set(coordinates.values())),
            "prior_local_counts": dict(previous),
            "chosen_coordinate": coordinates[best],
            "issued_recipe": result[0].params,
            "candidate_id": result[0].candidate_id,
        }
        previous[coordinates[best]] += 1
        with Path(spec.options["moves_path"]).open("a") as out:
            out.write(json.dumps(record) + "\n")
        return result


def portfolio(spec):
    policy = from_spec(spec)
    original = policy.operators["param-mutate"]
    assert type(original) is FiniteMutation
    space = dict(zip(original.keys, original.domains, strict=True))
    policy.operators["param-mutate"] = LocalCoverage(spec, space)
    return policy


def arm_spec(directory, seed, arm):
    mlp = task()
    is_portfolio = arm in ("control", "challenger")
    name = f"mlp-{seed}-{arm}"
    return {
        "campaign_id": name,
        "metric": mlp.metric,
        "direction": mlp.direction,
        "task_dir": str(directory),
        "command": [sys.executable, "-m", MODULE, "score", "mlp"],
        "seed": seed,
        "fidelity": 1.0,
        "max_trials": 12,
        "max_workers": 1,
        "max_attempts": 1,
        "trial_timeout_seconds": 90,
        "proposal_timeout_seconds": 30,
        "evaluation_mode": "smoke",
        "candidates": [{"candidate_id": "initial", "params": mlp.initial}],
        "search_space": mlp.space if is_portfolio else {},
        "proposer": MODULE + (":portfolio" if is_portfolio else ":FrozenBaseline"),
        "options": {
            "arm": arm,
            "moves_path": str(directory / f"{name}.moves.jsonl"),
            "search": {
                "operators": ["param-mutate", "crossover"],
                "replicate": False,
                "fidelities": [0.5, 1.0],
                "reduction": 3,
            },
        },
    }


def score():
    from torch.optim.optimizer import register_optimizer_step_post_hook

    from benchmarks import training

    envelope = json.loads(Path(os.environ["SWARM_TRIAL"]).read_text())
    streams = []
    updates = 0
    original_points = training._points

    def observed_points(stream, count, device):
        expected = (17, SEARCH)
        if len(streams) >= 2 or stream != expected[len(streams)] or count != 256 or device != "cpu":
            raise ValueError(
                "The prospective workload requires its frozen CPU train/search streams"
            )
        streams.append({"stream": stream, "cases": count, "device": device})
        return original_points(stream, count, device)

    def step_finished(optimizer, args, kwargs):
        nonlocal updates
        updates += 1

    PANEL_SEEDS["search"] = SEARCH
    training._points = observed_points
    hook = register_optimizer_step_post_hook(step_finished)
    try:
        sys.argv[1:] = ["mlp", "--protocol", "v1", "--size", "small", "--device", "cpu"]
        trial.main()
    finally:
        hook.remove()
        training._points = original_points
        record = {
            "trial_id": envelope["trial_id"],
            "completed_updates": updates,
            "streams": streams,
            "torch_threads": torch.get_num_threads(),
        }
        with Path(f"{envelope['campaign_id']}.workers.jsonl").open("a") as out:
            out.write(json.dumps(record) + "\n")


def run_arm(directory, seed, arm):
    spec = arm_spec(directory, seed, arm)
    name = spec["campaign_id"]
    path, db = directory / f"{name}.yaml", directory / f"{name}.sqlite"
    with path.open("x") as out:
        json.dump(spec, out)
    start = time.monotonic()
    process = subprocess.run(
        [str(Path(sys.executable).with_name("research")), "swarm", "run", path, "--db", db],
        capture_output=True,
        text=True,
    )
    elapsed = time.monotonic() - start
    (directory / f"{name}.stdout").write_text(process.stdout)
    (directory / f"{name}.stderr").write_text(process.stderr)
    worker_file = directory / f"{name}.workers.jsonl"
    workers = (
        {r["trial_id"]: r for r in map(json.loads, worker_file.read_text().splitlines())}
        if worker_file.exists()
        else {}
    )
    records = []
    if db.exists():
        for result in SQLiteStore(db).results(name):
            attempted = result.attempt.trial
            ok = result.status == "ok"
            records.append(
                {
                    "trial_id": attempted.trial_id,
                    "candidate_id": attempted.candidate.candidate_id,
                    "attempt_id": result.attempt.attempt_id,
                    "attempt_number": result.attempt.number,
                    "seed": attempted.seed,
                    "recipe": attempted.candidate.params,
                    "operator": attempted.candidate.operator,
                    "fidelity": attempted.fidelity,
                    "status": result.status,
                    "accuracy": result.metrics.get("accuracy") if ok else None,
                    "completed_updates": workers.get(attempted.trial_id, {}).get(
                        "completed_updates"
                    ),
                }
            )
    full = [r["accuracy"] for r in records if r["fidelity"] == 1.0 and r["accuracy"] is not None]
    moves_path = Path(spec["options"]["moves_path"])
    return {
        "arm": arm,
        "seed": seed,
        "seconds": elapsed,
        "cli_exit": process.returncode,
        "attempts": len(records),
        "distinct_recipes": len({json.dumps(r["recipe"], sort_keys=True) for r in records}),
        "rung_counts": dict(Counter(r["fidelity"] for r in records)),
        "statuses": dict(Counter(r["status"] for r in records)),
        "known_completed_updates": sum(r["completed_updates"] or 0 for r in records),
        "best_full_search_accuracy": max(full) if full else None,
        "local_moves": [json.loads(line) for line in moves_path.read_text().splitlines()]
        if moves_path.exists()
        else [],
        "trials": records,
    }


def preflight():
    mlp = task()
    assert platform.python_version() == "3.12.14" and torch.__version__ == "2.15.0"
    assert torch.get_num_threads() == 1 and mlp.device == "cpu" and mlp.protocol == "v1"
    assert mlp.initial == {"width": "8", "activation": "tanh", "learning_rate": "0.001"}
    assert PANEL_SEEDS["train"] == 17 and panel_size("mlp", "small", "train") == 256
    assert panel_size("mlp", "small", "search") == 256
    for label, expected in zip(
        ("search", "seed/0", "seed/1", "seed/2"), (SEARCH, *SEEDS), strict=True
    ):
        digest = hashlib.sha256(f"ars-mlp-local-coordinates-v1/{label}".encode()).digest()
        assert int.from_bytes(digest[:4], "big") == expected


def run(directory):
    preflight()
    root = Path(__file__).resolve().parents[1]
    os.environ["PYTHONPATH"] = str(root)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    directory.mkdir(parents=True, exist_ok=False)
    arms = []
    with (directory / "arms.jsonl").open("x") as output:
        for seed in SEEDS:
            for arm in ARMS:
                row = run_arm(directory, seed, arm)
                arms.append(row)
                output.write(json.dumps(row, allow_nan=False) + "\n")
                output.flush()
                print(seed, arm, row["attempts"], row["statuses"], flush=True)
    scores = {(r["arm"], r["seed"]): r["best_full_search_accuracy"] for r in arms}
    complete = all(r["attempts"] == 12 and r["best_full_search_accuracy"] is not None for r in arms)
    comparisons = {}
    for arm in ("control", "random", "greedy"):
        deltas = []
        for seed in SEEDS:
            left, right = scores["challenger", seed], scores[arm, seed]
            deltas.append(left - right if left is not None and right is not None else None)
        comparisons[arm] = {
            "paired_differences": deltas,
            "mean_difference": statistics.fmean(deltas) if None not in deltas else None,
        }
    evidence = {
        "protocol_sha256": PROTOCOL,
        "control_revision": CONTROL,
        "implementation_revision": revision,
        "python": platform.python_version(),
        "torch": torch.__version__,
        "device": "cpu",
        "torch_threads": torch.get_num_threads(),
        "training_stream": 17,
        "search_stream": SEARCH,
        "seeds": SEEDS,
        "trials_per_arm": 12,
        "complete": complete,
        "comparisons": comparisons,
        "prediction_passed": all(c["mean_difference"] > 0 for c in comparisons.values())
        if complete
        else None,
    }
    with (directory / "comparison.json").open("x") as out:
        json.dump(evidence, out, indent=2, allow_nan=False)
        out.write("\n")


if __name__ == "__main__":
    if sys.argv[1:] == ["score", "mlp"]:
        score()
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("directory", type=Path)
        run(parser.parse_args().directory.resolve())
