"""Evaluate a candidate from a research swarm worker's JSON trial envelope."""

import argparse
import json
import os
from collections.abc import Sequence
from pathlib import Path

from benchmarks.suite import Task, get_task


def parse_task(argv: Sequence[str] | None = None) -> Task:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=("program", "mlp", "nanogpt"))
    parser.add_argument("--device", choices=("cpu", "mps", "cuda"), default="cpu")
    parser.add_argument("--size", choices=("small", "full"), default="small")
    args = parser.parse_args(argv)
    if args.task != "program":
        import torch

        torch.set_num_threads(1)
    return get_task(args.task, device=args.device, size=args.size)


def main() -> None:
    task = parse_task()
    trial = json.loads(Path(os.environ["SWARM_TRIAL"]).read_text())
    metric = task.score(
        trial["candidate"]["params"], seed=trial["seed"], fidelity=trial["fidelity"]
    )
    Path(os.environ["SWARM_METRICS"]).write_text(json.dumps({task.metric: metric}, allow_nan=False))


if __name__ == "__main__":
    main()
