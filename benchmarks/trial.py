"""Evaluate a candidate from a research swarm worker's JSON trial envelope."""

import argparse
import json
import os
from pathlib import Path

from benchmarks.suite import get_task


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=("program", "mlp", "nanogpt"))
    parser.add_argument("--device", choices=("cpu", "mps", "cuda"), default="cpu")
    parser.add_argument("--size", choices=("small", "full"), default="small")
    args = parser.parse_args()
    if args.task != "program":
        import torch

        torch.set_num_threads(1)
    task = get_task(args.task, device=args.device, size=args.size)
    trial = json.loads(Path(os.environ["SWARM_TRIAL"]).read_text())
    metric = task.score(
        trial["candidate"]["params"], seed=trial["seed"], fidelity=trial["fidelity"]
    )
    Path(os.environ["SWARM_METRICS"]).write_text(json.dumps({task.metric: metric}, allow_nan=False))


if __name__ == "__main__":
    main()
