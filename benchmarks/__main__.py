"""Run a local search comparison and print its complete trial history as JSON."""

import argparse
import json
import platform
from dataclasses import asdict
from pathlib import Path

from benchmarks.ollama import OllamaPolicy
from benchmarks.study import open_study, run_campaign
from benchmarks.suite import Policy, comparison_seeds, get_task, greedy_search, random_search


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=("program", "mlp", "nanogpt"))
    parser.add_argument(
        "--strategy", choices=("random", "greedy", "baselines", "llm"), default="baselines"
    )
    parser.add_argument("--budget", type=int, default=12)
    parser.add_argument("--seed", type=int, help="defaults to the first registered search seed")
    parser.add_argument("--protocol", choices=("v1", "v2"), default="v1")
    parser.add_argument("--device", choices=("cpu", "mps", "cuda"), default="cpu")
    parser.add_argument("--size", choices=("small", "full"), default="small")
    parser.add_argument("--model", help="locally installed Ollama model for the llm strategy")
    parser.add_argument("--port", type=int, default=11434)
    parser.add_argument("--study", type=Path, help="SQLite panel history for this workload")
    args = parser.parse_args()
    if args.budget < 1:
        parser.error("--budget must be positive")
    if args.strategy == "llm" and (not args.model or args.task == "mlp" or args.budget < 2):
        parser.error("llm requires --model, program or nanogpt, and at least two trials")
    if args.task == "program" and args.device != "cpu":
        parser.error("program uses the CPU evaluator")
    seed = comparison_seeds(args.protocol)[0] if args.seed is None else args.seed
    task = get_task(args.task, device=args.device, size=args.size, protocol=args.protocol)
    runtime = {
        "machine": platform.machine(),
        "python": platform.python_version(),
        "device": args.device,
    }
    if args.task != "program":
        import torch

        torch.set_num_threads(1)
        runtime["torch"] = str(torch.__version__)
        if args.device == "mps" and not torch.backends.mps.is_available():
            parser.error("MPS is unavailable")
        if args.device == "cuda":
            if not torch.cuda.is_available():
                parser.error("CUDA is unavailable")
            runtime["accelerator"] = torch.cuda.get_device_name()
    strategies: dict[str, Policy] = {"random": random_search, "greedy": greedy_search}
    model = None
    if args.strategy == "llm":
        model = OllamaPolicy(args.model, args.port)
        strategies = {"llm": model}
    elif args.strategy != "baselines":
        strategies = {args.strategy: strategies[args.strategy]}

    runs = {}
    finalists = {}
    suffix = "" if args.protocol == "v1" else f"-{args.protocol}"
    path = (
        args.study
        or Path(".research/benchmarks") / f"{args.task}-{args.device}-{args.size}{suffix}.sqlite"
    )
    study = open_study(task, path)
    calibration = study.calibrate()
    failed = False
    for name, propose in strategies.items():
        arm = run_campaign(task, study, propose, budget=args.budget, seed=seed)
        trials = arm.trials
        runs[name] = [asdict(t) for t in trials]
        finalists[name] = {
            "selected": arm.selected,
            "confirmed_score": arm.confirmed_score,
            "decision": asdict(arm.decision) if arm.decision else None,
        }
        failed |= any(t.error is not None for t in trials) or bool(
            arm.decision and arm.decision.error
        )
    report = {
        "task": task.name,
        "metric": task.metric,
        "direction": task.direction,
        "size": args.size,
        "runtime": runtime,
        "protocol": args.protocol,
        "seed": seed,
        "panel": "search",
        "budget_per_strategy": args.budget,
        "runs": runs,
        "calibration": asdict(calibration),
        "panel_looks": study.look_counts(),
        "finalists": finalists,
    }
    if model:
        report["ollama"] = {
            "model": model.model,
            "input_tokens": model.input_tokens,
            "output_tokens": model.output_tokens,
        }
    print(json.dumps(report, allow_nan=False))
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
