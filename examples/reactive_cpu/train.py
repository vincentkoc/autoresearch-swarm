"""Ordinary CPU gradient descent with optional training-time numeric decisions."""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from dataclasses import dataclass
from pathlib import Path

from common import context, data, mse, read_input, report
from policy import CONFIG, policy

from research_swarm.domain.canonical import strict_json_loads
from research_swarm.reactive.contracts import NumericValue, Observation
from research_swarm.reactive.hook import ReactiveHook
from research_swarm.reactive.replay import ReplayHook
from research_swarm.reactive.runner import DecisionRunner
from research_swarm.reactive.transcript import ControlTranscript


@dataclass
class Controls:
    learning_rate: float = 0.01

    def read(self) -> tuple[NumericValue, ...]:
        return (NumericValue(name="learning_rate", value=self.learning_rate),)

    def apply(self, values: tuple[NumericValue, ...]) -> None:
        self.learning_rate = values[0].value


async def main(mode: str, external: bool) -> None:
    request = context()
    configuration, data_digest = data()
    output = Path(request.output_root)
    target = Controls()
    randomizer = random.Random(request.seed)
    weights = [randomizer.uniform(-0.1, 0.1) for _ in range(2)]
    inputs = configuration["train_x"]
    gate = asyncio.Event()
    hook = ReactiveHook.from_context(request, target) if mode == "live" else None
    runner = DecisionRunner(hook.journal, policy(gate)) if hook and not external else None
    replay = (
        ReplayHook.from_context(
            request,
            target,
            ControlTranscript.model_validate(strict_json_loads(read_input(request, "transcript"))),
        )
        if mode == "replay"
        else None
    )
    trajectory = []
    started = time.perf_counter()
    try:
        for step in range(1, configuration["steps"] + 1):
            if (time.perf_counter() - started) * 1000 >= request.time_budget_ms:
                raise RuntimeError("Example training budget ended before the fixed step count")
            learning_rate = target.learning_rate
            errors = [weights[0] * x + weights[1] - (2 * x + 1) for x in inputs]
            weights[0] -= (
                learning_rate
                * 2
                * sum(error * x for error, x in zip(errors, inputs, strict=True))
                / len(inputs)
            )
            weights[1] -= learning_rate * 2 * sum(errors) / len(inputs)
            status = "fixed"
            if hook:
                observed = Observation(
                    step=step,
                    diagnostics=(NumericValue(name="train_mse", value=mse(weights, inputs)),),
                )
                status = (await hook.boundary(observed)).status
                if status == "uncertain":
                    raise RuntimeError("Reactive control application became uncertain")
                if runner:
                    runner.poll()
                if step >= CONFIG["release_step"]:
                    gate.set()
            if replay:
                receipts = replay.boundary(step)
                if any(receipt.outcome == "uncertain" for receipt in receipts):
                    raise RuntimeError("Replay control application became uncertain")
                status = "replay"
            trajectory.append(
                {
                    "step": step,
                    "learning_rate": learning_rate,
                    "train_mse": mse(weights, inputs),
                    "boundary": status,
                }
            )
    finally:
        if hook:
            hook.close()
        if runner:
            runner.close()
        replay_summary = replay.close() if replay else None
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    provenance = {
        "mode": {
            "live": "reactive",
            "keep": "fixed_initial_controls",
            "replay": "control_schedule_replay",
        }[mode]
    }
    if replay_summary:
        if replay_summary.status != "complete":
            raise RuntimeError("Replay did not reproduce the complete control schedule")
        provenance.update(replay_summary.model_dump(mode="json"))
    checkpoint = {
        "format": "reactive-linear-v1",
        "seed": request.seed,
        "steps": configuration["steps"],
        "weights": weights,
        "data_manifest_digest": data_digest,
        **provenance,
    }
    (output / "model.json").write_text(json.dumps(checkpoint, allow_nan=False), encoding="utf-8")
    name = {"live": "model", "keep": "baseline", "replay": "replay_model"}[mode]
    outputs = [{"name": name, "path": "model.json", "media_type": "application/json"}]
    if hook:
        transcript = ControlTranscript.capture(hook.journal)
        (output / "transcript.json").write_text(transcript.model_dump_json(), encoding="utf-8")
        (output / "trajectory.json").write_text(
            json.dumps(trajectory, allow_nan=False), encoding="utf-8"
        )
        outputs.extend(
            [
                {"name": "transcript", "path": "transcript.json", "media_type": "application/json"},
                {"name": "trajectory", "path": "trajectory.json", "media_type": "application/json"},
            ]
        )
    if replay:
        outputs.append(
            {
                "name": "replay_trace",
                "path": "reactive-replay.jsonl",
                "media_type": "application/x-ndjson",
            }
        )
    report(
        request,
        outputs=outputs,
        phase_timings_ms={"active_training": elapsed_ms},
        diagnostics={"optimizer_steps": configuration["steps"], **provenance},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("live", "keep", "replay"), default="live")
    parser.add_argument(
        "--external", action="store_true", help="Serve decisions in a separate process"
    )
    args = parser.parse_args()
    asyncio.run(main(args.mode, args.external))
