"""Mutable Metal payload; preserve the CPU example's model and SGD semantics."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import random
import time
from pathlib import Path

import mlx.core as mx
from common import ALPHABET, context, corpus, dataset, report

LEARNING_RATE = 0.35


def loss(weights: mx.array, previous: int, target: int) -> mx.array:
    row = weights[previous]
    shifted = row - mx.max(row)
    return mx.log(mx.sum(mx.exp(shifted))) - shifted[target]


value_and_grad = mx.value_and_grad(loss)


def step(weights: mx.array, previous: int, target: int) -> tuple[mx.array, mx.array]:
    value, gradient = value_and_grad(weights, previous, target)
    return weights - LEARNING_RATE * gradient, value


def main() -> None:
    request = context()
    if not mx.metal.is_available():
        raise RuntimeError("This project requires MLX Metal; CPU fallback is forbidden")
    engine = request["parameters"].get("engine", {})
    if engine.get("profile") != "mlx-metal":
        raise ValueError("Metal training requires an explicitly registered MLX environment")
    manifest, data_digest = dataset()
    text = corpus(manifest["training_seed"], manifest["training_sentences"])
    tokens = [ALPHABET.index(character) for character in text]
    generator = random.Random(request["seed"])
    size = len(ALPHABET)
    initial = [[generator.gauss(0, 0.01) for _ in range(size)] for _ in range(size)]
    draws = hashlib.sha256()
    with mx.stream(mx.gpu):
        weights = mx.array(initial, dtype=mx.float32)
        mx.eval(weights)
        mx.synchronize(mx.gpu)
        # Materialize a disposable update without consuming the training RNG or
        # retaining warmup weights. The timed run starts at the original weights.
        warmup_started = time.perf_counter_ns()
        warm_weights, warm_loss = step(weights, tokens[0], tokens[1])
        mx.eval(warm_weights, warm_loss)
        mx.synchronize(mx.gpu)
        warmup_ms = (time.perf_counter_ns() - warmup_started) // 1_000_000
        del warm_weights, warm_loss
        steps = 0
        started = time.perf_counter_ns()
        while time.perf_counter_ns() - started < request["time_budget_ms"] * 1_000_000:
            index = generator.randrange(len(tokens) - 1)
            weights, value = step(weights, tokens[index], tokens[index + 1])
            mx.eval(weights, value)
            mx.synchronize(mx.gpu)
            draws.update(index.to_bytes(8, "big"))
            steps += 1
        elapsed_ms = (time.perf_counter_ns() - started) // 1_000_000
        exported = weights.tolist()
    if not steps:
        raise RuntimeError("No materialized GPU update completed")
    checkpoint = {
        "format": "cpu-bigram-v1",
        "alphabet": ALPHABET,
        "weights": exported,
        "seed": request["seed"],
        "train_steps": steps,
        "data_manifest_digest": data_digest,
    }
    (Path(request["output_root"]) / "model.json").write_text(
        json.dumps(checkpoint, allow_nan=False), encoding="utf-8"
    )
    report(
        request,
        outputs=[{"name": "model", "path": "model.json", "media_type": "application/json"}],
        phase_timings_ms={"warmup": warmup_ms, "active_training": elapsed_ms},
        diagnostics={
            "optimizer_steps": steps,
            "materialized_gpu_updates": steps,
            "draw_order_digest": "sha256:" + draws.hexdigest(),
            "mlx_version": importlib.metadata.version("mlx"),
            "device_name": mx.metal.device_info()["device_name"],
            "stream": "gpu",
            "precision": "float32",
            "compile_policy": "eager",
            "warmup_policy": "discarded_fixed_pair_without_rng_advance",
            "execution_fingerprint": engine["execution_fingerprint"],
        },
    )


if __name__ == "__main__":
    main()
