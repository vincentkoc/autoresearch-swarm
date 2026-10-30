"""Mutable research payload: train a character transition model with SGD."""

from __future__ import annotations

import json
import math
import random
import time
from pathlib import Path

from common import ALPHABET, context, corpus, dataset, report

LEARNING_RATE = 0.35


def main() -> None:
    request = context()
    manifest, data_digest = dataset()
    text = corpus(manifest["training_seed"], manifest["training_sentences"])
    tokens = [ALPHABET.index(character) for character in text]
    generator = random.Random(request["seed"])
    size = len(ALPHABET)
    weights = [[generator.gauss(0, 0.01) for _ in range(size)] for _ in range(size)]
    steps = 0
    started = time.perf_counter()
    stop_at = started + request["time_budget_ms"] / 1000
    while time.perf_counter() < stop_at:
        index = generator.randrange(len(tokens) - 1)
        previous, target = tokens[index : index + 2]
        row = weights[previous]
        maximum = max(row)
        probabilities = [math.exp(weight - maximum) for weight in row]
        total = sum(probabilities)
        for token in range(size):
            row[token] -= LEARNING_RATE * (probabilities[token] / total - (token == target))
        steps += 1
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    checkpoint = {
        "format": "cpu-bigram-v1",
        "alphabet": ALPHABET,
        "weights": weights,
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
        phase_timings_ms={"active_training": elapsed_ms},
        diagnostics={"optimizer_steps": steps},
    )
    print(f"optimizer_steps: {steps}")


if __name__ == "__main__":
    main()
