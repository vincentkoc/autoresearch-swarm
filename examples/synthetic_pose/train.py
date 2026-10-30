"""Mutable ordinary payload: fit a scalar linear predictor to fictional signals."""

from __future__ import annotations

import random
import time

from common import context, data, encode, output, report, sha256

LEARNING_RATE = 0.08
UPDATES = 120


def main() -> None:
    request = context()
    dataset, identities = data()
    people = dataset["partition"]["fit"]
    rows = [row for person in people for row in dataset["people"][person]]
    generator = random.Random(request["seed"])
    weight, bias = generator.uniform(-0.1, 0.1), generator.uniform(-0.1, 0.1)
    draw_order = []
    started = time.perf_counter()
    last_step_seconds = 0.0
    for _ in range(UPDATES):
        step_started = time.perf_counter()
        indices = list(range(len(rows)))
        generator.shuffle(indices)
        draw_order.extend(indices)
        dw = db = 0.0
        for index in indices:
            feature, target = rows[index]
            residual = weight * feature + bias - target
            dw += residual * feature
            db += residual
        weight -= LEARNING_RATE * dw / len(rows)
        bias -= LEARNING_RATE * db / len(rows)
        last_step_seconds = time.perf_counter() - step_started
    elapsed = time.perf_counter() - started
    if elapsed * 1000 > request["time_budget_ms"]:
        raise RuntimeError("Fixed-update fixture exceeded its declared training budget")
    checkpoint = {
        "format": "synthetic-pose-v1",
        "seed": request["seed"],
        "units": "radians",
        "fit_people": people,
        "weight": weight,
        "bias": bias,
        **identities,
    }
    checkpoint_digest = sha256(encode(checkpoint))
    fit_receipt = {
        "trial": request["trial_id"],
        "seed": request["seed"],
        "checkpoint_sha256": checkpoint_digest,
        "draw_order_sha256": sha256(encode(draw_order)),
        "draws": len(draw_order),
        "updates": UPDATES,
        "valid_frames": len(draw_order),
        "fit_seconds": elapsed,
        "last_step_seconds": last_step_seconds,
        "stop": "fixed_updates",
        **identities,
    }
    report(
        request,
        outputs=[output(request, "model", checkpoint), output(request, "fit_receipt", fit_receipt)],
        phase_timings_ms={"active_training": int(elapsed * 1000)},
        diagnostics={"updates": UPDATES},
    )


if __name__ == "__main__":
    main()
