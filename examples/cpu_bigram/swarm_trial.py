"""Fit a smoothed count model on training text and score the fixed validation corpus."""

import json
import math
import os
from collections import Counter
from itertools import pairwise
from pathlib import Path

from common import ALPHABET, corpus, dataset


def main() -> None:
    trial = json.loads(Path(os.environ["SWARM_TRIAL"]).read_text(encoding="utf-8"))
    alpha = trial["candidate"]["params"]["alpha"]
    if alpha <= 0:
        raise ValueError("Smoothing alpha must be positive")
    manifest, _ = dataset()
    train = corpus(manifest["training_seed"], manifest["training_sentences"])
    transitions = Counter(pairwise(train))
    contexts = Counter(train[:-1])
    validation = corpus(manifest["validation_seed"], manifest["validation_sentences"])
    bits = sum(
        -math.log2((transitions[a, b] + alpha) / (contexts[a] + len(ALPHABET) * alpha))
        for a, b in pairwise(validation)
    )
    metrics = {"bits_per_byte": bits / (len(validation) - 1)}
    Path(os.environ["SWARM_METRICS"]).write_text(json.dumps(metrics), encoding="utf-8")


if __name__ == "__main__":
    main()
