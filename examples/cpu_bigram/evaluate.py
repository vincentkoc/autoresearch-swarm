"""Protected scorer over the declared synthetic validation panel."""

from __future__ import annotations

import hashlib
import json
import math
from itertools import pairwise
from pathlib import Path

from common import ALPHABET, context, corpus, dataset, report


def main() -> None:
    request = context()
    reference = request["inputs"]["model"]
    content = Path(reference["path"]).read_bytes()
    if len(content) != reference["size_bytes"] or (
        "sha256:" + hashlib.sha256(content).hexdigest() != reference["digest"]
    ):
        raise ValueError("Evaluation input differs from its declared artifact")
    checkpoint = json.loads(content)
    manifest, data_digest = dataset()
    if (
        checkpoint["format"] != "cpu-bigram-v1"
        or checkpoint["alphabet"] != ALPHABET
        or checkpoint["seed"] != request["seed"]
        or checkpoint["data_manifest_digest"] != data_digest
    ):
        raise ValueError("Checkpoint contract does not match this evaluation")
    size = len(ALPHABET)
    weights = checkpoint["weights"]
    if len(weights) != size or any(len(row) != size for row in weights):
        raise ValueError("Checkpoint dimensions changed")
    if any(
        type(value) not in (float, int) or not math.isfinite(value)
        for row in weights
        for value in row
    ):
        raise ValueError("Checkpoint weights must be finite numbers")
    negative_log_probabilities = []
    for row in weights:
        maximum = max(row)
        normalizer = math.log(sum(math.exp(value - maximum) for value in row))
        negative_log_probabilities.append(
            [(normalizer - (value - maximum)) / math.log(2) for value in row]
        )
    text = corpus(manifest["validation_seed"], manifest["validation_sentences"])
    tokens = [ALPHABET.index(character) for character in text]
    bits = sum(negative_log_probabilities[a][b] for a, b in pairwise(tokens))
    value = bits / (len(text.encode("ascii")) - 1)
    report(
        request,
        measurements=[
            {
                "name": "val_bpb",
                "value": value,
                "unit": "bits_per_predicted_byte",
                "direction": "minimize",
            }
        ],
        observed_members=[manifest["panel"]],
        diagnostics={"evaluated_transitions": len(tokens) - 1},
    )
    print(f"val_bpb: {value:.6f}")


if __name__ == "__main__":
    main()
