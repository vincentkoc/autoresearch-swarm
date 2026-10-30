"""Protected data and file protocol helpers. No research framework dependency."""

from __future__ import annotations

import hashlib
import json
import os
import random
from pathlib import Path
from typing import Any

ALPHABET = " \n,.abcdefghijklmnopqrstuvwxyz"
ROOT = Path(__file__).resolve().parent


def context() -> dict[str, Any]:
    with Path(os.environ["RESEARCH_CONTEXT"]).open(encoding="utf-8") as stream:
        value: dict[str, Any] = json.load(stream)
    if value["schema_version"] != "1.0" or not value["context_digest"]:
        raise ValueError("Unsupported or unsealed research context")
    Path(value["output_root"]).mkdir(parents=True, exist_ok=True)
    return value


def dataset() -> tuple[dict[str, Any], str]:
    content = (ROOT / "dataset.json").read_bytes()
    return json.loads(content), "sha256:" + hashlib.sha256(content).hexdigest()


def corpus(seed: int, sentences: int) -> str:
    generator = random.Random(seed)
    subjects = ["fox", "bird", "cat", "dog", "hare"]
    colors = ["red", "brown", "grey", "white", "black"]
    places = ["garden", "forest", "hill", "field", "river"]
    result = []
    for _ in range(sentences):
        result.append(
            f"the {generator.choice(colors)} {generator.choice(subjects)} "
            f"walks by the {generator.choice(places)}.\n"
        )
    return "".join(result)


def report(value: dict[str, Any], **fields: Any) -> None:
    result = {
        "schema_version": "1.0",
        "context_digest": value["context_digest"],
        "observed_seed": value["seed"],
        **fields,
    }
    destination = Path(value["report_path"])
    pending = destination.with_suffix(".pending")
    pending.write_text(json.dumps(result, allow_nan=False), encoding="utf-8")
    os.replace(pending, destination)
