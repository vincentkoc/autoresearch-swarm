"""Protected workload I/O and training configuration."""

import hashlib
import json
import os
from pathlib import Path


def context():
    value = json.loads(Path(os.environ["RESEARCH_CONTEXT"]).read_bytes())
    if value["schema_version"] != "1.0" or not value["context_digest"]:
        raise ValueError("Unsupported research context")
    Path(value["output_root"]).mkdir(parents=True, exist_ok=True)
    return value


def read_input(request, name):
    reference = request["inputs"][name]
    if reference["size_bytes"] > 32 * 1024 * 1024:
        raise ValueError("Training input exceeds the bounded profile")
    with Path(reference["path"]).open("rb") as stream:
        content = stream.read(reference["size_bytes"] + 1)
    if (
        len(content) != reference["size_bytes"]
        or "sha256:" + hashlib.sha256(content).hexdigest() != reference["digest"]
    ):
        raise ValueError("Training input differs from its declared artifact")
    return content


def settings():
    return json.loads(Path(__file__).with_name("training.json").read_bytes())


def report(request, **fields):
    value = {
        "schema_version": "1.0",
        "context_digest": request["context_digest"],
        "observed_seed": request["seed"],
        **fields,
    }
    destination = Path(request["report_path"])
    pending = destination.with_suffix(".pending")
    pending.write_text(json.dumps(value, allow_nan=False), encoding="utf-8")
    os.replace(pending, destination)
