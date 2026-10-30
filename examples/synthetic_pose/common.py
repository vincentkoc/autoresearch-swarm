"""Protected project protocol and identities; no research-framework imports."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent


def encode(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def context() -> dict[str, Any]:
    request: dict[str, Any] = json.loads(Path(os.environ["RESEARCH_CONTEXT"]).read_bytes())
    if request["schema_version"] != "1.0" or not request["context_digest"]:
        raise ValueError("Unsealed workload context")
    Path(request["output_root"]).mkdir(parents=True, exist_ok=True)
    return request


def data() -> tuple[dict[str, Any], dict[str, str]]:
    raw = (ROOT / "dataset.json").read_bytes()
    dataset: dict[str, Any] = json.loads(raw)
    if dataset["units"] != "radians":
        raise ValueError("Synthetic targets require radians")
    return dataset, {
        "batch_sha256": sha256(raw),
        "partition_sha256": sha256(encode(dataset["partition"])),
        "manifest_sha256": sha256((ROOT / "research.toml").read_bytes()),
        "evaluator_sha256": sha256((ROOT / "evaluate.py").read_bytes()),
    }


def verified_input(request: dict[str, Any], name: str) -> tuple[dict[str, Any], str]:
    reference = request["inputs"][name]
    raw = Path(reference["path"]).read_bytes()
    digest = sha256(raw)
    if len(raw) != reference["size_bytes"] or "sha256:" + digest != reference["digest"]:
        raise ValueError("Input differs from its artifact identity")
    return json.loads(raw), digest


def output(request: dict[str, Any], name: str, value: dict[str, Any]) -> dict[str, str]:
    path = name + ".json"
    (Path(request["output_root"]) / path).write_bytes(encode(value))
    return {"name": name, "path": path, "media_type": "application/json"}


def report(request: dict[str, Any], **fields: Any) -> None:
    content = {
        "schema_version": "1.0",
        "context_digest": request["context_digest"],
        "observed_seed": request["seed"],
        **fields,
    }
    destination = Path(request["report_path"])
    pending = destination.with_suffix(".pending")
    pending.write_bytes(encode(content))
    os.replace(pending, destination)
