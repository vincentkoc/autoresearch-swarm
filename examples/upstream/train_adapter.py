"""Run the fixed upstream loop; its printed evaluation is diagnostic only."""

from __future__ import annotations

import json
import runpy
from dataclasses import asdict
from pathlib import Path

from contract import SOURCE_HASHES, digest, validate_checkpoint
from runtime import ROOT, file_digest, fixed_imports, load, report, stage_data, tensor_schema


def main():
    request, data, data_digest, candidate, _, preparation, execution = load(False)
    cache = stage_data(request, data, evaluation=False)
    with fixed_imports(preparation, cache, execution):
        namespace = runpy.run_path(str(ROOT / "train.py"), run_name="__main__")
    import torch
    from safetensors.torch import save_file

    model = namespace["model"]._orig_mod
    tensors = {
        name: tensor.detach().cpu().contiguous() for name, tensor in model.state_dict().items()
    }
    if any(not torch.isfinite(tensor).all().item() for tensor in tensors.values()):
        raise ValueError("Cannot export non-finite model state")
    output = Path(request["output_root"])
    weights = output / "weights.safetensors"
    save_file(tensors, str(weights))
    checkpoint = {
        "format": "karpathy-lr-safetensors-v1",
        "seed": request["seed"],
        "config": asdict(namespace["config"]),
        "candidate_digest": digest(candidate),
        "reference_digest": SOURCE_HASHES["train.py"],
        "data_digest": data_digest,
        "weights_digest": file_digest(weights),
        "optimizer_steps": namespace["step"],
        "active_training_ms": int(namespace["total_training_time"] * 1000),
        "tensors": tensor_schema(tensors),
    }
    validate_checkpoint(
        checkpoint,
        candidate=digest(candidate),
        data=data_digest,
        weights=checkpoint["weights_digest"],
    )
    (output / "checkpoint.json").write_text(
        json.dumps(checkpoint, allow_nan=False), encoding="utf-8"
    )
    report(
        request,
        outputs=[
            {"name": "weights", "path": "weights.safetensors"},
            {"name": "checkpoint", "path": "checkpoint.json", "media_type": "application/json"},
        ],
        phase_timings_ms={
            "active_training": checkpoint["active_training_ms"],
            "startup": int(namespace["startup_time"] * 1000),
        },
        diagnostics={
            "optimizer_steps": checkpoint["optimizer_steps"],
            "excluded_warmup_updates": 11,
            "upstream_printed_score": float(namespace["val_bpb"]),
        },
    )


if __name__ == "__main__":
    main()
