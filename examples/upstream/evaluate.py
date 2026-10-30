"""Independently reconstruct and score weights using only the frozen reference."""

from __future__ import annotations

import math
from pathlib import Path

from contract import (
    PANEL,
    bounded,
    digest,
    module_from_tree,
    reference_prefix,
    strict_json,
    validate_checkpoint,
)
from runtime import ROOT, fixed_imports, load, report, stage_data, tensor_schema, verify_input


def main():
    request, data, data_digest, candidate, reference, preparation, execution = load(True)
    cache = stage_data(request, data, evaluation=True)
    # Consume a private verified copy, not a path checked before a later load.
    output = Path(request["output_root"])
    weights = verify_input(request["inputs"]["weights"], output / "input.safetensors")
    metadata = verify_input(request["inputs"]["checkpoint"], output / "input.json")
    checkpoint = validate_checkpoint(
        strict_json(bounded(metadata)),
        candidate=digest(candidate),
        data=data_digest,
        weights=request["inputs"]["weights"]["digest"],
    )
    with fixed_imports(preparation, cache, execution) as prepared:
        import torch
        from safetensors.torch import load_file

        frozen = module_from_tree(
            "_frozen_upstream", ROOT / "reference/train.py", reference_prefix(reference)
        )
        torch.manual_seed(42)
        torch.cuda.manual_seed(42)
        torch.set_float32_matmul_precision("high")
        tokenizer = prepared.Tokenizer.from_directory()
        if tokenizer.get_vocab_size() != 8192:
            raise ValueError("Tokenizer vocabulary differs from the fixed model")
        config = frozen.GPTConfig(**checkpoint["config"])
        with torch.device("meta"):
            model = frozen.GPT(config)
        model.to_empty(device=torch.device("cuda"))
        model.init_weights()  # Preserves upstream mixed dtypes and non-persistent rotary buffers.
        tensors = load_file(str(weights), device="cpu")
        if tensor_schema(tensors) != checkpoint["tensors"] or tensor_schema(
            tensors
        ) != tensor_schema(model.state_dict()):
            raise ValueError("Tensor names, shapes or dtypes differ from the frozen model")
        if any(not torch.isfinite(tensor).all().item() for tensor in tensors.values()):
            raise ValueError("Model contains non-finite weights")
        model.load_state_dict(tensors, strict=True)
        model = torch.compile(model, dynamic=False)
        model.eval()
        with torch.amp.autocast(device_type="cuda", dtype=torch.bfloat16):
            score = float(prepared.evaluate_bpb(model, tokenizer, frozen.DEVICE_BATCH_SIZE))
    if not math.isfinite(score) or score < 0:
        raise ValueError("Invalid independent evaluation")
    report(
        request,
        measurements=[
            {"name": "val_bpb", "value": score, "unit": "bits_per_byte", "direction": "minimize"}
        ],
        observed_members=[PANEL],
        diagnostics={"evaluation_batch_size": 128, "evaluation_tokens": 20_971_520},
    )


if __name__ == "__main__":
    main()
