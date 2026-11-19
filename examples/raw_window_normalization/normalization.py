"""Per-user channel standardization for a generated raw-window workload."""

import hashlib
import json
import re
import sys

import torch


def _check_windows(windows: torch.Tensor) -> None:
    if windows.ndim != 3 or windows.numel() == 0 or not windows.is_floating_point():
        raise ValueError("expected nonempty floating-point [window, channel, time] data")


def reference_sha256(reference_windows: torch.Tensor) -> str:
    """Identify the dtype, shape and ordered sample bytes of an unlabeled reference."""
    _check_windows(reference_windows)
    values = reference_windows.detach().resolve_neg().cpu().contiguous()
    header = ["raw-window-reference-v1", str(values.dtype), list(values.shape)]
    digest = hashlib.sha256(json.dumps(header, separators=(",", ":")).encode("ascii") + b"\n")
    octets = values.flatten().unsqueeze(-1).view(torch.uint8)
    if sys.byteorder == "big":
        octets = octets.flip(1)
    digest.update(bytes(octets.flatten().tolist()))
    return digest.hexdigest()


def fit(user: str, reference_windows: torch.Tensor) -> bytes:
    """Fit on one user's unlabeled reference windows and serialize the fitted state."""
    _check_windows(reference_windows)
    if not user:
        raise ValueError("expected a user ID")
    reference = reference_windows.detach().to(device="cpu", copy=True)
    scale, mean = torch.std_mean(reference.to(torch.float64), dim=(0, 2), correction=0)
    scale = torch.where(scale == 0, 1.0, scale)
    return json.dumps(
        {
            "user": user,
            "reference_sha256": reference_sha256(reference),
            "mean": mean.tolist(),
            "scale": scale.tolist(),
        },
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def apply(
    user: str, windows: torch.Tensor, fitted: bytes, *, expected_reference_sha256: str | None = None
) -> torch.Tensor:
    """Normalize with state from the reference selected in the protected dataset manifest."""
    _check_windows(windows)
    state = json.loads(fitted)
    if state["user"] != user:
        raise ValueError("fitted state belongs to a different user")
    if not isinstance(expected_reference_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", expected_reference_sha256
    ):
        raise ValueError("missing or invalid expected reference fingerprint")
    if state.get("reference_sha256") is None:
        raise ValueError("fitted state has unverified reference provenance")
    if state["reference_sha256"] != expected_reference_sha256:
        raise ValueError("fitted state belongs to a different reference recording")
    mean = windows.new_tensor(state["mean"])
    scale = windows.new_tensor(state["scale"])
    if mean.shape != (windows.shape[1],) or scale.shape != mean.shape:
        raise ValueError("fitted state has a different channel layout")
    if not (mean.isfinite().all() and scale.isfinite().all() and (scale > 0).all()):
        raise ValueError("expected finite channel means and positive scales")
    return (windows - mean[:, None]) / scale[:, None]
