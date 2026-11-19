"""Per-user channel standardization for a generated raw-window workload."""

import json

import torch


def _check_windows(windows: torch.Tensor) -> None:
    if windows.ndim != 3 or windows.numel() == 0 or not windows.is_floating_point():
        raise ValueError("expected nonempty floating-point [window, channel, time] data")


def fit(user: str, reference_windows: torch.Tensor) -> bytes:
    """Fit on one user's unlabeled reference windows and serialize the fitted state."""
    _check_windows(reference_windows)
    if not user:
        raise ValueError("expected a user ID")
    reference = reference_windows.detach().to(device="cpu", dtype=torch.float64)
    scale, mean = torch.std_mean(reference, dim=(0, 2), correction=0)
    scale = torch.where(scale == 0, 1.0, scale)
    return json.dumps(
        {"user": user, "mean": mean.tolist(), "scale": scale.tolist()},
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def apply(user: str, windows: torch.Tensor, fitted: bytes) -> torch.Tensor:
    """Normalize new windows with the saved state for this user."""
    _check_windows(windows)
    state = json.loads(fitted)
    if state["user"] != user:
        raise ValueError("fitted state belongs to a different user")
    mean = windows.new_tensor(state["mean"])
    scale = windows.new_tensor(state["scale"])
    if mean.shape != (windows.shape[1],) or scale.shape != mean.shape:
        raise ValueError("fitted state has a different channel layout")
    if not (mean.isfinite().all() and scale.isfinite().all() and (scale > 0).all()):
        raise ValueError("expected finite channel means and positive scales")
    return (windows - mean[:, None]) / scale[:, None]
