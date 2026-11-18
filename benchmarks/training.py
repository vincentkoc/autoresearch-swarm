"""Torch is an optional dependency, loaded only by the two training workloads."""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import cast

import torch
from torch import Tensor, nn
from torch.nn import functional as F

from benchmarks.suite import PANEL_SEEDS, Candidate

MLP_SPACE = {
    "width": ("8", "16", "32", "64"),
    "activation": ("tanh", "relu", "gelu"),
    "learning_rate": ("0.001", "0.005", "0.02", "0.05"),
}

TRAINING_SPACE = {
    "optimizer": (
        "optimizer = AdamW(model.parameters(), lr=base_lr)",
        "optimizer = Adam(model.parameters(), lr=base_lr)",
        "optimizer = SGD(model.parameters(), lr=base_lr, momentum=0.9)",
    ),
    "rate": tuple(f"base_lr = {rate}" for rate in ("0.001", "0.005", "0.02", "0.04")),
    "schedule": (
        "return base_lr",
        "return base_lr * (1 - progress)",
        "return base_lr * (0.1 + 0.9 * (1 + cos(pi * progress)) / 2)",
    ),
}


Configure = Callable[[nn.Module], tuple[torch.optim.Optimizer, Callable[[float], float]]]


def _training_code(candidate: Candidate) -> Configure:
    # Only checked-in lines can enter this function. External model output selects
    # the edits. The search history retains their exact text for replay.
    if candidate.keys() != TRAINING_SPACE.keys() or any(
        candidate[key] not in lines for key, lines in TRAINING_SPACE.items()
    ):
        raise ValueError("unknown training code edit")
    source = (
        "def configure(model):\n"
        f"    {candidate['rate']}\n"
        f"    {candidate['optimizer']}\n"
        "    def schedule(progress):\n"
        f"        {candidate['schedule']}\n"
        "    return optimizer, schedule\n"
    )
    scope = {
        "__builtins__": {},
        "AdamW": torch.optim.AdamW,
        "Adam": torch.optim.Adam,
        "SGD": torch.optim.SGD,
        "cos": math.cos,
        "pi": math.pi,
    }
    exec(compile(source, "<benchmark-training-edit>", "exec"), scope)
    return cast(Configure, scope["configure"])


def _points(panel: str, count: int, device: str) -> tuple[Tensor, Tensor]:
    generator = torch.Generator().manual_seed(PANEL_SEEDS[panel])
    x = torch.randn(count, 4, generator=generator).to(device)
    return x, (x[:, 0] * x[:, 1] >= 0).long()


def mlp_accuracy(
    candidate: Candidate, seed: int, panel: str, device: str, size: str, fidelity: float
) -> float:
    steps, count = (24, 256) if size == "small" else (192, 4096)
    torch.manual_seed(seed)
    width = int(candidate["width"])
    activation = {"relu": nn.ReLU, "gelu": nn.GELU, "tanh": nn.Tanh}[candidate["activation"]]
    model = nn.Sequential(nn.Linear(4, width), activation(), nn.Linear(width, 2)).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=float(candidate["learning_rate"]))
    x, y = _points("train", count, device)
    generator = torch.Generator().manual_seed(seed)
    for _ in range(math.ceil(steps * fidelity)):
        indices = torch.randint(count, (64,), generator=generator).to(device)
        optimizer.zero_grad()
        torch.autograd.backward(F.cross_entropy(model(x[indices]), y[indices]))
        optimizer.step()
    with torch.inference_mode():
        x, y = _points(panel, count, device)
        return float((model(x).argmax(dim=-1) == y).float().mean().item())


class TinyGPT(nn.Module):
    """One pre-norm causal attention block and a next-token prediction head."""

    def __init__(self, context: int, width: int):
        super().__init__()
        self.token = nn.Embedding(16, width)
        self.position = nn.Parameter(torch.zeros(context, width))
        self.norm = nn.LayerNorm(width)
        self.qkv = nn.Linear(width, 3 * width)
        self.proj = nn.Linear(width, width)
        self.mlp = nn.Sequential(
            nn.LayerNorm(width), nn.Linear(width, 2 * width), nn.GELU(), nn.Linear(2 * width, width)
        )
        self.head = nn.Sequential(nn.LayerNorm(width), nn.Linear(width, 16))
        self.register_buffer("mask", torch.ones(context, context, dtype=torch.bool).triu(1))

    def forward(self, tokens: Tensor) -> Tensor:
        x = self.token(tokens) + self.position
        q, k, v = self.qkv(self.norm(x)).chunk(3, dim=-1)
        scores = (q @ k.transpose(-2, -1)) / math.sqrt(q.size(-1))
        attention = scores.masked_fill(self.mask, float("-inf")).softmax(dim=-1)
        x = x + self.proj(attention @ v)
        return cast(Tensor, self.head(x + self.mlp(x)))


def _sequences(panel: str, count: int, context: int, device: str) -> Tensor:
    # Synthetic modular walks with occasional random jumps. Training and all
    # evaluation splits have independent, fixed generators.
    generator = torch.Generator().manual_seed(PANEL_SEEDS[panel])
    start = torch.randint(16, (count, 1), generator=generator)
    stride = torch.randint(1, 5, (count, 1), generator=generator)
    jumps = torch.randint(16, (count, context), generator=generator)
    breaks = torch.rand(count, context, generator=generator) < 0.08
    deltas = torch.where(breaks, jumps, stride)
    return torch.cat([start, deltas], dim=1).cumsum(dim=1).remainder(16).to(device)


def _next_token_loss(model: TinyGPT, batch: Tensor) -> Tensor:
    logits = model(batch[:, :-1])
    return F.cross_entropy(logits.reshape(-1, 16), batch[:, 1:].reshape(-1))


def nanogpt_loss(
    candidate: Candidate, seed: int, panel: str, device: str, size: str, fidelity: float
) -> float:
    context, width, steps, batch_size = (12, 24, 12, 16) if size == "small" else (48, 96, 160, 64)
    torch.manual_seed(seed)
    model = TinyGPT(context, width).to(device)
    optimizer, schedule = _training_code(candidate)(model)
    train = _sequences("train", 8 * batch_size, context, device)
    generator = torch.Generator().manual_seed(seed)
    for step in range(math.ceil(steps * fidelity)):
        indices = torch.randint(len(train), (batch_size,), generator=generator).to(device)
        for group in optimizer.param_groups:
            group["lr"] = schedule(step / steps)
        optimizer.zero_grad()
        torch.autograd.backward(_next_token_loss(model, train[indices]))
        optimizer.step()
    with torch.inference_mode():
        validation = _sequences(panel, 2 * batch_size, context, device)
        return float(_next_token_loss(model, validation).item())
