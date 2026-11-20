"""Torch is an optional dependency, loaded only by the two training workloads."""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import cast

import torch
from torch import Tensor, nn
from torch.nn import functional as F

from benchmarks.suite import Candidate, panel_seed, panel_size

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


RATES_V2 = ("0.001", "0.002", "0.004", "0.008", "0.016", "0.032", "0.064")
MLP_V2 = {
    **MLP_SPACE,
    "learning_rate": RATES_V2,
    "depth": ("1", "2", "3"),
    "optimizer": ("AdamW", "SGD"),
    "weight_decay": ("0", "0.001", "0.01"),
}
TRAINING_V2 = {
    **TRAINING_SPACE,
    "rate": tuple(f"base_lr = {rate}" for rate in RATES_V2),
    "loss": tuple(
        f"return cross_entropy(logits, targets, label_smoothing={smoothing})"
        for smoothing in ("0", "0.03", "0.1", "0.2")
    ),
    "clip": (
        "return None",
        *(f"clip_grad_norm_(model.parameters(), {cap})" for cap in ("0.5", "1", "2")),
    ),
}

Configure = Callable[[nn.Module], tuple[torch.optim.Optimizer, Callable[[float], float]]]


Loss = Callable[[Tensor, Tensor], Tensor]
Clip = Callable[[nn.Module], None]


def _training_code(candidate: Candidate, protocol: str = "v1") -> tuple[Configure, Loss, Clip]:
    # Only checked-in lines can enter this function. External model output selects
    # the edits. The search history retains their exact text for replay.
    space = TRAINING_SPACE if protocol != "v2" else TRAINING_V2
    if candidate.keys() != space.keys() or any(
        candidate[key] not in lines for key, lines in space.items()
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
    loss_line = candidate.get("loss", "return cross_entropy(logits, targets)")
    clip_line = candidate.get("clip", "return None")
    source += f"def loss(logits, targets):\n    {loss_line}\ndef clip(model):\n    {clip_line}\n"
    scope = {
        "cross_entropy": F.cross_entropy,
        "clip_grad_norm_": nn.utils.clip_grad_norm_,
        "__builtins__": {},
        "AdamW": torch.optim.AdamW,
        "Adam": torch.optim.Adam,
        "SGD": torch.optim.SGD,
        "cos": math.cos,
        "pi": math.pi,
    }
    exec(compile(source, "<benchmark-training-edit>", "exec"), scope)
    return cast(Configure, scope["configure"]), cast(Loss, scope["loss"]), cast(Clip, scope["clip"])


def _points(stream: int, count: int, device: str) -> tuple[Tensor, Tensor]:
    generator = torch.Generator().manual_seed(stream)
    x = torch.randn(count, 4, generator=generator).to(device)
    return x, (x[:, 0] * x[:, 1] >= 0).long()


def mlp_accuracy(
    candidate: Candidate,
    seed: int,
    panel: str,
    device: str,
    size: str,
    fidelity: float,
    *,
    protocol: str = "v1",
    on_work: Callable[[], None] | None = None,
) -> float:
    steps = 24 if size == "small" else 192
    count = panel_size("mlp", size, "train")
    torch.manual_seed(seed)
    width = int(candidate["width"])
    activation = {"relu": nn.ReLU, "gelu": nn.GELU, "tanh": nn.Tanh}[candidate["activation"]]
    layers: list[nn.Module] = [nn.Linear(4, width), activation()]
    for _ in range(1, int(candidate.get("depth", "1"))):
        layers.extend([nn.Linear(width, width), activation()])
    model = nn.Sequential(*layers, nn.Linear(width, 2)).to(device)
    lr = float(candidate["learning_rate"])
    decay = float(candidate.get("weight_decay", "0"))
    optimizer: torch.optim.Optimizer
    if protocol != "v2":
        optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    elif candidate["optimizer"] == "SGD":
        optimizer = torch.optim.SGD(model.parameters(), lr=lr, weight_decay=decay, momentum=0.9)
    else:
        optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=decay)
    x, y = _points(panel_seed(protocol, "train"), count, device)
    generator = torch.Generator().manual_seed(seed)
    for _ in range(math.ceil(steps * fidelity)):
        indices = torch.randint(count, (64,), generator=generator).to(device)
        optimizer.zero_grad()
        torch.autograd.backward(F.cross_entropy(model(x[indices]), y[indices]))
        optimizer.step()
        if on_work is not None:
            on_work()
    with torch.inference_mode():
        x, y = _points(panel_seed(protocol, panel), panel_size("mlp", size, panel), device)
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


def _sequences(stream: int, count: int, context: int, device: str) -> Tensor:
    # Synthetic modular walks with occasional random jumps. Training and all
    # evaluation splits have independent, fixed generators.
    generator = torch.Generator().manual_seed(stream)
    start = torch.randint(16, (count, 1), generator=generator)
    stride = torch.randint(1, 5, (count, 1), generator=generator)
    jumps = torch.randint(16, (count, context), generator=generator)
    breaks = torch.rand(count, context, generator=generator) < 0.08
    deltas = torch.where(breaks, jumps, stride)
    return torch.cat([start, deltas], dim=1).cumsum(dim=1).remainder(16).to(device)


def _next_token_loss(model: TinyGPT, batch: Tensor, loss_fn: Loss = F.cross_entropy) -> Tensor:
    logits = model(batch[:, :-1])
    return loss_fn(logits.reshape(-1, 16), batch[:, 1:].reshape(-1))


def nanogpt_loss(
    candidate: Candidate,
    seed: int,
    panel: str,
    device: str,
    size: str,
    fidelity: float,
    *,
    protocol: str = "v1",
    on_work: Callable[[], None] | None = None,
) -> float:
    context, width, steps, batch_size = (12, 24, 12, 16) if size == "small" else (48, 96, 160, 64)
    torch.manual_seed(seed)
    model = TinyGPT(context, width).to(device)
    configure, loss_fn, clip = _training_code(candidate, protocol)
    optimizer, schedule = configure(model)
    train = _sequences(
        panel_seed(protocol, "train"), panel_size("nanogpt", size, "train"), context, device
    )
    generator = torch.Generator().manual_seed(seed)
    for step in range(math.ceil(steps * fidelity)):
        indices = torch.randint(len(train), (batch_size,), generator=generator).to(device)
        for group in optimizer.param_groups:
            group["lr"] = schedule(step / steps)
        optimizer.zero_grad()
        torch.autograd.backward(_next_token_loss(model, train[indices], loss_fn))
        clip(model)
        optimizer.step()
        if on_work is not None:
            on_work()
    with torch.inference_mode():
        validation = _sequences(
            panel_seed(protocol, panel), panel_size("nanogpt", size, panel), context, device
        )
        return float(_next_token_loss(model, validation).item())
