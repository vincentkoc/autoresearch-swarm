"""Price a dispatch estimate and measured attempt usage in campaign currency."""

from __future__ import annotations

import math
from decimal import Decimal

from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Trial

PRICE_FIELDS = (
    "price_per_hour",
    "price_per_million_input_tokens",
    "price_per_million_output_tokens",
)


def _price(spec: CampaignSpec, seconds: float, input_tokens: int, output_tokens: int) -> float:
    rates = (
        spec.price_per_hour or 0.0,
        spec.price_per_million_input_tokens or 0.0,
        spec.price_per_million_output_tokens or 0.0,
    )
    if any(not math.isfinite(n) or n < 0 for n in (*rates, seconds)):
        raise ValueError("elapsed time and prices must be finite and nonnegative")
    if any(type(n) is not int or n < 0 for n in (input_tokens, output_tokens)):
        raise ValueError("token counts must be nonnegative integers")
    hourly, input_rate, output_rate = (Decimal(str(n)) for n in rates)
    compute = hourly * Decimal(str(seconds)) / 3600
    tokens = (input_rate * input_tokens + output_rate * output_tokens) / 1_000_000
    total = float(compute + tokens)
    if not math.isfinite(total):
        raise ValueError("attempt cost exceeds the supported range")
    return total


class CostMeter:
    """Use the configured hourly rate per billable slot and per-million token prices."""

    def estimate(self, spec: CampaignSpec, trial: Trial) -> float:
        return _price(
            spec,
            spec.trial_timeout_seconds,
            spec.estimated_input_tokens,
            spec.estimated_output_tokens,
        )

    def charge(
        self,
        spec: CampaignSpec,
        attempt: Attempt,
        wall_seconds: float,
        input_tokens: int,
        output_tokens: int,
    ) -> float:
        return _price(spec, wall_seconds, input_tokens, output_tokens)
