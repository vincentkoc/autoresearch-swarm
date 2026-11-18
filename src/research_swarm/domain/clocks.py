"""Authenticated authority-time records; time evidence alone never grants execution."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.types import VersionedRecord

ClockMilliseconds = Annotated[int, Field(strict=True, ge=0, le=2**53 - 1)]


class ClockPolicy(VersionedRecord):
    """Admitted ratio bound: controller elapsed / worker elapsed is within 1 +/- ppm.

    This is not a per-clock accuracy claim or measured hardware qualification.
    """

    version: Literal["bounded-continuous-v1"] = "bounded-continuous-v1"
    maximum_roundtrip_ms: int = Field(default=1000, strict=True, ge=1, le=10_000)
    maximum_sample_age_ms: int = Field(default=30_000, strict=True, ge=1, le=300_000)
    maximum_uncertainty_ms: int = Field(default=2500, strict=True, ge=1, le=10_000)
    maximum_deadline_horizon_ms: int = Field(default=3_600_000, strict=True, ge=1, le=86_400_000)
    relative_drift_ppm: int = Field(default=1000, strict=True, ge=1, le=10_000)
    timestamp_guard_ms: int = Field(default=2, strict=True, ge=1, le=100)

    @property
    def digest(self) -> str:
        return record_digest("research.clock-policy", self.model_dump(mode="json"))
