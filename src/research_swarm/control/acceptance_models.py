"""Controller acceptance authority is separate from signed native execution authority."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from research_swarm.domain.clocks import ClockMode
from research_swarm.domain.types import ID, Digest, Revision, Timestamp, VersionedRecord
from research_swarm.domain.workers import WorkerBinding


class AcceptanceLease(VersionedRecord):
    attempt_id: ID
    binding: WorkerBinding
    dispatch_digest: Digest
    policy_version: Literal["native-window-v1", "legacy-unqualified-v1"]
    clock_mode: ClockMode
    clock_epoch: ID | None = None
    clock_policy_digest: Digest | None = None
    issued_at: Timestamp | None = None
    observed_at: Timestamp | None = None
    expires_at: Timestamp | None = None
    renew_until: Timestamp | None = None
    last_heartbeat_sequence: Revision = 0
    disposition: str = Field(default="", max_length=256)

    @model_validator(mode="after")
    def complete_authority(self) -> AcceptanceLease:
        times = (self.issued_at, self.observed_at, self.expires_at, self.renew_until)
        if self.policy_version == "native-window-v1":
            if any(value is None for value in times):
                raise ValueError("Issued acceptance leases require complete time bounds")
            assert self.issued_at is not None and self.observed_at is not None
            assert self.expires_at is not None and self.renew_until is not None
            if not self.issued_at <= self.observed_at < self.expires_at <= self.renew_until:
                raise ValueError("Acceptance lease time bounds are inconsistent")
            if self.clock_mode == "bounded-continuous-v1" and (
                self.clock_epoch is None or self.clock_policy_digest is None
            ):
                raise ValueError("Calibrated acceptance requires its issuing clock identity")
        elif any(value is not None for value in times):
            raise ValueError("Legacy attempts cannot acquire invented historical lease times")
        return self


class LeaseRenewal(VersionedRecord):
    attempt_id: ID
    dispatch_digest: Digest
    expected_lease_revision: Revision


class ExpireAcceptance(VersionedRecord):
    attempt_id: ID
