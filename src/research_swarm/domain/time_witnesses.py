"""Native time witness records. Authentication does not grant execution authority."""

from typing import Literal

from pydantic import Field, model_validator

from research_swarm.domain.assignments import PublicKeyHex, SignatureHex
from research_swarm.domain.canonical import MAX_SAFE_INTEGER, record_digest
from research_swarm.domain.clocks import ClockMilliseconds, ClockPolicy
from research_swarm.domain.types import ID, Digest, Name, VersionedRecord


class WitnessAuthorityScope(VersionedRecord):
    state_authority_id: Digest
    activation_id: ID
    workspace_id: ID
    controller_id: ID
    control_epoch: int = Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)
    assignment_key_id: Digest


class WitnessScope(WitnessAuthorityScope):
    provider_generation: ID

    @property
    def authority(self) -> WitnessAuthorityScope:
        return WitnessAuthorityScope.model_validate(
            self.model_dump(mode="json", exclude={"provider_generation"})
        )


class TimeWitnessRequest(VersionedRecord):
    protocol: Literal["native-time-witness-v1"] = "native-time-witness-v1"
    scope: WitnessScope
    operation_id: ID
    request_digest: Digest
    nonce: ID


def witness_key_id(public_key: str) -> str:
    return record_digest(
        "research.time-witness-public-key", {"algorithm": "ed25519", "public_key": public_key}
    )


class WitnessSource(VersionedRecord):
    """Pinned source configuration; its admission is separate from wire validity."""

    id: ID
    public_key: PublicKeyHex
    key_id: Digest
    native_profile: Name
    policy: ClockPolicy

    @model_validator(mode="after")
    def identified(self) -> "WitnessSource":
        if self.key_id != witness_key_id(self.public_key):
            raise ValueError("Witness source key identity differs")
        return self


class TimeWitnessPayload(VersionedRecord):
    request: TimeWitnessRequest
    source_id: ID
    source_epoch: ID
    native_profile: Name
    policy: ClockPolicy
    authority_time_ms: ClockMilliseconds


class SignedTimeWitness(VersionedRecord):
    algorithm: Literal["ed25519-jcs-v1"] = "ed25519-jcs-v1"
    key_id: Digest
    payload: TimeWitnessPayload
    signature: SignatureHex
