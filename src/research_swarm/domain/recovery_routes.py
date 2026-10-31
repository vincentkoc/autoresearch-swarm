"""Public recovery destinations bind an original worker to one elected target."""

from typing import Literal

from pydantic import Field

from research_swarm.domain.backup import RecoveryAdmission, RestoreReceipt
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.checkpoint_elections import SignedLogicalElection
from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.domain.workers import WorkerBinding


class RecoveryRouteRequest(VersionedRecord):
    route_id: ID
    attempt_id: ID
    credential_id: ID
    source_controller_url: str = Field(min_length=1, max_length=2048)
    destination_url: str = Field(min_length=1, max_length=2048)


class WorkerRecoveryRoute(RecoveryRouteRequest):
    purpose: Literal["original-journal-recovery-v1"] = "original-journal-recovery-v1"
    binding: WorkerBinding
    worker_data_digest: Digest
    principal_id: ID
    restoration: RestoreReceipt
    admission: RecoveryAdmission

    @property
    def digest(self) -> str:
        return record_digest("research.worker-recovery-route.v1", self.model_dump(mode="json"))


class SignedWorkerRecoveryRoute(VersionedRecord):
    route: WorkerRecoveryRoute
    target_signature: str = Field(pattern=r"^[0-9a-f]{128}$")
    logical_election: SignedLogicalElection | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @property
    def digest(self) -> str:
        return record_digest(
            "research.signed-worker-recovery-route.v1", self.model_dump(mode="json")
        )
