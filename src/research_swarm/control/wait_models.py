"""Wait registration, timer reconciliation and delivery acknowledgment commands."""

from pydantic import Field

from research_swarm.domain.orchestration_waits import WaitCondition
from research_swarm.domain.types import ID, Digest, VersionedRecord


class RecordOrchestrationWait(VersionedRecord):
    wait_id: ID
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
    configuration_digest: Digest
    condition: WaitCondition


class OrchestrationWaitTarget(VersionedRecord):
    wait_id: ID


class AcknowledgeWakeup(VersionedRecord):
    delivery_id: ID
    campaign_id: ID
    epoch: int = Field(strict=True, ge=1)
