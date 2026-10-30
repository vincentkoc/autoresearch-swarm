"""Verify immutable handoff evidence and acknowledge a fresh stateless driver view."""

from uuid import NAMESPACE_URL, uuid5

from research_swarm.client import ResearchClient
from research_swarm.control.orchestration_models import AcknowledgeDriverStart
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.event_recovery import (
    CreateRecoverySnapshot,
    RecoverySnapshot,
    RecoverySnapshotTarget,
)
from research_swarm.domain.orchestration_transfer import (
    DriverStart,
    TransferCheckpoint,
    TransferManifestChunk,
)
from research_swarm.recovery import SnapshotReader
from research_swarm.state.database import JsonObject


def recover_driver_start(client: ResearchClient, start_id: str, *, recovery_id: str) -> None:
    row = client.get("orchestration_start", start_id)
    if row["state"] == "acknowledged":
        return
    start = DriverStart.model_validate(row["data"])
    checkpoint_row = client.get("orchestration_checkpoint", start.checkpoint_id)
    checkpoint = TransferCheckpoint.model_validate(checkpoint_row["data"])
    if (
        record_digest("research.transfer-checkpoint-v1", checkpoint.model_dump(mode="json"))
        != start.checkpoint_digest
        or checkpoint.campaign_id != start.campaign_id
        or checkpoint.watermark != start.watermark
    ):
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Startup checkpoint identity differs")
    entries: list[JsonObject] = []
    for ordinal, chunk_id in enumerate(checkpoint.manifest_chunk_ids, 1):
        chunk = TransferManifestChunk.model_validate(
            client.get("orchestration_checkpoint_chunk", chunk_id)["data"]
        )
        if chunk.checkpoint_id != start.checkpoint_id or chunk.ordinal != ordinal:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Startup manifest order differs")
        entries.extend(chunk.entries)
    if (
        len(entries) != checkpoint.entry_count
        or record_digest("research.transfer-view-v1", {"entries": entries})
        != checkpoint.manifest_digest
    ):
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Startup manifest content differs")
    snapshot_id = str(
        uuid5(NAMESPACE_URL, "research.startup-snapshot:" + start_id + ":" + recovery_id)
    )
    snapshot = RecoverySnapshot.model_validate(
        client.command(
            "stream.create_snapshot",
            CreateRecoverySnapshot(snapshot_id=snapshot_id),
            idempotency_key=snapshot_id,
        )["result"]
    )
    try:
        if snapshot.watermark < checkpoint.watermark:
            raise ResearchError(ErrorCode.REVISION_CONFLICT, "Startup snapshot predates transfer")
        reader = SnapshotReader(client, snapshot)
        for _ in reader.items():
            pass
        if not reader.completed:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Startup snapshot is incomplete")
        client.command(
            "orchestration.acknowledge_start",
            AcknowledgeDriverStart(
                start_id=start_id,
                checkpoint_digest=start.checkpoint_digest,
                observed_watermark=snapshot.watermark,
            ),
            idempotency_key=record_digest(
                "research.driver-start-v1", {"start_id": start_id, "snapshot_id": snapshot_id}
            ),
            expected_revision=row["revision"],
        )
    finally:
        client.command(
            "stream.release_snapshot",
            RecoverySnapshotTarget(snapshot_id=snapshot_id),
            idempotency_key="release-" + snapshot_id,
        )
