"""Reconcile and change driver ownership without moving scientific or native authority."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import ExitStack
from uuid import NAMESPACE_URL, uuid5

from research_swarm.control.orchestration_models import (
    AcknowledgeDriverStart,
    TransferOrchestration,
)
from research_swarm.control.transfer_view import TransferView, inspect_transfer
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.orchestration import DriveTurn, OrchestrationBinding, OrchestrationOwner
from research_swarm.domain.orchestration_transfer import (
    MAX_CHUNK_BYTES,
    MAX_CHUNK_ENTRIES,
    MAX_MANIFEST_CHUNKS,
    DriverStart,
    DriverStartAcknowledgment,
    OrchestrationTransfer,
    TransferCheckpoint,
    TransferManifestChunk,
)
from research_swarm.domain.types import SCHEMA_VERSION, utc_now
from research_swarm.state.artifacts import ArtifactService
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.drive_closures import (
    close_unacknowledged,
    settle_acknowledged,
    target_acknowledgment,
)
from research_swarm.state.waits import fence_wait, watermark


def transfer_identifier(transfer_id: str, suffix: str) -> str:
    return str(uuid5(NAMESPACE_URL, "research-swarm:transfer:" + transfer_id + ":" + suffix))


def manifest_chunks(
    checkpoint_id: str, entries: tuple[JsonObject, ...]
) -> Iterator[TransferManifestChunk]:
    """Preserve manifest order while bounding both record count and encoded bytes."""
    ordinal = 1
    pending: list[JsonObject] = []

    def envelope_size() -> int:
        return len(
            canonical_bytes(
                {
                    "schema_version": SCHEMA_VERSION,
                    "checkpoint_id": checkpoint_id,
                    "ordinal": ordinal,
                    "entries": [],
                }
            )
        )

    size = envelope_size()
    for entry in entries:
        entry_size = len(canonical_bytes(entry))
        if pending and (
            len(pending) == MAX_CHUNK_ENTRIES or size + entry_size + 1 > MAX_CHUNK_BYTES
        ):
            yield TransferManifestChunk(
                checkpoint_id=checkpoint_id, ordinal=ordinal, entries=tuple(pending)
            )
            ordinal += 1
            pending = []
            size = envelope_size()
        if ordinal > MAX_MANIFEST_CHUNKS or size + entry_size > MAX_CHUNK_BYTES:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                "Transfer manifest exceeds the local chunk profile bound",
            )
        size += entry_size + bool(pending)
        pending.append(entry)
    if pending:
        yield TransferManifestChunk(
            checkpoint_id=checkpoint_id, ordinal=ordinal, entries=tuple(pending)
        )


def previous_transfer(transaction: Transaction, transfer_id: str, digest: str) -> JsonObject | None:
    try:
        row = transaction.get("orchestration_transfer", transfer_id)
    except ResearchError as error:
        if error.code == ErrorCode.NOT_FOUND:
            return None
        raise
    if row["data"]["request_digest"] != digest:
        raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Transfer identity has another intent")
    return row


def transfer_request_digest(context: CommandContext, intent: TransferOrchestration) -> str:
    return record_digest(
        "research.orchestration-transfer-request-v1",
        {"payload": intent.model_dump(mode="json"), "expected_revision": context.expected_revision},
    )


class OrchestrationTransferControl:
    def __init__(self, database: Database, artifacts: ArtifactService) -> None:
        self.database, self.artifacts = database, artifacts

    def transfer(self, context: CommandContext, intent: TransferOrchestration) -> JsonObject:
        for scope in ("read", "orchestration.manage", "orchestration.drive"):
            context.principal.require(scope)
        payload = intent.model_dump(mode="json")
        digest = transfer_request_digest(context, intent)

        def prepare() -> JsonObject:
            view = self.database.store.read(
                lambda connection: self._prepare_in(
                    Transaction(connection, context), intent, digest
                )
            )
            with ExitStack() as pins:
                if view is not None:
                    for reference in view.artifacts:
                        pins.enter_context(self.artifacts.pinned(context, reference))
                return self.database.execute(
                    context,
                    "orchestration.transfer",
                    payload,
                    "orchestration.manage",
                    lambda transaction: self._commit(transaction, intent, digest, view),
                )

        return self.database.with_preflight(
            context,
            "orchestration.transfer",
            payload,
            "orchestration.manage",
            prepare,
        )

    def _prepare_in(
        self, transaction: Transaction, intent: TransferOrchestration, digest: str
    ) -> TransferView | None:
        """Select one transfer view before adapter-specific artifact verification."""
        for scope in ("read", "orchestration.manage", "orchestration.drive"):
            transaction.context.principal.require(scope)
        if transaction.connection.execute(
            "SELECT 1 FROM records WHERE workspace_id=? AND kind='orchestration_owner' AND id=?",
            (transaction.workspace_id, transaction.context.principal.id),
        ).fetchone():
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "A driver cannot transfer its own authority"
            )
        existing = previous_transfer(transaction, intent.transfer_id, digest)
        return None if existing else inspect_transfer(transaction, intent)

    def _commit(
        self,
        transaction: Transaction,
        intent: TransferOrchestration,
        digest: str,
        verified: TransferView | None,
    ) -> JsonObject:
        previous = previous_transfer(transaction, intent.transfer_id, digest)
        if previous is not None:
            return previous
        current = inspect_transfer(transaction, intent)
        if (
            verified is None
            or current.digest != verified.digest
            or current.artifacts != verified.artifacts
        ):
            raise ResearchError(
                ErrorCode.REVISION_CONFLICT,
                "Transfer evidence changed during artifact verification",
            )
        binding = OrchestrationBinding.model_validate(current.binding["data"])
        now = utc_now()
        workspace = transaction.connection.execute(
            "SELECT controller_id,control_epoch FROM workspaces WHERE id=?",
            (transaction.workspace_id,),
        ).fetchone()
        assert workspace is not None
        chunks = []
        for chunk in manifest_chunks(intent.transfer_id, current.entries):
            chunk_id = transfer_identifier(intent.transfer_id, "chunk:" + str(chunk.ordinal))
            transaction.insert(
                "orchestration_checkpoint_chunk",
                chunk_id,
                chunk.model_dump(mode="json"),
                state="immutable",
                campaign_id=intent.campaign_id,
                parent_id=intent.transfer_id,
            )
            chunks.append(chunk_id)
        checkpoint = TransferCheckpoint(
            workspace_id=transaction.workspace_id,
            campaign_id=intent.campaign_id,
            controller_id=workspace["controller_id"],
            control_epoch=workspace["control_epoch"],
            watermark=watermark(transaction),
            campaign_revision=current.campaign["revision"],
            campaign_contract_digest=current.campaign["data"]["contract_digest"],
            binding_before=binding,
            manifest_digest=current.digest,
            manifest_chunk_ids=tuple(chunks),
            entry_count=len(current.entries),
            artifact_manifest_digest=current.artifact_digest,
            artifact_count=len(current.artifacts),
            captured_by=transaction.context.principal.id,
            captured_at=now,
        )
        checkpoint_digest = record_digest(
            "research.transfer-checkpoint-v1", checkpoint.model_dump(mode="json")
        )
        transaction.insert(
            "orchestration_checkpoint",
            intent.transfer_id,
            checkpoint.model_dump(mode="json"),
            state="immutable",
            campaign_id=intent.campaign_id,
        )
        transaction.insert(
            "orchestration_binding_history",
            intent.transfer_id,
            current.binding,
            state="immutable",
            campaign_id=intent.campaign_id,
        )
        if current.binding["state"] == "active":
            transaction.update(
                "orchestration_binding",
                intent.campaign_id,
                expected_revision=current.binding["revision"],
                state="fenced",
                data=current.binding["data"],
            )
        fence_wait(transaction, binding)
        pending_outcome = None
        if binding.pending_turn_id is not None:
            turn_row = transaction.get("drive_turn", binding.pending_turn_id)
            turn = DriveTurn.model_validate(turn_row["data"])
            if target_acknowledgment(transaction, turn) is not None:
                pending_outcome = settle_acknowledged(transaction, turn_row)["receipt"]
            else:
                closure_context = transaction.context.model_copy(
                    update={"expected_revision": turn_row["revision"]}
                )
                pending_outcome = close_unacknowledged(
                    Transaction(transaction.connection, closure_context),
                    turn_row,
                    epoch=binding.epoch,
                    finding=intent.reason,
                    request_digest=record_digest(
                        "research.transfer-turn-closure-v1",
                        {
                            "transfer_request_digest": digest,
                            "turn_id": turn_row["id"],
                        },
                    ),
                )["closure"]
        start_ids = {
            route: transfer_identifier(intent.transfer_id, "start:" + route)
            for route in intent.routes
        }
        successor = OrchestrationBinding(
            workspace_id=transaction.workspace_id,
            campaign_id=intent.campaign_id,
            epoch=binding.epoch + 1,
            adapter=intent.adapter,
            configuration_digest=intent.configuration_digest,
            routes=intent.routes,
            start_ids=start_ids,
            bound_by=transaction.context.principal.id,
            bound_at=now,
        )
        for route, actor in successor.routes.items():
            transaction.insert(
                "orchestration_owner",
                actor,
                OrchestrationOwner(
                    campaign_id=intent.campaign_id,
                    epoch=successor.epoch,
                    route=route,
                    actor_id=actor,
                ).model_dump(mode="json"),
                state="assigned",
                campaign_id=intent.campaign_id,
            )
            transaction.insert(
                "orchestration_start",
                start_ids[route],
                DriverStart(
                    campaign_id=intent.campaign_id,
                    epoch=successor.epoch,
                    adapter=successor.adapter,
                    actor_id=actor,
                    route=route,
                    configuration_digest=successor.configuration_digest,
                    checkpoint_id=intent.transfer_id,
                    checkpoint_digest=checkpoint_digest,
                    watermark=checkpoint.watermark,
                ).model_dump(mode="json"),
                state="pending",
                campaign_id=intent.campaign_id,
                parent_id=actor,
            )
        owner = transaction.get("orchestration_binding", intent.campaign_id)
        transaction.update(
            "orchestration_binding",
            intent.campaign_id,
            expected_revision=owner["revision"],
            state="active",
            data=successor.model_dump(mode="json"),
        )
        receipt = OrchestrationTransfer(
            campaign_id=intent.campaign_id,
            checkpoint_id=intent.transfer_id,
            checkpoint_digest=checkpoint_digest,
            previous_epoch=binding.epoch,
            successor=successor,
            start_ids=start_ids,
            pending_turn_outcome_id=pending_outcome["id"] if pending_outcome else None,
            pending_turn_outcome_kind=pending_outcome["kind"] if pending_outcome else None,
            request_digest=digest,
            reason=intent.reason,
            transferred_by=transaction.context.principal.id,
            transferred_at=now,
        )
        row = transaction.insert(
            "orchestration_transfer",
            intent.transfer_id,
            receipt.model_dump(mode="json"),
            state="immutable",
            campaign_id=intent.campaign_id,
        )
        transaction.event(
            "orchestration.transferred",
            row,
            {"previous_epoch": binding.epoch, "epoch": successor.epoch},
        )
        return row

    def acknowledge_start(
        self, context: CommandContext, intent: AcknowledgeDriverStart
    ) -> JsonObject:
        context.principal.require("read")

        def acknowledge(transaction: Transaction) -> JsonObject:
            row = transaction.get("orchestration_start", intent.start_id)
            start = DriverStart.model_validate(row["data"])
            if start.actor_id != context.principal.id:
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Startup belongs to another driver"
                )
            if intent.checkpoint_digest != start.checkpoint_digest:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Startup checkpoint differs"
                )
            if row["state"] == "acknowledged":
                return transaction.get("orchestration_start_ack", row["id"])
            owner = transaction.get("orchestration_binding", start.campaign_id)
            binding = OrchestrationBinding.model_validate(owner["data"])
            if (
                owner["state"] != "active"
                or binding.epoch != start.epoch
                or binding.routes.get(start.route) != start.actor_id
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Startup driver was replaced or fenced")
            if not start.watermark <= intent.observed_watermark <= watermark(transaction):
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT, "Startup has not observed its checkpoint"
                )
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Startup revision changed")
            receipt = transaction.insert(
                "orchestration_start_ack",
                row["id"],
                DriverStartAcknowledgment(
                    start_id=row["id"],
                    campaign_id=start.campaign_id,
                    epoch=start.epoch,
                    observed_watermark=intent.observed_watermark,
                    acknowledged_by=context.principal.id,
                    acknowledged_at=utc_now(),
                ).model_dump(mode="json"),
                state="immutable",
                campaign_id=start.campaign_id,
                parent_id=start.actor_id,
            )
            transaction.update(
                "orchestration_start",
                row["id"],
                expected_revision=row["revision"],
                data=row["data"],
                state="acknowledged",
            )
            transaction.event("orchestration.driver_started", receipt, {"start_id": row["id"]})
            return receipt

        return self.database.execute(
            context,
            "orchestration.acknowledge_start",
            intent.model_dump(mode="json"),
            "orchestration.drive",
            acknowledge,
        )
