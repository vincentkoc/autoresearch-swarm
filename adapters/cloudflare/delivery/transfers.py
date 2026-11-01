"""Bounded R2 verification before the shared atomic driver-transfer transaction."""

import asyncio
from typing import Literal

from research_swarm.control.dispatch import validate_command_input
from research_swarm.control.orchestration_transfer import transfer_request_digest
from research_swarm.control.recovery_operations import require_raw_operation
from research_swarm.control.transfer_view import TransferLimits
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Transaction

if __package__:
    from ..research.research_router import supports_transfer, validated
    from .orchestration import OrchestrationQuery, OrchestrationRead, OrchestrationRecordKind
else:
    from orchestration import OrchestrationQuery, OrchestrationRead, OrchestrationRecordKind
    from research_router import supports_transfer, validated

COMMANDS = frozenset({"orchestration.transfer"})
LIMITS = TransferLimits(
    max_records=4096,
    max_entries=8192,
    max_manifest_bytes=4 * 1024**2,
    max_artifacts=32,
    max_artifact_bytes=8 * 1024**2,
)
TransferRecordKind = Literal[
    OrchestrationRecordKind,
    "orchestration_transfer",
    "orchestration_checkpoint",
    "orchestration_checkpoint_chunk",
    "orchestration_binding_history",
    "orchestration_start",
    "orchestration_start_ack",
]


class TransferRead(OrchestrationRead):
    kind: TransferRecordKind


class TransferQuery(OrchestrationQuery):
    kind: TransferRecordKind


class HostedTransfers:
    commands = COMMANDS

    def __init__(self, router):
        self.router = router
        self.database, self.service = router.database, router.service.orchestration_transfer

    def _identity(self, token, context):
        if not supports_transfer(self.router.configuration):
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Transfer is unavailable")
        current = self.router._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Transfer credential changed")
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Transfer owner was cancelled")

    def query(self, token, context, operation, payload):
        self._identity(token, context)
        context.principal.require("read")
        if operation == "get":
            request = validated(TransferRead, payload)
            return self.router.records.get(context, request.kind, request.id)
        return self.router.records.list(context, validated(TransferQuery, payload))

    def _select(self, context, request, digest):
        require_raw_operation(
            self.database, context, "orchestration.transfer", request.model_dump(mode="json")
        )

        def read(sql):
            transaction = Transaction(sql, context)
            view = self.service._prepare_in(transaction, request, digest, limits=LIMITS)
            records = tuple(
                ArtifactCatalog.available(transaction, reference)
                for reference in (() if view is None else view.artifacts)
            )
            if any(
                record.reference.size_bytes > self.router.configuration.storage.max_object_bytes
                for record in records
            ):
                raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Transfer object exceeds bound")
            return view, records

        return self.database.store.read(read)

    async def command(self, token, context, operation, payload):
        self._identity(token, context)
        if operation not in COMMANDS:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Transfer is unavailable")
        for scope in ("read", "orchestration.drive"):
            context.principal.require(scope)
        definition, request = validate_command_input(context, operation, payload)
        normalized = request.model_dump(mode="json")
        previous = self.database.replay(context, operation, normalized, definition.scope)
        if previous is not None:
            return previous
        digest = transfer_request_digest(context, request)
        selected = self._select(context, request, digest)
        view, records = selected
        for record in records:
            await self.router.objects.read(
                record.workspace_id, self.router.service.artifacts._blob(record)
            )
            self._identity(token, context)
        self._identity(token, context)
        if self._select(context, request, digest) != selected:
            raise ResearchError(ErrorCode.STALE_FENCE, "Transfer evidence changed during byte I/O")

        def accept(transaction):
            for record in records:
                if ArtifactCatalog.available(transaction, record.reference) != record:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Transfer artifact changed")
            return self.service._commit(transaction, request, digest, view, limits=LIMITS)

        def commit():
            self._identity(token, context)
            return self.database.execute(context, operation, normalized, definition.scope, accept)

        return await self.router._witnessed(context, operation, normalized, commit)
