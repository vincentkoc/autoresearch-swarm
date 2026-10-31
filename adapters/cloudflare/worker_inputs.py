"""Attempt-scoped buffered inputs; complete R2 I/O before final witnessed authorization."""

import asyncio

from pydantic import ValidationError

from research_swarm.control.worker_artifacts import (
    WorkerArtifactRead,
    WorkerArtifacts,
    assigned_input,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Transaction
from research_swarm.state.workspace_gate import require_workspace_mutation
from research_swarm.workers.clocks import require_authority_clock

if __package__:
    from .research_router import supports_inputs
else:
    from research_router import supports_inputs


class HostedWorkerInputs:
    def __init__(self, router):
        self.router = router
        self.artifacts = WorkerArtifacts(
            router.database, router.service.artifacts, router.service.workers
        )

    def _identity(self, token, context):
        if not supports_inputs(self.router.configuration):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Scoped input delivery is unavailable"
            )
        current = self.router._context(token, idempotency_key=context.idempotency_key)
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Input credential changed")
        context.principal.require("worker.inputs")
        owner = asyncio.current_task()
        if owner is None or owner.cancelling():
            raise asyncio.CancelledError("Input read owner was cancelled")

    def _select(self, context, request):
        def select(sql):
            transaction = Transaction(sql, context)
            require_workspace_mutation(sql, transaction.workspace_id, "worker.input")
            worker, _, reference = assigned_input(transaction, request)
            if worker.clock_mode != "bounded-continuous-v1":
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Hosted inputs require calibrated time"
                )
            require_authority_clock(
                "hosted-native-witness-v1",
                self.router.configuration.source.policy,
                worker.clock_admission,
            )
            return ArtifactCatalog.available(transaction, reference)

        return self.router.database.store.read(select)

    async def read(self, token, context, payload, *, content):
        """Caller holds the exclusive gate until all reads and authorization finish."""
        self._identity(token, context)
        try:
            request = WorkerArtifactRead.model_validate(payload)
        except ValidationError as error:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid worker input target"
            ) from error
        selected = self._select(context, request)
        body = None
        if content:
            body = await self.router.objects.read(
                selected.workspace_id, self.router.service.artifacts._blob(selected)
            )
        self._identity(token, context)
        if self._select(context, request) != selected:
            raise ResearchError(ErrorCode.STALE_FENCE, "Input changed during storage I/O")

        def authorize():
            self._identity(token, context)
            if self._select(context, request) != selected:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Input changed during witness acquisition"
                )
            return self.artifacts._input_record(context, request, expected=selected)

        authorized = await self.router._witnessed(
            context, "worker.input", request.model_dump(mode="json"), authorize
        )
        # An expired or changed grant refuses before any bytes leave this method.
        # Immutable bytes escape; no storage reader, pin or cleanup obligation does.
        return authorized.reference, body
