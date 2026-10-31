"""Witnessed operational intent without inventing native process closure."""

import asyncio
from typing import Literal

from research_swarm.control.dispatch import dispatch_command, validate_command_input
from research_swarm.control.recovery_operations import require_raw_operation
from research_swarm.control.workers import worker_authority
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction

if __package__:
    from .evaluations import EvaluationQuery, EvaluationRead, EvaluationRecordKind
    from .research_router import supports_lifecycle, validated
else:
    from evaluations import EvaluationQuery, EvaluationRead, EvaluationRecordKind
    from research_router import supports_lifecycle, validated

COMMANDS = frozenset({"campaign.pause", "worker.heartbeat", "attempt.request_cancel"})
LifecycleRecordKind = Literal[
    EvaluationRecordKind, "worker_admission", "worker_presence", "attempt_control"
]


class LifecycleQuery(EvaluationQuery):
    kind: LifecycleRecordKind


class LifecycleRead(EvaluationRead):
    kind: LifecycleRecordKind


class HostedLifecycle:
    commands = COMMANDS

    def __init__(self, router):
        self.router, self.database = router, router.database

    def _identity(self, token, context):
        if not supports_lifecycle(self.router.configuration):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Lifecycle control is unavailable"
            )
        current = self.router._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Lifecycle credential changed")
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Lifecycle owner was cancelled")

    def query(self, token, context, operation, payload):
        self._identity(token, context)
        context.principal.require("read")
        if operation == "get":
            request = validated(LifecycleRead, payload)
            return self.router.records.get(context, request.kind, request.id)
        return self.router.records.list(context, validated(LifecycleQuery, payload))

    async def command(self, token, context, operation, payload):
        self._identity(token, context)
        if operation not in COMMANDS:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Lifecycle control is unavailable"
            )
        definition, request = validate_command_input(context, operation, payload)
        normalized = request.model_dump(mode="json")
        previous = self.database.replay(context, operation, normalized, definition.scope)
        if previous is not None:
            return previous
        require_raw_operation(self.database, context, operation, normalized)
        if operation == "worker.heartbeat":
            worker = self.database.store.read(
                lambda sql: worker_authority(
                    Transaction(sql, context),
                    request.binding.worker_id,
                    request.binding.worker_session_id,
                )
            )
            if worker.binding != request.binding:
                raise ResearchError(ErrorCode.STALE_FENCE, "Heartbeat binding differs")
            if worker.clock_mode != "bounded-continuous-v1":
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Hosted heartbeat requires calibrated time"
                )

        def commit():
            self._identity(token, context)
            return dispatch_command(self.router.service, context, operation, normalized)

        return await self.router._witnessed(context, operation, normalized, commit)
