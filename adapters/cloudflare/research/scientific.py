"""Witnessed scientific closeout over already authenticated ledger evidence."""

import asyncio
from typing import Literal

from research_swarm.control.dispatch import dispatch_command, validate_command_input
from research_swarm.control.recovery_operations import require_raw_operation
from research_swarm.domain.errors import ErrorCode, ResearchError

if __package__:
    from .lifecycle import LifecycleQuery, LifecycleRead, LifecycleRecordKind
    from .research_router import supports_scientific, validated
else:
    from lifecycle import LifecycleQuery, LifecycleRead, LifecycleRecordKind
    from research_router import supports_scientific, validated

COMMANDS = frozenset(
    {
        "promotion.assess",
        "promotion.reinitialize",
        "promotion.commit",
        "evidence.invalidate",
        "report.create",
        "campaign.complete",
    }
)
ScientificRecordKind = Literal[
    LifecycleRecordKind, "promotion", "evidence_correction", "report", "run_result"
]


class ScientificQuery(LifecycleQuery):
    kind: ScientificRecordKind


class ScientificRead(LifecycleRead):
    kind: ScientificRecordKind


class HostedScientific:
    commands = COMMANDS

    def __init__(self, router):
        self.router, self.database = router, router.database

    def _identity(self, token, context):
        if not supports_scientific(self.router.configuration):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Scientific closeout is unavailable"
            )
        current = self.router._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Scientific credential changed")
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Scientific owner was cancelled")

    def query(self, token, context, operation, payload):
        self._identity(token, context)
        context.principal.require("read")
        if operation == "get":
            request = validated(ScientificRead, payload)
            return self.router.records.get(context, request.kind, request.id)
        return self.router.records.list(context, validated(ScientificQuery, payload))

    async def command(self, token, context, operation, payload):
        self._identity(token, context)
        if operation not in COMMANDS:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Scientific closeout is unavailable"
            )
        definition, request = validate_command_input(context, operation, payload)
        normalized = request.model_dump(mode="json")
        previous = self.database.replay(context, operation, normalized, definition.scope)
        if previous is not None:
            return previous
        require_raw_operation(self.database, context, operation, normalized)

        def commit():
            self._identity(token, context)
            return dispatch_command(self.router.service, context, operation, normalized)

        return await self.router._witnessed(context, operation, normalized, commit)
