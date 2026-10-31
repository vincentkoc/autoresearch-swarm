"""Saved target delivery and event waits with one fresh witness per transaction."""

import asyncio
from typing import Literal

from pydantic import Field

from research_swarm.control.dispatch import dispatch_command, validate_command_input
from research_swarm.control.orchestration_waits import OrchestrationWaitControl
from research_swarm.control.recovery_operations import require_raw_operation
from research_swarm.control.time_authority import ClockAuthorityScope
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.state.database import Transaction

if __package__:
    from .research_router import (
        supports_orchestration,
        supports_startup,
        supports_timers,
        validated,
    )
    from .scientific import ScientificQuery, ScientificRead, ScientificRecordKind
else:
    from research_router import supports_orchestration, supports_startup, supports_timers, validated
    from scientific import ScientificQuery, ScientificRead, ScientificRecordKind

COMMANDS = frozenset(
    {
        "orchestration.bind",
        "orchestration.register_delivery",
        "orchestration.fence",
        "orchestration.prepare_turn",
        "orchestration.execute_turn",
        "orchestration.reconcile_turn",
        "orchestration.close_turn",
        "orchestration.record_wait",
        "orchestration.reconcile_wait",
        "orchestration.acknowledge_wakeup",
    }
)
TARGETS = frozenset(
    {
        "evaluation.authorize",
        "evaluation.complete",
        "promotion.assess",
        "promotion.commit",
        "campaign.pause",
        "campaign.complete",
        "report.create",
    }
)
OrchestrationRecordKind = Literal[
    ScientificRecordKind,
    "orchestration_binding",
    "delivery_registration",
    "drive_turn",
    "drive_receipt",
    "drive_closure",
    "orchestration_wait",
    "wait_readiness",
    "wakeup_delivery",
    "wakeup_acknowledgment",
]


class OrchestrationQuery(ScientificQuery):
    kind: OrchestrationRecordKind


class OrchestrationRead(ScientificRead):
    kind: OrchestrationRecordKind


class ContextRead(VersionedRecord):
    campaign_id: ID
    limit: int = Field(default=10, strict=True, ge=1, le=50)


class HostedOrchestration:
    commands = COMMANDS

    def __init__(self, router):
        self.router = router
        self.database, self.service = router.database, router.service.orchestration
        self.registration_policy = None
        if supports_timers(router.configuration):
            router.service.orchestration_waits = OrchestrationWaitControl(
                self.database, clock=self._clock
            )

    def _clock(self, transaction):
        authority = self.router.configuration.authority
        if transaction.workspace_id != authority.workspace_id:
            raise ResearchError(ErrorCode.STALE_FENCE, "Timer authority differs")
        observed = self.router.boundary.provider.observe(
            ClockAuthorityScope(
                authority.workspace_id,
                authority.controller_id,
                authority.control_epoch,
                authority.assignment_key_id,
            )
        )
        return observed.datetime.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def _identity(self, token, context):
        if not supports_orchestration(self.router.configuration):
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Orchestration is unavailable")
        current = self.router._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Driver credential changed")
        owner = asyncio.current_task()
        if owner is None or owner.cancelling():
            raise asyncio.CancelledError("Driver operation owner was cancelled")

    def query(self, token, context, operation, payload):
        self._identity(token, context)
        context.principal.require("read")
        if operation == "context":
            request = validated(ContextRead, payload)
            return self.router.records.context(context, request.campaign_id, limit=request.limit)
        if operation == "get":
            request = validated(OrchestrationRead, payload)
            return self.router.records.get(context, request.kind, request.id)
        return self.router.records.list(context, validated(OrchestrationQuery, payload))

    def _preflight(self, context, operation, request):
        require_raw_operation(self.database, context, operation, request.model_dump(mode="json"))
        if operation == "orchestration.bind":
            return
        if operation == "orchestration.register_delivery":
            if self.registration_policy is not None:
                self.registration_policy(context, request)
            return
        if operation == "orchestration.prepare_turn" and request.action.operation not in TARGETS:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Saved target is unavailable")
        if (
            operation == "orchestration.record_wait"
            and request.condition.wake_at is not None
            and not supports_timers(self.router.configuration)
        ):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Hosted timer waits are unavailable"
            )

        def read(sql):
            transaction = Transaction(sql, context)
            if operation in {
                "orchestration.execute_turn",
                "orchestration.reconcile_turn",
                "orchestration.close_turn",
            }:
                turn = transaction.get("drive_turn", request.turn_id)["data"]
                identifier = turn["campaign_id"]
                if (
                    operation == "orchestration.execute_turn"
                    and turn["action"]["operation"] not in TARGETS
                ):
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE, "Saved target is unavailable"
                    )
            elif operation == "orchestration.reconcile_wait":
                wait = transaction.get("orchestration_wait", request.wait_id)["data"]
                identifier = wait["campaign_id"]
                if wait["condition"]["wake_at"] is not None and not supports_timers(
                    self.router.configuration
                ):
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE, "Hosted timer waits are unavailable"
                    )
            else:
                identifier = request.campaign_id
            binding = transaction.get("orchestration_binding", identifier)["data"]
            if binding.get("start_ids") is not None and not supports_startup(
                self.router.configuration
            ):
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Successor startup is unavailable"
                )

        self.database.store.read(read)

    async def _execute(self, token, context, request):
        prepared = self.service.prepare_execution(context, request)
        if prepared.previous is not None:
            return prepared.previous
        operation = "orchestration.execute_turn"
        self._preflight(context, operation, request)
        if prepared.target_context is not None:
            try:
                await self.router._command_in_context(
                    token,
                    prepared.target_context,
                    prepared.turn.action.operation,
                    prepared.turn.action.payload,
                )
            except Exception:
                # A preflight refusal has no durable verdict. Only a real target
                # acknowledgment or explicit closure can finish the saved turn.
                self._identity(token, context)
                _, response, closed = self.service._read(context, request.turn_id, execute=True)
                if response is None and not closed:
                    raise

        def finish():
            self._identity(token, context)
            self._preflight(context, operation, request)
            return self.service._finish(context, operation, request)

        return await self.router._witnessed(
            context, operation, request.model_dump(mode="json"), finish
        )

    async def command(self, token, context, operation, payload):
        self._identity(token, context)
        if operation not in COMMANDS:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Orchestration is unavailable")
        definition, request = validate_command_input(context, operation, payload)
        if operation == "orchestration.execute_turn":
            return await self._execute(token, context, request)
        if operation == "orchestration.reconcile_turn":
            # Preserve the kernel's original actor/action-scope check before replay.
            self.service._read(context, request.turn_id, execute=False)
        normalized = request.model_dump(mode="json")
        previous = self.database.replay(context, operation, normalized, definition.scope)
        if previous is not None:
            return previous
        self._preflight(context, operation, request)

        def commit():
            self._identity(token, context)
            self._preflight(context, operation, request)
            return dispatch_command(self.router.service, context, operation, normalized)

        return await self.router._witnessed(context, operation, normalized, commit)
