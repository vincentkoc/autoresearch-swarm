"""Bounded actor snapshots and explicit successor acknowledgment over checked time."""

import asyncio

from research_swarm.control.dispatch import dispatch_command, validate_command_input
from research_swarm.control.recovery_operations import require_raw_operation
from research_swarm.control.time_authority import ClockAuthorityScope
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.event_recovery import RecoveryItem, RecoveryPage
from research_swarm.domain.orchestration_transfer import DriverStart
from research_swarm.state.database import Transaction
from research_swarm.state.event_recovery import (
    EventRecovery,
    SnapshotInstant,
    SnapshotLimits,
    snapshot_row,
)

if __package__:
    from .research_router import supports_startup, validated
else:
    from research_router import supports_startup, validated

COMMANDS = frozenset(
    {
        "stream.create_snapshot",
        "stream.release_snapshot",
        "stream.collect_expired",
        "orchestration.acknowledge_start",
    }
)
QUERIES = {"snapshot.page": RecoveryPage, "snapshot.item": RecoveryItem}
LIMITS = SnapshotLimits(max_items=4096, max_bytes=4 * 1024**2, max_retained_bytes=32 * 1024**2)


class HostedStartup:
    commands = COMMANDS

    def __init__(self, router):
        self.router, self.database = router, router.database
        if supports_startup(router.configuration):
            router.service.event_recovery = EventRecovery(
                self.database,
                router.service.event_recovery.public_kinds,
                limits=LIMITS,
                clock=self._clock,
            )
        self.service = router.service.event_recovery

    def _clock(self, transaction):
        authority = self.router.configuration.authority
        if transaction.workspace_id != authority.workspace_id:
            raise ResearchError(ErrorCode.STALE_FENCE, "Snapshot authority differs")
        observed = self.router.boundary.provider.observe(
            ClockAuthorityScope(
                authority.workspace_id,
                authority.controller_id,
                authority.control_epoch,
                authority.assignment_key_id,
            )
        )
        return SnapshotInstant(
            timestamp=observed.datetime.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            epoch=observed.epoch,
        )

    def _identity(self, token, context):
        if not supports_startup(self.router.configuration):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Successor recovery is unavailable"
            )
        current = self.router._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery credential changed")
        owner = asyncio.current_task()
        if owner is None or owner.cancelling():
            raise asyncio.CancelledError("Recovery operation owner was cancelled")

    def _preflight(self, context, operation, request):
        require_raw_operation(self.database, context, operation, request.model_dump(mode="json"))

        def read(sql):
            transaction = Transaction(sql, context)
            if operation == "stream.release_snapshot":
                snapshot_row(transaction, request.snapshot_id)
            elif operation == "orchestration.acknowledge_start":
                start = DriverStart.model_validate(
                    transaction.get("orchestration_start", request.start_id)["data"]
                )
                if start.actor_id != context.principal.id:
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Startup belongs to another driver"
                    )

        self.database.store.read(read)

    async def command(self, token, context, operation, payload):
        self._identity(token, context)
        if operation not in COMMANDS:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Recovery command is unavailable")
        definition, request = validate_command_input(context, operation, payload)
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

    async def query(self, token, context, operation, payload):
        self._identity(token, context)
        request = validated(QUERIES[operation], payload)
        require_raw_operation(self.database, context, operation, request.model_dump(mode="json"))
        self.service.validate_read(context, request)

        def read():
            self._identity(token, context)
            method = self.service.page if operation == "snapshot.page" else self.service.item
            return method(context, request)

        return await self.router._witnessed(
            context, operation, request.model_dump(mode="json"), read
        )
