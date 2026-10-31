"""Persist one exact action before delivery and reconcile its authoritative acknowledgment."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from research_swarm.control.orchestration_models import (
    BindOrchestration,
    CloseDriveTurn,
    DriveTurnTarget,
    FenceOrchestration,
    PrepareDriveTurn,
)
from research_swarm.crypto import Ed25519Provider
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, DriveGuard, guarded_context
from research_swarm.domain.orchestration import (
    DriveAction,
    DriveTurn,
    OrchestrationBinding,
    OrchestrationOwner,
    drive_turn_id,
)
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.drive_closures import (
    close_unacknowledged,
    closure_outcome,
    settle_acknowledged,
    target_acknowledgment,
)
from research_swarm.state.drive_guard import require_driver_started
from research_swarm.state.sql import SqlSession
from research_swarm.state.waits import consume_ready_wait, fence_wait


def fence_binding(transaction: Transaction, row: JsonObject, *, reason: str) -> JsonObject:
    binding = OrchestrationBinding.model_validate(row["data"])
    if row["state"] != "active":
        raise ResearchError(ErrorCode.STALE_FENCE, "Orchestration ownership changed")
    updated = transaction.update(
        "orchestration_binding",
        row["id"],
        expected_revision=row["revision"],
        state="fenced",
        data=row["data"],
    )
    transaction.event("orchestration.fenced", updated, {"reason": reason, "epoch": binding.epoch})
    fence_wait(transaction, binding)
    return updated


CONTROL_OPERATIONS = frozenset(
    {
        "search.request_proposal",
        "search.cancel_proposal",
        "search.admit_response",
        "search.observe_trial",
        "search.select_observation",
        "search.resolve_trial",
        "search.abandon_selection",
        "promotion.assess",
        "promotion.commit",
        "confirmation.create",
        "confirmation.assess",
        "evaluation.authorize",
        "campaign.pause",
        "campaign.complete",
        "campaign.capture_obligations",
        "campaign.complete_scientific",
        "campaign.close_resources",
        "report.create",
    }
)


def require_action_scopes(context: CommandContext, operation: str) -> None:
    from research_swarm.control.dispatch import COMMANDS

    definition = COMMANDS[operation]
    context.principal.require(definition.scope)
    for scope in definition.additional_scopes:
        context.principal.require(scope)


def normalized_action(context: CommandContext, action: DriveAction, route: str) -> DriveAction:
    from research_swarm.control.dispatch import COMMANDS

    allowed = CONTROL_OPERATIONS if route == "control" else {"evaluation.complete"}
    if action.operation not in allowed:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Operation is outside the driver route"
        )
    definition = COMMANDS[action.operation]
    require_action_scopes(context, action.operation)
    if definition.expected_revision and action.expected_revision is None:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Drive action requires its target revision"
        )
    try:
        payload = definition.request.model_validate(action.payload).model_dump(mode="json")
        return DriveAction(**{**action.model_dump(), "payload": payload})
    except ValidationError as error:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Drive action does not match its command"
        ) from error


def action_campaign(transaction: Transaction, action: DriveAction) -> str:
    if "campaign_id" in action.payload:
        identifier = str(action.payload["campaign_id"])
        return str(transaction.get("campaign", identifier)["id"])
    kind, field = {
        "promotion.commit": ("promotion", "decision_id"),
        "confirmation.assess": ("confirmation_plan", "plan_id"),
        "evaluation.authorize": ("job", "job_id"),
        "evaluation.complete": ("evaluation_request", "request_id"),
    }[action.operation]
    return str(transaction.get(kind, str(action.payload[field]))["campaign_id"])


@dataclass(frozen=True)
class PreparedTurnExecution:
    """Internal delivery selection; the target transaction still checks its saved guard."""

    turn: DriveTurn
    previous: JsonObject | None
    target_context: CommandContext | None = None


class OrchestrationControl:
    def __init__(
        self,
        database: Database,
        dispatch: Callable[[CommandContext, str, JsonObject], JsonObject],
        *,
        crypto_provider: Ed25519Provider | None = None,
    ) -> None:
        self.database, self.dispatch = database, dispatch
        self.crypto_provider = crypto_provider

    def bind(self, context: CommandContext, intent: BindOrchestration) -> JsonObject:
        def bind(transaction: Transaction) -> JsonObject:
            campaign = transaction.get("campaign", intent.campaign_id)
            if context.expected_revision != campaign["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Campaign revision changed")
            if campaign["data"]["contract_digest"] is None or campaign["state"] in {
                "completed",
                "cancelled",
                "failed",
            }:
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Binding requires a sealed nonterminal campaign"
                )
            if context.principal.id in intent.routes.values():
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Drivers require separate dedicated principals"
                )
            binding = OrchestrationBinding(
                **intent.model_dump(exclude={"schema_version"}),
                workspace_id=transaction.workspace_id,
                epoch=1,
                bound_by=context.principal.id,
                bound_at=utc_now(),
            )
            for route, actor_id in binding.routes.items():
                if transaction.connection.execute(
                    "SELECT 1 FROM commands WHERE workspace_id=? AND actor_id=? LIMIT 1",
                    (transaction.workspace_id, actor_id),
                ).fetchone():
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED,
                        "Driver principal already performed commands",
                    )
                transaction.insert(
                    "orchestration_owner",
                    actor_id,
                    OrchestrationOwner(
                        campaign_id=binding.campaign_id,
                        epoch=1,
                        route=route,
                        actor_id=actor_id,
                    ).model_dump(mode="json"),
                    state="assigned",
                    campaign_id=binding.campaign_id,
                )
            row = transaction.insert(
                "orchestration_binding",
                binding.campaign_id,
                binding.model_dump(mode="json"),
                state="active",
                campaign_id=binding.campaign_id,
            )
            transaction.event("orchestration.bound", row, {"adapter": binding.adapter, "epoch": 1})
            return row

        return self.database.execute(
            context,
            "orchestration.bind",
            intent.model_dump(mode="json"),
            "orchestration.manage",
            bind,
        )

    def prepare(self, context: CommandContext, intent: PrepareDriveTurn) -> JsonObject:
        def prepare(transaction: Transaction) -> JsonObject:
            owner = OrchestrationOwner.model_validate(
                transaction.get("orchestration_owner", context.principal.id)["data"]
            )
            if (owner.campaign_id, owner.epoch) != (intent.campaign_id, intent.epoch):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Driver does not own this campaign epoch"
                )
            action = normalized_action(context, intent.action, owner.route)
            if action.idempotency_key == context.idempotency_key:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Target and delivery keys must differ"
                )
            identifier = drive_turn_id(
                transaction.workspace_id, intent.campaign_id, intent.epoch, intent.ordinal
            )
            proposal = DriveTurn(
                campaign_id=intent.campaign_id,
                epoch=intent.epoch,
                ordinal=intent.ordinal,
                route=owner.route,
                actor_id=owner.actor_id,
                configuration_digest=intent.configuration_digest,
                planning_watermark=intent.planning_watermark,
                action=action,
                prepared_at=utc_now(),
            )
            try:
                existing = transaction.get("drive_turn", identifier)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
            else:
                saved = DriveTurn.model_validate(existing["data"])
                if saved.model_dump(exclude={"prepared_at"}) != proposal.model_dump(
                    exclude={"prepared_at"}
                ):
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Drive ordinal already has another intent"
                    )
                return existing
            row = transaction.get("orchestration_binding", intent.campaign_id)
            binding = OrchestrationBinding.model_validate(row["data"])
            if (
                row["state"] != "active"
                or binding.epoch != intent.epoch
                or binding.routes.get(owner.route) != context.principal.id
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Orchestration binding is fenced")
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Orchestration binding changed")
            require_driver_started(transaction, binding, owner.route)
            if binding.pending_turn_id is not None:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    "Reconcile the original turn before planning another",
                )
            if (
                binding.next_ordinal != intent.ordinal
                or binding.configuration_digest != intent.configuration_digest
            ):
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Drive ordinal or configuration changed"
                )
            watermark = transaction.connection.execute(
                "SELECT next_sequence-1 AS watermark FROM workspaces WHERE id=?",
                (transaction.workspace_id,),
            ).one()["watermark"]
            if intent.planning_watermark > watermark:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Planning watermark is in the future"
                )
            if action_campaign(transaction, action) != intent.campaign_id:
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Drive action targets another campaign"
                )
            consume_ready_wait(transaction, binding, intent.planning_watermark)
            if transaction.connection.execute(
                "SELECT 1 FROM commands WHERE workspace_id=? AND actor_id=? AND idempotency_key=?",
                (transaction.workspace_id, owner.actor_id, action.idempotency_key),
            ).fetchone():
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Target command key was already used"
                )
            key_id = str(
                uuid5(
                    NAMESPACE_URL,
                    record_digest(
                        "research.drive-command-key-v1",
                        {
                            "workspace_id": transaction.workspace_id,
                            "actor_id": owner.actor_id,
                            "key": action.idempotency_key,
                        },
                    ),
                )
            )
            transaction.insert(
                "drive_command_key",
                key_id,
                {"turn_id": identifier},
                state="assigned",
                campaign_id=intent.campaign_id,
            )
            turn = transaction.insert(
                "drive_turn",
                identifier,
                proposal.model_dump(mode="json"),
                state="prepared",
                campaign_id=intent.campaign_id,
            )
            transaction.update(
                "orchestration_binding",
                intent.campaign_id,
                expected_revision=row["revision"],
                state="active",
                data=binding.model_copy(
                    update={
                        "pending_turn_id": identifier,
                        "next_ordinal": intent.ordinal + 1,
                        "active_wait_id": None,
                    }
                ).model_dump(mode="json"),
            )
            transaction.event("orchestration.turn_prepared", turn, {"operation": action.operation})
            return turn

        return self.database.execute(
            context,
            "orchestration.prepare_turn",
            intent.model_dump(mode="json"),
            "orchestration.drive",
            prepare,
        )

    @staticmethod
    def _acknowledgment(transaction: Transaction, turn: DriveTurn) -> JsonObject | None:
        return target_acknowledgment(transaction, turn)

    def _read(
        self, context: CommandContext, identifier: str, *, execute: bool
    ) -> tuple[DriveTurn, JsonObject | None, bool]:
        def read(connection: SqlSession) -> tuple[DriveTurn, JsonObject | None, bool]:
            transaction = Transaction(connection, context)
            row = transaction.get("drive_turn", identifier)
            turn = DriveTurn.model_validate(row["data"])
            if context.principal.id != turn.actor_id:
                if execute:
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED,
                        "Only the original route actor executes a turn",
                    )
                context.principal.require("orchestration.manage")
                context.principal.require("read")
            else:
                require_action_scopes(context, turn.action.operation)
            response = self._acknowledgment(transaction, turn)
            return turn, response, row["state"] == "closed_unacknowledged"

        return self.database.store.read(read)

    def _finish(
        self, context: CommandContext, operation: str, intent: DriveTurnTarget
    ) -> JsonObject:
        return self.database.with_preflight(
            context,
            operation,
            intent.model_dump(mode="json"),
            "orchestration.drive",
            lambda: self._finish_preflight(context, operation, intent),
        )

    def _finish_preflight(
        self, context: CommandContext, operation: str, intent: DriveTurnTarget
    ) -> JsonObject:
        from research_swarm.control.recovery_turns import recovery_turn_context

        context = recovery_turn_context(
            self.database,
            context,
            operation,
            intent.model_dump(mode="json"),
            crypto_provider=self.crypto_provider,
        )

        def finish(transaction: Transaction) -> JsonObject:
            row = transaction.get("drive_turn", intent.turn_id)
            turn = DriveTurn.model_validate(row["data"])
            if context.principal.id != turn.actor_id:
                context.principal.require("orchestration.manage")
            if row["state"] == "closed_unacknowledged":
                return closure_outcome(transaction, row)
            return settle_acknowledged(transaction, row)

        return self.database.execute(
            context, operation, intent.model_dump(mode="json"), "orchestration.drive", finish
        )

    def prepare_execution(
        self, context: CommandContext, intent: DriveTurnTarget
    ) -> PreparedTurnExecution:
        """Select an exact target without dispatching or acknowledging it.

        Async transports can deliver the guarded target and finish its receipt under
        separate transaction boundaries. Preparation never grants mutation authority.
        """
        operation = "orchestration.execute_turn"
        context.principal.require("orchestration.drive")
        turn, response, closed = self._read(context, intent.turn_id, execute=True)
        previous = self.database.replay(
            context, operation, intent.model_dump(mode="json"), "orchestration.drive"
        )
        if previous is not None:
            return PreparedTurnExecution(turn, previous)
        if context.idempotency_key == turn.action.idempotency_key:
            raise ResearchError(
                ErrorCode.IDEMPOTENCY_CONFLICT, "Target and delivery keys must differ"
            )
        if response is None and not closed:
            target = guarded_context(
                CommandContext(
                    principal=context.principal,
                    idempotency_key=turn.action.idempotency_key,
                    expected_revision=turn.action.expected_revision,
                ),
                DriveGuard(intent.turn_id, turn.action.request_digest),
            )
            return PreparedTurnExecution(turn, None, target)
        return PreparedTurnExecution(turn, None)

    def execute(self, context: CommandContext, intent: DriveTurnTarget) -> JsonObject:
        prepared = self.prepare_execution(context, intent)
        if prepared.previous is not None:
            return prepared.previous
        if prepared.target_context is not None:
            try:
                self.dispatch(
                    prepared.target_context,
                    prepared.turn.action.operation,
                    prepared.turn.action.payload,
                )
            except Exception:
                # Preflight refusal is not a durable rejection. Recover only an actual target ack.
                _, response, closed = self._read(context, intent.turn_id, execute=True)
                if response is None and not closed:
                    raise
        return self._finish(context, "orchestration.execute_turn", intent)

    def reconcile(self, context: CommandContext, intent: DriveTurnTarget) -> JsonObject:
        operation = "orchestration.reconcile_turn"
        context.principal.require("orchestration.drive")
        turn, response, closed = self._read(context, intent.turn_id, execute=False)
        previous = self.database.replay(
            context, operation, intent.model_dump(mode="json"), "orchestration.drive"
        )
        if previous is not None:
            return previous
        if (
            context.principal.id == turn.actor_id
            and context.idempotency_key == turn.action.idempotency_key
        ):
            raise ResearchError(
                ErrorCode.IDEMPOTENCY_CONFLICT, "Target and delivery keys must differ"
            )
        if response is None and not closed:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Original action remains unresolved", retryable=True
            )
        return self._finish(context, operation, intent)

    def close(self, context: CommandContext, intent: CloseDriveTurn) -> JsonObject:
        context.principal.require("read")
        return self.database.with_preflight(
            context,
            "orchestration.close_turn",
            intent.model_dump(mode="json"),
            "orchestration.manage",
            lambda: self._close(context, intent),
        )

    def _close(self, context: CommandContext, intent: CloseDriveTurn) -> JsonObject:
        from research_swarm.control.recovery_turns import recovery_turn_context

        context = recovery_turn_context(
            self.database,
            context,
            "orchestration.close_turn",
            intent.model_dump(mode="json"),
            crypto_provider=self.crypto_provider,
        )
        request_digest = record_digest(
            "research.drive-closure-request-v1",
            {
                "payload": intent.model_dump(mode="json"),
                "expected_revision": context.expected_revision,
            },
        )
        return self.database.execute(
            context,
            "orchestration.close_turn",
            intent.model_dump(mode="json"),
            "orchestration.manage",
            lambda transaction: close_unacknowledged(
                transaction,
                transaction.get("drive_turn", intent.turn_id),
                epoch=intent.epoch,
                finding=intent.finding,
                request_digest=request_digest,
            ),
        )

    def fence(self, context: CommandContext, intent: FenceOrchestration) -> JsonObject:
        def fence(transaction: Transaction) -> JsonObject:
            row = transaction.get("orchestration_binding", intent.campaign_id)
            binding = OrchestrationBinding.model_validate(row["data"])
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Orchestration binding changed")
            if binding.epoch != intent.epoch or row["state"] != "active":
                raise ResearchError(ErrorCode.STALE_FENCE, "Orchestration ownership changed")
            return fence_binding(transaction, row, reason=intent.reason)

        return self.database.execute(
            context,
            "orchestration.fence",
            intent.model_dump(mode="json"),
            "orchestration.manage",
            fence,
        )
