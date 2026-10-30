"""Persist one exact action before delivery and reconcile its authoritative acknowledgment."""

from __future__ import annotations

from collections.abc import Callable
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from research_swarm.control.orchestration_models import (
    BindOrchestration,
    DriveTurnTarget,
    FenceOrchestration,
    PrepareDriveTurn,
)
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, DriveGuard, guarded_context
from research_swarm.domain.orchestration import (
    DriveAction,
    DriveReceipt,
    DriveTurn,
    OrchestrationBinding,
    OrchestrationOwner,
    drive_turn_id,
)
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.waits import consume_ready_wait, fence_wait

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


class OrchestrationControl:
    def __init__(
        self,
        database: Database,
        dispatch: Callable[[CommandContext, str, JsonObject], JsonObject],
    ) -> None:
        self.database, self.dispatch = database, dispatch

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
            ).fetchone()["watermark"]
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
        saved = transaction.connection.execute(
            "SELECT request_digest,response FROM commands "
            "WHERE workspace_id=? AND actor_id=? AND idempotency_key=?",
            (transaction.workspace_id, turn.actor_id, turn.action.idempotency_key),
        ).fetchone()
        if saved is None:
            return None
        if saved["request_digest"] != turn.action.request_digest:
            raise ResearchError(
                ErrorCode.IDEMPOTENCY_CONFLICT, "Target command key has another request"
            )
        return dict(strict_json_loads(saved["response"]))

    def _read(
        self, context: CommandContext, identifier: str, *, execute: bool
    ) -> tuple[DriveTurn, JsonObject | None]:
        with self.database.connect() as connection:
            connection.execute("BEGIN")
            transaction = Transaction(connection, context)
            turn = DriveTurn.model_validate(transaction.get("drive_turn", identifier)["data"])
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
            connection.commit()
        return turn, response

    def _finish(
        self, context: CommandContext, operation: str, intent: DriveTurnTarget
    ) -> JsonObject:
        def finish(transaction: Transaction) -> JsonObject:
            row = transaction.get("drive_turn", intent.turn_id)
            turn = DriveTurn.model_validate(row["data"])
            if context.principal.id != turn.actor_id:
                context.principal.require("orchestration.manage")
            response = self._acknowledgment(transaction, turn)
            if response is None:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Turn has no durable target acknowledgment"
                )
            if row["state"] == "settled":
                receipt = transaction.get("drive_receipt", row["id"])
            else:
                receipt = transaction.insert(
                    "drive_receipt",
                    row["id"],
                    DriveReceipt(
                        turn_id=row["id"],
                        target_command_id=response["command_id"],
                        target_request_digest=turn.action.request_digest,
                        target_response_digest=record_digest(
                            "research.drive-response-v1", response
                        ),
                        disposition=response["disposition"],
                        reconciled_by=context.principal.id,
                        reconciled_at=utc_now(),
                    ).model_dump(mode="json"),
                    state="immutable",
                    campaign_id=turn.campaign_id,
                )
                row = transaction.update(
                    "drive_turn",
                    row["id"],
                    expected_revision=row["revision"],
                    data=row["data"],
                    state="settled",
                )
                owner = transaction.get("orchestration_binding", turn.campaign_id)
                binding = OrchestrationBinding.model_validate(owner["data"])
                if binding.epoch == turn.epoch and binding.pending_turn_id == row["id"]:
                    transaction.update(
                        "orchestration_binding",
                        turn.campaign_id,
                        expected_revision=owner["revision"],
                        state=owner["state"],
                        data=binding.model_copy(update={"pending_turn_id": None}).model_dump(
                            mode="json"
                        ),
                    )
                transaction.event(
                    "orchestration.turn_settled", receipt, {"disposition": response["disposition"]}
                )
            return {"turn": row, "receipt": receipt, "response": response}

        return self.database.execute(
            context, operation, intent.model_dump(mode="json"), "orchestration.drive", finish
        )

    def execute(self, context: CommandContext, intent: DriveTurnTarget) -> JsonObject:
        operation = "orchestration.execute_turn"
        context.principal.require("orchestration.drive")
        turn, response = self._read(context, intent.turn_id, execute=True)
        previous = self.database.replay(
            context, operation, intent.model_dump(mode="json"), "orchestration.drive"
        )
        if previous is not None:
            return previous
        if context.idempotency_key == turn.action.idempotency_key:
            raise ResearchError(
                ErrorCode.IDEMPOTENCY_CONFLICT, "Target and delivery keys must differ"
            )
        if response is None:
            target = guarded_context(
                CommandContext(
                    principal=context.principal,
                    idempotency_key=turn.action.idempotency_key,
                    expected_revision=turn.action.expected_revision,
                ),
                DriveGuard(intent.turn_id, turn.action.request_digest),
            )
            try:
                self.dispatch(target, turn.action.operation, turn.action.payload)
            except Exception:
                # Preflight refusal is not a durable rejection. Recover only an actual target ack.
                _, response = self._read(context, intent.turn_id, execute=True)
                if response is None:
                    raise
        return self._finish(context, operation, intent)

    def reconcile(self, context: CommandContext, intent: DriveTurnTarget) -> JsonObject:
        operation = "orchestration.reconcile_turn"
        context.principal.require("orchestration.drive")
        turn, response = self._read(context, intent.turn_id, execute=False)
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
        if response is None:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Original action remains unresolved", retryable=True
            )
        return self._finish(context, operation, intent)

    def fence(self, context: CommandContext, intent: FenceOrchestration) -> JsonObject:
        def fence(transaction: Transaction) -> JsonObject:
            row = transaction.get("orchestration_binding", intent.campaign_id)
            binding = OrchestrationBinding.model_validate(row["data"])
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Orchestration binding changed")
            if binding.epoch != intent.epoch or row["state"] != "active":
                raise ResearchError(ErrorCode.STALE_FENCE, "Orchestration ownership changed")
            updated = transaction.update(
                "orchestration_binding",
                row["id"],
                expected_revision=row["revision"],
                state="fenced",
                data=row["data"],
            )
            transaction.event(
                "orchestration.fenced", updated, {"reason": intent.reason, "epoch": binding.epoch}
            )
            fence_wait(transaction, binding)
            return updated

        return self.database.execute(
            context,
            "orchestration.fence",
            intent.model_dump(mode="json"),
            "orchestration.manage",
            fence,
        )
