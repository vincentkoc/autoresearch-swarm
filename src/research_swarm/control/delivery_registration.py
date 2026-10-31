"""Immutable delegation to one gateway; durable records contain credential IDs only."""

from research_swarm.control.auth import CredentialStore
from research_swarm.control.orchestration_models import RegisterDelivery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.orchestration import (
    DeliveryRegistration,
    OrchestrationBinding,
    OrchestrationOwner,
)
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.sql import SqlSession


def delivery_principal(connection: SqlSession, workspace_id: str, identifier: str) -> Principal:
    """Read a current nonexpiring credential inside the caller's transaction."""
    row = connection.execute(
        "SELECT principal,revoked,expires_at FROM credentials WHERE workspace_id=? AND id=?",
        (workspace_id, identifier),
    ).fetchone()
    if row is None or row["revoked"] or row["expires_at"] is not None:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Delivery credential is unavailable")
    try:
        principal = Principal.model_validate_json(row["principal"])
    except ValueError:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Delivery principal is invalid"
        ) from None
    if principal.workspace_id != workspace_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Delivery workspace differs")
    return principal


def register_delivery(
    database: Database, context: CommandContext, intent: RegisterDelivery
) -> JsonObject:
    context.principal.require("auth.manage")

    def register(transaction: Transaction) -> JsonObject:
        if context.expected_revision != 0:
            raise ResearchError(ErrorCode.REVISION_CONFLICT, "Delivery registration is immutable")
        row = transaction.get("orchestration_binding", intent.campaign_id)
        binding = OrchestrationBinding.model_validate(row["data"])
        if (
            row["state"] != "active"
            or binding.workspace_id != transaction.workspace_id
            or binding.epoch != intent.epoch
            or binding.configuration_digest != intent.configuration_digest
            or set(binding.routes) != set(intent.route_credentials)
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Delivery binding is not current")
        gateway = delivery_principal(
            transaction.connection, transaction.workspace_id, intent.gateway_credential_id
        )
        CredentialStore._authorize(context.principal, gateway)
        gateway.require("orchestration.deliver")
        if gateway.id in binding.routes.values():
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Gateway must be separate")
        for route, identifier in intent.route_credentials.items():
            principal = delivery_principal(
                transaction.connection, transaction.workspace_id, identifier
            )
            CredentialStore._authorize(context.principal, principal)
            principal.require("read")
            principal.require("orchestration.drive")
            if principal.id != binding.routes[route]:
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Delivery route actor differs")
            owner = OrchestrationOwner.model_validate(
                transaction.get("orchestration_owner", principal.id)["data"]
            )
            if owner != OrchestrationOwner(
                campaign_id=binding.campaign_id,
                epoch=binding.epoch,
                route=route,
                actor_id=principal.id,
            ):
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Delivery route owner differs")
        existing = transaction.connection.execute(
            "SELECT 1 FROM records WHERE workspace_id=? AND kind='delivery_registration' AND id=?",
            (transaction.workspace_id, gateway.id),
        ).fetchone()
        if existing is not None:
            stored = transaction.get("delivery_registration", gateway.id)
            registration = DeliveryRegistration.model_validate(stored["data"])
            if (
                stored["state"] != "immutable"
                or registration.model_dump(include=set(RegisterDelivery.model_fields))
                != intent.model_dump()
                or registration.workspace_id != transaction.workspace_id
                or registration.gateway_actor_id != gateway.id
                or registration.routes != binding.routes
            ):
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Gateway is already registered")
            return stored
        registration = DeliveryRegistration(
            **intent.model_dump(),
            workspace_id=transaction.workspace_id,
            gateway_actor_id=gateway.id,
            routes=binding.routes,
            registered_by=context.principal.id,
            registered_at=utc_now(),
        )
        stored = transaction.insert(
            "delivery_registration",
            gateway.id,
            registration.model_dump(mode="json"),
            state="immutable",
            campaign_id=binding.campaign_id,
        )
        transaction.event("orchestration.delivery_registered", stored, {"epoch": binding.epoch})
        return stored

    return database.execute(
        context,
        "orchestration.register_delivery",
        intent.model_dump(mode="json"),
        "orchestration.manage",
        register,
    )
