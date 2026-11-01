"""Resolve one authenticated gateway from durable state and the sealed credential roster."""

from research_swarm.control.auth import CredentialStore
from research_swarm.control.delivery_registration import delivery_principal
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DeliveryRegistration as DurableRegistration
from research_swarm.orchestration.gateway_models import GatewayBinding

if __package__:
    from .delivery_router import DeliveryRegistration
else:
    from delivery_router import DeliveryRegistration


class DeploymentDeliveryRegistry:
    def __init__(self, router, manifest, secrets):
        self.router, self.manifest, self.secrets = router, manifest, secrets
        self.roster = {item.id: item for item in manifest.credentials}
        self.static = {
            self.roster[item.gateway_credential_id].principal.id: item
            for item in manifest.deliveries
        }

    def _current(self, sql, identifier):
        expected = self.roster.get(identifier)
        if expected is None or identifier not in self.secrets.credentials:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Credential is outside deployment")
        principal = delivery_principal(
            sql, self.manifest.configuration.authority.workspace_id, identifier
        )
        row = sql.execute(
            "SELECT token_digest FROM credentials WHERE id=?", (identifier,)
        ).fetchone()
        if principal != expected.principal or row["token_digest"] != expected.token_digest:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Deployment credential differs")
        return principal

    def preflight(self, context, request):
        """Called before witness acquisition and again immediately before the commit."""
        if context.principal.workspace_id != self.manifest.configuration.authority.workspace_id:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Delivery workspace differs")

        def check(sql):
            gateway = self._current(sql, request.gateway_credential_id)
            if gateway.id in self.static:
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT, "Gateway has a fixed deployment registration"
                )
            for identifier in request.route_credentials.values():
                self._current(sql, identifier)

        self.router.database.store.read(check)

    def resolve(self, principal, token):
        """Called under the delivery gate, before reading the caller's request body."""
        workspace_id = self.manifest.configuration.authority.workspace_id
        if principal.workspace_id != workspace_id:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Delivery workspace differs")

        def read(sql):
            row = sql.execute(
                "SELECT state,payload,campaign_id FROM records "
                "WHERE workspace_id=? AND kind='delivery_registration' AND id=?",
                (workspace_id, principal.id),
            ).fetchone()
            static = self.static.get(principal.id)
            if static is not None:
                if row is not None:
                    raise ResearchError(ErrorCode.REVISION_CONFLICT, "Delivery authorities overlap")
                gateway_id, route_ids, binding = (
                    static.gateway_credential_id,
                    static.route_credentials,
                    GatewayBinding.model_validate(static.binding.model_dump(mode="json")),
                )
            else:
                if row is None:
                    return None
                registration = DurableRegistration.model_validate_json(row["payload"])
                if (
                    row["state"] != "immutable"
                    or registration.gateway_actor_id != principal.id
                    or registration.workspace_id != workspace_id
                    or registration.campaign_id != row["campaign_id"]
                ):
                    raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Delivery record differs")
                gateway_id, route_ids = (
                    registration.gateway_credential_id,
                    registration.route_credentials,
                )
                binding = GatewayBinding(
                    workspace_id=workspace_id,
                    campaign_id=registration.campaign_id,
                    epoch=registration.epoch,
                    configuration_digest=registration.configuration_digest,
                    actors=registration.routes,
                )
            gateway = self._current(sql, gateway_id)
            if (
                gateway != principal
                or CredentialStore._digest(token) != self.roster[gateway_id].token_digest
            ):
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Gateway credential differs")
            gateway.require("orchestration.deliver")
            for route, identifier in route_ids.items():
                actor = self._current(sql, identifier)
                actor.require("read")
                actor.require("orchestration.drive")
                if actor.id != binding.actors[route]:
                    raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Delivery route differs")
            return DeliveryRegistration(
                binding,
                {
                    route: self.secrets.credentials[identifier]
                    for route, identifier in route_ids.items()
                },
            )

        return self.router.database.store.read(read)
