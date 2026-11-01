"""Fixed public deployment identity and separately loaded, non-serializable secrets."""

import hmac
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import Field, StringConstraints, field_serializer, field_validator, model_validator

from research_swarm.control.auth import CredentialStore
from research_swarm.crypto import public_key
from research_swarm.domain.assignments import signing_key_id
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.orchestration import DriverRoute
from research_swarm.domain.types import ID, Digest, Name, VersionedRecord
from research_swarm.orchestration.gateway_models import GatewayBinding
from research_swarm.state.sql import StoreIdentity

if __package__:
    from ..delivery.delivery_router import DeliveryRegistration
    from ..research.research_router import (
        HostedReconciliationConfiguration,
        HostedStartupConfiguration,
        HostedTimerConfiguration,
        dispatch_key_digest,
    )
else:
    from delivery_router import DeliveryRegistration
    from research_router import (
        HostedReconciliationConfiguration,
        HostedStartupConfiguration,
        HostedTimerConfiguration,
        dispatch_key_digest,
    )

MAX_DEPLOYMENT_BYTES = 256 * 1024
MAX_SECRET_BYTES = 32 * 1024
HexDigest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
Namespace = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]{1,63}$")]


def authority_id(namespace, object_id):
    return record_digest(
        "research.cloudflare-authority-v1",
        {
            "backend": "cloudflare-do",
            "namespace": namespace,
            "object_id": object_id,
        },
    )


def secret_digest(purpose, token):
    if (
        purpose not in {"bootstrap", "witness"}
        or not isinstance(token, str)
        or re.fullmatch(r"[A-Za-z0-9_-]{43,128}", token) is None
    ):
        raise ValueError("Invalid deployment credential")
    return record_digest("research.hosted-secret-v1", {"purpose": purpose, "token": token})


class DeploymentCredential(VersionedRecord):
    id: ID
    principal: Principal
    token_digest: HexDigest


def credential_manifest_digest(credentials):
    return record_digest(
        "research.hosted-credentials-v1",
        {
            "credentials": [
                item.model_dump(mode="json")
                for item in sorted(credentials, key=lambda item: item.id)
            ],
        },
    )


class DeploymentBinding(GatewayBinding):
    actors: Mapping[DriverRoute, ID] = Field(min_length=1, max_length=2)

    @field_validator("actors")
    @classmethod
    def freeze_actors(cls, value):
        return MappingProxyType(dict(value))

    @field_serializer("actors")
    def serialize_actors(self, value):
        return dict(value)


class DeploymentDelivery(VersionedRecord):
    gateway_credential_id: ID
    binding: DeploymentBinding
    route_credentials: Mapping[DriverRoute, ID] = Field(min_length=1, max_length=2)

    @field_validator("route_credentials")
    @classmethod
    def freeze_credentials(cls, value):
        return MappingProxyType(dict(value))

    @field_serializer("route_credentials")
    def serialize_credentials(self, value):
        return dict(value)


class DeploymentManifest(VersionedRecord):
    authority_binding: Literal["RESEARCH_AUTHORITY"] = "RESEARCH_AUTHORITY"
    artifact_binding: Literal["ARTIFACTS"] = "ARTIFACTS"
    namespace: Namespace
    authority_name: Name
    object_id: HexDigest
    configuration: (
        HostedStartupConfiguration | HostedReconciliationConfiguration | HostedTimerConfiguration
    ) = Field(discriminator="profile")
    witness_origin: str = Field(min_length=1, max_length=256)
    witness_timeout_ms: int = Field(default=1000, strict=True, ge=100, le=10000)
    witness_credential_digest: Digest
    bootstrap_credential_digest: Digest
    provisioning_actor_id: ID
    credentials: tuple[DeploymentCredential, ...] = Field(min_length=1, max_length=128)
    deliveries: tuple[DeploymentDelivery, ...] = Field(default=(), max_length=32)

    @property
    def digest(self):
        return record_digest("research.hosted-deployment-v1", self.model_dump(mode="json"))

    @model_validator(mode="after")
    def coherent(self):
        config = self.configuration
        if (
            config.store_location != self.namespace + "/" + self.object_id
            or config.authority.state_authority_id != authority_id(self.namespace, self.object_id)
            or config.authority.assignment_key_id != signing_key_id(config.assignment_public_key)
            or config.credential_manifest_digest != credential_manifest_digest(self.credentials)
            or config.authority.control_epoch < 1
        ):
            raise ValueError("Deployment authority differs")
        for values in (
            [item.id for item in self.credentials],
            [item.principal.id for item in self.credentials],
            [item.token_digest for item in self.credentials],
            [item.gateway_credential_id for item in self.deliveries],
        ):
            if len(set(values)) != len(values):
                raise ValueError("Deployment identities must be distinct")
        actors = {item.principal.id: item.principal for item in self.credentials}
        issuer = actors.get(self.provisioning_actor_id)
        if issuer is None:
            raise ValueError("Provisioning actor is absent")
        for item in self.credentials:
            if item.principal.workspace_id != config.authority.workspace_id:
                raise ValueError("Credential workspace differs")
            CredentialStore._authorize(issuer, item.principal)
        roster = {item.id: item.principal for item in self.credentials}
        for delivery in self.deliveries:
            gateway = roster.get(delivery.gateway_credential_id)
            if gateway is None or gateway.id in delivery.binding.actors.values():
                raise ValueError("Delivery gateway must be separate")
            gateway.require("orchestration.deliver")
            if delivery.binding.workspace_id != config.authority.workspace_id or set(
                delivery.route_credentials
            ) != set(delivery.binding.actors):
                raise ValueError("Delivery binding differs")
            for route, credential in delivery.route_credentials.items():
                principal = roster.get(credential)
                if principal is None or principal.id != delivery.binding.actors[route]:
                    raise ValueError("Delivery route actor differs")
                principal.require("read")
                principal.require("orchestration.drive")
        return self

    def require_store(self, identity: StoreIdentity):
        if identity != StoreIdentity("cloudflare-do", self.namespace + "/" + self.object_id):
            raise ResearchError(ErrorCode.STALE_FENCE, "Deployment physical authority differs")


@dataclass(frozen=True, repr=False)
class DeploymentSecrets:
    assignment_seed: bytes = field(repr=False)
    dispatch_key: bytes = field(repr=False)
    witness_consumer: str = field(repr=False)
    bootstrap: str = field(repr=False)
    credentials: Mapping[str, str] = field(repr=False)

    def __repr__(self):
        return "DeploymentSecrets(<redacted>)"

    def require_bootstrap(self, credential):
        if (
            not isinstance(credential, str)
            or re.fullmatch(r"[A-Za-z0-9_-]{43,128}", credential) is None
            or not hmac.compare_digest(credential, self.bootstrap)
        ):
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid provisioning credential")

    def registrations(self, manifest):
        roster = {item.id: item.principal for item in manifest.credentials}
        return {
            roster[item.gateway_credential_id].id: DeliveryRegistration(
                GatewayBinding.model_validate(item.binding.model_dump(mode="json")),
                {
                    route: self.credentials[identifier]
                    for route, identifier in item.route_credentials.items()
                },
            )
            for item in manifest.deliveries
        }


def load_deployment(public_json, secret_json, *, crypto, allow_loopback=False):
    """Only trusted deployment bindings enter here; errors never echo input values."""
    try:
        if (
            not isinstance(public_json, str)
            or not isinstance(secret_json, str)
            or len(public_json.encode()) > MAX_DEPLOYMENT_BYTES
            or len(secret_json.encode()) > MAX_SECRET_BYTES
            or type(allow_loopback) is not bool
        ):
            raise ValueError("Invalid deployment bounds")
        manifest = DeploymentManifest.model_validate(strict_json_loads(public_json))
        raw = strict_json_loads(secret_json)
        if not isinstance(raw, dict) or set(raw) != {
            "assignment_seed_hex",
            "dispatch_key_hex",
            "witness_consumer",
            "bootstrap",
            "credentials",
        }:
            raise ValueError("Invalid secret fields")
        origin = urlsplit(manifest.witness_origin)
        if (
            not origin.hostname
            or origin.username
            or origin.password
            or origin.path
            or origin.query
            or origin.fragment
            or manifest.witness_origin != origin.scheme + "://" + origin.netloc
            or not (
                origin.scheme == "https"
                or (allow_loopback and origin.scheme == "http" and origin.hostname == "127.0.0.1")
            )
        ):
            raise ValueError("Invalid witness origin")
        _ = origin.port
        if any(
            not isinstance(raw[name], str) or re.fullmatch(r"[0-9a-f]{64}", raw[name]) is None
            for name in ("assignment_seed_hex", "dispatch_key_hex")
        ):
            raise ValueError("Invalid deployment keys")
        seed, dispatch = (
            bytes.fromhex(raw["assignment_seed_hex"]),
            bytes.fromhex(raw["dispatch_key_hex"]),
        )
        config = manifest.configuration
        if (
            seed == dispatch
            or raw["witness_consumer"] == raw["bootstrap"]
            or public_key(crypto, seed).hex() != config.assignment_public_key
            or dispatch_key_digest(dispatch) != config.dispatch_key_digest
            or secret_digest("witness", raw["witness_consumer"])
            != manifest.witness_credential_digest
            or secret_digest("bootstrap", raw["bootstrap"]) != manifest.bootstrap_credential_digest
            or not isinstance(raw["credentials"], dict)
            or set(raw["credentials"]) != {item.id for item in manifest.credentials}
        ):
            raise ValueError("Deployment secrets differ")
        tokens = dict(raw["credentials"])
        secret_roles = (
            raw["assignment_seed_hex"],
            raw["dispatch_key_hex"],
            raw["witness_consumer"],
            raw["bootstrap"],
        )
        if len(set(secret_roles)) != len(secret_roles) or any(
            token in secret_roles for token in tokens.values()
        ):
            raise ValueError("Deployment credentials must have separate roles")
        for item in manifest.credentials:
            token = tokens[item.id]
            if (
                not isinstance(token, str)
                or re.fullmatch(r"rsw_[A-Za-z0-9_-]{43}", token) is None
                or CredentialStore._digest(token) != item.token_digest
            ):
                raise ValueError("Deployment credential differs")
        return manifest, DeploymentSecrets(
            seed, dispatch, raw["witness_consumer"], raw["bootstrap"], MappingProxyType(tokens)
        )
    except Exception:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid deployment configuration"
        ) from None
