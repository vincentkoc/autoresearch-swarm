"""Explicit local-only provisioning and probes for the real shared-service router."""

import sys
from typing import Any, Literal

from durable_sql import DurableObjectStore
from ed25519 import NobleEd25519
from exclusive import exclusive
from pydantic import Field
from r2_binding import R2BindingTransport
from research_router import (
    ExistingResearchRouter,
    HostedArtifactConfiguration,
    HostedConfiguration,
    dispatch_key_digest,
)
from witness_qualification import SEED, identifier

from research_swarm.control.auth import CredentialStore
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.crypto import public_key
from research_swarm.domain.artifacts import StoredBlob
from research_swarm.domain.assignments import signing_key_id
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.clocks import ClockPolicy
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.time_witnesses import (
    TimeWitnessPayload,
    WitnessAuthorityScope,
    WitnessSource,
    witness_key_id,
)
from research_swarm.domain.types import VersionedRecord
from research_swarm.state.database import SCHEMA, Database, Transaction, encode
from research_swarm.state.time_providers import TimeProviders
from research_swarm.storage.async_r2 import BoundedR2Configuration, BoundedR2Objects

NAMESPACE = "router-qualification"
OPERATOR_TOKEN, WORKER_TOKEN = "rsw_" + "O" * 43, "rsw_" + "W" * 43
DISPATCH_KEY = b"\x00" * 32


class RouterProbe(VersionedRecord):
    action: Literal[
        "provision", "command", "query", "inspect", "revoke", "expire", "upload", "bytes"
    ]
    key: str = Field(pattern=r"^[a-z0-9-]{1,64}$")
    operation: str = Field(default="", max_length=128)
    payload: dict[str, Any] = Field(default_factory=dict)
    token: str = Field(default="", max_length=256)
    expected_revision: int | None = Field(default=None, strict=True, ge=0)
    witness_origin: str = Field(max_length=256)
    timeout_ms: int = Field(default=8000, strict=True, ge=100, le=10000)
    bypass_gate: bool = Field(default=False, strict=True)
    artifact_profile: bool = Field(default=False, strict=True)
    content_hex: str = Field(default="", max_length=8192, pattern=r"^(?:[0-9a-f]{2})*$")
    offset: int = Field(default=0, strict=True, ge=0)
    r2_fault: Literal["none", "unavailable", "lost-put", "held-put"] = "none"


class UnserializedFixture:
    """Negative control only; deliberately omit both router gates."""

    async def blockConcurrencyWhile(self, callback):
        await callback()


def configuration(object_id, crypto):
    public = public_key(crypto, SEED).hex()
    source = WitnessSource(
        id=identifier("source"),
        public_key=public,
        key_id=witness_key_id(public),
        native_profile="linux-boottime-v1",
        policy=ClockPolicy(),
    )
    principals = (
        Principal(
            id=identifier("router-operator"),
            workspace_id=identifier("router-workspace"),
            scopes={"*"},
        ),
        Principal(
            id=identifier("router-worker-principal"),
            workspace_id=identifier("router-workspace"),
            scopes={"worker.query", "worker.publish"},
        ),
    )
    config = HostedConfiguration(
        store_location=f"{NAMESPACE}/{object_id}",
        authority=WitnessAuthorityScope(
            state_authority_id=record_digest("fixture.router-state", {"object_id": object_id}),
            activation_id=identifier("router-activation"),
            workspace_id=principals[0].workspace_id,
            controller_id=identifier("router-controller"),
            control_epoch=1,
            assignment_key_id=signing_key_id(public),
        ),
        source=source,
        assignment_public_key=public,
        dispatch_key_digest=dispatch_key_digest(DISPATCH_KEY),
        credential_manifest_digest=record_digest(
            "fixture.router-credentials",
            {
                "principals": [principal.model_dump(mode="json") for principal in principals],
                "tokens": [
                    CredentialStore._digest(token) for token in (OPERATOR_TOKEN, WORKER_TOKEN)
                ],
            },
        ),
    )
    return config, principals


def provision(state, object_id, crypto, config, principals):
    """One explicit synthetic setup operation. Never called by ordinary requests."""
    store = DurableObjectStore(
        state.storage, namespace=NAMESPACE, object_id=object_id, schema=SCHEMA
    )
    exists = store.read(
        lambda sql: sql.execute(
            "SELECT name FROM sqlite_master WHERE name='hosted_authority_config'"
        ).fetchone()
    )
    if exists is not None:
        raise ResearchError(ErrorCode.INVALID_TRANSITION, "Fixture was already provisioned")
    database = Database.from_store(store)
    database.bootstrap(config.authority.workspace_id, config.authority.controller_id)
    credentials = CredentialStore(database)
    for name, principal, token in zip(
        ("operator", "worker"), principals, (OPERATOR_TOKEN, WORKER_TOKEN), strict=True
    ):
        credentials.install(
            principals[0],
            principal,
            identifier=identifier("router-credential-" + name),
            token=token,
        )
    registry = TimeProviders(store.identity, crypto_provider=crypto)
    register = CommandContext(
        principal=principals[0], idempotency_key="register", expected_revision=0
    )
    initial = store.write(
        lambda sql: registry.register(Transaction(sql, register), config.authority, config.source)
    )
    admission = CommandContext(
        principal=principals[0], idempotency_key="qualify", expected_revision=initial.revision
    )
    binding, pending = registry.prepare(admission, initial, "fixture.admission", {})
    proof = pending.complete(
        canonical_bytes(
            WitnessSigner(config.source, SEED, crypto_provider=crypto)
            .sign(
                TimeWitnessPayload(
                    request=pending.request,
                    source_id=config.source.id,
                    source_epoch=identifier("epoch-1"),
                    native_profile=config.source.native_profile,
                    policy=config.source.policy,
                    authority_time_ms=1_793_000_000_000,
                )
            )
            .model_dump(mode="json")
        )
    )
    store.write(
        lambda sql: registry.requalify(
            Transaction(sql, admission), binding, proof, name="fixture.admission", payload={}
        )
    )

    def save(sql):
        sql.execute(
            "CREATE TABLE hosted_authority_config "
            "(singleton INTEGER PRIMARY KEY CHECK(singleton=1),digest TEXT,payload TEXT)"
        )
        sql.execute(
            "INSERT INTO hosted_authority_config VALUES (1,?,?)",
            (config.digest, encode(config.model_dump(mode="json"))),
        )

    store.write(save)
    return {
        "operator_token": OPERATOR_TOKEN,
        "worker_token": WORKER_TOKEN,
        "enrollment": {
            "worker_id": identifier("router-worker"),
            "session_id": identifier("router-session"),
            "principal_id": principals[1].id,
            "name": "Synthetic workerd worker",
            "clock_mode": "bounded-continuous-v1",
            "profiles": [
                {
                    "name": "cpu",
                    "backend": "process",
                    "cpu_slots": 1,
                    "execution_fingerprint": "sha256:" + "b" * 64,
                }
            ],
        },
    }


class FixtureR2Transport(R2BindingTransport):
    """Fault timing around actual R2 calls, never a replacement bucket implementation."""

    def __init__(self, bucket, module, config, probe, http):
        super().__init__(bucket, module, config)
        self.probe, self.http = probe, http

    async def get(self, *args, **kwargs):
        if self.probe.r2_fault == "unavailable":
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture R2 unavailable")
        return await super().get(*args, **kwargs)

    async def put(self, *args, **kwargs):
        if self.probe.r2_fault == "unavailable":
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture R2 unavailable")
        if self.probe.r2_fault == "held-put" and kwargs["if_absent"]:
            await self.http.request(
                self.probe.witness_origin,
                "/storage/" + self.probe.key,
                "{}",
                float(self.probe.timeout_ms),
            )
        result = await super().put(*args, **kwargs)
        if self.probe.r2_fault == "lost-put" and kwargs["if_absent"]:
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture discarded PUT response")
        return result


async def exercise(state, object_id, probe, module, transport, *, bucket=None, r2_module=None):
    crypto = NobleEd25519(module)
    config, principals = configuration(object_id, crypto)
    objects = None
    if probe.artifact_profile:
        config = HostedArtifactConfiguration(
            **config.model_dump(exclude={"profile"}),
            storage=BoundedR2Configuration(
                workspace_id=config.authority.workspace_id,
                bucket_identity="local-qualification",
                prefix="router-artifacts-" + object_id,
                max_object_bytes=4096,
            ),
            storage_limit_bytes=8192,
        )
        objects = BoundedR2Objects(
            config.storage,
            FixtureR2Transport(
                bucket,
                r2_module,
                config.storage,
                probe,
                transport,
            ),
        )

    def open_store():
        return DurableObjectStore.open_existing(
            state.storage, namespace=NAMESPACE, object_id=object_id
        )

    async def acquire(request):
        try:
            response = await transport.request(
                probe.witness_origin,
                "/witness/" + probe.key,
                canonical_bytes(request.model_dump(mode="json")).decode(),
                float(probe.timeout_ms),
            )
        except Exception as error:
            raise ResearchError(
                ErrorCode.TRANSPORT_UNAVAILABLE, "Local router witness unavailable"
            ) from error
        return response.encode()

    try:
        if probe.action == "provision":

            async def setup():
                return provision(state, object_id, crypto, config, principals)

            return await exclusive(state, setup)
        router = await ExistingResearchRouter.open(
            UnserializedFixture() if probe.bypass_gate else state,
            open_store,
            config,
            dispatch_key=DISPATCH_KEY,
            assignment_key=SEED,
            crypto=crypto,
            acquire=acquire,
            objects=objects,
        )
        if probe.action == "command":
            result = await router.command(
                probe.token,
                probe.operation,
                probe.payload,
                idempotency_key=probe.key,
                expected_revision=probe.expected_revision,
            )
        elif probe.action == "query":
            result = await router.query(probe.token, probe.operation, probe.payload)
        elif probe.action == "upload":
            result = {
                "received_bytes": await router.upload_part(
                    probe.token,
                    probe.payload["upload_id"],
                    probe.offset,
                    bytes.fromhex(probe.content_hex),
                )
            }
        elif probe.action == "bytes":

            async def read_bytes():
                context = router._context(probe.token, idempotency_key=probe.key)
                router.artifacts._admit(probe.token, context)
                _, upload = router.artifacts._upload(context, probe.payload["upload_id"])
                blob = StoredBlob(reference=upload.reference(), generation=upload.id)
                return {"content_hex": (await objects.read(upload.workspace_id, blob)).hex()}

            result = await exclusive(state, read_bytes)
        else:

            async def inspect():
                if probe.action == "revoke":
                    router.credentials.revoke(
                        principals[0], identifier("router-credential-operator")
                    )
                elif probe.action == "expire":
                    router.database.store.write(
                        lambda sql: sql.execute(
                            "UPDATE credentials SET expires_at=? WHERE id=?",
                            ("2999-01-01T00:00:00.000Z", identifier("router-credential-worker")),
                        )
                    )

                def snapshot(sql):
                    return {
                        "counts": {
                            table: sql.execute("SELECT COUNT(*) FROM " + table).one()[0]
                            for table in ("commands", "events", "records", "credentials")
                        },
                        "provider_state": sql.execute(
                            "SELECT state FROM records WHERE kind='time_provider'"
                        ).one()[0],
                        "retirements": sql.execute(
                            "SELECT COUNT(*) FROM records WHERE kind='time_source_retirement'"
                        ).one()[0],
                        "artifact_effects": sql.execute(
                            "SELECT COUNT(*) FROM records "
                            "WHERE kind IN ('artifact','artifact_pin','upload')"
                        ).one()[0],
                    }

                return router.database.store.read(snapshot)

            result = await exclusive(state, inspect)
        assert (
            not {"sqlite3", "research_swarm.state.sqlite", "research_swarm.crypto_native", "psutil"}
            & sys.modules.keys()
        )
        return result
    except ResearchError as error:
        return error.as_dict()
