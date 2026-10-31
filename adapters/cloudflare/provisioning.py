"""Two atomic provisioning phases with a fresh native witness between them."""

import asyncio

from research_swarm.control.auth import CredentialStore
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import ID, Digest, Timestamp, VersionedRecord, utc_now
from research_swarm.state.database import Transaction, encode
from research_swarm.state.schema_contracts import TABLES, require_credentials_schema, require_table
from research_swarm.state.time_providers import TimeProviders

if __package__:
    from .exclusive import exclusive
else:
    from exclusive import exclusive

OPERATION = "deployment.provision"


class ProvisioningReceipt(VersionedRecord):
    manifest_digest: Digest
    configuration_digest: Digest
    workspace_id: ID
    controller_id: ID
    activation_id: ID
    provider_generation: ID
    source_epoch: ID
    provisioned_at: Timestamp


def exists(sql, name):
    return (
        sql.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        is not None
    )


def intent(sql, manifest):
    require_table(
        sql, "hosted_provisioning", "singleton digest manifest state receipt", ("singleton",)
    )
    row = sql.execute("SELECT * FROM hosted_provisioning LIMIT 2").one()
    if (
        row["singleton"] != 1
        or row["digest"] != manifest.digest
        or strict_json_loads(row["manifest"]) != manifest.model_dump(mode="json")
        or row["state"] not in {"prepared", "complete"}
        or (row["receipt"] is None) != (row["state"] == "prepared")
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Provisioning intent differs")
    return row


def require_deployment(store, manifest):
    """Read-only reopen seal; never repair configuration or reinstall credentials."""
    manifest.require_store(store.identity)

    def read(sql):
        row = intent(sql, manifest)
        if row["state"] != "complete":
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Deployment is not provisioned")
        require_table(sql, "hosted_authority_config", "singleton digest payload", ("singleton",))
        configured = sql.execute("SELECT * FROM hosted_authority_config LIMIT 2").one()
        config = manifest.configuration
        if (
            configured["singleton"] != 1
            or configured["digest"] != config.digest
            or strict_json_loads(configured["payload"]) != config.model_dump(mode="json")
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Provisioned configuration differs")
        receipt = ProvisioningReceipt.model_validate_json(row["receipt"])
        if (
            receipt.manifest_digest != manifest.digest
            or receipt.configuration_digest != config.digest
            or receipt.workspace_id != config.authority.workspace_id
            or receipt.controller_id != config.authority.controller_id
            or receipt.activation_id != config.authority.activation_id
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Provisioning receipt differs")
        return receipt.model_dump(mode="json")

    return store.read(read)


def _fresh(sql):
    allowed = {item[0] for item in TABLES} | {"research_schema", "credentials"}
    names = sql.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND substr(name,1,7)!='sqlite_' AND substr(name,1,5)!='__cf_' LIMIT 65"
    ).fetchall()
    if len(names) > 64 or any(row[0] not in allowed for row in names):
        raise ResearchError(ErrorCode.SCHEMA_UNSUPPORTED, "Unowned state cannot be provisioned")
    for row in names:
        if (
            row[0] != "research_schema"
            and sql.execute(f'SELECT 1 FROM "{row[0]}" LIMIT 1').fetchone() is not None
        ):
            raise ResearchError(
                ErrorCode.INVALID_TRANSITION, "Existing state cannot be provisioned"
            )


def _roster(sql, manifest):
    require_credentials_schema(sql)
    if sql.execute("SELECT COUNT(*) FROM credentials").one()[0] != len(manifest.credentials):
        raise ResearchError(ErrorCode.STALE_FENCE, "Prepared credential roster differs")
    for expected in manifest.credentials:
        row = sql.execute("SELECT * FROM credentials WHERE id=?", (expected.id,)).fetchone()
        if (
            row is None
            or row["token_digest"] != expected.token_digest
            or row["workspace_id"] != expected.principal.workspace_id
            or Principal.model_validate_json(row["principal"]) != expected.principal
            or row["revoked"]
            or row["expires_at"] is not None
        ):
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Prepared credential is unavailable"
            )


def _prepare(store, manifest, secrets, registry, context):
    def prepare(sql):
        if not exists(sql, "hosted_provisioning"):
            _fresh(sql)
            sql.execute(
                "CREATE TABLE hosted_provisioning "
                "(singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                "digest TEXT NOT NULL,manifest TEXT NOT NULL,state TEXT NOT NULL,receipt TEXT)"
            )
            sql.execute(
                "INSERT INTO hosted_provisioning VALUES (1,?,?,'prepared',NULL)",
                (
                    manifest.digest,
                    encode(manifest.model_dump(mode="json")),
                ),
            )
            authority = manifest.configuration.authority
            sql.execute(
                "INSERT INTO workspaces(id,controller_id,control_epoch) VALUES (?,?,?)",
                (
                    authority.workspace_id,
                    authority.controller_id,
                    authority.control_epoch,
                ),
            )
            CredentialStore.initialize_in(sql)
            require_credentials_schema(sql)
            for credential in manifest.credentials:
                CredentialStore.install_in(
                    sql,
                    context.principal,
                    credential.principal,
                    identifier=credential.id,
                    token=secrets.credentials[credential.id],
                )
            registry.register(Transaction(sql, context), authority, manifest.configuration.source)
        row = intent(sql, manifest)
        if row["state"] == "complete":
            return None
        if exists(sql, "hosted_authority_config"):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Incomplete provisioning has public configuration"
            )
        _roster(sql, manifest)
        current = registry.load(Transaction(sql, context), manifest.configuration.authority)
        if current.state != "unqualified" or current.data.source != manifest.configuration.source:
            raise ResearchError(ErrorCode.STALE_FENCE, "Prepared source differs")
        return current

    return store.write(prepare)


async def provision(state, open_store, manifest, secrets, *, identity, credential, crypto, acquire):
    """Private operator entry only. The factory must initialize only an empty store."""
    secrets.require_bootstrap(credential)
    manifest.require_store(identity)

    async def initialize():
        store = open_store()
        manifest.require_store(store.identity)
        registry = TimeProviders(store.identity, crypto_provider=crypto)
        issuer = next(
            item.principal
            for item in manifest.credentials
            if item.principal.id == manifest.provisioning_actor_id
        )
        context = CommandContext(
            principal=issuer, idempotency_key="provision:" + manifest.digest, expected_revision=0
        )
        prepared = _prepare(store, manifest, secrets, registry, context)
        if prepared is None:
            return require_deployment(store, manifest)
        context = context.model_copy(update={"expected_revision": prepared.revision})
        payload = {"manifest_digest": manifest.digest}
        binding, pending = registry.prepare(context, prepared, OPERATION, payload)
        owner = asyncio.current_task()
        timeout = asyncio.timeout(10)
        try:
            async with timeout:
                response = await acquire(pending.request)
            if timeout.expired():
                raise TimeoutError("Provisioning witness arrived after its deadline")
        except TimeoutError:
            raise ResearchError(
                ErrorCode.TRANSPORT_UNAVAILABLE, "Provisioning witness unavailable", retryable=True
            ) from None
        if owner is None or owner.cancelling():
            raise asyncio.CancelledError("Provisioning owner was cancelled")
        proof = pending.complete(response)

        def complete(sql):
            row = intent(sql, manifest)
            if row["state"] != "prepared" or exists(sql, "hosted_authority_config"):
                raise ResearchError(ErrorCode.STALE_FENCE, "Provisioning state changed")
            _roster(sql, manifest)
            admitted = registry.requalify(
                Transaction(sql, context), binding, proof, name=OPERATION, payload=payload
            )
            receipt = ProvisioningReceipt(
                manifest_digest=manifest.digest,
                configuration_digest=manifest.configuration.digest,
                workspace_id=manifest.configuration.authority.workspace_id,
                controller_id=manifest.configuration.authority.controller_id,
                activation_id=manifest.configuration.authority.activation_id,
                provider_generation=admitted.data.scope.provider_generation,
                source_epoch=admitted.data.source_epoch,
                provisioned_at=utc_now(),
            ).model_dump(mode="json")
            sql.execute(
                "CREATE TABLE hosted_authority_config "
                "(singleton INTEGER PRIMARY KEY CHECK(singleton=1),digest TEXT,payload TEXT)"
            )
            sql.execute(
                "INSERT INTO hosted_authority_config VALUES (1,?,?)",
                (
                    manifest.configuration.digest,
                    encode(manifest.configuration.model_dump(mode="json")),
                ),
            )
            sql.execute(
                "UPDATE hosted_provisioning SET state='complete',receipt=? WHERE singleton=1",
                (encode(receipt),),
            )
            return receipt

        return store.write(complete)

    return await exclusive(state, initialize)
