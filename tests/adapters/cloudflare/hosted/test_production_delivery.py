"""Production delegation uses the sealed roster and ordinary post-bootstrap campaigns."""

import asyncio
from types import SimpleNamespace

import pytest

from adapters.cloudflare.deployment import DeploymentManifest
from adapters.cloudflare.provisioning import require_deployment
from adapters.cloudflare.runtime import serve
from research_swarm.control.auth import CredentialStore
from research_swarm.domain.canonical import canonical_bytes, content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.orchestration.async_delivery import AsyncDeliveryKernel
from research_swarm.state.database import Transaction, encode
from tests.adapters.cloudflare.hosted.test_hosted_research import admit, sdk
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.adapters.cloudflare.test_cloudflare_provisioning import rows, run
from tests.adapters.cloudflare.test_cloudflare_runtime import opened
from tests.projects.test_manifest import project as project

CONFIG = content_digest(b"post-bootstrap-driver")


@pytest.fixture
async def production(bootstrap, project):
    e = bootstrap
    e.manifest = DeploymentManifest.model_validate(
        {**e.manifest.model_dump(mode="json"), "deliveries": []}
    )
    e.bootstrap_receipt = await run(e)
    e.router = await opened(e)
    e.operator = SimpleNamespace(token=e.secrets.credentials[e.manifest.credentials[0].id])
    e.gateway, e.driver, e.evaluator = e.manifest.credentials[1:]
    loop = asyncio.get_running_loop()

    def admission():
        with sdk(e, loop) as client:
            return admit(client, project)

    e.admitted = await asyncio.to_thread(admission)
    campaign = e.admitted.campaign
    e.binding = (
        await e.router.command(
            e.operator.token,
            "orchestration.bind",
            {
                "campaign_id": campaign["id"],
                "configuration_digest": CONFIG,
                "adapter": "cloudflare-workflows",
                "routes": {"control": e.driver.principal.id, "evaluator": e.evaluator.principal.id},
            },
            idempotency_key="bind",
            expected_revision=campaign["revision"],
        )
    )["result"]
    e.intent = {
        "campaign_id": campaign["id"],
        "epoch": 1,
        "configuration_digest": CONFIG,
        "gateway_credential_id": e.gateway.id,
        "route_credentials": {"control": e.driver.id, "evaluator": e.evaluator.id},
    }
    e.reference = {
        "schema_version": "1.0",
        "workspace_id": e.manifest.configuration.authority.workspace_id,
        "campaign_id": campaign["id"],
        "epoch": 1,
        "configuration_digest": CONFIG,
        "actors": e.binding["data"]["routes"],
    }
    return e


async def register(e, *, key="register", intent=None):
    return await e.router.command(
        e.operator.token,
        "orchestration.register_delivery",
        e.intent if intent is None else intent,
        idempotency_key=key,
        expected_revision=0,
    )


async def delivery(e, operation="inspect", payload=None, *, token=None, before_body=None):
    async def read(limit):
        if before_body is not None:
            before_body()
        raw = canonical_bytes(e.reference if payload is None else payload)
        assert len(raw) <= limit
        return raw

    response = await serve(
        e.router,
        e.manifest,
        e.secrets,
        "POST",
        "/v1/delivery/" + operation,
        {"authorization": "Bearer " + (token or e.secrets.credentials[e.gateway.id])},
        read,
    )
    return response.status, strict_json_loads(response.body)


async def prepare(e):
    return (
        await e.router.command(
            e.secrets.credentials[e.driver.id],
            "orchestration.prepare_turn",
            {
                "campaign_id": e.binding["id"],
                "epoch": 1,
                "ordinal": 1,
                "configuration_digest": CONFIG,
                "planning_watermark": 0,
                "action": {
                    "operation": "report.create",
                    "payload": {"campaign_id": e.binding["id"]},
                    "idempotency_key": "report",
                },
            },
            idempotency_key="prepare",
            expected_revision=e.binding["revision"],
        )
    )["result"]


async def test_post_bootstrap_campaign_registers_and_delivers_across_lost_reply_and_reopen(
    production, monkeypatch
):
    e = production
    original_manifest = e.manifest.digest
    assert (await delivery(e))[0] == 403
    results = await asyncio.gather(*(register(e, key=f"register-{i}") for i in range(3)))
    assert all(result["result"] == results[0]["result"] for result in results)
    assert (await delivery(e, "bootstrap", {"binding": e.reference, "recovery_id": new_id()}))[1][
        "state"
    ] == "active"
    turn = await prepare(e)
    status, inspected = await delivery(e)
    assert status == 200 and inspected["work"]["record_id"] == turn["id"]
    original = AsyncDeliveryKernel.execute

    async def lost(self, work):
        await original(self, work)
        raise RuntimeError("Synthetic lost delivery reply")

    with monkeypatch.context() as patch:
        patch.setattr(AsyncDeliveryKernel, "execute", lost)
        assert (await delivery(e, "execute", inspected["work"]))[0] == 503
    e.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic outage")
    before = rows(e, "records"), rows(e, "events"), rows(e, "commands")
    calls = len(e.witness.requests)
    e.router = await opened(e)
    assert await register(e, key="register-0") == results[0]
    status, outcome = await delivery(e, "execute", inspected["work"])
    assert status == 200 and outcome["disposition"] == "accepted"
    assert (rows(e, "records"), rows(e, "events"), rows(e, "commands")) == before
    assert len(e.witness.requests) == calls
    assert e.manifest.digest == original_manifest
    assert require_deployment(e.store, e.manifest) == e.bootstrap_receipt
    assert len([row for row in rows(e, "records") if row["kind"] == "report"]) == 1
    assert len([row for row in rows(e, "records") if row["kind"] == "delivery_registration"]) == 1


@pytest.mark.parametrize("changed", ["outside-roster", "revoked", "expires", "principal", "digest"])
async def test_registration_roster_preflight_refuses_before_witness(production, changed):
    e, intent = production, production.intent
    if changed == "outside-roster":
        credentials = CredentialStore.open_existing(e.router.database)
        alias = credentials.issue(e.manifest.credentials[0].principal, e.driver.principal)
        intent = {
            **intent,
            "route_credentials": {**intent["route_credentials"], "control": alias.id},
        }
    else:
        column, value = {
            "revoked": ("revoked", 1),
            "expires": ("expires_at", "2099-01-01T00:00:00.000Z"),
            "principal": (
                "principal",
                encode(
                    e.driver.principal.model_copy(update={"id": new_id()}).model_dump(mode="json")
                ),
            ),
            "digest": ("token_digest", "0" * 64),
        }[changed]
        e.store.write(
            lambda sql: sql.execute(
                f"UPDATE credentials SET {column}=? WHERE id=?", (value, e.driver.id)
            )
        )
    before = rows(e, "records"), rows(e, "events"), rows(e, "commands")
    calls = len(e.witness.requests)
    with pytest.raises(ResearchError) as refused:
        await register(e, intent=intent)
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert (rows(e, "records"), rows(e, "events"), rows(e, "commands")) == before
    assert len(e.witness.requests) == calls


async def test_gateway_alias_and_changed_route_fail_before_body_or_delivery(production):
    e = production
    await register(e)
    credentials = CredentialStore.open_existing(e.router.database)
    alias = credentials.issue(e.manifest.credentials[0].principal, e.gateway.principal)

    def forbidden():
        pytest.fail("Unregistered credential must refuse before reading the body")

    assert (await delivery(e, token=alias.token, before_body=forbidden))[0] == 403
    credentials.revoke(e.manifest.credentials[0].principal, e.driver.id)
    assert (await delivery(e, before_body=forbidden))[0] == 403


async def test_revocation_during_body_read_is_rechecked_without_witness(production):
    e = production
    await register(e)
    calls = len(e.witness.requests)

    def revoke():
        CredentialStore.open_existing(e.router.database).revoke(
            e.manifest.credentials[0].principal, e.evaluator.id
        )

    assert (await delivery(e, before_body=revoke))[0] == 403
    assert len(e.witness.requests) == calls


async def test_fenced_binding_retires_delivery_without_retargeting_registration(production):
    e = production
    receipt = await register(e)
    await e.router.command(
        e.operator.token,
        "orchestration.fence",
        {"campaign_id": e.binding["id"], "epoch": 1, "reason": "Retire delivery"},
        idempotency_key="fence",
        expected_revision=e.binding["revision"],
    )
    before = rows(e, "records"), rows(e, "events"), len(e.witness.requests)
    assert await register(e) == receipt
    assert (await delivery(e))[1]["state"] == "retired"
    assert (rows(e, "records"), rows(e, "events"), len(e.witness.requests)) == before


async def test_registration_rechecks_roster_after_witness_wait(production):
    e = production
    e.witness.entered.clear()
    e.witness.release = asyncio.Event()
    before = rows(e, "records"), rows(e, "events"), rows(e, "commands")
    task = asyncio.create_task(register(e))
    await asyncio.wait_for(e.witness.entered.wait(), timeout=2)
    CredentialStore.open_existing(e.router.database).revoke(
        e.manifest.credentials[0].principal, e.driver.id
    )
    e.witness.release.set()
    with pytest.raises(ResearchError) as refused:
        await task
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert (rows(e, "records"), rows(e, "events"), rows(e, "commands")) == before


async def test_static_registration_cannot_be_replaced_or_shadow_durable_state(bootstrap):
    e = bootstrap
    await run(e)
    e.router = await opened(e)
    e.gateway = e.manifest.credentials[1]
    static = e.manifest.deliveries[0]
    e.reference = static.binding.model_dump(mode="json")
    e.operator = SimpleNamespace(token=e.secrets.credentials[e.manifest.credentials[0].id])
    e.intent = {
        "campaign_id": static.binding.campaign_id,
        "epoch": static.binding.epoch,
        "configuration_digest": static.binding.configuration_digest,
        "gateway_credential_id": static.gateway_credential_id,
        "route_credentials": dict(static.route_credentials),
    }
    before = rows(e, "records"), rows(e, "events"), rows(e, "commands")
    calls = len(e.witness.requests)
    with pytest.raises(ResearchError) as refused:
        await register(e)
    assert refused.value.code == ErrorCode.REVISION_CONFLICT
    assert (rows(e, "records"), rows(e, "events"), rows(e, "commands")) == before
    assert len(e.witness.requests) == calls
    context = CommandContext(
        principal=e.manifest.credentials[0].principal, idempotency_key=new_id()
    )
    # Simulate conflicting storage. The resolver must refuse without choosing a winner.
    e.store.write(
        lambda sql: Transaction(sql, context).insert(
            "delivery_registration",
            e.gateway.principal.id,
            {},
            state="immutable",
            campaign_id=static.binding.campaign_id,
        )
    )

    def forbidden():
        pytest.fail("Overlapping registrations must refuse before body consumption")

    status, result = await delivery(e, before_body=forbidden)
    assert status == 409 and result["error"]["code"] == "REVISION_CONFLICT"
