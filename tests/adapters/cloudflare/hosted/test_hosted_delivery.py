"""Bounded gateway delivery uses the existing authority gate and saved-work ledger."""

import asyncio

import pytest

from adapters.cloudflare.delivery_router import DeliveryRegistration, HostedDelivery
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import OrchestrationBinding
from research_swarm.domain.types import new_id
from research_swarm.orchestration.async_delivery import AsyncDeliveryKernel
from research_swarm.orchestration.delivery_contract import binding_reference
from research_swarm.orchestration.delivery_models import WorkOutcome
from research_swarm.orchestration.gateway_models import GatewayBinding
from tests.adapters.cloudflare.hosted.test_hosted_artifacts import current
from tests.adapters.cloudflare.hosted.test_hosted_orchestration import (
    CONFIG,
    campaign,
    domain_counts,
    drive,
    fence,
    prepare,
)
from tests.adapters.cloudflare.hosted.test_hosted_orchestration import evaluation as evaluation
from tests.adapters.cloudflare.hosted.test_hosted_orchestration import hosted as hosted
from tests.adapters.cloudflare.hosted.test_hosted_orchestration import lifecycle as lifecycle
from tests.adapters.cloudflare.hosted.test_hosted_orchestration import orchestrated as orchestrated
from tests.adapters.cloudflare.hosted.test_hosted_orchestration import project as project
from tests.adapters.cloudflare.hosted.test_hosted_orchestration import publication as publication
from tests.adapters.cloudflare.hosted.test_hosted_orchestration import research as research
from tests.adapters.cloudflare.hosted.test_hosted_orchestration import scientific as scientific

pytestmark = pytest.mark.parametrize("publication", ["evaluation"], indirect=True)


@pytest.fixture
async def delivery(orchestrated):
    e, h = orchestrated, orchestrated.h
    e.gateway = h.credentials.issue(
        h.principal,
        h.principal.model_copy(update={"id": new_id(), "scopes": {"orchestration.deliver"}}),
    )
    e.reference = GatewayBinding.from_reference(
        binding_reference(OrchestrationBinding.model_validate(e.binding["data"]))
    )
    e.tokens = {"control": e.driver.token, "evaluator": e.evaluator.token}
    e.registrations = {
        e.gateway.principal.id: DeliveryRegistration(e.reference, e.tokens),
    }
    e.facade = HostedDelivery(h.router, e.registrations)
    return e


async def send(e, operation, payload=None, *, body=None, token=None, consume=None):
    async def read(limit):
        if consume is not None:
            consume.append(limit)
        return body if body is not None else canonical_bytes(payload)

    response = await e.facade.handle(
        "POST",
        "/v1/delivery/" + operation,
        {"authorization": "Bearer " + (e.gateway.token if token is None else token)},
        read,
    )
    assert len(response.body) <= 16 * 1024
    assert "no-store" in response.headers["cache-control"].split(", ")
    return response.status, strict_json_loads(response.body)


async def work(e):
    status, pending = await send(e, "inspect", e.reference.model_dump(mode="json"))
    assert status == 200 and pending["state"] == "work"
    return pending["work"]


async def test_duplicate_delivery_recovers_one_effect_after_a_lost_reply(delivery, monkeypatch):
    e, h = delivery, delivery.h
    reference = e.reference.model_dump(mode="json")
    assert (await send(e, "bootstrap", {"binding": reference, "recovery_id": new_id()}))[1][
        "state"
    ] == "active"
    turn = await prepare(e)
    pending = await work(e)
    assert pending["record_id"] == turn["id"]
    original = AsyncDeliveryKernel.execute

    async def lost(self, request):
        await original(self, request)
        raise RuntimeError("Private route credential: " + e.driver.token)

    monkeypatch.setattr(AsyncDeliveryKernel, "execute", lost)
    status, uncertain = await send(e, "execute", pending)
    assert status == 503 and uncertain["error"]["code"] == "DELIVERY_UNCERTAIN"
    assert e.driver.token not in str(uncertain)
    monkeypatch.setattr(AsyncDeliveryKernel, "execute", original)
    before = domain_counts(h), len(h.witness.requests), len(h.transport.calls)
    responses = await asyncio.gather(*(send(e, "execute", pending) for _ in range(3)))
    assert responses == [responses[0]] * 3
    assert responses[0][0] == 200 and responses[0][1]["disposition"] == "accepted"
    assert (domain_counts(h), len(h.witness.requests), len(h.transport.calls)) == before
    for kind in ("report", "drive_receipt"):
        assert len((await h.router.query(h.operator.token, "list", {"kind": kind}))["items"]) == 1
    assert (await send(e, "inspect", reference))[1]["state"] == "idle"
    h.credentials.revoke(h.principal, e.driver.id)
    assert (await send(e, "execute", pending))[0] == 403  # Replay still needs live routes.


async def test_registration_is_copied_and_request_identity_cannot_reroute(delivery):
    e, h = delivery, delivery.h
    reference = e.reference.model_dump(mode="json")
    e.tokens["control"] = h.operator.token
    e.reference.actors["control"] = h.operator.principal.id
    e.registrations.clear()
    assert (await send(e, "inspect", reference))[1]["state"] == "idle"
    before = domain_counts(h), len(h.witness.requests)
    for changed in (
        {**reference, "epoch": 2},
        {**reference, "workspace_id": new_id()},
        {**reference, "configuration_digest": "sha256:" + "f" * 64},
        {**reference, "actors": {**reference["actors"], "control": new_id()}},
    ):
        assert (await send(e, "inspect", changed))[0] == 403
    assert (domain_counts(h), len(h.witness.requests)) == before


async def test_authentication_scope_and_current_credentials_precede_body(delivery):
    e, h = delivery, delivery.h
    reference, consumed = e.reference.model_dump(mode="json"), []
    assert (await send(e, "inspect", reference, token="invalid", consume=consumed))[0] == 401
    assert (await send(e, "inspect", reference, token=e.driver.token, consume=consumed))[0] == 403
    expired = h.credentials.issue(
        h.principal, e.gateway.principal, expires_at="2099-01-01T00:00:00.000Z"
    )
    assert (await send(e, "inspect", reference, token=expired.token, consume=consumed))[0] == 401
    unregistered = h.credentials.issue(
        h.principal, e.gateway.principal.model_copy(update={"id": new_id()})
    )
    assert (await send(e, "inspect", reference, token=unregistered.token, consume=consumed))[
        0
    ] == 403
    h.credentials.revoke(h.principal, e.gateway.id)
    assert (await send(e, "inspect", reference, consume=consumed))[0] == 401
    assert consumed == []


async def test_route_identity_and_registration_configuration_refuse_substitution(delivery):
    e, h = delivery, delivery.h
    wrong = {**e.tokens, "control": h.operator.token}
    e.facade = HostedDelivery(
        h.router, {e.gateway.principal.id: DeliveryRegistration(e.reference, wrong)}
    )
    assert (await send(e, "inspect", e.reference.model_dump(mode="json")))[0] == 403
    for actor, binding, tokens in (
        (e.driver.principal.id, e.reference, e.tokens),
        (e.gateway.principal.id, e.reference, {"control": e.driver.token}),
        (
            e.gateway.principal.id,
            e.reference.model_copy(update={"workspace_id": new_id()}),
            e.tokens,
        ),
    ):
        with pytest.raises(ValueError, match="Invalid trusted"):
            HostedDelivery(h.router, {actor: DeliveryRegistration(binding, tokens)})


async def test_event_wait_needs_an_authoritative_event_and_does_not_consume_it(delivery):
    e, h = delivery, delivery.h
    row = campaign(e)
    context = await h.router.query(h.operator.token, "context", {"campaign_id": row["id"]})
    binding = current(h, "orchestration_binding", row["id"])
    wait_id = new_id()
    await drive(
        e,
        "orchestration.record_wait",
        {
            "wait_id": wait_id,
            "campaign_id": row["id"],
            "epoch": 1,
            "configuration_digest": CONFIG,
            "condition": {
                "observed_watermark": context["watermark"],
                "event_types": ["report.created"],
            },
        },
        key="wait",
        revision=binding["revision"],
    )
    pending = await work(e)
    assert pending["record_id"] == wait_id
    assert (await send(e, "wait", pending))[1]["state"] == "pending"
    await drive(e, "report.create", {"campaign_id": row["id"]}, key="event", credential=h.operator)
    ready = await send(e, "wait", pending)
    assert ready[0] == 200 and ready[1]["state"] == "settled"
    assert ready[1]["disposition"] == "event" and ready[1]["watermark"] > context["watermark"]
    assert await send(e, "wait", pending) == ready
    assert current(h, "orchestration_wait", wait_id)["state"] == "ready"
    assert current(h, "wakeup_delivery", wait_id)["state"] == "pending"
    await fence(e)
    assert (await send(e, "inspect", e.reference.model_dump(mode="json")))[1]["state"] == "retired"


@pytest.mark.parametrize("failure", ["unexpected", "cancelled"])
async def test_failure_reaches_gate_and_request_clients_cannot_escape(
    delivery, monkeypatch, failure
):
    e, h = delivery, delivery.h
    captured, failures = [], []
    gate = h.gate.blockConcurrencyWhile

    async def observed(callback):
        try:
            await gate(callback)
        except BaseException as error:
            failures.append(type(error))
            raise

    async def interrupted(self, binding):
        client = next(iter(self.connections.values())).clients["control"]
        captured.append(client)
        # A detached task may not inherit the request's private gate ownership.
        with pytest.raises(ResearchError) as denied:
            await asyncio.create_task(client.get("campaign", binding.campaign_id))
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        if failure == "cancelled":
            raise asyncio.CancelledError()
        raise RuntimeError("Private failure " + e.evaluator.token)

    original = AsyncDeliveryKernel.inspect
    monkeypatch.setattr(h.gate, "blockConcurrencyWhile", observed)
    monkeypatch.setattr(AsyncDeliveryKernel, "inspect", interrupted)
    reference = e.reference.model_dump(mode="json")
    if failure == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            await send(e, "inspect", reference)
        assert failures == [asyncio.CancelledError]
    else:
        status, response = await send(e, "inspect", reference)
        assert status == 503 and e.evaluator.token not in str(response)
        assert failures == [RuntimeError]
    with pytest.raises(ResearchError) as ended:
        await captured[0].get("campaign", campaign(e)["id"])
    assert ended.value.code == ErrorCode.AUTHORIZATION_DENIED
    monkeypatch.setattr(AsyncDeliveryKernel, "inspect", original)
    assert (await send(e, "inspect", reference))[1]["state"] == "idle"


async def test_wire_bounds_are_sanitized_and_do_not_change_saved_work(delivery, monkeypatch):
    e, h = delivery, delivery.h
    await prepare(e)
    pending = await work(e)
    before = domain_counts(h), len(h.witness.requests)
    for body in (b"{" * (16 * 1024 + 1), b"{}", b'{"x":1,"x":2}', b"\xff"):
        assert (await send(e, "execute", body=body))[0] == 422
    assert (domain_counts(h), len(h.witness.requests)) == before

    async def oversized(self, request):
        return WorkOutcome("settled", request.record_id, evidence_id="x" * (16 * 1024))

    monkeypatch.setattr(AsyncDeliveryKernel, "execute", oversized)
    status, response = await send(e, "execute", pending)
    assert status == 503 and response["error"]["code"] == "DELIVERY_UNCERTAIN"
    assert len(str(response)) < 200


async def test_older_profile_refuses_delivery_before_body(scientific):
    facade, consumed = HostedDelivery(scientific.h.router, {}), []

    async def read(limit):
        consumed.append(limit)
        return b"{}"

    response = await facade.handle("POST", "/v1/delivery/inspect", {}, read)
    assert response.status == 501 and consumed == []
