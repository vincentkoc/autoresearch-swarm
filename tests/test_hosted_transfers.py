"""Queued research transfers through shared SQL and bounded asynchronous bytes."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from adapters.cloudflare import transfers as adapter
from adapters.cloudflare.research_router import HostedTransferConfiguration
from research_swarm.domain.canonical import content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction, encode
from research_swarm.storage.async_r2 import ObjectBytes
from tests.test_hosted_artifacts import current
from tests.test_hosted_http import send
from tests.test_hosted_research import hosted as hosted
from tests.test_hosted_research import project as project
from tests.test_hosted_research import research as research
from tests.test_hosted_research import setup

CONFIG = content_digest(b"hosted-transfer-driver")


def issue(h, scopes):
    return h.credentials.issue(
        h.principal, h.principal.model_copy(update={"id": new_id(), "scopes": set(scopes)})
    )


async def command(e, name, payload, *, key=None, revision=None, credential=None):
    return await e.h.router.command(
        (credential or e.h.operator).token,
        name,
        payload,
        idempotency_key=key or new_id(),
        expected_revision=revision,
    )


@pytest.fixture
async def queued(research, project):
    h = research
    h.config = HostedTransferConfiguration(**h.config.model_dump(exclude={"profile"}))
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    admitted = await setup(h, project)
    scopes = {"read", "orchestration.drive", "report.create", "campaign.manage"}
    e = SimpleNamespace(
        h=h,
        admitted=admitted,
        driver=issue(h, scopes),
        evaluator=issue(h, {"read", "orchestration.drive", "evaluation.publish"}),
        successor=issue(h, scopes),
        next_evaluator=issue(h, {"read", "orchestration.drive", "evaluation.publish"}),
    )
    row = current(h, "campaign", admitted.campaign["id"])
    e.binding = (
        await command(
            e,
            "orchestration.bind",
            {
                "campaign_id": row["id"],
                "adapter": "cloudflare-workflows",
                "configuration_digest": CONFIG,
                "routes": {"control": e.driver.principal.id, "evaluator": e.evaluator.principal.id},
            },
            revision=row["revision"],
        )
    )["result"]
    return e


async def request(e):
    row = current(e.h, "campaign", e.admitted.campaign["id"])
    if row["state"] != "paused":
        row = (
            await command(
                e,
                "campaign.pause",
                {"campaign_id": row["id"], "mode": "drain"},
                revision=row["revision"],
            )
        )["result"]
    binding = current(e.h, "orchestration_binding", row["id"])
    return {
        "transfer_id": new_id(),
        "campaign_id": row["id"],
        "campaign_revision": row["revision"],
        "epoch": 1,
        "adapter": "local-successor",
        "configuration_digest": CONFIG,
        "routes": {"control": e.successor.principal.id, "evaluator": e.next_evaluator.principal.id},
        "reason": "Move the paused queued fixture",
    }, binding["revision"]


def native_state(e):
    h = e.h
    return h.store.read(
        lambda sql: {
            "authority": dict(
                sql.execute("SELECT controller_id,control_epoch FROM workspaces").one()
            ),
            "budgets": [
                dict(row) for row in sql.execute("SELECT * FROM budgets ORDER BY dimension")
            ],
            "reservations": [
                dict(row) for row in sql.execute("SELECT * FROM budget_reservations ORDER BY id")
            ],
            "attempts": sql.execute("SELECT COUNT(*) FROM records WHERE kind='attempt'").one()[0],
        }
    )


async def rows(e, kind):
    return (await e.h.router.query(e.h.operator.token, "list", {"kind": kind}))["items"]


async def test_transfer_keeps_queued_work_and_native_authority_and_replays_without_io(queued):
    e, h = queued, queued.h
    payload, revision = await request(e)
    before = native_state(e)
    trial = current(h, "trial", e.admitted.trial["result"]["id"])
    job = current(h, "job", e.admitted.job["id"])
    scopes = {"read", "orchestration.manage", "orchestration.drive"}
    manager = issue(h, scopes)
    witness = len(h.witness.requests)
    accepted = await command(
        e, "orchestration.transfer", payload, key="transfer", revision=revision, credential=manager
    )
    assert len(h.witness.requests) == witness + 1
    assert native_state(e) == before and before["attempts"] == 0
    assert current(h, "trial", trial["id"]) == trial
    assert current(h, "job", job["id"]) == job and job["state"] == "ready"
    assert current(h, "campaign", payload["campaign_id"])["state"] == "paused"
    assert accepted["result"]["data"]["successor"]["epoch"] == 2
    assert len(await rows(e, "orchestration_checkpoint")) == 1
    assert len(await rows(e, "orchestration_start")) == 2
    assert all(row["state"] == "pending" for row in await rows(e, "orchestration_start"))
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture offline")
    h.transport.hide_reads = True
    before_io = len(h.witness.requests), len(h.transport.calls)
    assert (
        await command(
            e,
            "orchestration.transfer",
            payload,
            key="transfer",
            revision=revision,
            credential=manager,
        )
        == accepted
    )
    assert (len(h.witness.requests), len(h.transport.calls)) == before_io
    for missing in scopes:
        restricted = h.credentials.issue(
            h.principal, manager.principal.model_copy(update={"scopes": scopes - {missing}})
        )
        with pytest.raises(ResearchError) as denied:
            await command(
                e,
                "orchestration.transfer",
                payload,
                key="transfer",
                revision=revision,
                credential=restricted,
            )
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert (len(h.witness.requests), len(h.transport.calls)) == before_io
    h.witness.failure = None
    again = await command(
        e,
        "orchestration.transfer",
        payload,
        key="another-key",
        revision=revision,
        credential=manager,
    )
    assert again["result"] == accepted["result"]
    assert (len(h.witness.requests), len(h.transport.calls)) == (before_io[0] + 1, before_io[1])
    successor = current(h, "orchestration_binding", payload["campaign_id"])
    with pytest.raises(ResearchError) as startup:
        await command(
            e,
            "orchestration.prepare_turn",
            {
                "campaign_id": payload["campaign_id"],
                "epoch": 2,
                "ordinal": 1,
                "configuration_digest": CONFIG,
                "planning_watermark": 0,
                "action": {
                    "operation": "report.create",
                    "payload": {"campaign_id": payload["campaign_id"]},
                    "idempotency_key": "successor-report",
                },
            },
            revision=successor["revision"],
            credential=e.successor,
        )
    assert startup.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


async def prepare(e, outcome):
    campaign_id = e.admitted.campaign["id"]
    binding = current(e.h, "orchestration_binding", campaign_id)
    context = await e.h.router.query(e.h.operator.token, "context", {"campaign_id": campaign_id})
    action = {
        "operation": "campaign.pause" if outcome == "rejected" else "report.create",
        "payload": {
            "campaign_id": campaign_id,
            **({"mode": "drain"} if outcome == "rejected" else {}),
        },
        "idempotency_key": "saved-target",
        "expected_revision": 0 if outcome == "rejected" else None,
    }
    return (
        await command(
            e,
            "orchestration.prepare_turn",
            {
                "campaign_id": campaign_id,
                "epoch": 1,
                "ordinal": 1,
                "configuration_digest": CONFIG,
                "planning_watermark": context["watermark"],
                "action": action,
            },
            revision=binding["revision"],
            credential=e.driver,
        )
    )["result"]


@pytest.mark.parametrize("outcome", ["absent", "accepted", "rejected"])
async def test_transfer_resolves_saved_target_evidence_without_redelivery(
    queued, monkeypatch, outcome
):
    e = queued
    turn = await prepare(e, outcome)
    if outcome != "absent":

        def missing(*args, **kwargs):
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture receipt interrupted")

        with monkeypatch.context() as patch:
            patch.setattr(e.h.router.service.orchestration, "_finish", missing)
            with pytest.raises(ResearchError) as interrupted:
                await command(
                    e, "orchestration.execute_turn", {"turn_id": turn["id"]}, credential=e.driver
                )
            assert interrupted.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    reports = await rows(e, "report")
    payload, revision = await request(e)
    accepted = (await command(e, "orchestration.transfer", payload, revision=revision))["result"][
        "data"
    ]
    evidence = current(
        e.h, accepted["pending_turn_outcome_kind"], accepted["pending_turn_outcome_id"]
    )
    if outcome == "absent":
        assert evidence["kind"] == "drive_closure"
        assert current(e.h, "drive_turn", turn["id"])["state"] == "closed_unacknowledged"
    else:
        assert evidence["data"]["disposition"] == outcome
    assert await rows(e, "report") == reports
    assert len(reports) == int(outcome == "accepted")


@pytest.mark.parametrize(
    "fault", ["missing", "corrupt", "witness", "cancelled", "generation", "retention"]
)
async def test_byte_or_authority_failure_cannot_fence_or_create_a_successor(
    queued, monkeypatch, fault
):
    e, h = queued, queued.h
    payload, revision = await request(e)
    binding = current(h, "orchestration_binding", payload["campaign_id"])
    if fault == "missing":
        h.transport.hide_reads = True
    elif fault == "corrupt":
        for key, value in h.transport.objects.items():
            h.transport.objects[key] = ObjectBytes(b"x" * len(value.content), value.metadata)
    elif fault == "witness":
        h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture source offline")
    else:
        read = h.objects.read

        async def changed(workspace, blob):
            body = await read(workspace, blob)
            if fault == "cancelled":
                raise asyncio.CancelledError()
            if fault == "generation":
                row = current(h, "artifact", blob.reference.id)
                row["data"]["storage_generation"] = new_id()
                h.store.write(
                    lambda sql: sql.execute(
                        "UPDATE records SET payload=? WHERE kind='artifact' AND id=?",
                        (encode(row["data"]), row["id"]),
                    )
                )
            else:
                h.store.write(
                    lambda sql: sql.execute(
                        "UPDATE records SET state='released' "
                        "WHERE kind='artifact_reference' AND parent_id=?",
                        (blob.reference.id,),
                    )
                )
            return body

        monkeypatch.setattr(h.objects, "read", changed)
    with pytest.raises(asyncio.CancelledError if fault == "cancelled" else ResearchError):
        await command(e, "orchestration.transfer", payload, revision=revision)
    assert current(h, "orchestration_binding", payload["campaign_id"]) == binding
    assert await rows(e, "orchestration_checkpoint") == []
    assert await rows(e, "orchestration_start") == []


@pytest.mark.parametrize("fault", ["revoke", "cancel"])
async def test_transfer_rechecks_request_authority_after_its_witness(queued, monkeypatch, fault):
    e, h = queued, queued.h
    payload, revision = await request(e)
    binding = current(h, "orchestration_binding", payload["campaign_id"])
    operator = issue(h, {"read", "orchestration.drive", "orchestration.manage"})
    acquire = h.router.acquire

    async def changed(request):
        response = await acquire(request)
        if fault == "revoke":
            h.credentials.revoke(h.principal, operator.id)
        else:
            asyncio.current_task().cancel()
        return response

    monkeypatch.setattr(h.router, "acquire", changed)
    task = asyncio.create_task(
        command(e, "orchestration.transfer", payload, revision=revision, credential=operator)
    )
    with pytest.raises(ResearchError if fault == "revoke" else asyncio.CancelledError):
        await task
    assert current(h, "orchestration_binding", payload["campaign_id"]) == binding
    assert await rows(e, "orchestration_checkpoint") == []
    assert await rows(e, "orchestration_start") == []


async def test_competing_transfers_and_failed_event_keep_one_atomic_epoch(queued, monkeypatch):
    e = queued
    payload, revision = await request(e)
    binding = current(e.h, "orchestration_binding", payload["campaign_id"])
    event = Transaction.event

    def fail(self, name, *args, **kwargs):
        if name == "orchestration.transferred":
            raise RuntimeError("Fixture event failed")
        return event(self, name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="event failed"):
            await command(e, "orchestration.transfer", payload, key="first", revision=revision)
    assert current(e.h, "orchestration_binding", payload["campaign_id"]) == binding
    assert await rows(e, "orchestration_checkpoint") == []
    other = {**payload, "transfer_id": new_id()}
    outcomes = await asyncio.gather(
        command(e, "orchestration.transfer", payload, key="first", revision=revision),
        command(e, "orchestration.transfer", other, key="second", revision=revision),
        return_exceptions=True,
    )
    assert sum(isinstance(result, dict) for result in outcomes) == 1
    assert len(await rows(e, "orchestration_transfer")) == 1
    assert len(await rows(e, "orchestration_checkpoint")) == 1


async def test_limits_scopes_and_profile_metadata_are_explicit(queued, monkeypatch):
    e, h = queued, queued.h
    response = await send(h, "GET", "/v1/capabilities")
    value = strict_json_loads(response.body)
    assert value["commands"]["orchestration.transfer"]["required_scopes"] == [
        "orchestration.manage",
        "read",
        "orchestration.drive",
    ]
    scopes = set(value["commands"]["orchestration.transfer"]["required_scopes"])
    assert value["orchestration"]["transfer"]["successor_startup"] is False
    payload, revision = await request(e)
    before = len(h.witness.requests), len(h.transport.calls)
    for missing in scopes:
        credential = issue(h, scopes - {missing})
        with pytest.raises(ResearchError) as denied:
            await command(
                e, "orchestration.transfer", payload, revision=revision, credential=credential
            )
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    monkeypatch.setattr(adapter, "LIMITS", replace(adapter.LIMITS, max_manifest_bytes=1))
    with pytest.raises(ResearchError) as limit:
        await command(e, "orchestration.transfer", payload, revision=revision)
    assert limit.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert (len(h.witness.requests), len(h.transport.calls)) == before


async def test_old_profile_refuses_transfer_before_body_consumption(research):
    consumed = []
    response = await send(
        research, "POST", "/v1/commands/orchestration.transfer", payload={}, consume=consumed
    )
    assert response.status == 501 and consumed == []
