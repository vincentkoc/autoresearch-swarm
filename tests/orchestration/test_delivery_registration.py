"""Registration freezes delegation after campaign creation without storing secrets."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.orchestration_models import RegisterDelivery
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction, encode
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_orchestration_turns import CONFIG, fence


@pytest.fixture
def delegated(pipeline):
    p, issuer = pipeline, pipeline.runtime.principal()

    def credential(*scopes):
        return p.runtime.credentials.issue(
            issuer, issuer.model_copy(update={"id": new_id(), "scopes": frozenset(scopes)})
        )

    control = credential("read", "orchestration.drive", "report.create")
    evaluator = credential("read", "orchestration.drive", "evaluation.publish")
    gateway = credential("orchestration.deliver")
    campaign = p.client.get("campaign", p.campaign["id"])
    p.client.command(
        "orchestration.bind",
        {
            "campaign_id": campaign["id"],
            "configuration_digest": CONFIG,
            "routes": {"control": control.principal.id, "evaluator": evaluator.principal.id},
        },
        idempotency_key="bind",
        expected_revision=campaign["revision"],
    )
    intent = RegisterDelivery(
        campaign_id=campaign["id"],
        epoch=1,
        configuration_digest=CONFIG,
        gateway_credential_id=gateway.id,
        route_credentials={"control": control.id, "evaluator": evaluator.id},
    )
    return SimpleNamespace(
        p=p, control=control, evaluator=evaluator, gateway=gateway, intent=intent
    )


def register(e, *, key="register", client=None, intent=None, revision=0):
    return (client or e.p.client).command(
        "orchestration.register_delivery",
        intent or e.intent,
        idempotency_key=key,
        expected_revision=revision,
    )


def registrations(e):
    return e.p.client.list(RecordQuery(kind="delivery_registration"))["items"]


def test_registration_is_immutable_secret_free_and_reopens_with_exact_acknowledgment(delegated):
    e = delegated
    first = register(e)
    row = first["result"]
    assert row["id"] == e.gateway.principal.id and row["state"] == "immutable"
    assert row["data"]["routes"]["control"] == e.control.principal.id
    assert row["data"]["registered_by"] == e.p.runtime.principal().id
    assert registrations(e) == [row]
    assert register(e, key="same-delegation")["result"] == row
    reopened = LocalRuntime.open(e.p.runtime.root)
    with ResearchClient(LocalTransport(reopened)) as client:
        assert register(e, client=client) == first
        assert client.get("delivery_registration", row["id"]) == row
    state = e.p.runtime.database.store.read(
        lambda sql: "\n".join(
            str(dict(row))
            for table in ("records", "events", "commands", "outbox")
            for row in sql.execute("SELECT * FROM " + table).fetchall()
        )
    )
    assert all(item.token not in state for item in (e.control, e.evaluator, e.gateway))
    with pytest.raises(ResearchError) as changed:
        register(e, key="wrong-revision", revision=1)
    assert changed.value.code == ErrorCode.REVISION_CONFLICT


@pytest.mark.parametrize("changed", ["epoch", "configuration", "routes", "fenced"])
def test_registration_requires_the_exact_active_binding(delegated, changed):
    e, intent = delegated, delegated.intent
    if changed == "epoch":
        intent = intent.model_copy(update={"epoch": 2})
    elif changed == "configuration":
        intent = intent.model_copy(update={"configuration_digest": "sha256:" + "f" * 64})
    elif changed == "routes":
        intent = intent.model_copy(update={"route_credentials": {"control": e.control.id}})
    else:
        fence(e.p)
    with pytest.raises(ResearchError) as refused:
        register(e, intent=intent)
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert registrations(e) == []


@pytest.mark.parametrize(
    "changed", ["revoked", "expires", "missing", "scope", "workspace", "actor"]
)
def test_registration_checks_current_credential_identity_and_scopes(delegated, changed):
    e, intent = delegated, delegated.intent
    if changed == "missing":
        intent = intent.model_copy(update={"gateway_credential_id": new_id()})
    elif changed == "actor":
        intent = intent.model_copy(
            update={"route_credentials": {"control": e.evaluator.id, "evaluator": e.control.id}}
        )
    else:
        column, value = {
            "revoked": ("revoked", 1),
            "expires": ("expires_at", "2099-01-01T00:00:00.000Z"),
            "scope": (
                "principal",
                encode(
                    e.control.principal.model_copy(
                        update={"scopes": frozenset({"read"})}
                    ).model_dump(mode="json")
                ),
            ),
            "workspace": (
                "principal",
                encode(
                    e.control.principal.model_copy(update={"workspace_id": new_id()}).model_dump(
                        mode="json"
                    )
                ),
            ),
        }[changed]
        e.p.runtime.database.store.write(
            lambda sql: sql.execute(
                f"UPDATE credentials SET {column}=? WHERE id=?", (value, e.control.id)
            )
        )
    with pytest.raises(ResearchError) as refused:
        register(e, intent=intent)
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert registrations(e) == []


def test_gateway_actor_cannot_alias_a_driver_through_a_second_credential(delegated):
    e = delegated
    alias = e.p.runtime.credentials.issue(
        e.p.runtime.principal(),
        e.control.principal.model_copy(update={"scopes": frozenset({"orchestration.deliver"})}),
    )
    with pytest.raises(ResearchError) as refused:
        register(e, intent=e.intent.model_copy(update={"gateway_credential_id": alias.id}))
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert registrations(e) == []


def test_registration_requires_delegation_authority_even_through_direct_service(delegated):
    e = delegated
    for scopes in ({"orchestration.manage"}, {"auth.manage"}):
        context = CommandContext(
            principal=e.p.runtime.principal().model_copy(update={"scopes": frozenset(scopes)}),
            idempotency_key=new_id(),
            expected_revision=0,
        )
        with pytest.raises(ResearchError) as refused:
            e.p.runtime.service.register_delivery(context, e.intent)
        assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert registrations(e) == []


@pytest.mark.parametrize(
    "missing", ["orchestration.deliver", "report.create", "evaluation.publish"]
)
def test_registration_cannot_delegate_scopes_the_operator_does_not_hold(delegated, missing):
    e = delegated
    scopes = {
        "auth.manage",
        "orchestration.manage",
        "orchestration.deliver",
        "read",
        "orchestration.drive",
        "report.create",
        "evaluation.publish",
    } - {missing}
    credential = e.p.runtime.credentials.issue(
        e.p.runtime.principal(),
        e.p.runtime.principal().model_copy(update={"id": new_id(), "scopes": frozenset(scopes)}),
    )
    with (
        ResearchClient(
            LocalTransport(e.p.runtime, lambda: e.p.runtime.credentials.resolve(credential.token))
        ) as client,
        pytest.raises(ResearchError) as refused,
    ):
        register(e, client=client)
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert registrations(e) == []


def test_independent_connections_converge_and_conflicting_credentials_cannot_retarget(delegated):
    e, barrier = delegated, Barrier(2)

    def submit(key):
        runtime = LocalRuntime.open(e.p.runtime.root)
        with ResearchClient(LocalTransport(runtime)) as client:
            barrier.wait(timeout=10)
            return register(e, key=key, client=client)["result"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        rows = list(pool.map(submit, ("first", "second")))
    assert rows[0] == rows[1] and registrations(e) == [rows[0]]
    events = e.p.runtime.database.store.read(
        lambda sql: sql.execute(
            "SELECT count(*) AS n FROM events "
            "WHERE json_extract(payload,'$.type')='orchestration.delivery_registered'"
        ).fetchone()["n"]
    )
    assert events == 1
    alias = e.p.runtime.credentials.issue(e.p.runtime.principal(), e.control.principal)
    changed = e.intent.model_copy(
        update={"route_credentials": {**e.intent.route_credentials, "control": alias.id}}
    )
    with pytest.raises(ResearchError) as refused:
        register(e, key="retarget", intent=changed)
    assert refused.value.code == ErrorCode.REVISION_CONFLICT
    assert registrations(e) == [rows[0]]


def test_registration_effect_and_acknowledgment_roll_back_together(delegated, monkeypatch):
    e, original = delegated, Transaction.event

    def fail(self, kind, *args, **kwargs):
        if kind == "orchestration.delivery_registered":
            original(self, kind, *args, **kwargs)
            raise RuntimeError("Synthetic interruption after registration event")
        return original(self, kind, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="Synthetic interruption"):
            register(e)
    assert registrations(e) == []
    count = e.p.runtime.database.store.read(
        lambda sql: sql.execute(
            "SELECT count(*) AS n FROM commands WHERE idempotency_key='register'"
        ).fetchone()["n"]
    )
    assert count == 0
    assert register(e)["result"]["id"] == e.gateway.principal.id


def test_historical_receipt_survives_fencing_but_revoked_operator_cannot_replay(delegated):
    e = delegated
    operator = e.p.runtime.credentials.issue(
        e.p.runtime.principal(),
        e.p.runtime.principal().model_copy(update={"id": new_id()}),
    )
    with ResearchClient(
        LocalTransport(e.p.runtime, lambda: e.p.runtime.credentials.resolve(operator.token))
    ) as client:
        first = register(e, client=client)
        fence(e.p)
        e.p.runtime.credentials.revoke(e.p.runtime.principal(), e.gateway.id)
        assert register(e, client=client) == first
        with pytest.raises(ResearchError) as fresh:
            register(e, client=client, key="fresh")
        assert fresh.value.code == ErrorCode.STALE_FENCE
        e.p.runtime.credentials.revoke(e.p.runtime.principal(), operator.id)
        with pytest.raises(ResearchError) as revoked:
            register(e, client=client)
        assert revoked.value.code == ErrorCode.AUTHORIZATION_DENIED
