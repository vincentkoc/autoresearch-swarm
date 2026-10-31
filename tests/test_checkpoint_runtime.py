"""Fresh protected target runtime preparation under real signed logical recovery."""

import asyncio
import copy
import json
from dataclasses import replace

import pytest

from adapters.cloudflare.checkpoint_runtime import RECEIPT_KIND, prepare_target_runtime
from adapters.cloudflare.deployment import (
    DeploymentCredential,
    credential_manifest_digest,
    load_deployment,
    secret_digest,
)
from adapters.cloudflare.research_router import dispatch_key_digest
from adapters.cloudflare.runtime import open_router
from research_swarm.control.auth import CredentialStore
from research_swarm.crypto import public_key
from research_swarm.domain.assignments import signing_key_id
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction
from research_swarm.state.recovery_runtime import PLAN_KIND
from tests import test_checkpoint_admission as fixtures
from tests.test_checkpoint_admission import admit
from tests.test_checkpoint_import import make_recovery, snapshot
from tests.test_hosted_router import Gate, Witness

deployment = fixtures.deployment
bootstrap = fixtures.bootstrap
source = fixtures.source
sealed = fixtures.sealed
configured = fixtures.configured
imported = fixtures.imported
elected = fixtures.elected


def fresh_target_values(
    raw,
    crypto,
    *,
    seed=b"u" * 32,
    dispatch=b"v" * 32,
    bootstrap="T" * 43,
    consumer="Y" * 43,
    credential_letter="K",
):
    public = public_key(crypto, seed).hex()
    raw["configuration"]["assignment_public_key"] = public
    raw["configuration"]["authority"]["assignment_key_id"] = signing_key_id(public)
    raw["configuration"]["dispatch_key_digest"] = dispatch_key_digest(dispatch)
    members, tokens, ids = [], {}, {}
    for index, previous in enumerate(raw["credentials"]):
        identifier, token = new_id(), "rsw_" + chr(ord(credential_letter) + index) * 43
        ids[previous["id"]] = identifier
        tokens[identifier] = token
        members.append(
            DeploymentCredential(
                id=identifier,
                principal=previous["principal"],
                token_digest=CredentialStore._digest(token),
            )
        )
    raw["credentials"] = [item.model_dump(mode="json") for item in members]
    raw["configuration"]["credential_manifest_digest"] = credential_manifest_digest(members)
    for delivery in raw["deliveries"]:
        delivery["gateway_credential_id"] = ids[delivery["gateway_credential_id"]]
        delivery["route_credentials"] = {
            route: ids[identifier] for route, identifier in delivery["route_credentials"].items()
        }
    secrets = {
        "assignment_seed_hex": seed.hex(),
        "dispatch_key_hex": dispatch.hex(),
        "bootstrap": bootstrap,
        "witness_consumer": consumer,
        "credentials": tokens,
    }
    raw["bootstrap_credential_digest"] = secret_digest("bootstrap", secrets["bootstrap"])
    raw["witness_credential_digest"] = secret_digest("witness", secrets["witness_consumer"])
    return raw, secrets


def fresh_target(raw, source):
    raw, secrets = fresh_target_values(raw, source.crypto)
    return load_deployment(json.dumps(raw), json.dumps(secrets), crypto=source.crypto)


def test_independent_target_fixture_rosters_are_disjoint_before_import(deployment):
    raw, original, crypto = deployment
    tokens = [set(original["credentials"].values())]
    for letter in ("a", "e"):
        manifest, secrets = fresh_target_values(
            copy.deepcopy(raw), crypto, credential_letter=letter
        )
        load_deployment(json.dumps(manifest), json.dumps(secrets), crypto=crypto)
        tokens.append(set(secrets["credentials"].values()))
    assert len(set.union(*tokens)) == sum(map(len, tokens))


@pytest.fixture
async def recovery(sealed, tmp_path):
    return make_recovery(sealed, tmp_path, configure_target=fresh_target)


@pytest.fixture
async def target(elected):
    await admit(elected)
    elected.witness = Witness()
    elected.state = Gate()
    return elected


async def run(e, *, store=None, **changes):
    def open_store():
        e.opens += 1
        return e.target if store is None else store

    options = {
        "identity": e.target.identity,
        "credential": e.target_secrets.bootstrap,
        "target_json": json.dumps(e.target_config),
        "crypto": e.source.crypto,
        "acquire": e.witness,
        **changes,
    }
    return await prepare_target_runtime(
        e.state, open_store, e.manifest, e.target_secrets, **options
    )


def records(e, kind):
    return [row for row in snapshot(e.target)["records"] if row["kind"] == kind]


def assert_inactive(e):
    assert records(e, "workspace_gate")[0]["state"] == "recovering"
    names = snapshot(e.target)
    assert "hosted_provisioning" not in names and "hosted_authority_config" not in names
    assert not records(e, "recovery_activation")
    assert not records(e, "run_result")


async def test_prepare_preserves_original_history_and_replays_offline_without_reinstall(
    target, monkeypatch
):
    e = target
    before, source_before = snapshot(e.target), snapshot(e.source.store)
    e.source.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Source is offline")
    raw = await run(e)
    receipt = strict_json_loads(raw)["receipt"]
    assert receipt["provider"]["scope"]["control_epoch"] == 2
    assert (
        receipt["provider"]["scope"]["activation_id"]
        == e.manifest.configuration.authority.activation_id
    )
    assert len(records(e, PLAN_KIND)) == len(records(e, RECEIPT_KIND)) == 1
    after = snapshot(e.target)
    assert len(after["credentials"]) == len(before["credentials"]) + len(e.manifest.credentials)
    assert all(row in after["credentials"] for row in before["credentials"])
    assert all(row in after["records"] for row in before["records"])
    for table in (
        "budgets",
        "budget_reservations",
        "budget_settlements",
        "budget_entries",
        "commands",
    ):
        assert after[table] == before[table]
    assert snapshot(e.source.store) == source_before
    assert_inactive(e)
    calls = len(e.witness.requests)
    e.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Target witness offline")
    e.state = Gate()
    monkeypatch.setattr(CredentialStore, "install_in", lambda *a, **k: pytest.fail("reinstalled"))
    assert await run(e) == raw
    assert len(e.witness.requests) == calls and snapshot(e.target) == after
    with pytest.raises(ResearchError):
        await open_router(
            e.state,
            lambda: e.target,
            e.manifest,
            e.target_secrets,
            crypto=e.source.crypto,
            acquire=e.witness,
            objects=None,
        )


async def test_witness_interruption_resumes_only_the_persisted_preparation(target, monkeypatch):
    e = target
    e.witness.failure = TimeoutError("Synthetic no response")
    with pytest.raises(ResearchError) as failed:
        await run(e)
    assert failed.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    assert len(records(e, PLAN_KIND)) == 1 and not records(e, RECEIPT_KIND)
    before = snapshot(e.target)
    e.witness.failure = None
    e.state = Gate()
    monkeypatch.setattr(CredentialStore, "install_in", lambda *a, **k: pytest.fail("reinstalled"))
    await run(e)
    assert e.witness.requests[0].nonce != e.witness.requests[1].nonce
    assert snapshot(e.target)["credentials"] == before["credentials"]
    assert_inactive(e)


@pytest.mark.parametrize("stage", ["install", "register", "qualify", "receipt"])
async def test_transaction_failure_rolls_back_its_entire_phase(target, monkeypatch, stage):
    e = target
    before = snapshot(e.target)
    original_install = CredentialStore.install_in.__func__
    original_event = Transaction.event

    def install(cls, *args, **kwargs):
        original_install(cls, *args, **kwargs)
        raise RuntimeError("Synthetic interruption after credential insert")

    event = {
        "register": "time_provider.registered",
        "qualify": "time_provider.requalified",
        "receipt": "workspace.runtime_prepared",
    }.get(stage)

    def emit(self, name, *args, **kwargs):
        result = original_event(self, name, *args, **kwargs)
        if name == event:
            raise RuntimeError("Synthetic interruption after event")
        return result

    with monkeypatch.context() as patch:
        if stage == "install":
            patch.setattr(CredentialStore, "install_in", classmethod(install))
        else:
            patch.setattr(Transaction, "event", emit)
        with pytest.raises(RuntimeError, match="Synthetic interruption"):
            await run(e)
    if stage in {"install", "register"}:
        assert snapshot(e.target) == before
    else:
        assert len(records(e, PLAN_KIND)) == 1 and not records(e, RECEIPT_KIND)
        target_provider = [
            row
            for row in records(e, "time_provider")
            if row["id"] == e.manifest.configuration.authority.activation_id
        ]
        assert target_provider[0]["state"] == "unqualified"
    await run(e)
    assert_inactive(e)


@pytest.mark.parametrize(
    "change", ["revoked", "expiry", "missing", "principal", "provider", "plan", "admission"]
)
async def test_resume_refuses_changed_preparation_without_repair(target, change):
    e = target
    e.witness.failure = TimeoutError("Synthetic preparation interruption")
    with pytest.raises(ResearchError):
        await run(e)
    member = e.manifest.credentials[0]

    def damage(sql):
        if change == "revoked":
            sql.execute("UPDATE credentials SET revoked=1 WHERE id=?", (member.id,))
        elif change == "expiry":
            sql.execute(
                "UPDATE credentials SET expires_at='2099-01-01T00:00:00Z' WHERE id=?", (member.id,)
            )
        elif change == "missing":
            sql.execute("DELETE FROM credentials WHERE id=?", (member.id,))
        elif change == "principal":
            sql.execute(
                "UPDATE credentials SET principal=json_set(principal,'$.id',?) WHERE id=?",
                (new_id(), member.id),
            )
        else:
            kind = {
                "provider": "time_provider",
                "plan": PLAN_KIND,
                "admission": "recovery_admission",
            }[change]
            sql.execute("UPDATE records SET state='mutable' WHERE kind=?", (kind,))

    e.target.write(damage)
    before, calls = snapshot(e.target), len(e.witness.requests)
    e.witness.failure = None
    with pytest.raises(ResearchError):
        await run(e)
    assert snapshot(e.target) == before and len(e.witness.requests) == calls


@pytest.mark.parametrize("change", ["roster", "admission", "provider", "secrets"])
async def test_awaited_witness_cannot_hide_target_drift(target, change):
    e = target

    async def drift(request):
        response = await e.witness(request)
        if change == "secrets":
            # Direct adapter callers may supply a mutable mapping; recheck actual bytes.
            tokens[next(iter(tokens))] = "rsw_" + "Z" * 43
        else:
            e.target.write(
                lambda sql: sql.execute(
                    "UPDATE credentials SET revoked=1 WHERE id=?"
                    if change == "roster"
                    else "UPDATE records SET revision=revision+1 WHERE kind=?",
                    (
                        e.manifest.credentials[0].id
                        if change == "roster"
                        else "recovery_admission"
                        if change == "admission"
                        else "time_provider",
                    ),
                )
            )
        return response

    tokens = dict(e.target_secrets.credentials)
    e.target_secrets = replace(e.target_secrets, credentials=tokens)
    with pytest.raises(ResearchError):
        await run(e, acquire=drift)
    assert not records(e, RECEIPT_KIND)
    assert not any(
        row["state"] == "qualified"
        and row["id"] == e.manifest.configuration.authority.activation_id
        for row in records(e, "time_provider")
    )
    assert_inactive(e)


async def test_concurrent_exact_prepare_has_one_roster_provider_and_receipt(target):
    e = target
    first, second = await asyncio.gather(run(e), run(e))
    assert first == second and len(e.witness.requests) == 1
    assert len(records(e, PLAN_KIND)) == len(records(e, RECEIPT_KIND)) == 1


async def test_lost_qualification_ack_replays_the_exact_receipt_without_witness(target):
    e = target
    lost = []

    class LostReply:
        identity = e.target.identity

        def read(self, callback):
            return e.target.read(callback)

        def write(self, callback):
            result = e.target.write(callback)
            if records(e, RECEIPT_KIND) and not lost:
                lost.append(result)
                raise OSError("Synthetic lost committed preparation reply")
            return result

    with pytest.raises(OSError, match="lost committed"):
        await run(e, store=LostReply())
    before, calls = snapshot(e.target), len(e.witness.requests)
    e.witness.failure = TimeoutError("Offline after accepted commit")
    assert await run(e) == lost[0]
    assert snapshot(e.target) == before and len(e.witness.requests) == calls


@pytest.mark.parametrize("collision", ["id", "token"])
async def test_existing_credential_collision_rolls_back_the_entire_target_plan(target, collision):
    e = target
    # Preserve an existing row even when only one half of the uniqueness tuple collides.
    member = e.manifest.credentials[-1]
    old = snapshot(e.target)["credentials"][0]
    e.target.write(
        lambda sql: sql.execute(
            "INSERT INTO credentials(id,token_digest,workspace_id,principal,created_at) "
            "VALUES (?,?,?,?,?)",
            (
                member.id if collision == "id" else new_id(),
                member.token_digest
                if collision == "token"
                else CredentialStore._digest("rsw_" + "Z" * 43),
                old["workspace_id"],
                old["principal"],
                old["created_at"],
            ),
        )
    )
    before = snapshot(e.target)
    with pytest.raises(ResearchError, match="must be fresh"):
        await run(e)
    assert snapshot(e.target) == before and not e.witness.requests


async def test_completed_preparation_still_refuses_revoked_target_credentials(target):
    e = target
    await run(e)
    e.target.write(
        lambda sql: sql.execute(
            "UPDATE credentials SET revoked=1 WHERE id=?", (e.manifest.credentials[0].id,)
        )
    )
    before, calls = snapshot(e.target), len(e.witness.requests)
    with pytest.raises(ResearchError):
        await run(e)
    assert snapshot(e.target) == before and len(e.witness.requests) == calls


@pytest.mark.parametrize("completed", [False, True])
@pytest.mark.parametrize("field", ["principal", "provider", "metadata"])
async def test_replay_bounds_stored_rows_before_materializing_or_parsing(target, completed, field):
    e = target
    if completed:
        await run(e)
    else:
        e.witness.failure = TimeoutError("Synthetic preparation interruption")
        with pytest.raises(ResearchError):
            await run(e)

    def pad(sql):
        if field == "principal":
            sql.execute(
                "UPDATE credentials SET principal=principal || ? WHERE id=?",
                (" " * (256 * 1024), e.manifest.credentials[0].id),
            )
        elif field == "provider":
            sql.execute(
                "UPDATE records SET payload=payload || ? WHERE kind='time_provider' AND id=?",
                (" " * (64 * 1024), e.manifest.configuration.authority.activation_id),
            )
        else:
            sql.execute(
                "UPDATE records SET created_at=? WHERE kind='time_provider' AND id=?",
                ("x" * 65, e.manifest.configuration.authority.activation_id),
            )

    e.target.write(pad)
    before, calls = snapshot(e.target), len(e.witness.requests)
    e.witness.failure = None
    with pytest.raises(ResearchError):
        await run(e)
    assert snapshot(e.target) == before and len(e.witness.requests) == calls


@pytest.mark.parametrize("change", ["bootstrap", "assignment", "dispatch", "token"])
async def test_runtime_custody_refuses_before_store_factory(target, change):
    e = target
    options = {}
    if change == "bootstrap":
        options["credential"] = e.source.secrets.bootstrap
    elif change == "assignment":
        e.target_secrets = replace(e.target_secrets, assignment_seed=b"x" * 32)
    elif change == "dispatch":
        e.target_secrets = replace(e.target_secrets, dispatch_key=b"x" * 32)
    else:
        e.target_secrets = replace(
            e.target_secrets,
            credentials={key: "rsw_" + "Z" * 43 for key in e.target_secrets.credentials},
        )
    before, opens = snapshot(e.target), e.opens
    with pytest.raises(ResearchError):
        await run(e, **options)
    assert e.opens == opens and snapshot(e.target) == before
