"""Bootstrap is atomic, resumable and separate from ordinary existing-state opens."""

import asyncio
from types import SimpleNamespace

import pytest

from adapters.cloudflare.provisioning import provision, require_deployment
from research_swarm.control.auth import CredentialStore
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Database, Transaction
from research_swarm.state.sql import StoreIdentity
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_deployment import load
from tests.test_hosted_router import FixtureStore, Gate, Witness


@pytest.fixture
def bootstrap(tmp_path, deployment):
    manifest, secrets = load(deployment)
    database = Database(tmp_path / "provisioning.sqlite")
    store = FixtureStore(database.store)
    store.identity = StoreIdentity("cloudflare-do", manifest.configuration.store_location)
    return SimpleNamespace(
        manifest=manifest,
        secrets=secrets,
        crypto=deployment[2],
        database=database,
        store=store,
        state=Gate(),
        witness=Witness(),
        opens=0,
    )


async def run(e, **overrides):
    def open_store():
        e.opens += 1
        return e.store

    options = {
        "identity": e.store.identity,
        "credential": e.secrets.bootstrap,
        "crypto": e.crypto,
        "acquire": e.witness,
        **overrides,
    }
    return await provision(e.state, open_store, e.manifest, e.secrets, **options)


def rows(e, table):
    return e.store.read(lambda sql: [dict(row) for row in sql.execute(f"SELECT * FROM {table}")])


def prepared(e):
    assert rows(e, "hosted_provisioning")[0]["state"] == "prepared"
    assert len(rows(e, "credentials")) == len(e.manifest.credentials)
    assert [row["state"] for row in rows(e, "records") if row["kind"] == "time_provider"] == [
        "unqualified"
    ]
    with pytest.raises(ResearchError):
        require_deployment(e.store, e.manifest)
    assert (
        e.store.read(
            lambda sql: sql.execute(
                "SELECT name FROM sqlite_master WHERE name='hosted_authority_config'"
            ).fetchone()
        )
        is None
    )


async def test_bootstrap_and_target_admission_precede_any_store_factory(bootstrap):
    e = bootstrap
    for options in ({"credential": "wrong"}, {"identity": StoreIdentity("cloudflare-do", "other")}):
        with pytest.raises(ResearchError):
            await run(e, **options)
    assert e.opens == 0 and e.witness.requests == []
    assert rows(e, "workspaces") == []


async def test_interrupted_source_resumes_exact_manifest_and_replays_without_reinstalling(
    bootstrap, monkeypatch
):
    e = bootstrap
    e.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic source offline")
    with pytest.raises(ResearchError) as unavailable:
        await run(e)
    assert unavailable.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    prepared(e)
    credentials = rows(e, "credentials")
    generations = [row for row in rows(e, "records") if row["kind"] == "time_provider_generation"]
    e.witness.failure = None
    accepted = await run(e)
    assert require_deployment(e.store, e.manifest) == accepted
    assert rows(e, "credentials") == credentials
    assert e.witness.requests[0].nonce != e.witness.requests[1].nonce
    assert (
        len([row for row in rows(e, "records") if row["kind"] == "time_provider_generation"])
        == len(generations) + 1
    )
    admin = e.manifest.credentials[0].principal
    target = e.manifest.credentials[-1]
    CredentialStore.open_existing(Database.from_store(e.store)).revoke(admin, target.id)
    before = rows(e, "credentials"), rows(e, "records"), rows(e, "events")
    e.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic source offline")
    calls = len(e.witness.requests)

    def reinstall(*args, **kwargs):
        pytest.fail("Completed provisioning must never reinstall credentials")

    monkeypatch.setattr(CredentialStore, "install_in", reinstall)
    assert await run(e) == accepted
    assert len(e.witness.requests) == calls
    assert (rows(e, "credentials"), rows(e, "records"), rows(e, "events")) == before
    with pytest.raises(ResearchError) as revoked:
        CredentialStore.open_existing(Database.from_store(e.store)).resolve(
            e.secrets.credentials[target.id]
        )
    assert revoked.value.code == ErrorCode.AUTHORIZATION_DENIED


@pytest.mark.parametrize("stage", ["register", "qualify"])
async def test_transaction_failure_rolls_back_entire_preparation_or_completion(
    bootstrap, monkeypatch, stage
):
    e = bootstrap
    original = Transaction.event
    target = "time_provider.registered" if stage == "register" else "time_provider.requalified"

    def interrupted(self, name, *args, **kwargs):
        if name == target:
            raise RuntimeError("Synthetic provisioning transaction interrupted")
        return original(self, name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "event", interrupted)
        with pytest.raises(RuntimeError, match="transaction interrupted"):
            await run(e)
    if stage == "register":
        assert rows(e, "workspaces") == rows(e, "records") == rows(e, "events") == []
        assert (
            e.store.read(
                lambda sql: sql.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE name IN ('credentials','hosted_provisioning')"
                ).fetchall()
            )
            == []
        )
        assert e.witness.requests == []
    else:
        prepared(e)
    result = await run(e)
    assert require_deployment(e.store, e.manifest) == result
    assert len(rows(e, "credentials")) == len(e.manifest.credentials)


@pytest.mark.parametrize("change", ["missing", "revoked", "scope", "manifest"])
async def test_prepared_resume_refuses_changed_roster_or_manifest_without_repair(bootstrap, change):
    e = bootstrap
    e.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic source offline")
    with pytest.raises(ResearchError):
        await run(e)
    if change == "manifest":
        e.manifest = e.manifest.model_copy(update={"witness_timeout_ms": 2000})
    elif change == "missing":
        e.store.write(
            lambda sql: sql.execute(
                "DELETE FROM credentials WHERE id=?", (e.manifest.credentials[1].id,)
            )
        )
    elif change == "revoked":
        e.store.write(
            lambda sql: sql.execute(
                "UPDATE credentials SET revoked=1 WHERE id=?", (e.manifest.credentials[1].id,)
            )
        )
    else:
        e.store.write(
            lambda sql: sql.execute(
                "UPDATE credentials SET principal=json_set(principal,'$.scopes',json('[]')) "
                "WHERE id=?",
                (e.manifest.credentials[1].id,),
            )
        )
    before = rows(e, "credentials"), rows(e, "records"), len(e.witness.requests)
    e.witness.failure = None
    with pytest.raises(ResearchError):
        await run(e)
    assert (rows(e, "credentials"), rows(e, "records"), len(e.witness.requests)) == before


async def test_cancellation_after_witness_keeps_prepared_state_and_competing_resumes_commit_once(
    bootstrap,
):
    e = bootstrap

    async def cancel(request):
        result = await e.witness(request)
        asyncio.current_task().cancel()
        return result

    task = asyncio.create_task(run(e, acquire=cancel))
    with pytest.raises(asyncio.CancelledError):
        await task
    prepared(e)
    results = await asyncio.gather(run(e), run(e), run(e))
    assert results[0] == results[1] == results[2]
    assert len(e.witness.requests) == 2
    assert (
        len([row for row in rows(e, "events") if "time_provider.requalified" in row["payload"]])
        == 1
    )


async def test_existing_foreign_state_and_changed_complete_manifest_are_refused(bootstrap):
    e = bootstrap
    e.store.write(lambda sql: sql.execute("CREATE TABLE foreign_state (value TEXT)"))
    with pytest.raises(ResearchError) as foreign:
        await run(e)
    assert foreign.value.code == ErrorCode.SCHEMA_UNSUPPORTED
    assert rows(e, "workspaces") == [] and e.witness.requests == []
    e.store.write(lambda sql: sql.execute("DROP TABLE foreign_state"))
    await run(e)
    changed = e.manifest.model_copy(update={"authority_name": "different-name"})
    with pytest.raises(ResearchError) as routing:
        require_deployment(e.store, changed)
    assert routing.value.code == ErrorCode.STALE_FENCE


async def test_completion_rechecks_roster_after_source_response_before_qualifying(bootstrap):
    e = bootstrap

    async def revoke(request):
        response = await e.witness(request)
        e.store.write(
            lambda sql: sql.execute(
                "UPDATE credentials SET revoked=1 WHERE id=?",
                (e.manifest.credentials[-1].id,),
            )
        )
        return response

    with pytest.raises(ResearchError) as changed:
        await run(e, acquire=revoke)
    assert changed.value.code == ErrorCode.AUTHORIZATION_DENIED
    prepared(e)
    assert (
        len([row for row in rows(e, "events") if "time_provider.requalified" in row["payload"]])
        == 0
    )
