"""A selected hosted target enters recovery once without releasing execution or budget."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from adapters.cloudflare import checkpoint_admission
from adapters.cloudflare.checkpoint_admission import HostedRecoveryVerifier, admit_target
from adapters.cloudflare.checkpoint_election import elect_source
from adapters.cloudflare.checkpoint_target import TargetBinding
from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.domain.backup import RecoveryAdmission
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.checkpoint_elections import SignedLogicalElection
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.state.database import Database
from tests.test_checkpoint_capture import portable_rows
from tests.test_checkpoint_capture import sealed as sealed
from tests.test_checkpoint_import import recovery as recovery
from tests.test_checkpoint_import import snapshot
from tests.test_checkpoint_request import imported as imported
from tests.test_checkpoint_request import request
from tests.test_checkpoint_schema import source as source
from tests.test_checkpoint_target import configured as configured
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.test_hosted_router import FixtureStore, Gate
from tests.test_time_witnesses import SEED


@pytest.fixture
async def elected(imported):
    e = imported
    e.elected = await elect_source(
        Gate(),
        lambda: e.source.store,
        e.source.manifest,
        e.source.secrets,
        identity=e.source.store.identity,
        credential=e.source.secrets.bootstrap,
        source_json=json.dumps(
            {
                "schema_version": "1.0",
                "authority": e.source.manifest.configuration.authority.model_dump(mode="json"),
                "deployment_digest": e.source.manifest.digest,
                "pin": e.source.pin.model_dump(mode="json"),
                "custody_seed_hex": SEED.hex(),
            }
        ),
        request_json=await request(e),
        crypto=e.source.crypto,
    )
    e.binding = TargetBinding.model_validate(
        {key: value for key, value in e.target_config.items() if key != "custody_seed_hex"}
    )
    e.verifier = HostedRecoveryVerifier(e.manifest, e.binding)
    return e


async def admit(e, *, store=None, **changes):
    def open_store():
        e.opens += 1
        return e.target if store is None else store

    options = {
        "identity": e.target.identity,
        "credential": e.source.secrets.bootstrap,
        "target_json": json.dumps(e.target_config),
        "election_json": e.elected,
        "crypto": e.source.crypto,
        **changes,
    }
    return await admit_target(Gate(), open_store, e.manifest, e.source.secrets, **options)


def context(e):
    return CommandContext(
        principal=next(
            item.principal
            for item in e.manifest.credentials
            if item.principal.id == e.manifest.provisioning_actor_id
        ),
        idempotency_key="recovery-read",
    )


async def test_full_import_election_admission_preserves_obligations_and_stays_inactive(elected):
    e = elected
    before, original = snapshot(e.target), portable_rows(e.source)
    witness_calls = len(e.source.witness.requests)
    e.source.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic offline")
    raw = await admit(e)
    reply = strict_json_loads(raw)
    assert reply["disposition"] == "accepted"
    admission = RecoveryAdmission.model_validate(reply["result"]["data"])
    assert (
        admission.logical_election_digest
        == SignedLogicalElection.model_validate_json(e.elected).digest
    )
    after = snapshot(e.target)
    for table in (
        "budgets",
        "budget_reservations",
        "budget_settlements",
        "budget_entries",
        "credentials",
    ):
        assert after[table] == before[table]
    unchanged = {
        "attempt",
        "worker_session",
        "artifact",
        "artifact_reference",
        "orchestration_owner",
        "confirmation_exposure",
        "time_provider",
    }
    assert [row for row in after["records"] if row["kind"] in unchanged] == [
        row for row in before["records"] if row["kind"] in unchanged
    ]
    assert len(after["events"]) == len(before["events"]) + 1
    assert len(after["commands"]) == len(before["commands"]) + 1
    assert await admit(e) == raw and snapshot(e.target) == after
    assert portable_rows(e.source) == original
    assert len(e.source.witness.requests) == witness_calls
    database = Database.from_store(e.target, recovery_verifier=e.verifier)
    assert recovery_authority(database, context(e), crypto_provider=e.source.crypto) is not None
    with pytest.raises(ResearchError) as refused:
        recovery_authority(Database.from_store(e.target), context(e))
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    with pytest.raises(ResearchError):
        database.execute(context(e), "project.create", {}, "*", lambda tx: pytest.fail("dispatch"))
    with pytest.raises(ResearchError):
        await request(e)


async def test_independent_connection_race_and_lost_reply_replay_one_admission(
    elected, monkeypatch
):
    e = elected
    barrier = Barrier(2)
    path = e.target.delegate.path

    def race(_):
        opened = FixtureStore(Database(path).store)
        opened.identity = e.target.identity
        barrier.wait(timeout=10)
        return asyncio.run(admit(e, store=opened))

    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(race, range(2)))
    assert replies[0] == replies[1]
    before = snapshot(e.target)
    monkeypatch.setattr(checkpoint_admission, "sign", lambda *a: pytest.fail("re-sign"))
    monkeypatch.setattr(
        checkpoint_admission, "inspect_imported_target", lambda *a: pytest.fail("pristine replay")
    )
    assert await admit(e) == replies[0]
    assert snapshot(e.target) == before
    assert len([r for r in before["records"] if r["kind"] == "recovery_admission"]) == 1
    assert len([r for r in before["records"] if r["kind"] == "logical_recovery_election"]) == 1


async def test_commit_then_lost_reply_replays_after_reopening(elected):
    e = elected

    class Lost:
        identity = e.target.identity

        def write(self, callback):
            e.target.write(callback)
            raise OSError("Synthetic lost admission reply")

    with pytest.raises(OSError, match="lost admission reply"):
        await admit(e, store=Lost())
    before = snapshot(e.target)
    reopened = FixtureStore(Database(e.target.delegate.path).store)
    reopened.identity = e.target.identity
    reply = strict_json_loads(await admit(e, store=reopened))
    assert reply["disposition"] == "accepted"
    assert snapshot(e.target) == before


@pytest.mark.parametrize("point", ["after_companion", "after_transition", "signing"])
async def test_failure_rolls_back_companion_admission_fences_and_ack(elected, monkeypatch, point):
    e, original = elected, checkpoint_admission.admit_in
    before = snapshot(e.target)

    def fail(tx, admission):
        if point == "after_transition":
            original(tx, admission)
        raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic admission interruption")

    with monkeypatch.context() as patch:
        if point == "signing":
            patch.setattr(
                checkpoint_admission,
                "sign",
                lambda *a: (_ for _ in ()).throw(OSError("Synthetic signing interruption")),
            )
        else:
            patch.setattr(checkpoint_admission, "admit_in", fail)
        with pytest.raises((ResearchError, OSError)):
            await admit(e)
    assert snapshot(e.target) == before
    assert strict_json_loads(await admit(e))["disposition"] == "accepted"


@pytest.mark.parametrize(
    "damage", ["native_only", "signature", "target", "bootstrap", "receipt", "pristine"]
)
async def test_invalid_inputs_never_enter_recovery(elected, damage):
    e = elected
    options = {}
    if damage == "native_only":
        options["election_json"] = SignedLogicalElection.model_validate_json(
            e.elected
        ).election.signed_claim.model_dump_json()
    elif damage == "signature":
        options["election_json"] = (
            SignedLogicalElection.model_validate_json(e.elected)
            .model_copy(update={"signature": "0" * 128})
            .model_dump_json()
        )
    elif damage == "target":
        config = {
            **e.target_config,
            "intent": {**e.target_config["intent"], "target_location": "other/target"},
        }
        options["target_json"] = json.dumps(config)
    elif damage == "bootstrap":
        options["credential"] = "wrong"
    elif damage == "receipt":
        e.target.write(lambda sql: sql.execute("DELETE FROM records WHERE kind='restoration'"))
    else:
        e.target.write(
            lambda sql: sql.execute(
                "UPDATE records SET updated_at='changed' WHERE kind='workspace_gate'"
            )
        )
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await admit(e, **options)
    assert snapshot(e.target) == before


@pytest.mark.parametrize("damage", ["companion", "digest", "ack", "state", "deployment"])
async def test_replay_rechecks_current_lifecycle_and_protected_destination(elected, damage):
    e = elected
    await admit(e)
    if damage == "companion":
        e.target.write(
            lambda sql: sql.execute("DELETE FROM records WHERE kind='logical_recovery_election'")
        )
    elif damage == "digest":
        e.target.write(
            lambda sql: sql.execute(
                "UPDATE records SET payload=json_remove(payload,'$.logical_election_digest') "
                "WHERE kind='recovery_admission'"
            )
        )
    elif damage == "ack":
        e.target.write(
            lambda sql: sql.execute(
                "DELETE FROM commands WHERE idempotency_key LIKE 'logical-admit:%'"
            )
        )
    elif damage == "state":
        e.target.write(
            lambda sql: sql.execute("UPDATE records SET state='active' WHERE kind='workspace_gate'")
        )
    else:
        e.manifest = e.manifest.model_copy(update={"authority_name": "changed-runtime"})
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await admit(e)
    assert snapshot(e.target) == before
