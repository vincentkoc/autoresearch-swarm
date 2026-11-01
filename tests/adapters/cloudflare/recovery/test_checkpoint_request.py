"""Only a verified completed inactive target can sign a hosted election request."""

import asyncio
import json

import pytest

from adapters.cloudflare.recovery import checkpoint_target
from adapters.cloudflare.recovery.checkpoint_anchor import KIND, register_anchor_in
from adapters.cloudflare.recovery.checkpoint_election import elect_source
from adapters.cloudflare.recovery.checkpoint_import import IMPORT_TABLE
from adapters.cloudflare.recovery.checkpoint_target import request_target
from adapters.cloudflare.recovery.checkpoint_wire import encode_checkpoint
from research_swarm.checkpoints.elections import verify_election, verify_election_request
from research_swarm.checkpoints.fingerprint import fingerprint_state
from research_swarm.checkpoints.image_format import sign_manifest
from research_swarm.domain.checkpoint_elections import (
    SignedLogicalElection,
    SignedLogicalElectionRequest,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import encode
from tests.adapters.cloudflare.hosted.test_hosted_router import Gate
from tests.adapters.cloudflare.recovery.test_checkpoint_capture import capture, portable_rows
from tests.adapters.cloudflare.recovery.test_checkpoint_capture import sealed as sealed
from tests.adapters.cloudflare.recovery.test_checkpoint_import import recovery as recovery
from tests.adapters.cloudflare.recovery.test_checkpoint_import import snapshot, target_secrets
from tests.adapters.cloudflare.recovery.test_checkpoint_schema import source as source
from tests.adapters.cloudflare.recovery.test_checkpoint_target import call as import_call
from tests.adapters.cloudflare.recovery.test_checkpoint_target import configured as configured
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.workers.test_time_witnesses import SEED


@pytest.fixture
async def imported(configured):
    e = configured
    source = e.source
    source.store.write(
        lambda sql: register_anchor_in(
            sql,
            source.manifest,
            source.store.identity,
            source.pin,
            source.context,
            allow_create=True,
        )
    )
    e.image = capture(source)
    e.intent = e.intent.model_copy(update={"manifest_digest": e.image.manifest.digest})
    e.envelope = sign_manifest(e.image.manifest, SEED, crypto=source.crypto)
    e.bundle = encode_checkpoint(e.envelope, e.image.bodies)
    e.target_config["intent"] = e.intent.model_dump(mode="json")
    e.receipt = (await import_call(e))["receipt"]
    return e


async def request(e, *, store=None, **changes):
    def open_store():
        e.opens += 1
        return e.target if store is None else store

    options = {
        "identity": e.target.identity,
        "credential": target_secrets(e).bootstrap,
        "target_json": json.dumps(e.target_config),
        "crypto": e.source.crypto,
        **changes,
    }
    return await request_target(Gate(), open_store, e.manifest, target_secrets(e), **options)


def pins(e):
    return {
        "expected_intent": e.intent,
        "expected_backup_id": e.image.manifest.seal.backup_id,
        "expected_source": e.image.manifest.source_authority,
        "expected_deployment_digest": e.image.manifest.source_deployment_digest,
        "expected_custody": e.source.pin,
        "crypto": e.source.crypto,
    }


async def test_imported_target_requests_replay_without_writes_or_new_ids_and_source_elects(
    imported,
):
    e = imported
    before, original, calls = snapshot(e.target), portable_rows(e.source), []
    witness_calls = len(e.source.witness.requests)
    e.source.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic offline")

    class ReadOnly:
        identity = e.target.identity

        def write(self, callback):
            pytest.fail("Target request attempted a write")

        def read(self, callback):
            calls.append(True)

            class Guard:
                def __init__(self, sql):
                    self.sql = sql

                def execute(self, statement, parameters=()):
                    assert statement.lstrip().startswith(("SELECT", "PRAGMA"))
                    return self.sql.execute(statement, parameters)

            return e.target.read(lambda sql: callback(Guard(sql)))

    first, second = await asyncio.gather(request(e, store=ReadOnly()), request(e, store=ReadOnly()))
    assert first == second and len(calls) == 2
    assert await request(e) == first
    signed = SignedLogicalElectionRequest.model_validate_json(first)
    assert verify_election_request(signed, **pins(e)).intent == e.intent
    source_config = {
        "schema_version": "1.0",
        "authority": e.source.manifest.configuration.authority.model_dump(mode="json"),
        "deployment_digest": e.source.manifest.digest,
        "pin": e.source.pin.model_dump(mode="json"),
        "custody_seed_hex": SEED.hex(),
    }
    elected = await elect_source(
        Gate(),
        lambda: e.source.store,
        e.source.manifest,
        e.source.secrets,
        identity=e.source.store.identity,
        credential=e.source.secrets.bootstrap,
        source_json=json.dumps(source_config),
        request_json=first,
        crypto=e.source.crypto,
    )
    assert verify_election(SignedLogicalElection.model_validate_json(elected), **pins(e))
    assert snapshot(e.target) == before and portable_rows(e.source) == original
    assert len(e.source.witness.requests) == witness_calls
    assert before["records"] == snapshot(e.target)["records"]


async def test_empty_target_never_initializes_and_legacy_import_cannot_request(configured):
    e = configured
    before = snapshot(e.target)
    with pytest.raises(ResearchError) as refused:
        await request(e)
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert snapshot(e.target) == before
    await import_call(e)
    before = snapshot(e.target)
    with pytest.raises(ResearchError) as refused:
        await request(e)
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert snapshot(e.target) == before


@pytest.mark.parametrize(
    "change", ["bootstrap", "seed", "location", "deployment", "source", "custody"]
)
async def test_current_protected_configuration_refuses_before_opener(imported, change):
    e = imported
    options = {}
    if change == "bootstrap":
        options["credential"] = "wrong"
    elif change == "seed":
        e.target_config["custody_seed_hex"] = bytes([197]).hex() * 32
    elif change in {"location", "deployment"}:
        key = "target_location" if change == "location" else "target_deployment_digest"
        e.target_config["intent"][key] = (
            "other/location" if change == "location" else "sha256:" + "0" * 64
        )
    elif change == "source":
        e.target_config["expected_source"]["control_epoch"] += 1
    else:
        e.target_config["expected_custody"]["control_epoch"] += 1
    before, opens = snapshot(e.target), e.opens
    with pytest.raises(ResearchError):
        await request(e, **options)
    assert e.opens == opens and snapshot(e.target) == before


@pytest.mark.parametrize(
    "change",
    [
        "fingerprint",
        "gate",
        "phase",
        "receipt",
        "seal",
        "history",
        "anchor",
        "missing_anchor",
        "missing_import",
    ],
)
async def test_completed_import_requires_exact_fingerprint_receipt_seal_and_anchor(
    imported, change
):
    e = imported

    def alter(sql):
        if change == "fingerprint":
            sql.execute("UPDATE commands SET response='changed'")
            return
        if change == "missing_import":
            sql.execute(f"DROP TABLE {IMPORT_TABLE}")
            return
        if change == "receipt":
            value = {**e.receipt, "backup_id": new_id()}
            sql.execute(f"UPDATE {IMPORT_TABLE} SET receipt=?", (encode(value),))
        elif change == "gate":
            sql.execute("UPDATE records SET state='active' WHERE kind='workspace_gate'")
        elif change == "phase":
            sql.execute("UPDATE records SET state='admitted' WHERE kind='restoration'")
        elif change == "seal":
            sql.execute(
                "UPDATE records SET payload=json_set(payload,'$.reason','changed') "
                "WHERE kind='workspace_gate'"
            )
        elif change == "history":
            sql.execute("UPDATE records SET state='changed' WHERE kind='workspace_seal'")
        elif change == "anchor":
            sql.execute(
                "UPDATE records SET payload=json_set(payload,'$.source_authority.activation_id',?) "
                "WHERE kind=?",
                (new_id(), KIND),
            )
        elif change == "missing_anchor":
            sql.execute("DELETE FROM records WHERE kind=?", (KIND,))
        # Exercise semantic checks independently of the already-qualified fingerprint gate.
        sql.execute(f"UPDATE {IMPORT_TABLE} SET state_digest=?", (fingerprint_state(sql),))

    e.target.write(alter)
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await request(e)
    assert snapshot(e.target) == before


async def test_signer_failure_does_not_change_imported_target(imported, monkeypatch):
    e = imported
    before = snapshot(e.target)

    def fail(*args):
        raise RuntimeError("Synthetic signer unavailable")

    monkeypatch.setattr(checkpoint_target, "sign", fail)
    with pytest.raises(RuntimeError, match="signer unavailable"):
        await request(e)
    assert snapshot(e.target) == before
