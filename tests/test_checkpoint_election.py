"""Only the original authority may atomically elect one exact restoration destination."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from adapters.cloudflare import checkpoint_election
from adapters.cloudflare.checkpoint_anchor import TABLE
from adapters.cloudflare.checkpoint_election import elect_source
from adapters.cloudflare.checkpoint_wire import decode_checkpoint
from research_swarm.checkpoints.elections import verify_election
from research_swarm.crypto import public_key
from research_swarm.domain.backup import RestoreClaimRequest, SignedRestoreRequest
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.checkpoint_elections import (
    LogicalElectionRequest,
    SignedLogicalElection,
    SignedLogicalElectionRequest,
)
from research_swarm.domain.checkpoint_images import LogicalImportIntent, SignedLogicalCheckpoint
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import encode
from research_swarm.state.sql import StoreIdentity
from tests.test_checkpoint_anchor import anchor, metadata
from tests.test_checkpoint_capture import portable_rows
from tests.test_checkpoint_elections import TARGET_SEED, signature
from tests.test_checkpoint_schema import source as source
from tests.test_checkpoint_source import call as source_call
from tests.test_checkpoint_source import configured as configured
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.test_hosted_router import Gate


@pytest.fixture
async def ready(configured):
    e = configured
    await source_call(e)
    e.encoded = await source_call(e, capture=True)
    envelope, _ = decode_checkpoint(e.encoded)
    e.image = SignedLogicalCheckpoint.model_validate_json(envelope).manifest
    return e


def proposal(e, target="a", **changes):
    scope = e.image.source_authority
    intent = LogicalImportIntent(
        restore_id=new_id(),
        manifest_digest=e.image.digest,
        target_location="synthetic-target/" + target,
        target_deployment_digest=content_digest(target.encode()),
        target_public_key=public_key(e.crypto, TARGET_SEED).hex(),
        operator_id=e.manifest.provisioning_actor_id,
    ).model_copy(update=changes)
    native = RestoreClaimRequest(
        backup_id=e.image.seal.backup_id,
        manifest_digest=intent.manifest_digest,
        workspace_id=scope.workspace_id,
        controller_id=scope.controller_id,
        source_custody_id=e.pin.custody_id,
        source_control_epoch=scope.control_epoch,
        restore_id=intent.restore_id,
        target_public_key=intent.target_public_key,
        target_control_epoch=scope.control_epoch + 1,
    )
    request = LogicalElectionRequest(
        intent=intent,
        source_authority=scope,
        source_deployment_digest=e.manifest.digest,
        signed_request=SignedRestoreRequest(
            request=native, signature=signature(e.crypto, TARGET_SEED, native)
        ),
    )
    return SignedLogicalElectionRequest(
        request=request, signature=signature(e.crypto, TARGET_SEED, request)
    )


async def submit(e, request, *, store=None, **changes):
    options = {
        "identity": e.store.identity,
        "credential": e.secrets.bootstrap,
        "source_json": json.dumps(e.source_config),
        "request_json": request.model_dump_json(),
        "crypto": e.crypto,
        **changes,
    }
    return await elect_source(
        Gate(), lambda: e.store if store is None else store, e.manifest, e.secrets, **options
    )


async def test_election_preserves_capture_and_replays_original_signed_bytes_after_lost_reply(
    ready, monkeypatch
):
    e = ready
    request = proposal(e)
    before, witness_calls = portable_rows(e), len(e.witness.requests)
    e.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic offline witness")

    class LostReply:
        identity = e.store.identity

        def write(self, callback):
            e.store.write(callback)
            raise ConnectionError("Synthetic lost committed reply")

    with pytest.raises(ConnectionError, match="lost committed reply"):
        await submit(e, request, store=LostReply())
    stored = metadata(e)[0]["election_payload"]
    assert anchor(e).state == "elected"
    assert portable_rows(e) == before and await source_call(e, capture=True) == e.encoded
    monkeypatch.setattr(
        checkpoint_election, "capture_checkpoint", lambda *a, **kw: pytest.fail("recaptured replay")
    )
    monkeypatch.setattr(
        checkpoint_election, "sign", lambda *a, **kw: pytest.fail("re-signed replay")
    )
    assert await submit(e, request) == stored
    assert len(e.witness.requests) == witness_calls
    signed = SignedLogicalElection.model_validate_json(stored)
    verified = verify_election(
        signed,
        expected_intent=request.request.intent,
        expected_backup_id=e.image.seal.backup_id,
        expected_source=e.image.source_authority,
        expected_deployment_digest=e.manifest.digest,
        expected_custody=e.pin,
        crypto=e.crypto,
    )
    assert verified.signed_request == request


@pytest.mark.parametrize("same", [False, True])
async def test_independent_connection_race_selects_one_destination_and_exact_retries(ready, same):
    e = ready
    first = proposal(e)
    second = first if same else proposal(e, "b", restore_id=first.request.intent.restore_id)
    before, barrier = portable_rows(e), Barrier(2)

    def run(request):
        barrier.wait(timeout=5)
        try:
            return asyncio.run(submit(e, request))
        except ResearchError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(run, (first, second)))
    accepted = [value for value in results if isinstance(value, str)]
    assert len(accepted) == (2 if same else 1)
    assert len(set(accepted)) == 1
    if not same:
        assert next(value for value in results if isinstance(value, ResearchError)).code == (
            ErrorCode.IDEMPOTENCY_CONFLICT
        )
    assert metadata(e)[0]["election_payload"] == accepted[0]
    assert portable_rows(e) == before
    assert await source_call(e, capture=True) == e.encoded


@pytest.mark.parametrize(
    "change",
    [
        "credential",
        "identity",
        "signature",
        "image",
        "cut",
        "operator",
        "same_location",
        "same_deployment",
        "source",
        "backup",
    ],
)
async def test_invalid_request_or_changed_source_never_elects(ready, change):
    e = ready
    request, options = proposal(e), {}
    if change == "credential":
        options["credential"] = "wrong"
    elif change == "identity":
        options["identity"] = StoreIdentity("cloudflare-do", "other/authority")
    elif change == "signature":
        request = request.model_copy(update={"signature": "0" * 128})
    elif change in {"image", "operator", "same_location", "same_deployment"}:
        changed = {
            "image": {"manifest_digest": content_digest(b"other image")},
            "operator": {"operator_id": new_id()},
            "same_location": {"target_location": e.store.identity.location},
            "same_deployment": {"target_deployment_digest": e.manifest.digest},
        }[change]
        request = proposal(e, **changed)
    elif change == "cut":
        e.store.write(lambda sql: sql.execute("UPDATE commands SET response='changed'"))
    elif change == "source":
        value = request.request.model_copy(
            update={"source_deployment_digest": content_digest(b"other source")}
        )
        request = SignedLogicalElectionRequest(
            request=value, signature=signature(e.crypto, TARGET_SEED, value)
        )
    else:
        native = request.request.signed_request.request.model_copy(update={"backup_id": new_id()})
        value = request.request.model_copy(
            update={
                "signed_request": SignedRestoreRequest(
                    request=native, signature=signature(e.crypto, TARGET_SEED, native)
                )
            }
        )
        request = SignedLogicalElectionRequest(
            request=value, signature=signature(e.crypto, TARGET_SEED, value)
        )
    before, original = portable_rows(e), metadata(e)
    with pytest.raises(ResearchError):
        await submit(e, request, **options)
    assert metadata(e) == original and anchor(e).state == "open"
    assert portable_rows(e) == before


async def test_failure_during_signing_rolls_back_without_consuming_election(ready, monkeypatch):
    e = ready
    request, original = proposal(e), metadata(e)
    before, real_sign, calls = portable_rows(e), checkpoint_election.sign, []

    def fail_second(*args):
        calls.append(True)
        if len(calls) == 2:
            raise RuntimeError("Synthetic interrupted source signature")
        return real_sign(*args)

    with monkeypatch.context() as patch:
        patch.setattr(checkpoint_election, "sign", fail_second)
        with pytest.raises(RuntimeError, match="interrupted source"):
            await submit(e, request)
    assert portable_rows(e) == before and metadata(e) == original
    assert await submit(e, request)


@pytest.mark.parametrize("change", ["revoked", "deleted", "signature"])
async def test_elected_replay_rechecks_operator_anchor_and_signatures(ready, change):
    e = ready
    request = proposal(e)
    await submit(e, request)
    if change == "revoked":
        e.store.write(lambda sql: sql.execute("UPDATE credentials SET revoked=1"))
    elif change == "deleted":
        e.store.write(lambda sql: sql.execute(f"DELETE FROM {TABLE}"))
    else:
        raw = json.loads(metadata(e)[0]["election_payload"])
        raw["signature"] = "0" * 128
        e.store.write(
            lambda sql: sql.execute(f"UPDATE {TABLE} SET election_payload=?", (encode(raw),))
        )
    before, original = portable_rows(e), metadata(e)
    with pytest.raises(ResearchError):
        await submit(e, request)
    assert portable_rows(e) == before and metadata(e) == original
