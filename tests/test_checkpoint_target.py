"""Protected target import validates source bytes before initializing any target state."""

import asyncio
import json
import traceback
from dataclasses import replace

import pytest

from adapters.cloudflare import checkpoint_import
from adapters.cloudflare.checkpoint_target import import_target, load_target
from adapters.cloudflare.checkpoint_wire import MAX_RPC_BYTES, encode_checkpoint
from adapters.cloudflare.provisioning import provision
from adapters.cloudflare.runtime import open_router
from research_swarm.checkpoints.image_format import sign_manifest
from research_swarm.crypto import public_key
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.sql import StoreIdentity
from tests.test_checkpoint_capture import portable_rows
from tests.test_checkpoint_capture import sealed as sealed
from tests.test_checkpoint_import import TARGET_SEED, snapshot, target_secrets
from tests.test_checkpoint_import import recovery as recovery
from tests.test_checkpoint_schema import source as source
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.test_hosted_router import Gate


@pytest.fixture
async def configured(recovery):
    e = recovery
    e.target_config = {
        "schema_version": "1.0",
        "intent": e.intent.model_dump(mode="json"),
        "expected_source": e.source.manifest.configuration.authority.model_dump(mode="json"),
        "expected_custody": e.source.pin.model_dump(mode="json"),
        "custody_seed_hex": TARGET_SEED.hex(),
    }
    e.bundle = encode_checkpoint(e.envelope, e.image.bodies)
    e.state, e.opens = Gate(), 0
    return e


async def call(e, **changes):
    def open_store():
        e.opens += 1
        return e.target

    options = {
        "identity": e.target.identity,
        "credential": target_secrets(e).bootstrap,
        "target_json": json.dumps(e.target_config),
        "bundle_json": e.bundle,
        "crypto": e.source.crypto,
        **changes,
    }
    return await import_target(e.state, open_store, e.manifest, target_secrets(e), **options)


async def test_target_imports_once_and_reverifies_concurrent_replies(configured, monkeypatch):
    e = configured
    original = portable_rows(e.source)
    accepted, concurrent = await asyncio.gather(call(e), call(e))
    assert concurrent == accepted
    assert accepted["receipt"]["target_control_epoch"] == 2
    assert accepted["receipt"]["restore_id"] == e.intent.restore_id
    assert portable_rows(e.source) == original
    before = snapshot(e.target)
    e.state = Gate()  # New private runtime owner; durable target state remains.
    monkeypatch.setattr(
        checkpoint_import, "restore_inactive_in", lambda *a, **k: pytest.fail("repeated transform")
    )
    assert await call(e) == accepted
    assert snapshot(e.target) == before


@pytest.mark.parametrize(
    "change",
    [
        "credential",
        "target",
        "seed",
        "digest",
        "source",
        "custody",
        "operator",
        "location",
        "deployment",
        "malformed",
        "oversized",
        "unicode",
        "signature",
        "chunk",
        "bootstrap_pair",
    ],
)
async def test_untrusted_import_refuses_before_target_factory(configured, change):
    e = configured
    before, changed = snapshot(e.target), {}
    if change == "credential":
        changed["credential"] = "wrong"
    elif change == "target":
        changed["identity"] = StoreIdentity("cloudflare-do", "other")
    elif change == "seed":
        e.target_config["custody_seed_hex"] = bytes([197]).hex() * 32
    elif change == "digest":
        e.target_config["intent"]["manifest_digest"] = "sha256:" + "0" * 64
    elif change == "source":
        e.target_config["expected_source"]["activation_id"] = new_id()
    elif change == "custody":
        e.target_config["expected_custody"]["custody_id"] = new_id()
    elif change == "operator":
        e.target_config["intent"]["operator_id"] = new_id()
    elif change == "location":
        e.target_config["intent"]["target_location"] = "different/target"
    elif change == "deployment":
        e.target_config["intent"]["target_deployment_digest"] = "sha256:" + "0" * 64
    elif change in {"malformed", "oversized", "unicode"}:
        changed["bundle_json"] = {
            "malformed": "{}",
            "oversized": " " * (MAX_RPC_BYTES + 1),
            "unicode": "é",
        }[change]
    elif change == "signature":
        altered = json.loads(e.envelope)
        altered["signature"] = "0" * 128
        changed["bundle_json"] = encode_checkpoint(json.dumps(altered).encode(), e.image.bodies)
    elif change == "chunk":
        changed["bundle_json"] = encode_checkpoint(
            e.envelope, (e.image.bodies[0] + b" ", *e.image.bodies[1:])
        )
    else:
        e.source.secrets = replace(e.source.secrets, bootstrap="x" * 43)
        changed["credential"] = "x" * 43
    with pytest.raises(ResearchError):
        await call(e, **changed)
    assert e.opens == 0 and snapshot(e.target) == before


@pytest.mark.parametrize("phase", ["first", "replay"])
async def test_current_bootstrap_authentication_precedes_private_configuration_read(
    configured, phase
):
    e = configured
    if phase == "replay":
        await call(e)
    before, opens = snapshot(e.target), e.opens

    class Unreadable:
        def __str__(self):
            pytest.fail("Unauthenticated private configuration was rendered")

    with pytest.raises(ResearchError) as refused:
        await call(e, credential="wrong", target_json=Unreadable())
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert e.opens == opens and snapshot(e.target) == before


async def test_signed_relational_failure_cannot_publish_partial_import(configured):
    e = configured
    # Correct signature, incorrect source cut: only the relational importer may accept it.
    changed = e.image.manifest.model_copy(
        update={"snapshot_evidence_through": e.image.manifest.snapshot_evidence_through + 1}
    )
    e.target_config["intent"]["manifest_digest"] = changed.digest
    from tests.test_time_witnesses import SEED

    e.bundle = encode_checkpoint(
        sign_manifest(changed, SEED, crypto=e.source.crypto), e.image.bodies
    )
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await call(e)
    assert e.opens == 1 and snapshot(e.target) == before


async def test_imported_target_cannot_provision_or_open_ordinary_router(configured):
    e = configured
    await call(e)
    before = snapshot(e.target)

    async def witness(_):
        pytest.fail("Inactive import attempted witness qualification")

    with pytest.raises(ResearchError):
        await provision(
            e.state,
            lambda: e.target,
            e.manifest,
            e.source.secrets,
            identity=e.target.identity,
            credential=e.source.secrets.bootstrap,
            crypto=e.source.crypto,
            acquire=witness,
        )
    with pytest.raises(ResearchError):
        await open_router(
            e.state,
            lambda: e.target,
            e.manifest,
            e.source.secrets,
            crypto=e.source.crypto,
            acquire=witness,
            objects=None,
        )
    assert snapshot(e.target) == before


async def test_completed_deployment_cannot_be_replaced_by_private_import(configured):
    e = configured
    await provision(
        e.state,
        lambda: e.target,
        e.manifest,
        e.source.secrets,
        identity=e.target.identity,
        credential=e.source.secrets.bootstrap,
        crypto=e.source.crypto,
        acquire=e.source.witness,
    )
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await call(e)
    assert snapshot(e.target) == before


@pytest.mark.parametrize(
    "change",
    ["missing", "extra", "duplicate", "bad_key", "large", "pin_scope", "assignment", "dispatch"],
)
def test_protected_target_configuration_is_bounded_and_opaque(configured, change):
    e = configured
    value = json.loads(json.dumps(e.target_config))
    private = "synthetic-sensitive-input" * 4
    if change == "missing":
        del value["intent"]
    elif change == "extra":
        value["unexpected"] = private
    elif change == "bad_key":
        value["custody_seed_hex"] = private
    elif change == "large":
        value["custody_seed_hex"] = private * 300
    elif change == "pin_scope":
        value["expected_custody"]["control_epoch"] += 1
    elif change in {"assignment", "dispatch"}:
        seed = (
            e.source.secrets.assignment_seed
            if change == "assignment"
            else e.source.secrets.dispatch_key
        )
        value["custody_seed_hex"] = seed.hex()
        value["intent"]["target_public_key"] = public_key(e.source.crypto, seed).hex()
    encoded = json.dumps(value)
    if change == "duplicate":
        encoded = '{"schema_version":"1.0",' + encoded[1:]
    with pytest.raises(ResearchError) as refused:
        load_target(encoded, e.manifest, e.source.secrets, credential=e.source.secrets.bootstrap)
    assert private not in "".join(traceback.format_exception(refused.value))
    assert refused.value.__suppress_context__


def test_target_custody_is_not_serializable_or_printable(configured):
    e = configured
    value = load_target(
        json.dumps(e.target_config),
        e.manifest,
        e.source.secrets,
        credential=e.source.secrets.bootstrap,
    )
    assert repr(value) == "TargetCustody(<redacted>)"
    with pytest.raises(TypeError):
        json.dumps(value)
