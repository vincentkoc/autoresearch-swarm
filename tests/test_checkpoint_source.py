"""Protected source custody and resumable control on the shared SQL boundary."""

import json
import traceback
from dataclasses import replace

import pytest

from adapters.cloudflare.checkpoint_source import capture_source, load_source, seal_source
from adapters.cloudflare.checkpoint_wire import decode_checkpoint
from research_swarm.checkpoints.capture import CaptureLimits, preflight_rows
from research_swarm.checkpoints.image_format import verify_image
from research_swarm.control.backup import BackupControl
from research_swarm.control.worker_records import WorkerData
from research_swarm.crypto import public_key
from research_swarm.domain.backup import BackupCustodyPin
from research_swarm.domain.checkpoint_images import SignedLogicalCheckpoint
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.domain.workers import WorkerBinding
from research_swarm.state.database import encode
from research_swarm.state.sql import StoreIdentity
from tests.test_checkpoint_capture import portable_rows
from tests.test_checkpoint_schema import source as source
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.test_cloudflare_provisioning import rows

CUSTODY_SEED = bytes([193]) * 32


@pytest.fixture
async def configured(source):
    e = source
    scope = e.manifest.configuration.authority
    e.pin = BackupCustodyPin(
        custody_id=new_id(),
        workspace_id=scope.workspace_id,
        controller_id=scope.controller_id,
        control_epoch=scope.control_epoch,
        public_key=public_key(e.crypto, CUSTODY_SEED).hex(),
    )
    e.source_config = {
        "schema_version": "1.0",
        "deployment_digest": e.manifest.digest,
        "authority": scope.model_dump(mode="json"),
        "pin": e.pin.model_dump(mode="json"),
        "custody_seed_hex": CUSTODY_SEED.hex(),
    }
    e.request = {
        "schema_version": "1.0",
        "request_id": new_id(),
        "expected_revision": 0,
        "seal": {
            "backup_id": new_id(),
            "expected_controller_id": scope.controller_id,
            "expected_control_epoch": scope.control_epoch,
            "reason": "Synthetic private source checkpoint",
        },
    }
    return e


async def call(e, *, capture=False, **overrides):
    def open_store():
        e.opens += 1
        return e.store

    options = {
        "identity": e.store.identity,
        "credential": e.secrets.bootstrap,
        "source_json": json.dumps(e.source_config),
        "request_json": json.dumps(
            {"backup_id": e.request["seal"]["backup_id"]} if capture else e.request
        ),
        "crypto": e.crypto,
        **overrides,
    }
    return await (capture_source if capture else seal_source)(
        e.state, open_store, e.manifest, e.secrets, **options
    )


async def test_source_seals_replays_and_signs_exact_bytes_during_witness_outage(configured):
    e = configured
    requests = len(e.witness.requests)
    e.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic source offline")
    result = await call(e)
    assert result["seal"]["result"]["state"] == "sealed"
    assert result["custody"]["result"]["data"] == e.pin.model_dump(mode="json")
    assert result["seal"]["command_id"] != result["custody"]["command_id"]
    before = portable_rows(e)
    assert await call(e) == result
    encoded = await call(e, capture=True)
    assert await call(e, capture=True) == encoded
    envelope, bodies = decode_checkpoint(encoded)
    image = SignedLogicalCheckpoint.model_validate_json(envelope).manifest
    assert (
        verify_image(
            envelope,
            bodies,
            expected_digest=image.digest,
            expected_source=e.manifest.configuration.authority,
            expected_custody=e.pin,
            crypto=e.crypto,
        ).manifest
        == image
    )
    assert portable_rows(e) == before
    assert len(e.witness.requests) == requests
    assert CUSTODY_SEED.hex() not in encoded
    assert all(token not in encoded for token in e.secrets.credentials.values())


async def test_interrupted_seal_rolls_back_both_phases_and_retries_exactly(configured, monkeypatch):
    e = configured
    before = portable_rows(e)

    def interrupted(*args, **kwargs):
        raise RuntimeError("Synthetic interruption before custody transaction")

    with monkeypatch.context() as patch:
        patch.setattr(BackupControl, "register_custody", interrupted)
        with pytest.raises(RuntimeError, match="before custody"):
            await call(e)
    assert portable_rows(e) == before
    events = rows(e, "events")
    accepted = await call(e)
    assert accepted["seal"]["result"]["revision"] == 1
    assert len(rows(e, "events")) == len(events) + 3
    before = portable_rows(e)
    assert await call(e) == accepted
    assert portable_rows(e) == before


@pytest.mark.parametrize("operation", ["seal", "capture"])
async def test_revoked_operator_refuses_even_saved_private_command(configured, operation):
    e = configured
    await call(e)
    e.store.write(
        lambda sql: sql.execute(
            "UPDATE credentials SET revoked=1 WHERE id=?", (e.manifest.credentials[0].id,)
        )
    )
    before = portable_rows(e)
    with pytest.raises(ResearchError) as refused:
        await call(e, capture=operation == "capture")
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert portable_rows(e) == before


async def test_live_admission_rechecks_each_phase_before_cached_outcome(configured, monkeypatch):
    e = configured
    before = portable_rows(e)
    original = BackupControl.register_custody

    def changed(control, *args, **kwargs):
        control.database.store.sql.execute(
            "UPDATE credentials SET revoked=1 WHERE id=?", (e.manifest.credentials[0].id,)
        )
        return original(control, *args, **kwargs)

    monkeypatch.setattr(BackupControl, "register_custody", changed)
    with pytest.raises(ResearchError) as refused:
        await call(e)
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert portable_rows(e) == before


@pytest.mark.parametrize("change", ["reason", "revision", "backup", "request", "pin", "seed"])
async def test_finalized_source_refuses_changed_request_or_custody(configured, change):
    e = configured
    await call(e)
    before = portable_rows(e)
    if change == "reason":
        e.request["seal"]["reason"] = "Changed interpretation"
    elif change == "revision":
        e.request["expected_revision"] = 1
    elif change == "backup":
        e.request["seal"]["backup_id"] = new_id()
    elif change == "request":
        e.request["request_id"] = new_id()
    elif change == "pin":
        e.source_config["pin"]["custody_id"] = new_id()
    else:
        changed = bytes([194]) * 32
        e.source_config["custody_seed_hex"] = changed.hex()
        e.source_config["pin"]["public_key"] = public_key(e.crypto, changed).hex()
    with pytest.raises(ResearchError):
        await call(e)
    assert portable_rows(e) == before


@pytest.mark.parametrize("capture", [False, True])
@pytest.mark.parametrize("change", ["bootstrap", "physical", "source", "request", "bootstrap_pair"])
async def test_outer_admission_refuses_before_store_factory(configured, capture, change):
    e = configured
    before, opens = portable_rows(e), e.opens
    changes = {
        "bootstrap": {"credential": "wrong"},
        "physical": {"identity": StoreIdentity("cloudflare-do", "wrong")},
        "source": {"source_json": None},
        "request": {"request_json": " " * 16385},
        "bootstrap_pair": {"credential": "x" * 43},
    }
    if change == "bootstrap_pair":
        e.secrets = replace(e.secrets, bootstrap="x" * 43)
    with pytest.raises(ResearchError):
        await call(e, capture=capture, **changes[change])
    assert e.opens == opens and portable_rows(e) == before


@pytest.mark.parametrize(
    "change",
    [
        "digest",
        "authority",
        "pin_epoch",
        "assignment",
        "dispatch",
        "extra",
        "missing",
        "key",
        "size",
    ],
)
def test_protected_custody_configuration_is_exact_and_opaque(configured, change):
    e = configured
    value = json.loads(json.dumps(e.source_config))
    private = "synthetic-sensitive-value" * 4
    if change == "digest":
        value["deployment_digest"] = "sha256:" + "0" * 64
    elif change == "authority":
        value["authority"]["activation_id"] = new_id()
    elif change == "pin_epoch":
        value["pin"]["control_epoch"] += 1
    elif change in {"assignment", "dispatch"}:
        seed = e.secrets.assignment_seed if change == "assignment" else e.secrets.dispatch_key
        value["custody_seed_hex"] = seed.hex()
        value["pin"]["public_key"] = public_key(e.crypto, seed).hex()
    elif change == "extra":
        value["extra"] = private
    elif change == "missing":
        del value["pin"]
    else:
        value["custody_seed_hex"] = private * (100 if change == "size" else 1)
    with pytest.raises(ResearchError) as refused:
        load_source(
            json.dumps(value),
            e.manifest,
            e.secrets,
            credential=e.secrets.bootstrap,
            crypto=e.crypto,
        )
    assert private not in "".join(traceback.format_exception(refused.value))
    assert CUSTODY_SEED.hex() not in str(refused.value.as_dict())


def test_custody_repr_never_serializes_key(configured):
    e = configured
    custody = load_source(
        json.dumps(e.source_config),
        e.manifest,
        e.secrets,
        credential=e.secrets.bootstrap,
        crypto=e.crypto,
    )
    assert repr(custody) == "SourceCustody(<redacted>)"
    with pytest.raises(TypeError):
        json.dumps(custody)


@pytest.mark.parametrize("bound", ["rows", "generated_row_bytes"])
async def test_final_capacity_refusal_rolls_back_seal_custody_and_acknowledgments(
    configured, bound
):
    e = configured
    scope = e.manifest.configuration.authority
    if bound == "rows":
        count = sum(map(len, portable_rows(e).values()))
        additions = CaptureLimits().rows - 4 - count
    else:
        additions = 1800

    def fill(sql):
        for _ in range(additions):
            identifier = new_id()
            payload = (
                encode(
                    WorkerData(
                        binding=WorkerBinding(
                            workspace_id=scope.workspace_id,
                            controller_id=scope.controller_id,
                            worker_id=identifier,
                            worker_session_id=new_id(),
                            control_epoch=scope.control_epoch,
                        ),
                        principal_id=e.manifest.provisioning_actor_id,
                        name="Synthetic inactive worker",
                        profiles=(),
                        max_concurrency=1,
                    ).model_dump(mode="json")
                )
                if bound == "generated_row_bytes"
                else "{}"
            )
            sql.execute(
                "INSERT INTO records VALUES (?,?,?,1,'inactive',NULL,NULL,?,'t','t')",
                (
                    scope.workspace_id,
                    "worker" if bound != "rows" else "fixture",
                    identifier,
                    payload,
                ),
            )

    e.store.write(fill)
    # Establish that the preexisting source fits: only the generated seal/ack can overflow.
    e.store.read(lambda sql: preflight_rows(sql, CaptureLimits()))
    before = portable_rows(e)
    with pytest.raises(ResearchError) as refused:
        await call(e)
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert portable_rows(e) == before
    assert not [row for row in rows(e, "records") if row["kind"] == "workspace_gate"]
