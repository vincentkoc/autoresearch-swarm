"""An activated target uses ordinary research APIs and can become a checkpoint source."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from adapters.cloudflare.checkpoint_source import capture_source, seal_source
from adapters.cloudflare.checkpoint_wire import decode_checkpoint
from research_swarm.crypto import public_key
from research_swarm.domain.backup import BackupCustodyPin
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.checkpoint_images import SignedLogicalCheckpoint
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.types import new_id
from tests import test_checkpoint_activation as fixtures
from tests.test_checkpoint_activation import activate, opened
from tests.test_checkpoint_import import snapshot
from tests.test_checkpoint_runtime import records
from tests.test_checkpoint_runtime import recovery as recovery
from tests.test_hosted_research import admit, enroll_and_claim, sdk
from tests.test_hosted_router import Gate
from tests.test_manifest import project as project

deployment = fixtures.deployment
bootstrap = fixtures.bootstrap
source = fixtures.source
sealed = fixtures.sealed
configured = fixtures.configured
imported = fixtures.imported
elected = fixtures.elected
target = fixtures.target
prepared = fixtures.prepared
ready = fixtures.ready


async def test_activated_target_admits_research_and_new_epoch_worker_through_sdk(ready, project):
    e = ready
    raw = await activate(e)
    e.router = await opened(e)
    principal = e.manifest.credentials[0].principal
    e.operator = SimpleNamespace(token=e.target_secrets.credentials[e.manifest.credentials[0].id])
    loop = asyncio.get_running_loop()

    def work():
        with sdk(e, loop) as client:
            admitted = admit(client, project)
            claimed, _ = enroll_and_claim(client, admitted, principal.id)
            return claimed

    claimed = await asyncio.to_thread(work)
    assignment = claimed["result"]["data"]["signed_assignment"]["payload"]
    assert assignment["dispatch"]["control_epoch"] == 2
    assert (
        claimed["result"]["data"]["signed_assignment"]["key_id"]
        == e.runtime.plan.authority.assignment_key_id
    )
    before, calls = snapshot(e.target), len(e.witness.requests)
    # Completed activation replay is history, even when new research is unresolved.
    e.witness.failure = TimeoutError("Synthetic witness offline")
    assert await activate(e) == raw
    assert snapshot(e.target) == before and len(e.witness.requests) == calls


async def test_completed_target_reseals_and_exports_with_both_custody_bindings(ready):
    e = ready
    activated = await activate(e)
    authority, seed = e.manifest.configuration.authority, b"S" * 32
    pin = BackupCustodyPin(
        custody_id=new_id(),
        workspace_id=authority.workspace_id,
        controller_id=authority.controller_id,
        control_epoch=authority.control_epoch,
        public_key=public_key(e.source.crypto, seed).hex(),
    )
    source_config = {
        "deployment_digest": e.manifest.digest,
        "authority": authority.model_dump(mode="json"),
        "pin": pin.model_dump(mode="json"),
        "custody_seed_hex": seed.hex(),
    }
    request = {
        "request_id": new_id(),
        "expected_revision": records(e, "workspace_gate")[0]["revision"],
        "seal": {
            "backup_id": new_id(),
            "expected_controller_id": authority.controller_id,
            "expected_control_epoch": 2,
            "reason": "Synthetic successor export",
        },
    }
    arguments = {
        "identity": e.target.identity,
        "credential": e.target_secrets.bootstrap,
        "source_json": json.dumps(source_config),
        "request_json": json.dumps(request),
        "crypto": e.source.crypto,
        "recovery_binding": e.binding,
    }
    e.witness.failure = TimeoutError("Synthetic witness offline")
    sealed = await seal_source(Gate(), lambda: e.target, e.manifest, e.target_secrets, **arguments)
    assert sealed["seal"]["result"]["data"]["control_epoch"] == 2
    arguments["request_json"] = json.dumps({"backup_id": request["seal"]["backup_id"]})
    encoded = await capture_source(
        Gate(), lambda: e.target, e.manifest, e.target_secrets, **arguments
    )
    envelope, _ = decode_checkpoint(encoded)
    image = SignedLogicalCheckpoint.model_validate_json(envelope).manifest
    assert image.source_authority == authority and image.source_custody == pin
    assert image.seal.backup_id == request["seal"]["backup_id"]
    assert await activate(e) == activated
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await capture_source(
            Gate(),
            lambda: e.target,
            e.manifest,
            e.target_secrets,
            **dict(arguments, recovery_binding=None),
        )
    assert snapshot(e.target) == before
    assert strict_json_loads(activated)["disposition"] == "accepted"
