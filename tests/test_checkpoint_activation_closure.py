"""Original execution evidence must close before the fresh hosted runtime activates."""

import json

import pytest

from adapters.cloudflare.checkpoint_artifacts import restore_target_artifact
from adapters.cloudflare.checkpoint_closure import reconcile_target_attempt
from adapters.cloudflare.recovery_router import ExistingRecoveryRouter
from research_swarm.control.worker_artifacts import WorkerAttemptTarget
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ResearchError
from tests import test_checkpoint_activation as fixtures
from tests.test_checkpoint_activation import activate, opened
from tests.test_checkpoint_import import snapshot
from tests.test_checkpoint_runtime import recovery as recovery
from tests.test_hosted_recovery import closure_request, upload
from tests.test_hosted_recovery import source as source
from tests.test_hosted_router import Gate
from tests.test_manifest import project as project
from tests.test_production_delivery import production as production

deployment = fixtures.deployment
bootstrap = fixtures.bootstrap
sealed = fixtures.sealed
configured = fixtures.configured
imported = fixtures.imported
elected = fixtures.elected
target = fixtures.target
prepared = fixtures.prepared
ready = fixtures.ready


async def test_original_attempt_cannot_be_skipped_and_closed_evidence_survives_activation(ready):
    e = ready
    before, calls = snapshot(e.target), len(e.witness.requests)
    with pytest.raises(ResearchError):
        await activate(e)
    assert snapshot(e.target) == before and len(e.witness.requests) == calls
    for blob in e.image.manifest.artifacts:
        body = await e.source.router.objects.read(
            e.manifest.configuration.authority.workspace_id, blob
        )
        await restore_target_artifact(
            Gate(),
            lambda: e.target,
            e.manifest,
            e.target_secrets,
            identity=e.target.identity,
            credential=e.target_secrets.bootstrap,
            target_json=json.dumps(e.target_config),
            crypto=e.source.crypto,
            objects=e.objects,
            request_json=json.dumps(
                {"artifact_id": blob.reference.id, "generation": blob.generation}
            ),
            content_hex=body.hex(),
        )
    e.router = await ExistingRecoveryRouter.open(
        Gate(),
        lambda: e.target,
        e.manifest,
        e.target_secrets,
        target_json=json.dumps(e.target_config),
        crypto=e.source.crypto,
        objects=e.objects,
    )
    e.token = e.source.original.token
    e.target_request = WorkerAttemptTarget(
        worker_id=e.source.worker.binding.worker_id,
        session_id=e.source.worker.binding.worker_session_id,
        attempt_id=e.source.entry.dispatch.attempt_id,
    )
    _, submitted = await upload(e)
    closure = await reconcile_target_attempt(
        Gate(),
        lambda: e.target,
        e.manifest,
        e.target_secrets,
        identity=e.target.identity,
        credential=e.target_secrets.bootstrap,
        target_json=json.dumps(e.target_config),
        crypto=e.source.crypto,
        objects=e.objects,
        request_json=json.dumps(closure_request(e, submitted["result"])),
    )
    assert strict_json_loads(closure)["disposition"] == "accepted"
    before = snapshot(e.target)
    activated = await activate(e)
    assert strict_json_loads(activated)["disposition"] == "accepted"
    after = snapshot(e.target)
    for table in ("budgets", "budget_reservations", "budget_settlements", "budget_entries"):
        assert after[table] == before[table]
    current = await opened(e)
    with pytest.raises(ResearchError):
        await current.command(e.token, "worker.claim_job", {}, idempotency_key="old-worker-claim")
    assert not [row for row in after["records"] if row["kind"] == "run_result"]
