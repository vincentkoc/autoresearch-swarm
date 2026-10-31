"""A hosted proof can bind an actual original installation without contacting its target."""

from types import SimpleNamespace

import pytest

from research_swarm.client import ResearchClient
from research_swarm.crypto import sign
from research_swarm.domain.backup import SignedRestoreRequest
from research_swarm.domain.canonical import canonical_bytes, content_digest, strict_json_loads
from research_swarm.domain.checkpoint_elections import (
    LogicalElection,
    LogicalElectionRequest,
    SignedLogicalElection,
    SignedLogicalElectionRequest,
)
from research_swarm.domain.checkpoint_images import LogicalImportIntent
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.recovery_routes import SignedWorkerRecoveryRoute
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.runtime import read_private, write_private
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.recovery import (
    original_recovery_lock,
    original_stores,
    recover_worker_attempt,
)
from research_swarm.workers.recovery_routes import install_recovery_route, load_recovery_route
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_recovery_routes import assert_original_unchanged
from tests.test_recovery_routes import routed as routed
from tests.test_worker_recovery import recoverable as recoverable


@pytest.fixture
def delegated(routed, pipeline):
    r = routed
    provider = r.target.service.crypto_provider
    target = strict_json_loads(read_private(r.target.root, "restore-custody.json", limit=16_384))
    source = strict_json_loads(
        read_private(pipeline.runtime.root, "backup-custody.json", limit=16_384)
    )

    def signed(key, digest):
        return sign(provider, bytes.fromhex(key), digest.encode()).hex()

    request = r.signed.route.restoration.claim_request()
    payload = LogicalElectionRequest(
        intent=LogicalImportIntent(
            restore_id=request.restore_id,
            manifest_digest=request.manifest_digest,
            target_location="synthetic-namespace/target-object",
            target_deployment_digest=content_digest(b"fixture target deployment"),
            target_public_key=request.target_public_key,
            operator_id=r.signed.route.admission.operator_id,
        ),
        source_authority=WitnessAuthorityScope(
            state_authority_id=content_digest(b"fixture source store"),
            activation_id=new_id(),
            assignment_key_id=content_digest(b"fixture assignment key"),
            workspace_id=request.workspace_id,
            controller_id=request.controller_id,
            control_epoch=request.source_control_epoch,
        ),
        source_deployment_digest=content_digest(b"fixture source deployment"),
        signed_request=SignedRestoreRequest(
            request=request, signature=signed(target["private_key"], request.digest)
        ),
    )
    election = LogicalElection(
        signed_request=SignedLogicalElectionRequest(
            request=payload, signature=signed(target["private_key"], payload.digest)
        ),
        signed_claim=r.signed.route.admission.signed_claim,
    )
    envelope = SignedLogicalElection(
        election=election, signature=signed(source["private_key"], election.digest)
    )
    admission = r.signed.route.admission.model_copy(
        update={"logical_election_digest": envelope.digest}
    )
    admission = admission.model_copy(
        update={"target_signature": signed(target["private_key"], admission.proof_digest)}
    )

    def route_signature(route):
        return SignedWorkerRecoveryRoute(
            route=route,
            target_signature=signed(target["private_key"], route.digest),
            logical_election=envelope,
        )

    route = route_signature(r.signed.route.model_copy(update={"admission": admission}))
    return SimpleNamespace(r=r, route=route, sign=route_signature)


def test_logical_route_installs_offline_reopens_and_refuses_stripped_proof_before_network(
    delegated, monkeypatch
):
    d, r = delegated, delegated.r

    def forbidden(*args, **kwargs):
        pytest.fail("Offline route validation reached network or execution")

    monkeypatch.setattr(ResearchClient, "connect", forbidden)
    monkeypatch.setattr(LocalWorker, "__init__", forbidden)
    installed = install_recovery_route(r.native.root, d.route, r.pin)
    assert install_recovery_route(r.native.root, d.route, r.pin) == installed
    with original_recovery_lock(r.native.root, r.native.attempt_id) as root:
        bundle, journal, resources, _ = original_stores(root)
        assert (
            load_recovery_route(
                root, d.route.route.route_id, r.native.attempt_id, bundle, journal, resources
            )
            == installed
        )
    assert_original_unchanged(r)
    path = r.native.root / "recovery-routes" / (d.route.route.route_id + ".json")
    wire = installed.model_dump(mode="json")
    del wire["signed"]["logical_election"]
    write_private(path, canonical_bytes(wire))
    with pytest.raises(ResearchError):
        recover_worker_attempt(r.native.root, r.native.attempt_id, route_id=d.route.route.route_id)
    assert_original_unchanged(r)


def test_target_signature_cannot_replace_original_installation_or_rebind_route_id(
    delegated, monkeypatch
):
    d, r = delegated, delegated.r

    def forbidden(*args, **kwargs):
        pytest.fail("Offline route installation contacted its target")

    monkeypatch.setattr(ResearchClient, "connect", forbidden)
    for field, value in (
        ("credential_id", new_id()),
        ("worker_data_digest", content_digest(b"other worker")),
        ("binding", d.route.route.binding.model_copy(update={"worker_session_id": new_id()})),
    ):
        changed = d.sign(d.route.route.model_copy(update={field: value}))
        with pytest.raises(ResearchError, match="original installation"):
            install_recovery_route(r.native.root, changed, r.pin)
    assert not (r.native.root / "recovery-routes").exists()
    install_recovery_route(r.native.root, d.route, r.pin)
    changed = d.sign(
        d.route.route.model_copy(update={"destination_url": "https://another.example"})
    )
    with pytest.raises(ResearchError) as conflict:
        install_recovery_route(r.native.root, changed, r.pin)
    assert conflict.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert_original_unchanged(r)
