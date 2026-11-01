"""Offline worker routing trusts source custody, never a target's self-description."""

from types import SimpleNamespace

import pytest

from research_swarm.control.recovery_routes import (
    verify_logical_recovery_route,
    verify_recovery_route,
)
from research_swarm.crypto import sign
from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.recovery_routes import SignedWorkerRecoveryRoute, WorkerRecoveryRoute
from research_swarm.domain.workers import WorkerBinding
from tests.adapters.cloudflare.recovery.test_checkpoint_elections import TARGET_SEED
from tests.adapters.cloudflare.recovery.test_checkpoint_elections import election as election
from tests.adapters.cloudflare.recovery.test_recovery_elections import proof as proof
from tests.workers.test_time_witnesses import identifier


def sign_admission(e, admission):
    return admission.model_copy(
        update={
            "target_signature": sign(e.crypto, TARGET_SEED, admission.proof_digest.encode()).hex()
        }
    )


def sign_route(e, route, election=None):
    return SignedWorkerRecoveryRoute(
        route=route,
        logical_election=election,
        target_signature=sign(e.crypto, TARGET_SEED, route.digest.encode()).hex(),
    )


@pytest.fixture
def routes(proof):
    p, e = proof, proof.e
    receipt = p.receipt.model_copy(
        update={"unresolved_attempt_ids": (identifier(100),), "worker_ids": (identifier(101),)}
    )
    admission = sign_admission(
        e, p.native.model_copy(update={"restoration_digest": receipt.digest})
    )
    route = WorkerRecoveryRoute(
        route_id=identifier(102),
        attempt_id=identifier(100),
        credential_id=identifier(103),
        source_controller_url="https://source.example",
        destination_url="https://target.example",
        binding=WorkerBinding(
            workspace_id=receipt.workspace_id,
            controller_id=receipt.controller_id,
            worker_id=identifier(101),
            worker_session_id=identifier(104),
            control_epoch=receipt.source_control_epoch,
        ),
        worker_data_digest=content_digest(b"fixture worker"),
        principal_id=identifier(105),
        restoration=receipt,
        admission=admission,
    )
    native = sign_route(e, route)
    logical = sign_route(
        e,
        route.model_copy(
            update={
                "admission": sign_admission(
                    e, admission.model_copy(update={"logical_election_digest": e.election.digest})
                )
            }
        ),
        e.election,
    )
    return SimpleNamespace(e=e, pin=e.manifest.source_custody, native=native, logical=logical)


def test_native_wire_stays_unchanged_and_logical_proof_roundtrips(routes):
    r = routes
    wire = r.native.model_dump(mode="json")
    assert "logical_election" not in wire
    assert canonical_bytes(wire) == canonical_bytes(
        r.native.model_copy(update={"logical_election": None}).model_dump(mode="json")
    )
    verify_recovery_route(r.native, r.pin)
    assert r.native.route.digest == (
        "sha256:96dd10ecfbff00a96ca939d513c68e0fbdda50f63787078933bb5094de45051a"
    )
    assert r.native.digest == (
        "sha256:08b8b876f388521fc15c088943b6579215c6b673c3cb3c4d941ed639d9c993cb"
    )
    assert r.native.target_signature == (
        "34ae8eb3b9ec843c49247965fcc3736286ec594fb8be299571509b91487fe11fa26"
        "927301ca42ae88eddf8bef942856586855f6b830ace86e536b2fa80bdd10a"
    )
    restored = SignedWorkerRecoveryRoute.model_validate_json(
        canonical_bytes(r.logical.model_dump(mode="json"))
    )
    assert restored == r.logical
    verify_logical_recovery_route(restored, r.pin)
    assert r.native.digest != r.logical.digest
    assert r.logical.digest == (
        "sha256:7c273544427228be69ff38f6dc830059703a01a1dd990be8a8f640e46725a921"
    )


@pytest.mark.parametrize(
    "damage", ["missing", "native", "digest", "claim", "wrong_pin", "operator"]
)
def test_logical_route_requires_complete_proof_and_independent_source_pin(routes, damage):
    r, changed, pin = routes, routes.logical, routes.pin
    if damage == "missing":
        changed = changed.model_copy(update={"logical_election": None})
    elif damage == "native":
        changed = r.native.model_copy(update={"logical_election": r.e.election})
    elif damage == "wrong_pin":
        pin = pin.model_copy(update={"public_key": "0" * 64})
    else:
        admission = changed.route.admission
        admission = admission.model_copy(
            update={"logical_election_digest": "sha256:" + "a" * 64}
            if damage == "digest"
            else {"operator_id": identifier(200)}
            if damage == "operator"
            else {
                "signed_claim": admission.signed_claim.model_copy(update={"signature": "0" * 128})
            }
        )
        changed = sign_route(
            r.e,
            changed.route.model_copy(update={"admission": sign_admission(r.e, admission)}),
            r.e.election,
        )
    with pytest.raises(ResearchError):
        verify_logical_recovery_route(changed, pin)
    with pytest.raises(ResearchError):
        verify_recovery_route(changed, pin)


@pytest.mark.parametrize("signature", ["source_envelope", "target_envelope", "target_request"])
def test_target_cannot_reauthorize_an_invalid_source_delegation(routes, signature):
    r, election = routes, routes.e.election
    signed_request = election.election.signed_request
    if signature == "source_envelope":
        election = election.model_copy(update={"signature": "0" * 128})
    else:
        if signature == "target_envelope":
            signed_request = signed_request.model_copy(update={"signature": "0" * 128})
        else:
            request = signed_request.request
            signed_request = signed_request.model_copy(
                update={
                    "request": request.model_copy(
                        update={
                            "signed_request": request.signed_request.model_copy(
                                update={"signature": "0" * 128}
                            )
                        }
                    )
                }
            )
        election = election.model_copy(
            update={
                "election": election.election.model_copy(update={"signed_request": signed_request})
            }
        )
    # The target knows its own key: valid outer signatures cannot replace source authorization.
    admission = sign_admission(
        r.e,
        r.logical.route.admission.model_copy(update={"logical_election_digest": election.digest}),
    )
    changed = sign_route(r.e, r.logical.route.model_copy(update={"admission": admission}), election)
    with pytest.raises(ResearchError):
        verify_logical_recovery_route(changed, r.pin)


@pytest.mark.parametrize(
    "field", ["destination_url", "source_controller_url", "credential_id", "attempt_id", "binding"]
)
def test_logical_route_signature_binds_destination_and_original_worker(routes, field):
    r = routes
    value = "https://changed.example" if "url" in field else identifier(200)
    if field == "binding":
        value = r.logical.route.binding.model_copy(update={"worker_session_id": identifier(200)})
    changed = r.logical.model_copy(
        update={"route": r.logical.route.model_copy(update={field: value})}
    )
    with pytest.raises(ResearchError):
        verify_logical_recovery_route(changed, r.pin)
