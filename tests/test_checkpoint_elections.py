"""Hosted destination signatures retain native preimages and require external pins."""

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from research_swarm.checkpoints.elections import verify_election, verify_election_request
from research_swarm.control.recovery_proofs import verify_claim, verify_request
from research_swarm.crypto import CryptoProviderError, public_key, sign
from research_swarm.domain.backup import (
    RestoreClaim,
    RestoreClaimRequest,
    SignedRestoreClaim,
    SignedRestoreRequest,
)
from research_swarm.domain.canonical import MAX_SAFE_INTEGER, canonical_bytes, content_digest
from research_swarm.domain.checkpoint_elections import (
    LogicalElection,
    LogicalElectionRequest,
    SignedLogicalElection,
    SignedLogicalElectionRequest,
)
from research_swarm.domain.checkpoint_images import LogicalImportIntent
from research_swarm.domain.errors import ErrorCode, ResearchError
from tests.test_checkpoint_images import image
from tests.test_time_witnesses import SEED, identifier

TARGET_SEED = b"t" * 32


def signature(crypto, seed, payload):
    return sign(crypto, seed, payload.digest.encode("ascii")).hex()


@pytest.fixture
def election():
    manifest, _, crypto = image()
    intent = LogicalImportIntent(
        restore_id=identifier(90),
        manifest_digest=manifest.digest,
        target_location="synthetic-namespace/target-object",
        target_deployment_digest=content_digest(b"target deployment"),
        target_public_key=public_key(crypto, TARGET_SEED).hex(),
        operator_id=manifest.operator_id,
    )
    source = manifest.source_authority
    native_request = RestoreClaimRequest(
        backup_id=manifest.seal.backup_id,
        manifest_digest=manifest.digest,
        workspace_id=source.workspace_id,
        controller_id=source.controller_id,
        source_custody_id=manifest.source_custody.custody_id,
        source_control_epoch=source.control_epoch,
        restore_id=intent.restore_id,
        target_public_key=intent.target_public_key,
        target_control_epoch=source.control_epoch + 1,
    )
    request = LogicalElectionRequest(
        intent=intent,
        source_authority=source,
        source_deployment_digest=manifest.source_deployment_digest,
        signed_request=SignedRestoreRequest(
            request=native_request, signature=signature(crypto, TARGET_SEED, native_request)
        ),
    )
    signed_request = SignedLogicalElectionRequest(
        request=request, signature=signature(crypto, TARGET_SEED, request)
    )
    claim = RestoreClaim(
        claim_id=identifier(91),
        request=native_request,
        source_public_key=manifest.source_custody.public_key,
        claimed_at="2026-01-01T00:00:00.000Z",
    )
    elected = LogicalElection(
        signed_request=signed_request,
        signed_claim=SignedRestoreClaim(claim=claim, signature=signature(crypto, SEED, claim)),
    )
    return SimpleNamespace(
        manifest=manifest,
        intent=intent,
        request=signed_request,
        election=SignedLogicalElection(
            election=elected, signature=signature(crypto, SEED, elected)
        ),
        crypto=crypto,
        pins={
            "expected_intent": intent,
            "expected_backup_id": manifest.seal.backup_id,
            "expected_source": source,
            "expected_deployment_digest": manifest.source_deployment_digest,
            "expected_custody": manifest.source_custody,
            "crypto": crypto,
        },
    )


def test_roundtrip_retains_native_preimages_and_requires_hosted_envelopes(election):
    e = election
    request = verify_election_request(e.request, **e.pins)
    verified = verify_election(e.election, **e.pins)
    assert request == e.request.request and verified == e.election.election
    assert verify_request(request.signed_request) == verified.signed_claim.claim.request
    assert (
        verify_claim(verified.signed_claim, source_public_key=e.manifest.source_custody.public_key)
        == verified.signed_claim.claim
    )
    assert (
        SignedLogicalElection.model_validate_json(
            canonical_bytes(e.election.model_dump(mode="json"))
        )
        == e.election
    )
    with pytest.raises(ValidationError):
        verify_election(verified.signed_claim.model_dump(mode="json"), **e.pins)
    with pytest.raises(ValidationError):
        verify_election_request(request.signed_request.model_dump(mode="json"), **e.pins)


def test_native_and_hosted_digest_domains_have_stable_distinct_preimages(election):
    request, elected = election.request.request, election.election.election
    assert request.signed_request.request.digest == (
        "sha256:1237da66bac7503cf5319fcc5cee47b2538a0bb494bd9532f0a229dcf88f845a"
    )
    assert request.digest == (
        "sha256:00c973dc2116f954b015c6dc714a0ff944e4cc7da06c530d6d8ef2119b865a50"
    )
    assert elected.signed_claim.claim.digest == (
        "sha256:bdc4dd699d7197b1e8a557a5064cb7646bdb68d86405860709e4655d9bb73f5d"
    )
    assert elected.digest == (
        "sha256:3bf12ab58b9b390fc054618a321e9b1a6a0f0b6fbafec79a090983c0d78903be"
    )
    assert (
        request.signed_request.signature,
        election.request.signature,
        elected.signed_claim.signature,
        election.election.signature,
    ) == (
        "1a167c1a93e478510f3997c80901fae683cdcc66de530532804ea9ff6ac1edd613"
        "d8c19c96fa5bfafe69c744a4ca76dd0e5a13f1fba72f091e707d6110addd05",
        "c9804abb9a9b36f6ad48269b2a18a15cb5ce3a515a506c87469f2644f18514d234"
        "51638365f986ea4dda18123a83419710cc28e1989c629c09bda505b89cc308",
        "02e614ddb9a5f415bb84f30bce52c2927942a33090893fb73e5f44714f269a1a5d"
        "75d06094b4d09dc570ac077dcd43d7a9c61b65e5aeaffb5f2a980f7f5ce309",
        "eda618e4ed621799031732a4267874c44b6b40bda3b765d4eb9d6cc5ed79ff51e"
        "599918b42f50dd89f670970cba2b50e2913883d8f034e1360869b19f4fd2808",
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("restore_id", identifier(99)),
        ("manifest_digest", content_digest(b"other image")),
        ("target_location", "synthetic-namespace/other-object"),
        ("target_deployment_digest", content_digest(b"other deployment")),
        ("target_public_key", "a" * 64),
        ("operator_id", identifier(99)),
    ],
)
def test_valid_proof_cannot_authorize_another_target_intent(election, field, value):
    pins = {**election.pins, "expected_intent": election.intent.model_copy(update={field: value})}
    with pytest.raises(ResearchError) as refused:
        verify_election(election.election, **pins)
    assert refused.value.code == ErrorCode.STALE_FENCE


@pytest.mark.parametrize(
    "field,value",
    [
        ("state_authority_id", content_digest(b"other authority")),
        ("activation_id", identifier(99)),
        ("workspace_id", identifier(99)),
        ("controller_id", identifier(99)),
        ("control_epoch", 2),
        ("assignment_key_id", content_digest(b"other assignment key")),
    ],
)
def test_full_source_scope_is_externally_pinned(election, field, value):
    pins = {
        **election.pins,
        "expected_source": election.manifest.source_authority.model_copy(update={field: value}),
    }
    with pytest.raises(ResearchError) as refused:
        verify_election(election.election, **pins)
    assert refused.value.code == ErrorCode.STALE_FENCE


@pytest.mark.parametrize("pin", ["deployment", "custody", "source_key"])
def test_source_deployment_and_custody_are_not_taken_from_the_envelope(election, pin):
    pins = dict(election.pins)
    if pin == "deployment":
        pins["expected_deployment_digest"] = content_digest(b"other deployment")
    else:
        pins["expected_custody"] = election.manifest.source_custody.model_copy(
            update={"custody_id": identifier(99)} if pin == "custody" else {"public_key": "a" * 64}
        )
    with pytest.raises(ResearchError) as refused:
        verify_election(election.election, **pins)
    assert refused.value.code == ErrorCode.STALE_FENCE


@pytest.mark.parametrize(
    "layer", ["native_target", "hosted_target", "native_source", "hosted_source"]
)
def test_each_signature_is_checked_even_if_outer_envelopes_are_resigned(election, layer):
    e = election
    raw = e.election.election.model_dump(mode="json")
    target = raw["signed_request"]
    if layer == "native_target":
        target["request"]["signed_request"]["signature"] = "0" * 128
        target["signature"] = signature(
            e.crypto, TARGET_SEED, LogicalElectionRequest.model_validate(target["request"])
        )
    elif layer == "hosted_target":
        target["signature"] = "0" * 128
    elif layer == "native_source":
        raw["signed_claim"]["signature"] = "0" * 128
    changed = LogicalElection.model_validate(raw)
    signed = SignedLogicalElection(
        election=changed,
        signature="0" * 128 if layer == "hosted_source" else signature(e.crypto, SEED, changed),
    )
    with pytest.raises(ResearchError) as refused:
        verify_election(signed, **e.pins)
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED


@pytest.mark.parametrize(
    "field,value",
    [
        ("restore_id", identifier(99)),
        ("manifest_digest", content_digest(b"other")),
        ("target_public_key", "a" * 64),
        ("workspace_id", identifier(99)),
        ("controller_id", identifier(99)),
        ("source_control_epoch", 2),
        ("target_control_epoch", 3),
    ],
)
def test_cross_layer_disagreement_refuses_even_with_revalidated_model_instances(
    election, field, value
):
    request = election.request.request
    invalid = request.model_copy(
        update={
            "signed_request": request.signed_request.model_copy(
                update={"request": request.signed_request.request.model_copy(update={field: value})}
            )
        }
    )
    with pytest.raises(ValidationError):
        verify_election_request(
            election.request.model_copy(update={"request": invalid}), **election.pins
        )


def test_mismatched_native_claim_cannot_be_attached_to_hosted_request(election):
    raw = election.election.election.model_dump(mode="json")
    raw["signed_claim"]["claim"]["request"]["restore_id"] = identifier(99)
    with pytest.raises(ValidationError):
        LogicalElection.model_validate(raw)


def test_source_epoch_exhaustion_and_shared_custody_key_refuse(election):
    raw = election.request.request.model_dump(mode="json")
    raw["source_authority"]["control_epoch"] = MAX_SAFE_INTEGER
    raw["signed_request"]["request"].update(
        source_control_epoch=MAX_SAFE_INTEGER, target_control_epoch=MAX_SAFE_INTEGER + 1
    )
    with pytest.raises(ValidationError):
        LogicalElectionRequest.model_validate(raw)
    raw_election = election.election.election.model_dump(mode="json")
    raw_election["signed_claim"]["claim"]["source_public_key"] = election.intent.target_public_key
    with pytest.raises(ValidationError):
        LogicalElection.model_validate(raw_election)


def test_target_cannot_name_another_backup_under_the_same_image_digest(election):
    e = election
    request = e.request.request
    native = request.signed_request.request.model_copy(update={"backup_id": identifier(99)})
    changed = request.model_copy(
        update={
            "signed_request": SignedRestoreRequest(
                request=native, signature=signature(e.crypto, TARGET_SEED, native)
            )
        }
    )
    signed = SignedLogicalElectionRequest(
        request=changed, signature=signature(e.crypto, TARGET_SEED, changed)
    )
    with pytest.raises(ResearchError) as refused:
        verify_election_request(signed, **e.pins)
    assert refused.value.code == ErrorCode.STALE_FENCE


def test_crypto_provider_failure_remains_an_infrastructure_failure(election):
    class Broken:
        def verify(self, *args):
            raise RuntimeError("Synthetic provider unavailable")

    with pytest.raises(CryptoProviderError):
        verify_election(election.election, **{**election.pins, "crypto": Broken()})
