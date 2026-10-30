from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from uuid import UUID

import pytest
from pydantic import ValidationError

from research_swarm.domain.assignments import AssignmentPayload, SignedAssignment
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import (
    DispatchEnvelope,
    ExperimentSpec,
    MetricDefinition,
    SourceRef,
    StageSpec,
)
from research_swarm.domain.workers import EngineCapabilities, WorkerBinding
from research_swarm.workers.signatures import AssignmentSigner, AssignmentVerifier, signing_bytes

NOW = "2026-01-01T00:00:00.000Z"
EXPIRES = "2026-01-01T00:00:30.000Z"
DEADLINE = "2026-01-01T00:05:00.000Z"
PRIVATE_KEY = bytes(range(32))  # Synthetic fixture, never an enrollment key.


def identifier(value: int) -> str:
    return str(UUID(int=value))


def assignment_payload() -> AssignmentPayload:
    digest = content_digest(b"synthetic")
    spec = ExperimentSpec(
        workspace_id=identifier(1),
        project_id=identifier(2),
        campaign_id=identifier(3),
        candidate_id=identifier(4),
        trial_id=identifier(5),
        source=SourceRef(artifact_id=identifier(6), tree_digest=digest),
        protocol_digest=digest,
        comparison_class="cpu-v1",
        environment_digest=digest,
        evaluator_contract_digest=digest,
        data_manifest_digest=digest,
        metrics=(MetricDefinition(name="loss", unit="mse", direction="minimize"),),
        stages=(
            StageSpec(
                id="train",
                kind="train",
                argv=("python", "train.py"),
                required_artifacts=("data",),
            ),
        ),
        seed=7,
        policy_version="test-v1",
    ).sealed()
    return AssignmentPayload(
        dispatch=DispatchEnvelope(
            id=identifier(7),
            workspace_id=spec.workspace_id,
            controller_id=identifier(8),
            campaign_id=spec.campaign_id,
            trial_id=spec.trial_id,
            job_id=identifier(9),
            attempt_id=identifier(10),
            worker_id=identifier(11),
            worker_session_id=identifier(12),
            spec_digest=spec.spec_digest,
            control_epoch=1,
            fencing_generation=1,
            reservation_ids=(identifier(13),),
            stage_id="train",
            expires_at=EXPIRES,
            hard_deadline_at=DEADLINE,
            launch_permit_digest=digest,
        ),
        spec=spec,
        profile=EngineCapabilities(
            name="python-cpu",
            backend="python",
            execution_fingerprint=digest,
            cpu_slots=1,
        ),
        source_artifact=ArtifactRef(id=spec.source.artifact_id, digest=digest, size_bytes=100),
        source_tree_digest=digest,
        inputs={"data": ArtifactRef(id=identifier(14), digest=digest, size_bytes=10)},
        network_policy="inherit",
        issued_at=NOW,
    )


def signer(payload: AssignmentPayload, key: bytes = PRIVATE_KEY) -> AssignmentSigner:
    binding = WorkerBinding.model_validate(
        {field: getattr(payload.dispatch, field) for field in WorkerBinding.model_fields}
    )
    return AssignmentSigner(binding, key)


def test_canonical_signature_golden_and_public_only_verification_in_another_process() -> None:
    payload = assignment_payload()
    authority = signer(payload)
    signed = authority.sign(payload)
    assert authority.authority.public_key == (
        "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8"
    )
    assert hashlib.sha256(signing_bytes(payload, signed.key_id)).hexdigest() == (
        "d2e6abe5eaa8e9c5ebfba1324f5965c0b7ec14752c60754215b1a01515ff6e57"
    )
    assert signed.signature == (
        "f9837886c6fbde426897f867ee7d53345b47cfbd7762de07879ee77eab3bf11fda"
        "8bc81f24a88f80713541f392795a5b22a3b23521bc40fc3256894538b48d01"
    )
    wire = json.dumps(
        {
            "authority": authority.authority.model_dump(mode="json"),
            "assignment": signed.model_dump(mode="json"),
        }
    )
    assert PRIVATE_KEY.hex() not in wire
    code = (
        "import json,sys; "
        "from research_swarm.domain.assignments import AssignmentAuthority,SignedAssignment; "
        "from research_swarm.workers.signatures import AssignmentVerifier; "
        "wire=json.load(sys.stdin); "
        "v=AssignmentVerifier(AssignmentAuthority.model_validate(wire['authority'])); "
        "assignment=SignedAssignment.model_validate(wire['assignment']); "
        f"result=v.verify(assignment,authority_now={NOW!r}); "
        "print(result.dispatch.envelope.attempt_id)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        input=wire,
        text=True,
        capture_output=True,
        timeout=10,
        check=True,
    )
    assert result.stdout.strip() == payload.dispatch.attempt_id


@pytest.mark.parametrize(
    "field",
    [
        "profile",
        "input",
        "source",
        "deadline",
        "reservation",
        "evaluator",
        "issued",
        "spec",
    ],
)
def test_complete_assignment_is_authenticated(field: str) -> None:
    payload = assignment_payload()
    authority = signer(payload)
    signed = authority.sign(payload)
    wire = signed.model_dump(mode="json")
    changed = wire["payload"]
    if field == "profile":
        changed["profile"]["execution_fingerprint"] = content_digest(b"another environment")
    elif field == "input":
        changed["inputs"]["data"]["digest"] = content_digest(b"different data")
    elif field == "source":
        changed["source_artifact"]["digest"] = content_digest(b"different archive")
    elif field == "deadline":
        changed["dispatch"]["hard_deadline_at"] = "2026-01-02T00:00:00.000Z"
    elif field == "reservation":
        changed["dispatch"]["reservation_ids"] = [identifier(15)]
    elif field == "evaluator":
        changed["evaluation_request_id"] = identifier(16)
    elif field == "issued":
        changed["issued_at"] = "2025-12-31T23:59:59.000Z"
    else:
        changed["spec"]["seed"] = 8
        changed["spec"]["spec_digest"] = None
        resealed = ExperimentSpec.model_validate(changed["spec"]).sealed()
        changed["spec"] = resealed.model_dump(mode="json")
        changed["dispatch"]["spec_digest"] = resealed.spec_digest
    with pytest.raises(ResearchError) as error:
        AssignmentVerifier(authority.authority).verify(
            SignedAssignment.model_validate(wire),
            authority_now=NOW,
        )
    assert error.value.code == ErrorCode.AUTHORIZATION_DENIED


@pytest.mark.parametrize(
    "field,value",
    [
        ("worker_id", identifier(99)),
        ("worker_session_id", identifier(99)),
        ("controller_id", identifier(99)),
        ("control_epoch", 2),
    ],
)
def test_signing_and_verification_are_bound_to_one_enrollment(field: str, value: object) -> None:
    payload = assignment_payload()
    original = signer(payload)
    changed = payload.model_copy(
        update={
            "dispatch": payload.dispatch.model_copy(update={field: value}),
        }
    )
    with pytest.raises(ResearchError, match="another worker authority"):
        original.sign(changed)
    signed_for_another = signer(changed).sign(changed)
    with pytest.raises(ResearchError) as error:
        AssignmentVerifier(original.authority).verify(signed_for_another, authority_now=NOW)
    assert error.value.code == ErrorCode.STALE_FENCE


def test_pinned_key_and_launch_window_cannot_be_replaced() -> None:
    payload = assignment_payload()
    authority = signer(payload)
    verifier = AssignmentVerifier(authority.authority)
    with pytest.raises(ResearchError, match="authentication failed"):
        verifier.verify(
            signer(payload, bytes(reversed(PRIVATE_KEY))).sign(payload), authority_now=NOW
        )
    signed = authority.sign(payload)
    with pytest.raises(ResearchError, match="not yet valid"):
        verifier.verify(signed, authority_now="2025-12-31T23:59:59.999Z")
    for expired in (EXPIRES, DEADLINE):
        with pytest.raises(ResearchError, match="expired"):
            verifier.verify(signed, authority_now=expired)
        observed = verifier.verify(signed, authority_now=expired, allow_expired_observation=True)
        assert observed.dispatch.envelope == payload.dispatch
    with pytest.raises(ValidationError):
        verifier.verify(signed, authority_now="not a timestamp")


def test_inconsistent_source_inputs_and_offline_authority_are_not_signable() -> None:
    payload = assignment_payload()
    authority = signer(payload)
    for update in (
        {"source_tree_digest": content_digest(b"other tree")},
        {"inputs": {}},
        {"offline_policy": "continue_forever"},
        {"issued_at": DEADLINE},
    ):
        with pytest.raises(ValidationError):
            authority.sign(payload.model_copy(update=update))


def test_algorithm_and_network_policy_cannot_change_after_signature() -> None:
    payload = assignment_payload()
    payload = payload.model_copy(
        update={
            "profile": payload.profile.model_copy(
                update={
                    "network_policies": frozenset({"inherit", "denied"}),
                }
            ),
        }
    )
    authority = signer(payload)
    signed = authority.sign(payload)
    verifier = AssignmentVerifier(authority.authority)
    changed = signed.model_copy(
        update={
            "payload": payload.model_copy(update={"network_policy": "denied"}),
        }
    )
    with pytest.raises(ResearchError, match="authentication failed"):
        verifier.verify(changed, authority_now=NOW)
    with pytest.raises(ResearchError, match="authentication failed"):
        verifier.verify(signed.model_copy(update={"algorithm": "none"}), authority_now=NOW)
