from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import UUID

import pytest

from research_swarm.control.time_witnesses import (
    MAX_WITNESS_BYTES,
    PendingTimeWitness,
    WitnessSigner,
    WitnessSourceChanged,
    decode_time_witness,
    witness_epoch,
    witness_signing_bytes,
)
from research_swarm.crypto import public_key, resolve_crypto, sign
from research_swarm.domain.canonical import canonical_bytes, content_digest, record_digest
from research_swarm.domain.clocks import ClockPolicy
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import (
    SignedTimeWitness,
    TimeWitnessPayload,
    TimeWitnessRequest,
    WitnessScope,
    WitnessSource,
    witness_key_id,
)
from tests.domain.test_cross_language import node
from tests.paths import TEST_ROOT

# Public RFC 8032 vector, never an operational credential.
SEED = bytes.fromhex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
FIXTURE = TEST_ROOT / "conformance" / "time-witness-v1.json"


def identifier(number: int) -> str:
    return str(UUID(int=number))


def source() -> WitnessSource:
    public = public_key(resolve_crypto(None), SEED).hex()
    return WitnessSource(
        id=identifier(1),
        public_key=public,
        key_id=witness_key_id(public),
        native_profile="linux-boottime-v1",
        policy=ClockPolicy(),
    )


def scope() -> WitnessScope:
    return WitnessScope(
        state_authority_id=record_digest("fixture.authority", {"object": "test-state"}),
        activation_id=identifier(2),
        provider_generation=identifier(3),
        workspace_id=identifier(4),
        controller_id=identifier(5),
        control_epoch=1,
        assignment_key_id="sha256:" + "a" * 64,
    )


def pending() -> PendingTimeWitness:
    return PendingTimeWitness(
        source(), scope(), identifier(6), record_digest("fixture.operation", {"command": "test"})
    )


def payload(request: TimeWitnessRequest) -> TimeWitnessPayload:
    configured = source()
    return TimeWitnessPayload(
        request=request,
        source_id=configured.id,
        source_epoch=identifier(7),
        native_profile=configured.native_profile,
        policy=configured.policy,
        authority_time_ms=1_793_433_333_000,
    )


def encoded(value: SignedTimeWitness) -> bytes:
    return canonical_bytes(value.model_dump(mode="json"))


def signed(value: TimeWitnessPayload) -> SignedTimeWitness:
    return WitnessSigner(source(), SEED).sign(value)


def authenticate_arbitrary(value: TimeWitnessPayload) -> SignedTimeWitness:
    """A cryptographically authentic but possibly misconfigured source response."""
    key = source().key_id
    return SignedTimeWitness(
        key_id=key,
        payload=value,
        signature=sign(resolve_crypto(None), SEED, witness_signing_bytes(key, value)).hex(),
    )


def test_one_completion_wins_across_independent_threads() -> None:
    waiting = pending()
    response = encoded(signed(payload(waiting.request)))

    def complete(_: int) -> str:
        try:
            result = waiting.complete(response)
            return result.epoch
        except ResearchError as error:
            assert error.code == ErrorCode.INVALID_TRANSITION
            return "consumed"

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(complete, range(8)))
    assert outcomes.count("consumed") == 7
    assert outcomes.count(witness_epoch(scope(), source(), identifier(7))) == 1


@pytest.mark.parametrize("field", ["nonce", "operation_id", "request_digest", "scope"])
def test_valid_signature_cannot_substitute_an_operation(field: str) -> None:
    waiting = pending()
    request = waiting.request.model_dump(mode="json")
    if field == "scope":
        request[field]["workspace_id"] = identifier(99)
    elif field == "request_digest":
        request[field] = "sha256:" + "b" * 64
    else:
        request[field] = identifier(99)
    response = signed(payload(TimeWitnessRequest.model_validate(request)))
    with pytest.raises(ResearchError) as error:
        waiting.complete(encoded(response))
    assert error.value.code == ErrorCode.STALE_FENCE
    with pytest.raises(ResearchError, match="consumed"):
        waiting.complete(encoded(signed(payload(waiting.request))))


@pytest.mark.parametrize("field", ["source_id", "native_profile", "policy"])
def test_authenticated_response_cannot_change_pinned_source_configuration(field: str) -> None:
    waiting = pending()
    changed = payload(waiting.request).model_dump(mode="json")
    if field == "policy":
        changed[field]["relative_drift_ppm"] += 1
    else:
        changed[field] = identifier(99) if field == "source_id" else "unqualified-clock"
    value = TimeWitnessPayload.model_validate(changed)
    with pytest.raises(ResearchError, match="source configuration differs"):
        signed(value)
    with pytest.raises(ResearchError) as error:
        waiting.complete(encoded(authenticate_arbitrary(value)))
    assert error.value.code == ErrorCode.STALE_FENCE
    assert isinstance(error.value, WitnessSourceChanged)
    assert error.value.evidence.witness.payload == value
    assert error.value.evidence.expected_source == source()
    assert error.value.as_dict()["error"]["details"] == {}
    assert "evidence" not in error.value.as_dict()


@pytest.mark.parametrize("invalid", ["nonce", "scope", "signature"])
def test_wrong_request_or_signature_cannot_create_source_retirement_evidence(invalid: str) -> None:
    waiting = pending()
    changed = payload(waiting.request).model_dump(mode="json")
    changed["native_profile"] = "unqualified-clock"
    if invalid == "nonce":
        changed["request"]["nonce"] = identifier(99)
    elif invalid == "scope":
        changed["request"]["scope"]["provider_generation"] = identifier(99)
    response = authenticate_arbitrary(TimeWitnessPayload.model_validate(changed)).model_dump(
        mode="json"
    )
    if invalid == "signature":
        response["signature"] = "0" * 128
    with pytest.raises(ResearchError) as error:
        waiting.complete(canonical_bytes(response))
    assert not isinstance(error.value, WitnessSourceChanged)
    assert error.value.code == (
        ErrorCode.AUTHORIZATION_DENIED if invalid == "signature" else ErrorCode.STALE_FENCE
    )
    with pytest.raises(ResearchError, match="consumed"):
        waiting.complete(encoded(signed(payload(waiting.request))))


@pytest.mark.parametrize("field", ["signature", "key_id", "timestamp"])
def test_tampered_authenticated_bytes_fail(field: str) -> None:
    waiting = pending()
    response = signed(payload(waiting.request)).model_dump(mode="json")
    if field == "timestamp":
        response["payload"]["authority_time_ms"] += 1
    else:
        response[field] = "0" * 128 if field == "signature" else "sha256:" + "0" * 64
    with pytest.raises(ResearchError) as error:
        waiting.complete(canonical_bytes(response))
    assert error.value.code == ErrorCode.AUTHORIZATION_DENIED


@pytest.mark.parametrize("bad", [b"{", b"{}", b'{"x":1,"x":2}', b" " * (MAX_WITNESS_BYTES + 1)])
def test_malformed_completion_consumes_request_without_retrying_old_observation(bad: bytes) -> None:
    waiting = pending()
    with pytest.raises(ValueError):
        waiting.complete(bad)
    with pytest.raises(ResearchError, match="consumed"):
        waiting.complete(encoded(signed(payload(waiting.request))))


def test_new_pending_context_cannot_consume_previous_nonce() -> None:
    first, second = pending(), pending()
    assert first.request.nonce != second.request.nonce
    with pytest.raises(ResearchError, match="operation binding differs"):
        second.complete(encoded(signed(payload(first.request))))


def test_another_signature_domain_cannot_authenticate_a_witness() -> None:
    waiting = pending()
    response = signed(payload(waiting.request))
    wrong_domain = canonical_bytes(
        {
            "domain": "research.worker-clock",
            "algorithm": response.algorithm,
            "key_id": response.key_id,
            "payload": response.payload.model_dump(mode="json"),
        }
    )
    changed = response.model_dump(mode="json")
    changed["signature"] = sign(resolve_crypto(None), SEED, wrong_domain).hex()
    with pytest.raises(ResearchError) as error:
        waiting.complete(canonical_bytes(changed))
    assert error.value.code == ErrorCode.AUTHORIZATION_DENIED


def test_profile_and_policy_are_frozen_in_the_source_record() -> None:
    waiting = pending()
    with pytest.raises(AttributeError):
        waiting.source = source()  # type: ignore[misc]
    with pytest.raises(ValueError):
        waiting.source.policy.relative_drift_ppm = 1
    with pytest.raises(ValueError, match="key identity differs"):
        WitnessSource.model_validate({**source().model_dump(mode="json"), "public_key": "0" * 64})


def test_each_authority_generation_component_changes_epoch_but_request_fields_do_not() -> None:
    original = witness_epoch(scope(), source(), identifier(7))
    assert UUID(original).version == 8
    for field in WitnessScope.model_fields:
        if field == "schema_version":
            continue
        changed = scope().model_dump(mode="json")
        if field == "control_epoch":
            changed[field] += 1
        elif field in {"state_authority_id", "assignment_key_id"}:
            changed[field] = "sha256:" + "b" * 64
        else:
            changed[field] = identifier(99)
        assert (
            witness_epoch(WitnessScope.model_validate(changed), source(), identifier(7)) != original
        )
    for field in ("id", "native_profile", "policy", "public_key"):
        changed = source().model_dump(mode="json")
        if field == "policy":
            changed[field]["timestamp_guard_ms"] += 1
        elif field == "public_key":
            changed[field] = public_key(resolve_crypto(None), bytes(range(32))).hex()
            changed["key_id"] = witness_key_id(changed[field])
        else:
            changed[field] = identifier(99) if field == "id" else "another-qualified-profile"
        assert (
            witness_epoch(scope(), WitnessSource.model_validate(changed), identifier(7)) != original
        )
    assert witness_epoch(scope(), source(), identifier(8)) != original
    first, second = pending(), pending()
    observed = payload(second.request).model_dump(mode="json")
    observed["authority_time_ms"] += 1_000
    assert (
        first.complete(encoded(signed(payload(first.request)))).epoch
        == second.complete(encoded(signed(TimeWitnessPayload.model_validate(observed)))).epoch
        == original
    )


def test_frozen_signature_bytes_and_epoch_match_independent_javascript() -> None:
    fixture: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
    expected = fixture["expected"]
    configured = WitnessSource.model_validate(fixture["source"])
    witness = decode_time_witness(canonical_bytes(fixture["witness"]))
    assert encoded(WitnessSigner(configured, SEED).sign(witness.payload)) == encoded(witness)
    assert (
        content_digest(witness_signing_bytes(witness.key_id, witness.payload))
        == expected["signing_digest"]
    )
    assert (
        witness_epoch(witness.payload.request.scope, configured, witness.payload.source_epoch)
        == expected["epoch"]
    )
    assert node("time-witness", fixture) == {**expected, "valid_signature": True}
    fixture["witness"]["payload"]["request"]["scope"]["control_epoch"] += 1
    assert not node("time-witness", fixture)["valid_signature"]
