from __future__ import annotations

import builtins

import pytest

from research_swarm import crypto
from research_swarm.control.service import ResearchService
from research_swarm.crypto_native import NativeEd25519
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.workers.clock_signatures import ClockSigner, ClockVerifier
from research_swarm.workers.signatures import AssignmentSigner, AssignmentVerifier
from research_swarm.workers.timed_launch import capture_timing, verify_timing
from tests.domain.test_assignment_signatures import NOW, PRIVATE_KEY, assignment_payload, signer
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.workers.test_authority_clock import challenge, timed_worker
from tests.workers.test_clock_estimates import fixture


class ObservedProvider(NativeEd25519):
    def __init__(self):
        self.calls = []

    def public_key(self, seed):
        self.calls.append("public_key")
        return super().public_key(seed)

    def sign(self, seed, message):
        self.calls.append("sign")
        return super().sign(seed, message)

    def verify(self, public_key, message, signature):
        self.calls.append("verify")
        return super().verify(public_key, message, signature)


def signing_probe():
    estimate, grant, clock, challenge = fixture()
    evidence = capture_timing(grant, estimate, estimate.received).evidence
    return {
        "authority": clock.authority.model_dump(mode="json"),
        "assignment": grant.assignment.assignment.model_dump(mode="json"),
        "reply": estimate.verified.reply.model_dump(mode="json"),
        "challenge": challenge.model_dump(mode="json"),
        "grant": grant.grant.model_dump(mode="json"),
        "timing": evidence.model_dump(mode="json"),
    }


def test_injected_codecs_keep_native_bytes_without_loading_native_default(monkeypatch):
    estimate, grant, native_clock, challenge = fixture()
    payload = assignment_payload()
    native_assignment = signer(payload).sign(payload)
    provider = ObservedProvider()
    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.startswith(("research_swarm.crypto_native", "cryptography")):
            raise AssertionError("Injected provider fell back to native cryptography")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    authority = AssignmentSigner(
        native_clock.authority.binding, PRIVATE_KEY, crypto_provider=provider
    )
    signed = authority.sign(payload)
    assert signed == native_assignment
    AssignmentVerifier(authority.authority, crypto_provider=provider).verify(
        signed, authority_now=NOW
    )
    clock = ClockSigner(authority.authority.binding, PRIVATE_KEY, crypto_provider=provider)
    assert clock.reply(estimate.verified.reply.payload) == estimate.verified.reply
    assert clock.bind(grant.grant.payload) == grant.grant
    verifier = ClockVerifier(clock.authority, crypto_provider=provider)
    assert verifier.reply(estimate.verified.reply, challenge) == estimate.verified
    assert verifier.binding(grant.grant, signed) == grant
    evidence = capture_timing(grant, estimate, estimate.received).evidence
    assert (
        verify_timing(
            evidence, clock.authority, grant.grant, signed, crypto_provider=provider
        ).evidence
        == evidence
    )
    assert provider.calls.count("sign") == 3
    assert provider.calls.count("verify") == 7  # Includes nested assignment checks.


@pytest.mark.parametrize(
    ("operation", "bad"),
    [
        (operation, bad)
        for operation in ("public_key", "sign", "verify")
        for bad in (None, True, 1, "00", b"", b"x" * 33)
        if not (operation == "verify" and bad is True)
    ],
)
def test_invalid_provider_outputs_fail_closed(operation, bad):
    class Malformed(NativeEd25519):
        pass

    setattr(Malformed, operation, lambda *args: bad)
    provider = Malformed()
    with pytest.raises(crypto.CryptoProviderError):
        if operation == "public_key":
            crypto.public_key(provider, PRIVATE_KEY)
        elif operation == "sign":
            crypto.sign(provider, PRIVATE_KEY, b"message")
        else:
            crypto.verify(provider, b"k" * 32, b"message", b"s" * 64)


@pytest.mark.parametrize("error", [ValueError("provider failed"), OSError("provider unavailable")])
def test_provider_failure_is_not_an_authorization_rejection(error):
    class Unavailable(NativeEd25519):
        def verify(self, *args):
            raise error

    authority = signer(assignment_payload())
    signed = authority.sign(assignment_payload())
    with pytest.raises(crypto.CryptoProviderError) as failed:
        AssignmentVerifier(authority.authority, crypto_provider=Unavailable()).verify(
            signed, authority_now=NOW
        )
    assert failed.value.__cause__ is error
    altered = signed.model_copy(update={"signature": "00" * 64})
    with pytest.raises(ResearchError) as denied:
        AssignmentVerifier(authority.authority).verify(altered, authority_now=NOW)
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED


def test_async_provider_is_refused_and_coroutine_closed():
    created = []

    class Asynchronous(NativeEd25519):
        def verify(self, *args):
            async def result():
                return True

            value = result()
            created.append(value)
            return value

    with pytest.raises(crypto.CryptoProviderError, match="synchronously"):
        crypto.verify(Asynchronous(), b"k" * 32, b"message", b"s" * 64)
    assert created[0].cr_frame is None


def test_services_keep_independent_providers_and_verification_only_mode(pipeline):
    _, worker, token = timed_worker(pipeline)
    runtime = pipeline.runtime
    original = runtime.service.workers
    providers = [ObservedProvider(), ObservedProvider()]
    services = [
        ResearchService(
            runtime.database,
            runtime.artifacts,
            dispatch_key=original.dispatch_key,
            assignment_key=original._assignment_key,
            authority_time_provider=original._authority_time_provider,
            crypto_provider=provider,
        )
        for provider in providers
    ]
    principal = runtime.credentials.resolve(token)
    for index in (0, 1, 0):
        other_before = providers[1 - index].calls.copy()
        services[index].workers.clock(
            CommandContext(principal=principal, idempotency_key=new_id()), challenge(worker)
        )
        assert providers[1 - index].calls == other_before
        assert services[index].results.crypto_provider is providers[index]
        assert services[index].worker_turnover.crypto_provider is providers[index]
    assert [provider.calls.count("sign") for provider in providers] == [2, 1]
    verifier = ResearchService(runtime.database, runtime.artifacts, crypto_provider=providers[0])
    assert verifier.workers is None and verifier.results.crypto_provider is providers[0]
