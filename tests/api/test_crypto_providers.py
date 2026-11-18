from __future__ import annotations

import builtins

import pytest

from research_swarm import crypto
from research_swarm.crypto_native import NativeEd25519
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.workers.signatures import AssignmentSigner, AssignmentVerifier
from tests.domain.test_assignment_signatures import NOW, PRIVATE_KEY, assignment_payload, signer
from tests.integration.test_local_pipeline import pipeline as pipeline


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


def test_injected_codecs_keep_native_bytes_without_loading_native_default(monkeypatch):
    payload = assignment_payload()
    native_signer = signer(payload)
    native_assignment = native_signer.sign(payload)
    provider = ObservedProvider()
    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.startswith(("research_swarm.crypto_native", "cryptography")):
            raise AssertionError("Injected provider fell back to native cryptography")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    authority = AssignmentSigner(
        native_signer.authority.binding, PRIVATE_KEY, crypto_provider=provider
    )
    signed = authority.sign(payload)
    assert signed == native_assignment
    AssignmentVerifier(authority.authority, crypto_provider=provider).verify(
        signed, authority_now=NOW
    )
    assert provider.calls.count("sign") == 1
    assert provider.calls.count("verify") >= 1


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
