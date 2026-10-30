"""Synchronous Ed25519 primitives; envelopes and authority checks stay with callers."""

import inspect
from collections.abc import Callable
from typing import Protocol, TypeVar

T = TypeVar("T")


class Ed25519Provider(Protocol):
    def public_key(self, seed: bytes) -> bytes: ...
    def sign(self, seed: bytes, message: bytes) -> bytes: ...
    def verify(self, public_key: bytes, message: bytes, signature: bytes) -> bool: ...


class CryptoProviderError(RuntimeError):
    """Infrastructure failure, distinct from an unauthenticated envelope."""


def resolve_crypto(provider: Ed25519Provider | None) -> Ed25519Provider:
    if provider is not None:
        return provider
    from research_swarm.crypto_native import NativeEd25519

    return NativeEd25519()


def _call(operation: Callable[[], T]) -> T:
    try:
        result = operation()
    except Exception as error:
        raise CryptoProviderError("Ed25519 provider operation failed") from error
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise CryptoProviderError("Ed25519 provider must complete synchronously")
    return result


def _seed(seed: bytes) -> None:
    if not isinstance(seed, bytes) or len(seed) != 32:
        raise ValueError("Ed25519 requires a 32-byte seed")


def public_key(provider: Ed25519Provider, seed: bytes) -> bytes:
    _seed(seed)
    result = _call(lambda: provider.public_key(seed))
    if not isinstance(result, bytes) or len(result) != 32:
        raise CryptoProviderError("Ed25519 provider returned an invalid public key")
    return result


def sign(provider: Ed25519Provider, seed: bytes, message: bytes) -> bytes:
    _seed(seed)
    result = _call(lambda: provider.sign(seed, message))
    if not isinstance(result, bytes) or len(result) != 64:
        raise CryptoProviderError("Ed25519 provider returned an invalid signature")
    return result


def verify(provider: Ed25519Provider, key: bytes, message: bytes, signature: bytes) -> None:
    if (
        not isinstance(key, bytes)
        or len(key) != 32
        or not isinstance(signature, bytes)
        or len(signature) != 64
    ):
        raise ValueError("Invalid Ed25519 public key or signature size")
    result = _call(lambda: provider.verify(key, message, signature))
    if type(result) is not bool:
        raise CryptoProviderError("Ed25519 verification must return a boolean")
    if not result:
        raise ValueError("Ed25519 signature authentication failed")
