"""Native cryptography backend, loaded only when no provider is injected."""

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey


class NativeEd25519:
    def public_key(self, seed: bytes) -> bytes:
        return Ed25519PrivateKey.from_private_bytes(seed).public_key().public_bytes_raw()

    def sign(self, seed: bytes, message: bytes) -> bytes:
        return Ed25519PrivateKey.from_private_bytes(seed).sign(message)

    def verify(self, public_key: bytes, message: bytes, signature: bytes) -> bool:
        try:
            Ed25519PublicKey.from_public_bytes(public_key).verify(signature, message)
        except (InvalidSignature, ValueError):
            return False
        return True
