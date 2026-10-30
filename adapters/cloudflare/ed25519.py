"""An already-loaded, synchronous Noble module behind the shared crypto port."""


class NobleEd25519:
    def __init__(self, module):
        self.module = module

    def public_key(self, seed: bytes) -> bytes:
        return bytes.fromhex(self.module.publicKey(seed.hex()))

    def sign(self, seed: bytes, message: bytes) -> bytes:
        return bytes.fromhex(self.module.sign(seed.hex(), message.hex()))

    def verify(self, public_key: bytes, message: bytes, signature: bytes) -> bool:
        # Do not coerce a Promise/proxy into a truthy authentication result.
        return self.module.verify(public_key.hex(), message.hex(), signature.hex())
