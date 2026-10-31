"""Preloaded JavaScript R2 binding for the bounded async generation port."""

from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.storage.async_r2 import BoundedR2Configuration, ObjectBytes


class R2BindingTransport:
    def __init__(self, bucket, module, config):
        # The deployment supplies this fixed binding. Requests never select a bucket.
        # Convert env.ARTIFACTS with workers.python_to_rpc at the runtime boundary.
        self.bucket, self.module = bucket, module
        self.config = BoundedR2Configuration.model_validate(config)

    @property
    def configuration_digest(self):
        return self.config.digest

    async def get(self, key, *, max_bytes):
        if type(max_bytes) is not int or not 0 <= max_bytes <= self.config.max_object_bytes:
            raise ValueError("Invalid bounded R2 read")
        try:
            text = await self.module.get(self.bucket, key, float(max_bytes))
        except Exception as error:
            raise ResearchError(
                ErrorCode.TRANSPORT_UNAVAILABLE, "R2 read was not confirmed", retryable=True
            ) from error
        try:
            if not isinstance(text, str) or len(text) > max_bytes * 2 + 16384:
                raise ValueError("Invalid bounded R2 response")
            value = strict_json_loads(text)
            if value is None:
                return None
            if not isinstance(value, dict) or set(value) != {"content_hex", "metadata"}:
                raise ValueError("Invalid R2 response fields")
            encoded, metadata = value["content_hex"], value["metadata"]
            if (
                not isinstance(encoded, str)
                or len(encoded) > max_bytes * 2
                or not isinstance(metadata, dict)
            ):
                raise ValueError("Invalid bounded R2 content")
            content = bytes.fromhex(encoded)
            if content.hex() != encoded:
                raise ValueError("R2 content encoding differs")
            return ObjectBytes(content, metadata)
        except (TypeError, ValueError) as error:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "R2 response is invalid") from error

    async def put(self, key, content, metadata, *, if_absent):
        if not isinstance(content, bytes) or len(content) > self.config.max_object_bytes:
            raise ValueError("Invalid bounded R2 write")
        snapshot = ObjectBytes(content, metadata)
        encoded = canonical_bytes(dict(snapshot.metadata)).decode()
        if len(encoded) > 8192 or type(if_absent) is not bool:
            raise ValueError("Invalid R2 write metadata or condition")
        try:
            result = await self.module.put(self.bucket, key, content.hex(), encoded, if_absent)
        except Exception as error:
            raise ResearchError(
                ErrorCode.TRANSPORT_UNAVAILABLE, "R2 write was not confirmed", retryable=True
            ) from error
        if type(result) is not bool:
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "R2 write returned no outcome")
        return result
