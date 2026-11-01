"""Strict response decoding for the bounded Workers request-body bridge."""

from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError


class HTTPBodyReader:
    def __init__(self, request, module):
        # Supply workers.python_to_rpc(request), not a Python request proxy.
        self.request, self.module = request, module

    async def __call__(self, limit):
        if type(limit) is not int or not 0 <= limit <= 1024**2:
            raise ValueError("Invalid request body bound")
        try:
            text = await self.module.readBody(self.request, float(limit))
            if not isinstance(text, str) or len(text) > limit * 2 + 64:
                raise ValueError("Invalid body bridge response")
            value = strict_json_loads(text)
            if value == {"error": "limit"}:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Request body exceeds its bound"
                )
            if not isinstance(value, dict) or set(value) != {"content_hex"}:
                raise ValueError("Body read was not confirmed")
            encoded = value["content_hex"]
            if not isinstance(encoded, str) or len(encoded) > limit * 2:
                raise ValueError("Invalid body content")
            result = bytes.fromhex(encoded)
            if result.hex() != encoded:
                raise ValueError("Invalid body encoding")
            return result
        except ResearchError:
            raise
        except Exception as error:
            raise ResearchError(
                ErrorCode.TRANSPORT_UNAVAILABLE, "Request body was not confirmed", retryable=True
            ) from error
