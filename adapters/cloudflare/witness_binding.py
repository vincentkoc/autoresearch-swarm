"""Bounded witness exchange; source signature and one-use admission stay in core."""

import re
from urllib.parse import urlsplit

from research_swarm.control.time_witnesses import MAX_WITNESS_BYTES
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import TimeWitnessRequest


class WitnessHTTPTransport:
    """Construct from deployment configuration, never from an incoming API request.

    The module is imported before entering the Durable Object operation gate.
    Loopback HTTP is an explicit local qualification option, disabled by default.
    """

    def __init__(self, module, origin, credential, *, timeout_ms=1000, allow_loopback=False):
        try:
            url = urlsplit(origin)
            valid_origin = (
                isinstance(origin, str)
                and url.hostname
                and not (url.username or url.password or url.path or url.query or url.fragment)
                and origin == url.scheme + "://" + url.netloc
                and (
                    url.scheme == "https"
                    or (
                        allow_loopback is True
                        and url.scheme == "http"
                        and url.hostname == "127.0.0.1"
                    )
                )
            )
            _ = url.port
        except (ValueError, TypeError, AttributeError):
            valid_origin = False
        if (
            not valid_origin
            or type(allow_loopback) is not bool
            or type(timeout_ms) is not int
            or not 100 <= timeout_ms <= 10000
            or not isinstance(credential, str)
            or re.fullmatch(r"[A-Za-z0-9_-]{43,128}", credential) is None
        ):
            raise ValueError("Invalid witness transport configuration")
        self._module, self._origin, self._credential = module, origin, credential
        self._timeout_ms, self._allow_loopback = timeout_ms, allow_loopback

    async def __call__(self, request):
        request = TimeWitnessRequest.model_validate(request)
        encoded = canonical_bytes(request.model_dump(mode="json"))
        if len(encoded) > MAX_WITNESS_BYTES:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Witness request exceeds its bound"
            )
        try:
            text = await self._module.request(
                self._origin,
                self._credential,
                encoded.decode(),
                float(self._timeout_ms),
                self._allow_loopback,
            )
            if not isinstance(text, str):
                raise ValueError("Invalid witness transport response")
            result = text.encode()
            if len(result) > MAX_WITNESS_BYTES:
                raise ValueError("Witness response exceeds its bound")
            return result
        except Exception:
            # Do not preserve provider errors that could contain URLs or credentials.
            raise ResearchError(
                ErrorCode.TRANSPORT_UNAVAILABLE,
                "Time witness transport unavailable",
                retryable=True,
            ) from None
