"""Strict body bridge decoding; JavaScript and workerd tests own stream proof."""

import pytest

from adapters.cloudflare.runtime.http_binding import HTTPBodyReader
from research_swarm.domain.errors import ErrorCode, ResearchError


class Module:
    def __init__(self, response):
        self.response, self.calls = response, []

    async def readBody(self, request, limit):
        self.calls.append(limit)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


async def test_binary_body_and_empty_requests_are_exact():
    assert await HTTPBodyReader(object(), Module('{"content_hex":"00ff80"}'))(3) == b"\x00\xff\x80"
    assert await HTTPBodyReader(object(), Module('{"content_hex":""}'))(0) == b""


async def test_limit_refusal_is_a_request_error_with_no_successful_bytes():
    with pytest.raises(ResearchError) as refused:
        await HTTPBodyReader(object(), Module('{"error":"limit"}'))(1)
    assert refused.value.code == ErrorCode.PROJECT_CONTRACT_INVALID


@pytest.mark.parametrize(
    "response",
    [
        '{"error":"unconfirmed"}',
        '{"content_hex":"0000"}',
        '{"content_hex":"FF"}',
        '{"content_hex":"0 0"}',
        '{"content_hex":0}',
        '{"content_hex":"00","extra":1}',
        "null",
        True,
        RuntimeError("private producer detail"),
    ],
)
async def test_unconfirmed_or_invalid_body_is_not_dispatchable(response):
    with pytest.raises(ResearchError) as refused:
        await HTTPBodyReader(object(), Module(response))(1)
    assert refused.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    assert "private" not in refused.value.message


@pytest.mark.parametrize("limit", [-1, True, 1.5, 1024**2 + 1])
async def test_invalid_limit_never_enters_runtime_bridge(limit):
    module = Module('{"content_hex":""}')
    with pytest.raises(ValueError):
        await HTTPBodyReader(object(), module)(limit)
    assert module.calls == []
