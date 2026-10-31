"""Transport decoding; cryptographic and operation admission remain in core."""

import pytest

from adapters.cloudflare.witness_binding import WitnessHTTPTransport
from research_swarm.domain.errors import ErrorCode, ResearchError
from tests.test_time_witnesses import encoded, payload, pending, signed

TOKEN = "public_fixture_consumer_" + "x" * 32


class Module:
    def __init__(self, response):
        self.response, self.calls = response, []

    async def request(self, *args):
        self.calls.append(args)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


async def test_transport_preserves_source_bytes_for_independent_one_use_verification():
    waiting = pending()
    witness = signed(payload(waiting.request))
    module = Module(encoded(witness).decode())
    transport = WitnessHTTPTransport(module, "https://witness.example.test", TOKEN)
    result = await transport(waiting.request)
    assert waiting.complete(result).witness == witness
    assert len(module.calls) == 1
    assert module.calls[0][0:2] == ("https://witness.example.test", TOKEN)
    assert module.calls[0][3:] == (1000.0, False)
    assert TOKEN not in repr(transport)


@pytest.mark.parametrize("response", [True, "é" * 8193, "\ud800", RuntimeError(TOKEN)])
async def test_invalid_responses_and_provider_diagnostics_are_not_returned(response):
    transport = WitnessHTTPTransport(Module(response), "https://witness.example.test", TOKEN)
    with pytest.raises(ResearchError) as refused:
        await transport(pending().request)
    assert refused.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    assert refused.value.retryable
    assert TOKEN not in str(refused.value)
    assert refused.value.__suppress_context__


@pytest.mark.parametrize(
    "origin,options",
    [
        ("http://witness.example.test", {}),
        ("http://127.0.0.1", {}),
        ("http://localhost", {"allow_loopback": True}),
        ("https://user:secret@witness.example.test", {}),
        ("https://witness.example.test/path", {}),
        ("https://witness.example.test?query", {}),
        ("https://witness.example.test#fragment", {}),
        ("https://witness.example.test:invalid", {}),
        (None, {}),
        ("https://witness.example.test", {"timeout_ms": True}),
        ("https://witness.example.test", {"timeout_ms": 99}),
        ("https://witness.example.test", {"allow_loopback": "true"}),
    ],
)
def test_invalid_configuration_is_refused_without_io(origin, options):
    module = Module("unused")
    with pytest.raises(ValueError, match="Invalid witness transport configuration"):
        WitnessHTTPTransport(module, origin, TOKEN, **options)
    assert module.calls == []


async def test_loopback_mode_is_explicit_and_invalid_input_does_not_call_runtime():
    module = Module("{}")
    transport = WitnessHTTPTransport(module, "http://127.0.0.1:8080", TOKEN, allow_loopback=True)
    with pytest.raises(ValueError):
        await transport({"invalid": True})
    assert module.calls == []
    assert await transport(pending().request) == b"{}"  # The shared verifier must still refuse it.
    with pytest.raises(ValueError):
        pending().complete(b"{}")
