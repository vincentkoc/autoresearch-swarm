"""Strict Python bridge decoding; provider behavior is qualified in workerd."""

import pytest

from adapters.cloudflare.runtime.r2_binding import R2BindingTransport
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.storage.async_r2 import BoundedR2Configuration


class Module:
    def __init__(self, value):
        self.value, self.calls = value, []

    async def get(self, *arguments):
        self.calls.append(arguments)
        if isinstance(self.value, Exception):
            raise self.value
        return self.value

    put = get


def transport(value):
    config = BoundedR2Configuration(
        workspace_id=new_id(), bucket_identity="synthetic", prefix="fixture", max_object_bytes=64
    )
    module = Module(value)
    return R2BindingTransport(object(), module, config), module


async def test_null_is_absence_and_binary_content_is_exact():
    missing, _ = transport("null")
    assert await missing.get("fixture", max_bytes=0) is None
    present, _ = transport('{"content_hex":"00ff80","metadata":{"research-state":"payload"}}')
    result = await present.get("fixture", max_bytes=3)
    assert result.content == b"\x00\xff\x80" and result.metadata["research-state"] == "payload"


@pytest.mark.parametrize(
    "value",
    [
        True,
        "[]",
        "{",
        '{"content_hex":"00"}',
        '{"content_hex":"00 00","metadata":{}}',
        '{"content_hex":"FF","metadata":{}}',
        '{"content_hex":"0000","metadata":{}}',
        '{"content_hex":"00","metadata":{"nested":[]}}',
        '{"content_hex":"00","metadata":{},"unrequested":true}',
    ],
)
async def test_malformed_or_oversized_bridge_response_is_not_content_evidence(value):
    store, _ = transport(value)
    with pytest.raises(ResearchError) as refused:
        await store.get("fixture", max_bytes=1)
    assert refused.value.code == ErrorCode.ARTIFACT_UNVERIFIED


@pytest.mark.parametrize("value", [True, False])
async def test_put_preserves_success_and_atomic_conditional_conflict(value):
    store, module = transport(value)
    assert await store.put("fixture", b"\x00\xff", {"state": "payload"}, if_absent=True) is value
    assert module.calls[0][2:] == ("00ff", '{"state":"payload"}', True)


@pytest.mark.parametrize("operation", ["get", "put"])
async def test_transport_errors_are_uncertain_and_do_not_expose_provider_details(operation):
    store, _ = transport(RuntimeError("private fixture provider detail"))
    with pytest.raises(ResearchError) as refused:
        if operation == "get":
            await store.get("fixture", max_bytes=64)
        else:
            await store.put("fixture", b"x", {}, if_absent=True)
    assert refused.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    assert refused.value.retryable and "private" not in str(refused.value.as_dict())


async def test_bridge_cannot_claim_a_write_with_a_nonboolean_outcome():
    store, _ = transport(1)
    with pytest.raises(ResearchError) as refused:
        await store.put("fixture", b"x", {}, if_absent=True)
    assert refused.value.code == ErrorCode.TRANSPORT_UNAVAILABLE


async def test_oversized_calls_do_not_enter_the_javascript_bridge():
    store, module = transport(True)
    with pytest.raises(ValueError):
        await store.get("fixture", max_bytes=65)
    with pytest.raises(ValueError):
        await store.put("fixture", b"x" * 65, {}, if_absent=True)
    assert module.calls == []
