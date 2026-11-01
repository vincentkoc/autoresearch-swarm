"""Opaque checkpoint transport rejects ambiguity before allocating oversized bodies."""

import base64
import json

import pytest

from adapters.cloudflare.recovery import checkpoint_wire as wire
from research_swarm.checkpoints.image_format import sign_manifest, verify_image
from research_swarm.domain.errors import ErrorCode, ResearchError
from tests.adapters.cloudflare.recovery.test_checkpoint_capture import capture
from tests.adapters.cloudflare.recovery.test_checkpoint_capture import sealed as sealed
from tests.adapters.cloudflare.recovery.test_checkpoint_schema import source as source
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.workers.test_time_witnesses import SEED


def test_ascii_wire_preserves_exact_opaque_bytes_and_order():
    encoded = wire.encode_checkpoint(b"\0\xff", (b"\xff\0", b"second"))
    assert encoded == (
        '{"schema_version":"1.0","envelope_base64":"AP8=","chunks_base64":["/wA=","c2Vjb25k"]}'
    )
    assert wire.decode_checkpoint(encoded) == (b"\0\xff", (b"\xff\0", b"second"))


async def test_signed_source_image_survives_transport_without_normalizing_sql(sealed):
    image = capture(sealed)
    envelope = sign_manifest(image.manifest, SEED, crypto=sealed.crypto)
    transported = wire.decode_checkpoint(wire.encode_checkpoint(envelope, image.bodies))
    assert transported == (envelope, image.bodies)
    verified = verify_image(
        *transported,
        expected_digest=image.manifest.digest,
        expected_source=sealed.manifest.configuration.authority,
        expected_custody=sealed.pin,
        crypto=sealed.crypto,
    )
    assert verified.manifest == image.manifest


def test_maximum_source_byte_profile_fits_complete_rpc_string():
    limits = wire.LIMITS
    envelope = b"e" * (limits.manifest_bytes + 256)
    chunks = (b"x" * limits.chunk_bytes,) * (limits.image_bytes // limits.chunk_bytes)
    encoded = wire.encode_checkpoint(envelope, chunks)
    assert 5 * 1024**2 < len(encoded) < wire.MAX_RPC_BYTES
    assert encoded.isascii()
    assert wire.decode_checkpoint(encoded) == (envelope, chunks)


@pytest.mark.parametrize(
    "encoded",
    [
        None,
        b"{}",
        "[]",
        "{}",
        '{"schema_version":"1.0","schema_version":"1.0"}',
        '["' + "x" * 1024 + '"]',
        "[" * 1100 + "]" * 1100,
        '{"schema_version":NaN}',
        "\N{SNOWMAN}",
    ],
)
def test_invalid_shape_and_json_fail_opaquely(encoded):
    with pytest.raises(ResearchError) as refused:
        wire.decode_checkpoint(encoded)
    assert refused.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert refused.value.__cause__ is None
    assert str(refused.value) == "Checkpoint transport is invalid"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", "2.0"),
        ("extra", "no extensions"),
        ("envelope_base64", 7),
        ("envelope_base64", ""),
        ("envelope_base64", "eA==\n"),
        ("envelope_base64", "eB=="),  # Nonzero unused padding bits.
        ("envelope_base64", "eA==="),
        ("envelope_base64", "é"),
        ("chunks_base64", "eA=="),
        ("chunks_base64", []),
        ("chunks_base64", [""]),
        ("chunks_base64", [None]),
        ("chunks_base64", ["eA=="] * (wire.LIMITS.chunks + 1)),
    ],
)
def test_noncanonical_base64_and_unknown_fields_refuse(field, value):
    payload = json.loads(wire.encode_checkpoint(b"e", (b"c",)))
    payload[field] = value
    with pytest.raises(ResearchError):
        wire.decode_checkpoint(json.dumps(payload))


def test_string_cap_is_checked_before_json_parser(monkeypatch):
    def parse(_):
        pytest.fail("Oversized RPC string reached the JSON parser")

    monkeypatch.setattr(wire, "strict_json_loads", parse)
    with pytest.raises(ResearchError):
        wire.decode_checkpoint(" " * (wire.MAX_RPC_BYTES + 1))


@pytest.mark.parametrize("bound", ["envelope", "chunk", "total", "count"])
def test_sender_and_receiver_enforce_the_same_decoded_bounds(bound):
    limits = wire.LIMITS
    envelope, chunks = b"e", [b"c"]
    if bound == "envelope":
        envelope *= limits.manifest_bytes + 257
    elif bound == "chunk":
        chunks = [b"c" * (limits.chunk_bytes + 1)]
    elif bound == "total":
        chunks = [b"c" * limits.chunk_bytes] * (limits.image_bytes // limits.chunk_bytes) + [b"c"]
    else:
        chunks *= limits.chunks + 1
    with pytest.raises(ResearchError):
        wire.encode_checkpoint(envelope, chunks)
    encoded = json.dumps(
        {
            "schema_version": "1.0",
            "envelope_base64": base64.b64encode(envelope).decode(),
            "chunks_base64": [base64.b64encode(body).decode() for body in chunks],
        }
    )
    assert len(encoded) < wire.MAX_RPC_BYTES
    with pytest.raises(ResearchError):
        wire.decode_checkpoint(encoded)


def test_aggregate_overflow_is_refused_before_decoding_another_chunk(monkeypatch):
    monkeypatch.setattr(wire, "LIMITS", wire.LIMITS.model_copy(update={"image_bytes": 3}))
    payload = '{"schema_version":"1.0","envelope_base64":"ZQ==","chunks_base64":["eHh4","eA=="]}'
    original, calls = base64.b64decode, []

    def decode(value, **kwargs):
        calls.append(value)
        return original(value, **kwargs)

    monkeypatch.setattr(wire.base64, "b64decode", decode)
    with pytest.raises(ResearchError):
        wire.decode_checkpoint(payload)
    assert calls == ["ZQ==", "eHh4"]
