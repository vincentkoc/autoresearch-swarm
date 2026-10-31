"""Bounded ASCII RPC transport for opaque signed image bytes; no verification authority."""

import base64
import binascii
import json

from research_swarm.checkpoints.capture import CaptureLimits
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError

MAX_RPC_BYTES = 6 * 1024**2
LIMITS = CaptureLimits()
FIELDS = {"schema_version", "envelope_base64", "chunks_base64"}


def invalid():
    return ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Checkpoint transport is invalid")


def _bytes(envelope, bodies):
    if (
        type(envelope) is not bytes
        or not 0 < len(envelope) <= LIMITS.manifest_bytes + 256
        or type(bodies) not in {tuple, list}
        or not 0 < len(bodies) <= LIMITS.chunks
        or any(
            type(body) is not bytes or not 0 < len(body) <= LIMITS.chunk_bytes for body in bodies
        )
        or sum(map(len, bodies)) > LIMITS.image_bytes
    ):
        raise invalid()


def encode_checkpoint(envelope, bodies):
    """Serialize only admitted byte sizes; the receiving importer verifies their meaning."""
    _bytes(envelope, bodies)
    encoded = json.dumps(
        {
            "schema_version": "1.0",
            "envelope_base64": base64.b64encode(envelope).decode("ascii"),
            "chunks_base64": [base64.b64encode(body).decode("ascii") for body in bodies],
        },
        separators=(",", ":"),
        ensure_ascii=True,
    )
    if len(encoded) > MAX_RPC_BYTES:
        raise invalid()
    return encoded


def _decode(value, limit):
    if type(value) is not str or not 0 < len(value) <= 4 * ((limit + 2) // 3):
        raise invalid()
    try:
        decoded = base64.b64decode(value, validate=True)
        if len(decoded) > limit or base64.b64encode(decoded).decode("ascii") != value:
            raise invalid()
        return decoded
    except (ValueError, binascii.Error):
        raise invalid() from None


def decode_checkpoint(encoded):
    """Refuse oversized strings before parsing and bound each decoded allocation."""
    if type(encoded) is not str or len(encoded) > MAX_RPC_BYTES or not encoded.isascii():
        raise invalid()
    try:
        value = strict_json_loads(encoded)
    except (ValueError, RecursionError):
        raise invalid() from None
    if (
        type(value) is not dict
        or set(value) != FIELDS
        or value["schema_version"] != "1.0"
        or type(value["chunks_base64"]) is not list
        or not 0 < len(value["chunks_base64"]) <= LIMITS.chunks
    ):
        raise invalid()
    envelope = _decode(value["envelope_base64"], LIMITS.manifest_bytes + 256)
    bodies, remaining = [], LIMITS.image_bytes
    for chunk in value["chunks_base64"]:
        body = _decode(chunk, min(LIMITS.chunk_bytes, remaining))
        bodies.append(body)
        remaining -= len(body)
    _bytes(envelope, bodies)
    return envelope, tuple(bodies)
