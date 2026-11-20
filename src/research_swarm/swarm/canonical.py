"""RFC 8785 payload identities shared by every implementation."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from decimal import Decimal
from typing import IO, Any

MAX_SAFE_INTEGER = 9_007_199_254_740_991
HASH_FORMAT_VERSION = "1"


def _check_value(value: Any) -> None:
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        if abs(value) > MAX_SAFE_INTEGER:
            raise ValueError("Integer exceeds the interoperable JSON range; use a decimal string")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Non-finite JSON number")
        return
    if isinstance(value, str):
        value.encode("utf-8", errors="strict")
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError("JSON object keys must be strings")
            _check_value(key)
            _check_value(child)
        return
    if isinstance(value, (list, tuple)):
        for child in value:
            _check_value(child)
        return
    raise ValueError(f"Unsupported JSON value: {type(value).__name__}")


def _number(value: float) -> str:
    if value == 0:
        return "0"
    shortest = repr(value)
    if 1e-6 <= abs(value) < 1e21:
        # Existing report and project hashes use fixed notation within this range.
        fixed = format(Decimal(shortest), "f")
        return fixed.rstrip("0").rstrip(".") if "." in fixed else fixed
    mantissa, exponent = shortest.split("e")
    return f"{mantissa.removesuffix('.0')}e{int(exponent):+d}"


def _encode(value: Any) -> str:
    if isinstance(value, Mapping):
        keys = sorted(value, key=lambda key: key.encode("utf-16be"))
        return "{" + ",".join(_encode(key) + ":" + _encode(value[key]) for key in keys) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_encode(item) for item in value) + "]"
    if isinstance(value, float):
        return _number(value)
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def canonical_bytes(value: Any) -> bytes:
    """Preserve existing v1 project, replay and archived report identities."""
    _check_value(value)
    return _encode(value).encode("utf-8")


def content_digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


CHUNK_SIZE = 1024 * 1024


def hash_stream(
    source: IO[bytes],
    *,
    output: IO[bytes] | None = None,
    limit: int | None = None,
    overflow: str | Exception = "Stream exceeds its byte allowance",
) -> tuple[str, int]:
    """Hash and optionally copy bytes, reading at most one byte beyond a declared limit."""
    if limit is not None and limit < 0:
        raise ValueError("Byte limit must be nonnegative")
    digest = hashlib.sha256()
    size = 0
    while block := source.read(CHUNK_SIZE if limit is None else min(CHUNK_SIZE, limit - size + 1)):
        size += len(block)
        if limit is not None and size > limit:
            if isinstance(overflow, Exception):
                raise overflow
            raise ValueError(overflow)
        digest.update(block)
        if output is not None:
            output.write(block)
    return "sha256:" + digest.hexdigest(), size


def record_digest(domain: str, payload: Mapping[str, Any]) -> str:
    """Hash a domain-separated payload, never an envelope containing its own hash."""
    if not domain:
        raise ValueError("A digest domain is required")
    return content_digest(
        canonical_bytes(
            {"domain": domain, "hash_format_version": HASH_FORMAT_VERSION, "payload": payload}
        )
    )


def _object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON number: {value}")


def strict_json_loads(data: str | bytes) -> Any:
    result = json.loads(data, object_pairs_hook=_object_pairs, parse_constant=_invalid_constant)
    _check_value(result)
    return result
