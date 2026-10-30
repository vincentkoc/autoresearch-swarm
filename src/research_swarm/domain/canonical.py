"""RFC 8785 payload identities shared by every implementation."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from typing import Any

import rfc8785

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


def canonical_bytes(value: Any) -> bytes:
    """Preserve absent/null and Unicode contents, using JCS number semantics."""
    _check_value(value)
    return rfc8785.dumps(value)


def content_digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


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
