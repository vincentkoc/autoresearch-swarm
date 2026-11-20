import io
import math

import pytest

from research_swarm.domain.canonical import (
    MAX_SAFE_INTEGER,
    canonical_bytes,
    record_digest,
    strict_json_loads,
)


def test_archived_identity_preserves_number_notation_and_unicode_key_order() -> None:
    data = {"z": [-0.0, 1e-7, 1e-6, 1e20, 1e21], "�": 0, "😀": "é"}
    expected = '{"z":[0,1e-7,0.000001,100000000000000000000,1e+21],"😀":"é","�":0}'
    assert canonical_bytes(data) == expected.encode("utf-8")


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, MAX_SAFE_INTEGER + 1])
def test_nonportable_numbers_fail(value: object) -> None:
    with pytest.raises(ValueError):
        canonical_bytes({"value": value})


@pytest.mark.parametrize("source", ['{"a":1,"a":2}', '{"n":NaN}', '{"n":Infinity}'])
def test_ambiguous_input_fails(source: str) -> None:
    with pytest.raises(ValueError):
        strict_json_loads(source)


def test_digest_ignores_key_insertion_order() -> None:
    assert record_digest("x", {"b": 2, "a": 1}) == record_digest("x", {"a": 1, "b": 2})


def test_streaming_digest_preserves_bounds_and_copies_only_accepted_bytes() -> None:
    from research_swarm.swarm.canonical import CHUNK_SIZE, content_digest, hash_stream

    data = b"x" * CHUNK_SIZE + b"y"
    for limit in (None, len(data)):
        copy = io.BytesIO()
        assert hash_stream(io.BytesIO(data), output=copy, limit=limit) == (
            content_digest(data),
            len(data),
        )
        assert copy.getvalue() == data
    copy = io.BytesIO()
    oversized = io.BytesIO(data + b"more")
    with pytest.raises(ValueError, match="exceeds"):
        hash_stream(oversized, output=copy, limit=CHUNK_SIZE)
    assert oversized.tell() == CHUNK_SIZE + 1
    assert copy.getvalue() == data[:CHUNK_SIZE]
