import math

import pytest

from research_swarm.domain.canonical import (
    MAX_SAFE_INTEGER,
    canonical_bytes,
    record_digest,
    strict_json_loads,
)


def test_canonical_number_unicode_and_key_order() -> None:
    value = {"z": 1e30, "a": -0.0, "text": "é"}
    assert canonical_bytes(value) == b'{"a":0,"text":"\xc3\xa9","z":1e+30}'
    assert canonical_bytes({"\ufffd": 1, "\U0001f600": 2}) == ('{"😀":2,"�":1}'.encode())


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, MAX_SAFE_INTEGER + 1])
def test_nonportable_numbers_fail(value: object) -> None:
    with pytest.raises(ValueError):
        canonical_bytes({"value": value})


@pytest.mark.parametrize("source", ['{"a":1,"a":2}', '{"n":NaN}', '{"n":Infinity}'])
def test_ambiguous_input_fails(source: str) -> None:
    with pytest.raises(ValueError):
        strict_json_loads(source)


def test_digest_preserves_absence_and_domain() -> None:
    absent = record_digest("research.example", {})
    assert absent != record_digest("research.example", {"optional": None})
    assert absent != record_digest("research.other", {})
    assert record_digest("x", {"b": 2, "a": 1}) == record_digest("x", {"a": 1, "b": 2})


def test_unicode_is_not_silently_normalized() -> None:
    assert canonical_bytes("é") != canonical_bytes("e\u0301")
