import math

import pytest

from research_swarm.domain.canonical import (
    MAX_SAFE_INTEGER,
    canonical_bytes,
    record_digest,
    strict_json_loads,
)


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
