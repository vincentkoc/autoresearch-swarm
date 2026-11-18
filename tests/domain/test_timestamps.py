import pytest
from pydantic import TypeAdapter

from research_swarm.domain.types import Timestamp


@pytest.mark.parametrize("timestamp", ["yesterday", "2026-02-30T00:00:00.000Z", "2026-01-01"])
def test_timestamp_rejects_ambiguous_or_invalid_values(timestamp: str) -> None:
    with pytest.raises(ValueError):
        TypeAdapter(Timestamp).validate_python(timestamp)
