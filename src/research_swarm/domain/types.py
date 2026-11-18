"""Shared primitives with explicit units and wire representations."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import uuid4

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    StringConstraints,
)

from research_swarm.domain.canonical import canonical_bytes

JsonObject = dict[str, Any]

ID = Annotated[
    str,
    StringConstraints(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
]
Digest = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]
Name = Annotated[str, StringConstraints(min_length=1, max_length=128)]
MediaType = Annotated[
    str,
    StringConstraints(
        pattern=r"^[A-Za-z0-9!#$%&'*+.^_`|~-]+/[A-Za-z0-9!#$%&'*+.^_`|~-]+$",
        min_length=3,
        max_length=128,
    ),
]
Milliseconds = Annotated[int, Field(strict=True, ge=0)]
ByteCount = Annotated[int, Field(strict=True, ge=0)]
SCHEMA_VERSION = "1.0"


def valid_timestamp(value: str) -> str:
    datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    return value


Timestamp = Annotated[
    str,
    StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"),
    AfterValidator(valid_timestamp),
]


def new_id() -> str:
    return str(uuid4())


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def stable_strings(values: frozenset[str]) -> list[str]:
    return sorted(values, key=canonical_bytes)


StringSet = Annotated[
    frozenset[str],
    PlainSerializer(stable_strings, return_type=list[str], when_used="json"),
    Field(json_schema_extra={"x-ordering": "canonical-json-bytes", "uniqueItems": True}),
]


class Record(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, allow_inf_nan=False, revalidate_instances="always"
    )


class VersionedRecord(Record):
    schema_version: str = Field(default=SCHEMA_VERSION, pattern=r"^1\.0$")
