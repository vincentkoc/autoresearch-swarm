"""Durable command outcomes differ from refusals before command admission."""

from typing import Literal

from pydantic import Field, JsonValue

from research_swarm.domain.errors import ErrorCode
from research_swarm.domain.types import ID, Record


class ErrorDetail(Record):
    code: ErrorCode
    message: str
    retryable: bool = Field(strict=True)
    details: dict[str, JsonValue]


class CommandAccepted(Record):
    command_id: ID
    disposition: Literal["accepted"] = "accepted"
    result: dict[str, JsonValue]


class CommandRejected(Record):
    command_id: ID
    disposition: Literal["rejected"] = "rejected"
    error: ErrorDetail


class RequestRejected(Record):
    error: ErrorDetail


class UploadPartAccepted(Record):
    offset: int = Field(strict=True, ge=0)
