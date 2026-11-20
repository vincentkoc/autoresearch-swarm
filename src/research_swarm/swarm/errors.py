"""Stable errors shared by all transports and adapters."""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    NOT_FOUND = "NOT_FOUND"
    SCHEMA_UNSUPPORTED = "SCHEMA_UNSUPPORTED"
    CAPABILITY_UNAVAILABLE = "CAPABILITY_UNAVAILABLE"
    PROJECT_CONTRACT_INVALID = "PROJECT_CONTRACT_INVALID"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    REVISION_CONFLICT = "REVISION_CONFLICT"
    INVALID_TRANSITION = "INVALID_TRANSITION"
    RESOURCE_UNAVAILABLE = "RESOURCE_UNAVAILABLE"
    TRANSPORT_UNAVAILABLE = "TRANSPORT_UNAVAILABLE"
    EXECUTION_UNKNOWN = "EXECUTION_UNKNOWN"
    ARTIFACT_UNVERIFIED = "ARTIFACT_UNVERIFIED"
    AUTHORIZATION_DENIED = "AUTHORIZATION_DENIED"


class ResearchError(Exception):
    """A domain failure with transport-independent semantics."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code.value,
                "message": self.message,
                "retryable": self.retryable,
                "details": self.details,
            },
        }
