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
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    RESOURCE_UNAVAILABLE = "RESOURCE_UNAVAILABLE"
    DELIVERY_UNCERTAIN = "DELIVERY_UNCERTAIN"
    TRANSPORT_UNAVAILABLE = "TRANSPORT_UNAVAILABLE"
    ACCEPTANCE_EXPIRED = "ACCEPTANCE_EXPIRED"
    EXECUTION_UNKNOWN = "EXECUTION_UNKNOWN"
    STALE_FENCE = "STALE_FENCE"
    ARTIFACT_UNVERIFIED = "ARTIFACT_UNVERIFIED"
    COMPARISON_INELIGIBLE = "COMPARISON_INELIGIBLE"
    EVALUATION_INCOMPLETE = "EVALUATION_INCOMPLETE"
    EXPOSURE_LIMIT_REACHED = "EXPOSURE_LIMIT_REACHED"
    INCUMBENT_REVISION_CONFLICT = "INCUMBENT_REVISION_CONFLICT"
    AUTHORIZATION_DENIED = "AUTHORIZATION_DENIED"
    ADAPTER_CAPABILITY_UNSUPPORTED = "ADAPTER_CAPABILITY_UNSUPPORTED"
    EVIDENCE_INVALIDATED = "EVIDENCE_INVALIDATED"
    EVENT_CURSOR_EXPIRED = "EVENT_CURSOR_EXPIRED"
    SNAPSHOT_EXPIRED = "SNAPSHOT_EXPIRED"


class ResearchError(Exception):
    """A domain failure with transport-independent semantics."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
        command_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details or {}
        self.command_id = command_id

    def as_dict(self) -> dict[str, Any]:
        return {
            **(
                {"command_id": self.command_id, "disposition": "rejected"}
                if self.command_id
                else {}
            ),
            "error": {
                "code": self.code.value,
                "message": self.message,
                "retryable": self.retryable,
                "details": self.details,
            },
        }
