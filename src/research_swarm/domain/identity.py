"""Actor identities and idempotent command envelopes."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from pydantic import Field, PrivateAttr

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Name, Record, Revision, StringSet, new_id


class Principal(Record):
    id: ID
    workspace_id: ID
    scopes: StringSet

    def require(self, scope: str) -> None:
        if scope not in self.scopes and "*" not in self.scopes:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED,
                f"Required scope: {scope}",
                details={"scope": scope},
            )


class CommandContext(Record):
    principal: Principal
    idempotency_key: Name
    command_id: ID = Field(default_factory=new_id)
    expected_revision: Revision | None = None
    _drive_guard: DriveGuard | None = PrivateAttr(default=None)
    _recovery_guard: RecoveryGuard | None = PrivateAttr(default=None)
    _recovery_operation: RecoveryOperationGuard | None = PrivateAttr(default=None)


@dataclass(frozen=True)
class RecoveryOperationGuard:
    """One attempt-bound recovery operation and its exact internal artifact effects."""

    authority: tuple[str, str]
    restore_id: str
    restoration_digest: str
    admission_digest: str
    gate_revision: int
    control_epoch: int
    attempt_id: str
    worker_id: str
    session_id: str
    worker_principal_id: str
    actor_id: str
    root_operation: str
    root_request_digest: str
    upload_id: str | None = None
    artifact_id: str | None = None
    derived_request_digest: str | None = None
    cleanup_pin_id: str | None = None


@dataclass(frozen=True)
class RecoveryGuard:
    """Native verification capability; never serialized or accepted from a caller."""

    authority: tuple[str, str]
    request_digest: str
    restore_id: str
    restoration_digest: str
    custody_pin_digest: str
    target_public_key: str
    gate_revision: int
    control_epoch: int
    watermark: int


@dataclass(frozen=True)
class DriveGuard:
    """Server-created capability, never accepted from a wire command or principal."""

    turn_id: str
    root_request_digest: str
    derived_request_digest: str | None = None
    cleanup_pin_id: str | None = None


def guarded_context(context: CommandContext, guard: DriveGuard) -> CommandContext:
    copied = context.model_copy()
    copied._drive_guard = guard
    return copied


def artifact_context(
    context: CommandContext,
    key: str,
    operation: str,
    payload: dict[str, Any],
    *,
    cleanup_pin_id: str | None = None,
) -> CommandContext:
    """Derive an exact internal artifact operation from the original trusted turn."""
    derived = CommandContext(principal=context.principal, idempotency_key=key)
    if context._recovery_operation is not None:
        if operation not in {
            "artifact.pin",
            "artifact.unpin",
            "worker.finish_upload_cleanup",
            "worker.finish_upload_abort",
        } or (cleanup_pin_id is not None and operation != "artifact.unpin"):
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Recovery cannot derive this operation"
            )
        derived._recovery_operation = replace(
            context._recovery_operation,
            derived_request_digest=record_digest(
                "research.command",
                {"operation": operation, "expected_revision": None, "payload": payload},
            ),
            cleanup_pin_id=cleanup_pin_id,
        )
    if context._drive_guard is None:
        return derived
    if operation not in {
        "artifact.pin",
        "artifact.unpin",
        "artifact.begin_upload",
        "artifact.finalize_upload",
        "artifact.finish_upload",
    }:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Turn cannot derive this operation")
    if cleanup_pin_id is not None and operation != "artifact.unpin":
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Only an owned read pin can outlive fencing"
        )
    return guarded_context(
        derived,
        DriveGuard(
            turn_id=context._drive_guard.turn_id,
            root_request_digest=context._drive_guard.root_request_digest,
            derived_request_digest=record_digest(
                "research.command",
                {
                    "operation": operation,
                    "expected_revision": None,
                    "payload": payload,
                },
            ),
            cleanup_pin_id=cleanup_pin_id,
        ),
    )
