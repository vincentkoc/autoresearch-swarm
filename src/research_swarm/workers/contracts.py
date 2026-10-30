"""Worker facts are distinct from controller acceptance and scientific results."""

from __future__ import annotations

from pathlib import PurePosixPath, PureWindowsPath
from typing import Literal

from pydantic import Field, model_validator

from research_swarm.domain.types import (
    ID,
    ByteCount,
    Digest,
    Milliseconds,
    Timestamp,
    VersionedRecord,
)


class NativeCommand(VersionedRecord):
    argv: tuple[str, ...] = Field(min_length=1)
    cwd: str
    timeout_ms: Milliseconds
    grace_ms: Milliseconds = 1000
    cleanup_ms: Milliseconds = 5000
    output_limit_bytes: ByteCount = 1024 * 1024

    @model_validator(mode="after")
    def valid_command(self) -> NativeCommand:
        if not self.argv[0] or any("\0" in value for value in (*self.argv, self.cwd)):
            raise ValueError("Native command requires a nonempty executable and NUL-free inputs")
        if not (PurePosixPath(self.cwd).is_absolute() or PureWindowsPath(self.cwd).is_absolute()):
            raise ValueError("Native command requires an absolute working directory")
        if not self.timeout_ms or not self.cleanup_ms:
            raise ValueError("Native command requires positive execution and cleanup deadlines")
        if self.grace_ms >= self.cleanup_ms:
            raise ValueError("Cleanup deadline must leave time after graceful cancellation")
        if self.output_limit_bytes > 64 * 1024 * 1024:
            raise ValueError("Per-stream output retention cannot exceed 64 MiB")
        return self


class NativeIdentity(VersionedRecord):
    backend: Literal["posix_group", "windows_job"]
    pid: int = Field(strict=True, gt=0)
    start_token: str
    scope_id: ID


class CapturedOutput(VersionedRecord):
    name: Literal["stdout", "stderr"]
    retained_bytes: ByteCount
    observed_bytes: ByteCount
    digest: Digest
    eof: bool


class ProcessClosure(VersionedRecord):
    identity: NativeIdentity
    scope_closed: bool
    scope: Literal["posix_process_group", "windows_job"]
    observed_escaped_pids: tuple[int, ...] = ()
    findings: tuple[str, ...] = ()


class NativeOutcome(VersionedRecord):
    reason: Literal[
        "exited",
        "cancelled",
        "deadline",
        "not_started",
        "supervisor_error",
        "descendants_remaining",
    ]
    exit_code: int | None = Field(default=None, strict=True)
    prepared_at: Timestamp
    released_at: Timestamp | None
    finished_at: Timestamp
    elapsed_ms: Milliseconds
    closure: ProcessClosure
    stdout: CapturedOutput
    stderr: CapturedOutput

    @model_validator(mode="after")
    def valid_outcome(self) -> NativeOutcome:
        if self.reason == "not_started" and self.released_at is not None:
            raise ValueError("A released process cannot be reported as not started")
        if self.closure.scope_closed and self.exit_code is None:
            raise ValueError("Owned process closure requires its observed exit code")
        return self
