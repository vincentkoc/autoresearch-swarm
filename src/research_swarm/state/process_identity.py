"""Same-host process identity for local resource reconciliation, never a kill target."""

from __future__ import annotations

import os
import socket

from pydantic import Field

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import Record


class LocalProcessIdentity(Record):
    host: str
    pid: int = Field(strict=True, gt=0)
    created_at_ms: int = Field(strict=True, gt=0)

    @classmethod
    def current(cls) -> LocalProcessIdentity:
        import psutil

        return cls(
            host=socket.gethostname(),
            pid=os.getpid(),
            created_at_ms=int(psutil.Process().create_time() * 1000),
        )

    def require_closed(self) -> None:
        # This proof is valid only for the local SQLite profile, not a shared/network DB.
        import psutil

        if self.host != socket.gethostname():
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Process closure needs its native host"
            )
        try:
            process = psutil.Process(self.pid)
            if int(process.create_time() * 1000) != self.created_at_ms:
                return
            if process.status() == psutil.STATUS_ZOMBIE:
                return
        except psutil.NoSuchProcess:
            return
        except psutil.AccessDenied as error:
            raise ResearchError(
                ErrorCode.EXECUTION_UNKNOWN, "Native process status is unavailable"
            ) from error
        raise ResearchError(ErrorCode.EXECUTION_UNKNOWN, "The owning process is still alive")
