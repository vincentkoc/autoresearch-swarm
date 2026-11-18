"""Read-only process identity and exit evidence retained by native fixtures."""

from __future__ import annotations

from typing import Any

import psutil


class ProcessWitness:
    def __init__(self, process: psutil.Process) -> None:
        self.pid = process.pid
        self.created_at = process.create_time()
        self.parent_pid = process.ppid()
        assert process.is_running()
        assert psutil.Process(self.pid).create_time() == self.created_at

    def report(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "pid": self.pid,
            "created_at_hex": self.created_at.hex(),
            "parent_pid": self.parent_pid,
        }
        try:
            current = psutil.Process(self.pid)
            result.update(
                running=current.is_running(),
                identity_matches=current.create_time() == self.created_at,
            )
        except psutil.Error as error:
            result["process_error"] = type(error).__name__
        return result
