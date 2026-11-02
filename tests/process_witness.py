"""Read-only process identity and exit evidence retained by native fixtures."""

from __future__ import annotations

import ctypes
import sys
from typing import Any

import psutil


class ProcessWitness:
    def __init__(self, process: psutil.Process) -> None:
        self.pid = process.pid
        self.created_at = process.create_time()
        self.parent_pid = process.ppid()
        self.kernel: Any = None
        self.handle: Any = None
        if sys.platform == "win32":
            from ctypes import wintypes

            self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            self.kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            self.kernel.OpenProcess.restype = wintypes.HANDLE
            self.kernel.GetExitCodeProcess.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(wintypes.DWORD),
            ]
            self.kernel.GetExitCodeProcess.restype = wintypes.BOOL
            self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            self.kernel.CloseHandle.restype = wintypes.BOOL
            self.handle = self.kernel.OpenProcess(0x1000 | 0x100000, False, self.pid)
            if not self.handle:
                raise ctypes.WinError(ctypes.get_last_error())
        try:
            assert process.is_running()
            assert psutil.Process(self.pid).create_time() == self.created_at
        except BaseException:
            self.close()
            raise

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
        if self.handle is not None:
            from ctypes import wintypes

            code = wintypes.DWORD()
            if self.kernel.GetExitCodeProcess(self.handle, ctypes.byref(code)):
                result["windows_exit_code"] = code.value
            else:
                result["windows_query_error"] = ctypes.get_last_error()
        return result

    def close(self) -> None:
        if self.handle is not None:
            self.kernel.CloseHandle(self.handle)
            self.handle = None
