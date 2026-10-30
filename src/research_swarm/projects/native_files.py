"""Read source through native identities before inspecting any file content."""

from __future__ import annotations

import ctypes
import os
import stat
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO, cast

if sys.platform == "win32":

    def _kernel() -> Any:
        return ctypes.WinDLL("kernel32", use_last_error=True)

    def _windows_error() -> OSError:
        return ctypes.WinError(ctypes.get_last_error())
else:

    def _kernel() -> Any:
        raise RuntimeError("Windows source reader is unavailable")

    def _windows_error() -> OSError:
        return OSError("Windows source reader is unavailable")


class SourceReader:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.descriptor: int | None = None
        self.handle: int | None = None
        self.windows_root: str | None = None

    def __enter__(self) -> SourceReader:
        if sys.platform != "win32":
            if os.name != "posix":
                raise ValueError("No qualified source reader for this platform")
            self.descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        else:
            from ctypes import wintypes

            kernel = _kernel()
            create = kernel.CreateFileW
            create.argtypes = [
                wintypes.LPCWSTR,
                wintypes.DWORD,
                wintypes.DWORD,
                ctypes.c_void_p,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.HANDLE,
            ]
            create.restype = wintypes.HANDLE
            # Excluding FILE_SHARE_DELETE keeps the anchored root from being renamed.
            handle = create(str(self.root), 0x80000000, 0x00000003, None, 3, 0x02200000, None)
            if handle == ctypes.c_void_p(-1).value or handle is None:
                raise _windows_error()
            self.handle = int(handle)
            try:
                self.windows_root = self._windows_path(self.handle)
            except BaseException:
                self.close()
                raise
        return self

    @staticmethod
    def _windows_path(handle: int) -> str:
        from ctypes import wintypes

        kernel = _kernel()
        final_path = kernel.GetFinalPathNameByHandleW
        final_path.argtypes = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD]
        final_path.restype = wintypes.DWORD
        buffer = ctypes.create_unicode_buffer(32768)
        length = final_path(handle, buffer, len(buffer), 0)
        if not length or length >= len(buffer):
            raise OSError("Cannot establish the native source path")
        return os.path.normcase(os.path.normpath(buffer.value))

    def close(self) -> None:
        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None
        if self.handle is not None:
            from ctypes import wintypes

            kernel = _kernel()
            kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel.CloseHandle.restype = wintypes.BOOL
            kernel.CloseHandle(self.handle)
            self.handle = None

    def __exit__(self, *_: object) -> None:
        self.close()

    @contextmanager
    def open(self, relative: str) -> Iterator[BinaryIO]:
        descriptor = -1
        if self.descriptor is not None and sys.platform != "win32":
            parent = os.dup(self.descriptor)
            try:
                parts = relative.split("/")
                for part in parts[:-1]:
                    child = os.open(
                        part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
                    )
                    os.close(parent)
                    parent = child
                descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
            finally:
                os.close(parent)
        elif self.handle is not None and sys.platform == "win32":
            import msvcrt

            descriptor = os.open(
                self.root.joinpath(*relative.split("/")), os.O_RDONLY | os.O_BINARY
            )
            try:
                actual = self._windows_path(msvcrt.get_osfhandle(descriptor))
                assert self.windows_root is not None
                expected = os.path.normcase(
                    os.path.normpath(os.path.join(self.windows_root, *relative.split("/")))
                )
                if actual != expected:
                    raise ValueError("Source path traversed a substituted directory")
            except BaseException:
                os.close(descriptor)
                raise
        else:
            raise RuntimeError("Source reader is not open")
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("Source entries must be private regular files")
            with os.fdopen(descriptor, "rb") as stream:
                descriptor = -1
                yield cast(BinaryIO, stream)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
