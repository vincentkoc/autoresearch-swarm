"""Suspended creation inside a Windows Job Object, without an assignment gap."""

from __future__ import annotations

import ctypes as c
import os
import subprocess
import sys
from ctypes import wintypes as w
from typing import Any, BinaryIO, cast

import psutil

from research_swarm.domain.types import new_id
from research_swarm.workers.contracts import NativeCommand, NativeIdentity, ProcessClosure


class _Startup(c.Structure):
    _fields_ = [
        ("cb", w.DWORD),
        ("reserved", w.LPWSTR),
        ("desktop", w.LPWSTR),
        ("title", w.LPWSTR),
        ("x", w.DWORD),
        ("y", w.DWORD),
        ("width", w.DWORD),
        ("height", w.DWORD),
        ("chars_x", w.DWORD),
        ("chars_y", w.DWORD),
        ("fill", w.DWORD),
        ("flags", w.DWORD),
        ("show", w.WORD),
        ("reserved_size", w.WORD),
        ("reserved_bytes", c.c_void_p),
        ("stdin", w.HANDLE),
        ("stdout", w.HANDLE),
        ("stderr", w.HANDLE),
    ]


class _StartupEx(c.Structure):
    _fields_ = [("startup", _Startup), ("attributes", c.c_void_p)]


class _ProcessInfo(c.Structure):
    _fields_ = [
        ("process", w.HANDLE),
        ("thread", w.HANDLE),
        ("pid", w.DWORD),
        ("tid", w.DWORD),
    ]


class _BasicLimits(c.Structure):
    _fields_ = [
        ("process_time", c.c_int64),
        ("job_time", c.c_int64),
        ("flags", w.DWORD),
        ("minimum_working_set", c.c_size_t),
        ("maximum_working_set", c.c_size_t),
        ("active_process_limit", w.DWORD),
        ("affinity", c.c_size_t),
        ("priority", w.DWORD),
        ("scheduling", w.DWORD),
    ]


class _ExtendedLimits(c.Structure):
    _fields_ = [
        ("basic", _BasicLimits),
        ("io", c.c_uint64 * 6),
        ("process_memory", c.c_size_t),
        ("job_memory", c.c_size_t),
        ("peak_process_memory", c.c_size_t),
        ("peak_job_memory", c.c_size_t),
    ]


class _Accounting(c.Structure):
    _fields_ = [
        ("user", c.c_int64),
        ("kernel", c.c_int64),
        ("period_user", c.c_int64),
        ("period_kernel", c.c_int64),
        ("page_faults", w.DWORD),
        ("total_processes", w.DWORD),
        ("active_processes", w.DWORD),
        ("terminated_processes", w.DWORD),
    ]


if sys.platform == "win32":
    import msvcrt

    def _native_handle(descriptor: int) -> int:
        return msvcrt.get_osfhandle(descriptor)

    def _error() -> OSError:
        return c.WinError(c.get_last_error())

    def _kernel() -> Any:
        kernel = c.WinDLL("kernel32", use_last_error=True)
        declarations = {
            "CreateJobObjectW": ([c.c_void_p, w.LPCWSTR], w.HANDLE),
            "SetInformationJobObject": ([w.HANDLE, c.c_int, c.c_void_p, w.DWORD], w.BOOL),
            "QueryInformationJobObject": (
                [w.HANDLE, c.c_int, c.c_void_p, w.DWORD, c.c_void_p],
                w.BOOL,
            ),
            "TerminateJobObject": ([w.HANDLE, w.UINT], w.BOOL),
            "InitializeProcThreadAttributeList": (
                [c.c_void_p, w.DWORD, w.DWORD, c.POINTER(c.c_size_t)],
                w.BOOL,
            ),
            "UpdateProcThreadAttribute": (
                [c.c_void_p, w.DWORD, c.c_size_t, c.c_void_p, c.c_size_t, c.c_void_p, c.c_void_p],
                w.BOOL,
            ),
            "DeleteProcThreadAttributeList": ([c.c_void_p], None),
            "CreateProcessW": (
                [
                    w.LPCWSTR,
                    w.LPWSTR,
                    c.c_void_p,
                    c.c_void_p,
                    w.BOOL,
                    w.DWORD,
                    c.c_void_p,
                    w.LPCWSTR,
                    c.c_void_p,
                    c.c_void_p,
                ],
                w.BOOL,
            ),
            "IsProcessInJob": ([w.HANDLE, w.HANDLE, c.POINTER(w.BOOL)], w.BOOL),
            "ResumeThread": ([w.HANDLE], w.DWORD),
            "CloseHandle": ([w.HANDLE], w.BOOL),
            "WaitForSingleObject": ([w.HANDLE, w.DWORD], w.DWORD),
            "GetExitCodeProcess": ([w.HANDLE, c.POINTER(w.DWORD)], w.BOOL),
            "PeekNamedPipe": (
                [w.HANDLE, c.c_void_p, w.DWORD, c.c_void_p, c.POINTER(w.DWORD), c.c_void_p],
                w.BOOL,
            ),
        }
        for name, (arguments, result) in declarations.items():
            function = getattr(kernel, name)
            function.argtypes = arguments
            function.restype = result
        return kernel
else:

    def _native_handle(descriptor: int) -> int:
        raise OSError("Windows job supervision is unavailable")

    def _error() -> OSError:
        return OSError("Windows job supervision is unavailable")

    def _kernel() -> Any:
        raise OSError("Windows job supervision is unavailable")


class WindowsChild:
    kernel: Any
    identity: NativeIdentity
    job: int | None
    process: int | None
    thread: int | None
    stdout: BinaryIO
    stderr: BinaryIO

    def __init__(self, command: NativeCommand, environment: dict[str, str]) -> None:
        if sys.platform != "win32":
            raise OSError("Windows job supervision is unavailable")
        self.kernel = _kernel()
        self.job: int | None = None
        self.process: int | None = None
        self.thread: int | None = None
        self.stdout: BinaryIO
        self.stderr: BinaryIO
        descriptors: list[int] = []
        attributes: Any = None
        initialized = False
        try:
            self.job = self.kernel.CreateJobObjectW(None, None)
            if not self.job:
                raise _error()
            limits = _ExtendedLimits()
            limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            if not self.kernel.SetInformationJobObject(
                self.job, 9, c.byref(limits), c.sizeof(limits)
            ):
                raise _error()
            stdin_fd = os.open(os.devnull, os.O_RDONLY)
            descriptors.append(stdin_fd)
            stdout_read, stdout_write = os.pipe()
            descriptors.extend((stdout_read, stdout_write))
            stderr_read, stderr_write = os.pipe()
            descriptors.extend((stderr_read, stderr_write))
            child_fds = (stdin_fd, stdout_write, stderr_write)
            for descriptor in child_fds:
                os.set_inheritable(descriptor, True)
            handles = (w.HANDLE * 3)(*(_native_handle(fd) for fd in child_fds))
            jobs = (w.HANDLE * 1)(self.job)
            size = c.c_size_t()
            self.kernel.InitializeProcThreadAttributeList(None, 2, 0, c.byref(size))
            if not size.value:
                raise _error()
            attributes = c.create_string_buffer(size.value)
            if not self.kernel.InitializeProcThreadAttributeList(attributes, 2, 0, c.byref(size)):
                raise _error()
            initialized = True
            # HANDLE_LIST and JOB_LIST are both applied atomically at creation.
            for attribute, values in ((0x20002, handles), (0x2000D, jobs)):
                if not self.kernel.UpdateProcThreadAttribute(
                    attributes, 0, attribute, values, c.sizeof(values), None, None
                ):
                    raise _error()
            startup = _StartupEx()
            startup.startup.cb = c.sizeof(startup)
            startup.startup.flags = 0x100  # STARTF_USESTDHANDLES
            startup.startup.stdin, startup.startup.stdout, startup.startup.stderr = handles
            startup.attributes = c.cast(attributes, c.c_void_p)
            info = _ProcessInfo()
            arguments = c.create_unicode_buffer(subprocess.list2cmdline(command.argv))
            env = c.create_unicode_buffer(
                "\0".join(f"{key}={value}" for key, value in sorted(environment.items())) + "\0\0"
            )
            # SUSPENDED | UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO | NO_WINDOW
            flags = 0x4 | 0x400 | 0x80000 | 0x8000000
            if not self.kernel.CreateProcessW(
                command.argv[0],
                arguments,
                None,
                None,
                True,
                flags,
                env,
                command.cwd,
                c.byref(startup),
                c.byref(info),
            ):
                raise _error()
            self.process, self.thread = int(info.process), int(info.thread)
            member = w.BOOL()
            if (
                not self.kernel.IsProcessInJob(self.process, self.job, c.byref(member))
                or not member
            ):
                raise OSError("New process did not enter its required job")
            self.identity = NativeIdentity(
                backend="windows_job",
                pid=int(info.pid),
                scope_id=new_id(),
                start_token=f"native-created:{psutil.Process(info.pid).create_time().hex()}",
            )
            self.stdout = cast(BinaryIO, os.fdopen(stdout_read, "rb", buffering=0))
            descriptors.remove(stdout_read)
            self.stderr = cast(BinaryIO, os.fdopen(stderr_read, "rb", buffering=0))
            descriptors.remove(stderr_read)
        except BaseException:
            if self.job:
                self.kernel.TerminateJobObject(self.job, 125)
            self.close()
            raise
        finally:
            for descriptor in descriptors:
                os.close(descriptor)
            if initialized:
                self.kernel.DeleteProcThreadAttributeList(attributes)

    def release(self) -> None:
        if self.thread is None:
            raise RuntimeError("Process barrier is already released")
        if self.kernel.ResumeThread(self.thread) == 0xFFFFFFFF:
            raise _error()
        self.kernel.CloseHandle(self.thread)
        self.thread = None

    def read(self, stream: BinaryIO, maximum: int) -> bytes | None:
        if sys.platform != "win32":
            raise OSError("Windows job supervision is unavailable")
        available = w.DWORD()
        if not self.kernel.PeekNamedPipe(
            _native_handle(stream.fileno()), None, 0, None, c.byref(available), None
        ):
            error = _error()
            if error.winerror == 109:  # ERROR_BROKEN_PIPE
                return b""
            raise error
        if not available.value:
            return None
        return os.read(stream.fileno(), min(maximum, available.value))

    def active(self) -> bool:
        result = self.kernel.WaitForSingleObject(self.process, 0)
        if result == 0xFFFFFFFF:
            raise _error()
        return bool(result == 258)  # WAIT_TIMEOUT

    def stop(self, *, force: bool) -> None:
        # Headless workloads have no reliable console signal. Job termination is explicit.
        if not self.kernel.TerminateJobObject(self.job, 130):
            raise _error()

    def closure(self) -> ProcessClosure:
        accounting = _Accounting()
        if not self.kernel.QueryInformationJobObject(
            self.job, 1, c.byref(accounting), c.sizeof(accounting), None
        ):
            raise _error()
        return ProcessClosure(
            identity=self.identity,
            scope="windows_job",
            scope_closed=accounting.active_processes == 0,
        )

    def exit_code(self) -> int | None:
        if self.active():
            return None
        code = w.DWORD()
        if not self.kernel.GetExitCodeProcess(self.process, c.byref(code)):
            raise _error()
        return int(code.value)

    def close(self) -> None:
        for name in ("stdout", "stderr"):
            stream = getattr(self, name, None)
            if stream is not None:
                stream.close()
        for name in ("thread", "process", "job"):
            handle = getattr(self, name, None)
            if handle is not None:
                self.kernel.CloseHandle(handle)
                setattr(self, name, None)
