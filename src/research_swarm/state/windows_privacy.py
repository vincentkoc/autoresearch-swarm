"""Native ACL admission for private Windows runtime state.

The profile permits the process user and SYSTEM. It rejects unsupported ACLs
instead of approximating Windows effective-access evaluation. Same-user code
and operating-system administrators remain inside the host trust boundary.
"""

from __future__ import annotations

import ctypes
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from ctypes import wintypes
from functools import lru_cache
from pathlib import Path
from typing import Any, NoReturn

from research_swarm.domain.errors import ErrorCode, ResearchError

PTR = ctypes.c_void_p
PPTR = ctypes.POINTER(PTR)
DWORD = wintypes.DWORD
BOOL = wintypes.BOOL
HANDLE = wintypes.HANDLE
FULL_ACCESS = 0x001F01FF


class SecurityAttributes(ctypes.Structure):
    _fields_ = [("length", DWORD), ("descriptor", PTR), ("inherit", BOOL)]


class SidAndAttributes(ctypes.Structure):
    _fields_ = [("sid", PTR), ("attributes", DWORD)]


class TokenUser(ctypes.Structure):
    _fields_ = [("user", SidAndAttributes)]


class TokenOwner(ctypes.Structure):
    _fields_ = [("owner", PTR)]


class AclSize(ctypes.Structure):
    _fields_ = [("count", DWORD), ("used", DWORD), ("free", DWORD)]


class AceHeader(ctypes.Structure):
    _fields_ = [("kind", wintypes.BYTE), ("flags", wintypes.BYTE), ("size", wintypes.WORD)]


class FileInformation(ctypes.Structure):
    _fields_ = [
        ("attributes", DWORD),
        ("created", wintypes.FILETIME),
        ("accessed", wintypes.FILETIME),
        ("written", wintypes.FILETIME),
        ("volume", DWORD),
        ("size_high", DWORD),
        ("size_low", DWORD),
        ("links", DWORD),
        ("index_high", DWORD),
        ("index_low", DWORD),
    ]


def _denied(message: str, *, details: dict[str, Any] | None = None) -> ResearchError:
    return ResearchError(ErrorCode.AUTHORIZATION_DENIED, message, details=details)


@lru_cache(maxsize=1)
def _api() -> dict[str, Any]:
    if sys.platform != "win32":
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Windows ACL APIs are unavailable")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    security = ctypes.WinDLL("advapi32", use_last_error=True)
    functions: dict[str, Any] = {}
    definitions = [
        (kernel, "GetCurrentProcess", [], HANDLE),
        (kernel, "GetCurrentThread", [], HANDLE),
        (kernel, "CloseHandle", [HANDLE], BOOL),
        (kernel, "LocalFree", [PTR], PTR),
        (kernel, "CreateDirectoryW", [wintypes.LPCWSTR, ctypes.POINTER(SecurityAttributes)], BOOL),
        (kernel, "MoveFileExW", [wintypes.LPCWSTR, wintypes.LPCWSTR, DWORD], BOOL),
        (
            kernel,
            "CreateFileW",
            [
                wintypes.LPCWSTR,
                DWORD,
                DWORD,
                ctypes.POINTER(SecurityAttributes),
                DWORD,
                DWORD,
                HANDLE,
            ],
            HANDLE,
        ),
        (kernel, "GetFileInformationByHandle", [HANDLE, ctypes.POINTER(FileInformation)], BOOL),
        (
            kernel,
            "GetVolumeInformationByHandleW",
            [
                HANDLE,
                wintypes.LPWSTR,
                DWORD,
                ctypes.POINTER(DWORD),
                ctypes.POINTER(DWORD),
                ctypes.POINTER(DWORD),
                wintypes.LPWSTR,
                DWORD,
            ],
            BOOL,
        ),
        (security, "OpenProcessToken", [HANDLE, DWORD, ctypes.POINTER(HANDLE)], BOOL),
        (security, "OpenThreadToken", [HANDLE, DWORD, BOOL, ctypes.POINTER(HANDLE)], BOOL),
        (
            security,
            "DuplicateTokenEx",
            [
                HANDLE,
                DWORD,
                ctypes.POINTER(SecurityAttributes),
                ctypes.c_int,
                ctypes.c_int,
                ctypes.POINTER(HANDLE),
            ],
            BOOL,
        ),
        (security, "SetTokenInformation", [HANDLE, ctypes.c_int, PTR, DWORD], BOOL),
        (security, "SetThreadToken", [ctypes.POINTER(HANDLE), HANDLE], BOOL),
        (
            security,
            "GetTokenInformation",
            [HANDLE, ctypes.c_int, PTR, DWORD, ctypes.POINTER(DWORD)],
            BOOL,
        ),
        (security, "ConvertSidToStringSidW", [PTR, ctypes.POINTER(wintypes.LPWSTR)], BOOL),
        (
            security,
            "ConvertStringSecurityDescriptorToSecurityDescriptorW",
            [wintypes.LPCWSTR, DWORD, PPTR, ctypes.POINTER(DWORD)],
            BOOL,
        ),
        (
            security,
            "GetSecurityInfo",
            [HANDLE, ctypes.c_int, DWORD, PPTR, PPTR, PPTR, PPTR, PPTR],
            DWORD,
        ),
        (
            security,
            "GetSecurityDescriptorControl",
            [PTR, ctypes.POINTER(wintypes.WORD), ctypes.POINTER(DWORD)],
            BOOL,
        ),
        (security, "GetAclInformation", [PTR, PTR, DWORD, ctypes.c_int], BOOL),
        (security, "GetAce", [PTR, DWORD, PPTR], BOOL),
        (security, "IsValidSid", [PTR], BOOL),
        (security, "IsValidAcl", [PTR], BOOL),
    ]
    for library, name, arguments, result in definitions:
        function = getattr(library, name)
        function.argtypes = arguments
        function.restype = result
        functions[name] = function
    return functions


def _check(result: Any) -> None:
    if not result:
        if sys.platform == "win32":
            raise ctypes.WinError(ctypes.get_last_error())
        raise OSError("Windows ACL operation failed")


def _sid(pointer: Any) -> str:
    api = _api()
    if not api["IsValidSid"](pointer):
        raise _denied("Invalid SID in native security descriptor")
    text = wintypes.LPWSTR()
    _check(api["ConvertSidToStringSidW"](pointer, ctypes.byref(text)))
    try:
        assert text.value is not None
        return text.value
    finally:
        api["LocalFree"](ctypes.cast(text, PTR))


@lru_cache(maxsize=1)
def current_user_sid() -> str:
    api = _api()
    token = HANDLE()
    _check(api["OpenProcessToken"](api["GetCurrentProcess"](), 0x0008, ctypes.byref(token)))
    try:
        size = DWORD()
        api["GetTokenInformation"](token, 1, None, 0, ctypes.byref(size))
        if not 1 <= size.value <= 64 * 1024:
            raise _denied("Unexpected Windows token metadata size")
        buffer = ctypes.create_string_buffer(size.value)
        _check(api["GetTokenInformation"](token, 1, buffer, size, ctypes.byref(size)))
        return _sid(ctypes.cast(buffer, ctypes.POINTER(PTR))[0])
    finally:
        api["CloseHandle"](token)


def _token_information(token: Any, kind: int) -> Any:
    api, size = _api(), DWORD()
    api["GetTokenInformation"](token, kind, None, 0, ctypes.byref(size))
    if not 1 <= size.value <= 64 * 1024:
        raise _denied("Unexpected Windows token metadata size")
    buffer = ctypes.create_string_buffer(size.value)
    _check(api["GetTokenInformation"](token, kind, buffer, size, ctypes.byref(size)))
    return buffer


def _fatal_owner_restore() -> NoReturn:
    # A thread with un-restored authority must never return to a reusable pool.
    os._exit(125)


@contextmanager
def sqlite_creation_owner() -> Iterator[None]:
    """Give generated SQLite files the user owner, without changing process authority.

    Duplicate the effective same-user token and change only its default owner.
    Keep the scope synchronous and restore the exact prior thread token on exit.
    Existing files are never repaired or admitted through this creation scope.
    """
    if sys.platform != "win32":
        raise RuntimeError("Windows SQLite ownership scope is unavailable")
    api = _api()
    source, temporary = HANDLE(), HANDLE()
    try:
        prior = bool(
            api["OpenThreadToken"](api["GetCurrentThread"](), 0x000E, True, ctypes.byref(source))
        )
        if not prior:
            if ctypes.get_last_error() != 1008:  # ERROR_NO_TOKEN is the only fallback.
                _check(False)
            _check(
                api["OpenProcessToken"](api["GetCurrentProcess"](), 0x000A, ctypes.byref(source))
            )
        level = 2  # SecurityImpersonation for a primary source token.
        if prior:
            level_info = _token_information(source, 9)  # TokenImpersonationLevel
            level = ctypes.c_int.from_buffer(level_info).value
            if level not in (2, 3):
                raise _denied("SQLite creation requires impersonation or delegation authority")
        user_info = _token_information(source, 1)  # TokenUser
        user = TokenUser.from_buffer(user_info).user.sid
        if _sid(user) != current_user_sid():
            raise _denied("SQLite creation requires the process user's effective identity")
        # QUERY | IMPERSONATE | ADJUST_DEFAULT; the handle is not inheritable.
        _check(api["DuplicateTokenEx"](source, 0x008C, None, level, 2, ctypes.byref(temporary)))
        owner = TokenOwner(user)
        _check(api["SetTokenInformation"](temporary, 4, ctypes.byref(owner), ctypes.sizeof(owner)))
        owner_info = _token_information(temporary, 4)
        if _sid(TokenOwner.from_buffer(owner_info).owner) != current_user_sid():
            raise _denied("SQLite creation owner was not established")
        try:
            _check(api["SetThreadToken"](None, temporary))
            yield
        finally:
            if not api["SetThreadToken"](None, source if prior else None):
                _fatal_owner_restore()
    finally:
        if temporary.value:
            api["CloseHandle"](temporary)
        if source.value:
            api["CloseHandle"](source)


@contextmanager
def _security(directory: bool) -> Iterator[SecurityAttributes]:
    api = _api()
    owner = current_user_sid()
    flags = "OICI" if directory else ""
    subjects = (owner,) if owner == "S-1-5-18" else (owner, "S-1-5-18")
    sddl = "O:" + owner + "D:P" + "".join(f"(A;{flags};FA;;;{sid})" for sid in subjects)
    descriptor = PTR()
    _check(
        api["ConvertStringSecurityDescriptorToSecurityDescriptorW"](
            sddl, 1, ctypes.byref(descriptor), None
        )
    )
    try:
        yield SecurityAttributes(ctypes.sizeof(SecurityAttributes), descriptor, False)
    finally:
        api["LocalFree"](descriptor)


def verify_handle(handle: int, *, directory: bool, protected: bool) -> None:
    api = _api()
    info = FileInformation()
    _check(api["GetFileInformationByHandle"](handle, ctypes.byref(info)))
    if bool(info.attributes & 0x10) != directory or info.attributes & 0x400:
        raise _denied("Private state cannot use a reparse point or a different object type")
    if not directory and info.links != 1:
        raise _denied("Private state requires a single-link regular file")
    flags = DWORD()
    _check(
        api["GetVolumeInformationByHandleW"](
            handle, None, 0, None, None, ctypes.byref(flags), None, 0
        )
    )
    if not flags.value & 0x8:
        raise _denied("Private state requires persistent native ACLs")
    owner, dacl, descriptor = PTR(), PTR(), PTR()
    code = api["GetSecurityInfo"](
        handle,
        1,
        0x1 | 0x4,
        ctypes.byref(owner),
        None,
        ctypes.byref(dacl),
        None,
        ctypes.byref(descriptor),
    )
    if code:
        raise OSError(code, "Cannot read native security descriptor")
    try:
        user = current_user_sid()
        owner_matches = bool(owner) and _sid(owner) == user
        dacl_valid = bool(dacl) and bool(api["IsValidAcl"](dacl))
        if not owner_matches or not dacl_valid:
            raise _denied(
                "Private state has an unexpected owner or absent DACL",
                details={
                    "owner_matches": owner_matches,
                    "dacl_present": bool(dacl),
                    "dacl_valid": dacl_valid,
                },
            )
        control, revision = wintypes.WORD(), DWORD()
        _check(
            api["GetSecurityDescriptorControl"](
                descriptor, ctypes.byref(control), ctypes.byref(revision)
            )
        )
        if protected and not control.value & 0x1000:
            raise _denied("Private state DACL permits parent inheritance")
        size = AclSize()
        _check(api["GetAclInformation"](dacl, ctypes.byref(size), ctypes.sizeof(size), 2))
        subjects = {user, "S-1-5-18"}
        granted = dict.fromkeys(subjects, 0)
        inherited = dict.fromkeys(subjects, 0)
        for index in range(size.count):
            ace = PTR()
            _check(api["GetAce"](dacl, index, ctypes.byref(ace)))
            assert ace.value is not None
            header = AceHeader.from_address(ace.value)
            if header.kind != 0 or header.size < 12:
                raise _denied("Private state uses an unsupported ACL profile")
            sid = _sid(ace.value + 8)
            if sid not in subjects:
                raise _denied("Private state grants access to another account")
            mask = DWORD.from_address(ace.value + 4).value
            if not header.flags & 0x8:
                granted[sid] |= mask
            if directory:
                if header.flags & 0x4:
                    raise _denied("Private state inheritance cannot stop at the next generation")
                if header.flags & 0x3 == 0x3:
                    inherited[sid] |= mask
        if any(mask & FULL_ACCESS != FULL_ACCESS for mask in granted.values()):
            raise _denied("Private state requires owner and SYSTEM full control")
        if directory and any(mask & FULL_ACCESS != FULL_ACCESS for mask in inherited.values()):
            raise _denied("Private directory requires propagating owner and SYSTEM full control")
    finally:
        api["LocalFree"](descriptor)


@contextmanager
def verified_path(path: Path, *, directory: bool, protected: bool = False) -> Iterator[int]:
    api = _api()
    handle = api["CreateFileW"](
        str(path), 0x00020000 | 0x80, 0x3, None, 3, 0x02000000 | 0x00200000, None
    )
    if handle is None or handle == ctypes.c_void_p(-1).value:
        _check(False)
    try:
        verify_handle(int(handle), directory=directory, protected=protected)
        yield int(handle)
    finally:
        api["CloseHandle"](handle)


@contextmanager
def private_directory(path: Path, *, exclusive: bool = False) -> Iterator[None]:
    """Create missing components privately and pin the admitted runtime root."""
    if not path.parent.is_dir():
        with private_directory(path.parent):
            pass
    api = _api()
    if exclusive or not path.exists():
        with _security(True) as attributes:
            result = api["CreateDirectoryW"](str(path), ctypes.byref(attributes))
            if not result and (
                exclusive or sys.platform != "win32" or ctypes.get_last_error() != 183
            ):
                _check(result)
    with verified_path(path, directory=True, protected=True):
        yield


def create_private_descriptor(path: Path) -> int:
    if sys.platform != "win32":
        raise RuntimeError("Windows private file creation is unavailable")
    import msvcrt

    api = _api()
    with _security(False) as attributes:
        handle = api["CreateFileW"](
            str(path), 0x40000000 | 0x00020000 | 0x80, 0, ctypes.byref(attributes), 1, 0x80, None
        )
    if handle is None or handle == ctypes.c_void_p(-1).value:
        _check(False)
    try:
        verify_handle(int(handle), directory=False, protected=True)
        return msvcrt.open_osfhandle(int(handle), os.O_WRONLY | os.O_BINARY)
    except BaseException:
        api["CloseHandle"](handle)
        raise


def replace_write_through(source: Path, destination: Path) -> None:
    """Publish a closed, flushed private file without a weaker rename fallback."""
    _check(_api()["MoveFileExW"](str(source), str(destination), 0x1 | 0x8))


def verify_descriptor(descriptor: int, *, protected: bool = True) -> None:
    if sys.platform != "win32":
        raise RuntimeError("Windows private file verification is unavailable")
    import msvcrt

    verify_handle(msvcrt.get_osfhandle(descriptor), directory=False, protected=protected)
