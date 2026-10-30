"""Frozen, stdlib-only registration gate executed inside the admitted native scope."""

from __future__ import annotations

import hashlib
import importlib.machinery
import json
import os
import runpy
import sys
import types
from pathlib import Path
from typing import Any


def digest(path: str, maximum: int) -> str:
    value = hashlib.sha256()
    total = 0
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            total += len(chunk)
            if total > maximum:
                raise ValueError("Registered file exceeds its bound")
            value.update(chunk)
    return "sha256:" + value.hexdigest()


def normalized(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: normalized(item)
            for key, item in value.items()
            if item is not None
            and not (key == "memory_bytes" and item == 0)
            and not (key == "unified_memory" and item is False)
        }
    if isinstance(value, (tuple, list)):
        return [normalized(item) for item in value]
    return value


def verify(payload: dict[str, Any]) -> None:
    if digest(sys.executable, 1024 * 1024 * 1024) != payload["executable_digest"]:
        raise ValueError("Registered interpreter binary differs")
    actual_identity = {
        "executable": sys.executable,
        "prefix": sys.prefix,
        "base_prefix": sys.base_prefix,
    }
    if actual_identity != payload["interpreter"]:
        raise ValueError("Registered interpreter identity differs")
    source = payload["probe"]
    if digest(source["path"], 1024 * 1024) != source["digest"]:
        raise ValueError("Frozen probe differs")
    functions = runpy.run_path(source["path"], run_name="registered_environment_probe")
    observed = functions["observe"](payload["profile"], payload["logical_device_index"])
    if observed["status"] != "available":
        raise ValueError("Registered execution environment is unavailable")
    actual = observed["descriptor"]
    if "device" in actual:
        # The admitted environment masks one physical device as logical device zero.
        # Other fields, including its physical identity, must still match exactly.
        actual["device"]["index"] = payload["descriptor"]["device"]["index"]
    if normalized(actual) != normalized(payload["descriptor"]):
        raise ValueError("Registered execution environment changed")


def launch(arguments: list[str]) -> None:
    # -I isolates the registration check. Only after it passes does project code
    # receive the ordinary script/module import path. No shell or activation script.
    if arguments[0] == "-m":
        sys.path.insert(0, os.getcwd())
        sys.argv = arguments[1:]
        runpy.run_module(arguments[1], run_name="__main__", alter_sys=True)
    elif arguments[0] == "-c":
        sys.path.insert(0, "")
        sys.argv = ["-c", *arguments[2:]]
        module = types.ModuleType("__main__")
        module.__package__ = None
        module.__spec__ = None
        sys.modules["__main__"] = module
        exec(compile(arguments[1], "<string>", "exec"), module.__dict__)
    else:
        script = str(Path(arguments[0]).absolute())
        sys.path.insert(0, str(Path(script).parent))
        sys.argv = arguments
        module = types.ModuleType("__main__")
        module.__file__ = script
        module.__package__ = None
        module.__spec__ = None
        module.__loader__ = importlib.machinery.SourceFileLoader("__main__", script)
        sys.modules["__main__"] = module
        exec(compile(Path(script).read_bytes(), script, "exec"), module.__dict__)


def main() -> None:
    try:
        with Path(sys.argv[1]).open(encoding="utf-8") as stream:
            text = stream.read(1024 * 1024 + 1)
        if len(text) > 1024 * 1024:
            raise ValueError("Launch binding exceeds its bound")
        payload = json.loads(text)
        verify(payload)
    except Exception:
        print("Registered environment verification failed", file=sys.stderr)
        raise SystemExit(78) from None
    launch(sys.argv[2:])


if __name__ == "__main__":
    main()
