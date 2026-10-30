"""Executed with the selected interpreter; this file depends only on its stdlib."""

from __future__ import annotations

import importlib
import importlib.metadata
import importlib.util
import json
import os
import platform
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


def cpu_model() -> str:
    if sys.platform == "darwin":
        result = subprocess.run(
            ["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True,
            text=True,
            timeout=2,
            check=True,
        )
        return result.stdout.strip()
    if sys.platform.startswith("linux"):
        with Path("/proc/cpuinfo").open(encoding="utf-8") as stream:
            for line in stream.read(65536).splitlines():
                if line.partition(":")[0].strip() in {"model name", "Hardware"}:
                    return line.partition(":")[2].strip()
    return platform.processor() or "unobserved"


def inventory() -> list[dict[str, str]]:
    packages: list[dict[str, str]] = []
    for distribution in importlib.metadata.distributions():
        if len(packages) >= 4096:
            raise ValueError("Package inventory exceeds the bound")
        name = re.sub(r"[-_.]+", "-", distribution.metadata["Name"]).lower()
        packages.append({"name": name, "version": distribution.version})
    return sorted(packages, key=lambda item: (item["name"], item["version"]))


def torch_device(torch: Any, backend: str, index: int) -> dict[str, Any] | None:
    if backend == "mps":
        if not torch.backends.mps.is_built() or not torch.backends.mps.is_available() or index:
            return None
        return {
            "index": 0,
            "name": cpu_model(),
            "physical_id": "apple-integrated-0",
            "unified_memory": True,
        }
    # ROCm deliberately implements the torch.cuda API. Build identity disambiguates it.
    hip = getattr(torch.version, "hip", None)
    cuda = getattr(torch.version, "cuda", None)
    if (backend == "rocm" and not hip) or (backend == "cuda" and (hip or not cuda)):
        return None
    if not torch.cuda.is_available() or index >= torch.cuda.device_count():
        return None
    properties = torch.cuda.get_device_properties(index)
    identity = getattr(properties, "uuid", None)
    if identity is not None:
        identity = str(identity)
    architecture = (
        str(getattr(properties, "gcnArchName", "unobserved"))
        if backend == "rocm"
        else f"sm_{properties.major}{properties.minor}"
    )
    return {
        "index": index,
        "name": properties.name,
        "architecture": architecture,
        "physical_id": identity,
        "memory_bytes": properties.total_memory,
    }


def observe(profile: str, index: int) -> dict[str, Any]:
    framework, backend, module = {
        "python-cpu": ("python", "cpu", None),
        "pytorch-cuda": ("pytorch", "cuda", "torch"),
        "pytorch-rocm": ("pytorch", "rocm", "torch"),
        "pytorch-mps": ("pytorch", "mps", "torch"),
        "mlx-metal": ("mlx", "metal", "mlx.core"),
    }[profile]
    if backend in {"mps", "metal"} and (
        sys.platform != "darwin" or platform.machine().lower() not in {"arm64", "aarch64"}
    ):
        return {"status": "platform_unsupported"}
    if module is not None and importlib.util.find_spec(module.split(".")[0]) is None:
        return {"status": "dependency_missing"}
    descriptor: dict[str, Any] = {
        "profile": profile,
        "framework": framework,
        "device_backend": backend,
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "cpu_model": cpu_model(),
        "cpu_count": os.cpu_count() or 1,
        "python_version": platform.python_version(),
        "python_implementation": sys.implementation.name,
        "python_cache_tag": sys.implementation.cache_tag or "unobserved",
        "packages": inventory(),
    }
    if framework == "pytorch":
        torch = importlib.import_module("torch")
        device = torch_device(torch, backend, index)
        if device is None:
            return {"status": "backend_unavailable"}
        descriptor.update(
            framework_version=str(torch.__version__),
            device=device,
            runtime_version=(
                getattr(torch.version, "hip", None)
                if backend == "rocm"
                else getattr(torch.version, "cuda", None)
                if backend == "cuda"
                else None
            ),
        )
    elif framework == "mlx":
        mx = importlib.import_module("mlx.core")
        if index or not mx.metal.is_available():
            return {"status": "backend_unavailable"}
        info = mx.metal.device_info()
        descriptor.update(
            framework_version=importlib.metadata.version("mlx"),
            device={
                "index": 0,
                "name": info["device_name"],
                "architecture": str(info.get("architecture", "unobserved")),
                "physical_id": "apple-integrated-0",
                "unified_memory": True,
            },
        )
    elif index:
        return {"status": "backend_unavailable"}
    return {"status": "available", "descriptor": descriptor}


def main() -> None:
    try:
        result = observe(sys.argv[1], int(sys.argv[2]))
        if result["status"] == "available":
            result["identity"] = {
                "executable": sys.executable,
                "prefix": sys.prefix,
                "base_prefix": sys.base_prefix,
            }
    except Exception:
        # Import errors can contain paths, credentials, or arbitrary package output.
        result = {"status": "probe_failed"}
    print(json.dumps(result, allow_nan=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
