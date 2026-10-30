"""Bounded native probing of an explicitly selected Python environment."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import field_validator

from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.types import VersionedRecord
from research_swarm.engines.custody import probe_custody
from research_swarm.engines.models import EngineDescriptor, ProbeReport, ProbeStatus
from research_swarm.engines.registry import engine_definition
from research_swarm.projects.workload import absolute_path
from research_swarm.workers.contracts import NativeCommand
from research_swarm.workers.supervision import base_environment, run_native

PROBE_OUTPUT_LIMIT = 512 * 1024


class InterpreterIdentity(VersionedRecord):
    executable: str
    prefix: str
    base_prefix: str
    _paths = field_validator("executable", "prefix", "base_prefix")(absolute_path)


@dataclass(frozen=True)
class ProbeExecution:
    report: ProbeReport
    identity: InterpreterIdentity | None = None


def _source() -> str:
    return Path(__file__).with_name("_probe.py").read_text(encoding="utf-8")


def _report(profile: str, payload: Any) -> ProbeExecution:
    if not isinstance(payload, dict) or set(payload) - {"status", "descriptor", "identity"}:
        raise ValueError("Invalid probe envelope")
    descriptor = (
        EngineDescriptor.model_validate(payload["descriptor"]) if "descriptor" in payload else None
    )
    report = ProbeReport(
        profile=profile,
        status=payload["status"],
        descriptor=descriptor,
        descriptor_digest=descriptor.fingerprint if descriptor is not None else None,
    )
    identity = InterpreterIdentity.model_validate(payload["identity"]) if descriptor else None
    if descriptor is None and "identity" in payload:
        raise ValueError("Failed probe contains interpreter identity")
    return ProbeExecution(report, identity)


def observe_engine(
    profile: str,
    *,
    executable: Path | None = None,
    device_index: int = 0,
    timeout_ms: int = 15000,
    probe_directory: Path | None = None,
) -> ProbeExecution:
    """No installs, tensors, controller state, or caller environment secrets are involved."""
    engine_definition(profile)
    if type(device_index) is not int or not 0 <= device_index < 1024:
        raise ValueError("Device index must be a bounded nonnegative integer")
    if type(timeout_ms) is not int or not 100 <= timeout_ms <= 60000:
        raise ValueError("Probe deadline must be between 100 ms and 60 seconds")
    interpreter = (executable or Path(sys.executable)).expanduser().absolute()
    status: ProbeStatus = "probe_failed"
    # Fail known pre-launch checks before recording uncertain creation custody.
    try:
        if not interpreter.is_file() or not os.access(interpreter, os.X_OK):
            return ProbeExecution(ProbeReport(profile=profile, status=status))
        payload = _source()
    except (OSError, ValueError):
        return ProbeExecution(ProbeReport(profile=profile, status=status))
    with probe_custody(profile, probe_directory) as custody:
        root = custody.work
        try:
            outcome = run_native(
                NativeCommand(
                    argv=(str(interpreter), "-I", "-c", payload, profile, str(device_index)),
                    cwd=str(root),
                    timeout_ms=timeout_ms,
                    grace_ms=100,
                    cleanup_ms=2000,
                    output_limit_bytes=PROBE_OUTPUT_LIMIT,
                ),
                root / "output",
                environment=base_environment(),
                on_held=custody.held,
            )
        except BaseException:
            raise custody.unknown() from None
        custody.observed(outcome)
        try:
            if (
                outcome.stdout.observed_bytes > PROBE_OUTPUT_LIMIT
                or outcome.stderr.observed_bytes > PROBE_OUTPUT_LIMIT
            ):
                status = "output_limit"
            elif outcome.reason in {"deadline", "not_started"}:
                status = "timeout"
            elif (
                outcome.reason == "exited"
                and outcome.exit_code == 0
                and outcome.closure.scope_closed
                and outcome.stdout.eof
                and outcome.stderr.eof
            ):
                return _report(
                    profile, strict_json_loads((root / "output" / "stdout").read_bytes())
                )
        except (OSError, ValueError, KeyError, RecursionError, ResearchError):
            pass
    return ProbeExecution(ProbeReport(profile=profile, status=status))


def probe_engine(
    profile: str,
    *,
    executable: Path | None = None,
    device_index: int = 0,
    timeout_ms: int = 15000,
    probe_directory: Path | None = None,
) -> ProbeReport:
    """Return public observations without local interpreter paths or diagnostics."""
    return observe_engine(
        profile,
        executable=executable,
        device_index=device_index,
        timeout_ms=timeout_ms,
        probe_directory=probe_directory,
    ).report
