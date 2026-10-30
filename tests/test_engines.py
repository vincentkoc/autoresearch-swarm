from __future__ import annotations

import json
import os
import subprocess
import sys
import venv
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psutil
import pytest
from pydantic import ValidationError

from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.engines import (
    ENGINE_DEFINITIONS,
    _probe,
    custody,
    probe,
    probe_engine,
    registration,
)
from research_swarm.engines.models import DeviceObservation, EngineDescriptor, PackageVersion
from research_swarm.engines.probe import InterpreterIdentity, ProbeExecution
from research_swarm.engines.registration import (
    EnvironmentRegistration,
    ExecutionPolicy,
    read_registration,
    register_environment,
    verify_registration,
)
from research_swarm.runtime import write_private

CONTRACT = content_digest(b"synthetic environment contract")


@pytest.fixture(autouse=True)
def private_probe_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "probe-state"
    monkeypatch.setattr(custody, "default_probe_directory", lambda: root)
    monkeypatch.setattr(registration, "default_probe_directory", lambda: root)


def descriptor(**updates: Any) -> EngineDescriptor:
    return EngineDescriptor(
        **{
            "profile": "python-cpu",
            "framework": "python",
            "device_backend": "cpu",
            "system": "Synthetic",
            "release": "1",
            "machine": "test64",
            "cpu_model": "Fixture processor",
            "cpu_count": 8,
            "python_version": "3.12.0",
            "python_implementation": "cpython",
            "python_cache_tag": "cpython-312",
            "packages": (),
            **updates,
        }
    )


def test_registry_imports_no_optional_framework_and_separates_layers() -> None:
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import research_swarm.engines; "
            "assert not any(x in sys.modules for x in ('torch', 'mlx', 'numpy'))",
        ],
        capture_output=True,
        timeout=10,
    )
    assert process.returncode == 0, process.stderr
    assert {(item.framework, item.device_backend) for item in ENGINE_DEFINITIONS} == {
        ("python", "cpu"),
        ("pytorch", "cuda"),
        ("pytorch", "rocm"),
        ("pytorch", "mps"),
        ("mlx", "metal"),
    }


def test_real_cpu_probe_is_path_free_and_does_not_claim_workload_qualification() -> None:
    observation = probe_engine("python-cpu")
    assert observation.status == "available"
    assert observation.descriptor is not None
    assert observation.descriptor.python_version == sys.version.split()[0]
    assert observation.descriptor_digest == observation.descriptor.fingerprint
    assert observation.qualification == "environment_probe"
    assert str(Path(sys.executable)) not in observation.model_dump_json()
    assert str(Path.home()) not in observation.model_dump_json()


@pytest.mark.parametrize(
    ("script", "expected"),
    [
        ("print('not json')", "probe_failed"),
        ('print(\'{"status":"available"}\')', "probe_failed"),
        ('print(\'{"status":"dependency_missing","secret":"hidden"}\')', "probe_failed"),
        ('print(\'{"status":"dependency_missing"}\')', "dependency_missing"),
        ("import sys; sys.stderr.write('private diagnostic'); raise SystemExit(1)", "probe_failed"),
        ("import time; time.sleep(10)", "timeout"),
        ("print('x' * 600000)", "output_limit"),
        ("import sys; sys.stderr.write('x' * 600000)", "output_limit"),
    ],
)
def test_native_probe_bounds_and_opaque_errors(
    monkeypatch: pytest.MonkeyPatch,
    script: str,
    expected: str,
) -> None:
    monkeypatch.setattr(probe, "_source", lambda: script)
    result = probe_engine("python-cpu", timeout_ms=1000)
    assert result.status == expected
    assert result.descriptor is None
    assert "private diagnostic" not in result.model_dump_json()


def test_unresolved_probe_preserves_custody_and_blocks_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    native = probe.run_native
    calls = 0

    def unknown(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        outcome = native(*args, **kwargs)
        return outcome.model_copy(
            update={
                "closure": outcome.closure.model_copy(
                    update={
                        "scope_closed": False,
                        "findings": ("Synthetic unresolved closure",),
                    }
                )
            }
        )

    monkeypatch.setattr(probe, "run_native", unknown)
    for profile in ("python-cpu", "pytorch-cuda"):
        with pytest.raises(ResearchError) as failure:
            probe_engine(profile)
        assert failure.value.code == ErrorCode.EXECUTION_UNKNOWN
    assert calls == 1
    record = custody.ProbeRecord.model_validate_json(
        (tmp_path / "probe-state" / "pending.json").read_bytes()
    )
    assert record.native is not None and record.outcome is not None
    assert not record.outcome.closure.scope_closed
    assert (tmp_path / "probe-state" / record.id / "output" / "stdout").exists()
    assert record.outcome.closure.findings == ("Synthetic unresolved closure",)


def test_probe_interruption_before_identity_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def lost_creation(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("Synthetic lost creation acknowledgment")

    monkeypatch.setattr(probe, "run_native", lost_creation)
    for _ in range(2):
        with pytest.raises(ResearchError) as failure:
            probe_engine("python-cpu")
        assert failure.value.code == ErrorCode.EXECUTION_UNKNOWN


def test_missing_interpreter_does_not_poison_probe_custody(tmp_path: Path) -> None:
    assert probe_engine("python-cpu", executable=tmp_path / "absent").status == "probe_failed"
    assert not (tmp_path / "probe-state" / "pending.json").exists()
    assert probe_engine("python-cpu").status == "available"


def test_missing_probe_payload_does_not_create_custody(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def missing() -> str:
        raise FileNotFoundError("Synthetic missing probe payload")

    monkeypatch.setattr(probe, "_source", missing)
    assert probe_engine("python-cpu").status == "probe_failed"
    assert not (tmp_path / "probe-state" / "pending.json").exists()


def test_closed_probe_cleanup_recovers_without_unknown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cleanup = custody.ProbeCustody.cleanup
    calls = 0

    def interrupted(self: custody.ProbeCustody) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("Synthetic interrupted cleanup")
        cleanup(self)

    monkeypatch.setattr(custody.ProbeCustody, "cleanup", interrupted)
    with pytest.raises(OSError, match="interrupted cleanup"):
        probe_engine("python-cpu")
    assert probe_engine("python-cpu").status == "available"
    assert not (tmp_path / "probe-state" / "pending.json").exists()


def test_probe_cannot_inherit_secrets_or_visibility(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RESEARCH_TEST_SECRET", "private")
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "hidden")
    monkeypatch.setenv("PYTHONPATH", "invalid-path")
    script = (
        "import os, json; "
        "assert not any(k in os.environ for k in "
        "('RESEARCH_TEST_SECRET','CUDA_VISIBLE_DEVICES','PYTHONPATH')); "
        "print(json.dumps({'status':'backend_unavailable'}))"
    )
    monkeypatch.setattr(probe, "_source", lambda: script)
    assert probe_engine("python-cpu").status == "backend_unavailable"


def test_probe_deadline_closes_its_descendant(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    marker = tmp_path / "probe-child.pid"
    script = (
        "import subprocess, sys, pathlib, time; "
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
        f"pathlib.Path({str(marker)!r}).write_text(str(child.pid)); time.sleep(30)"
    )
    monkeypatch.setattr(probe, "_source", lambda: script)
    assert probe_engine("python-cpu", timeout_ms=2000).status == "timeout"
    pid = int(marker.read_text())
    try:
        process = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    assert not process.is_running() or process.status() == psutil.STATUS_ZOMBIE


def fake_torch(*, hip: str | None = None, cuda: str | None = "12.8") -> Any:
    return SimpleNamespace(
        version=SimpleNamespace(hip=hip, cuda=cuda),
        __version__="synthetic",
        cuda=SimpleNamespace(
            is_available=lambda: True,
            device_count=lambda: 1,
            get_device_properties=lambda _: SimpleNamespace(
                name="Fixture GPU",
                major=8,
                minor=0,
                gcnArchName="gfx000",
                uuid="fixture-physical-gpu",
                total_memory=1024,
            ),
        ),
        backends=SimpleNamespace(
            mps=SimpleNamespace(
                is_built=lambda: True,
                is_available=lambda: True,
            )
        ),
    )


def test_cuda_and_rocm_share_api_but_not_build_identity() -> None:
    cuda, hip = fake_torch(), fake_torch(hip="7", cuda=None)
    assert _probe.torch_device(cuda, "rocm", 0) is None
    assert _probe.torch_device(hip, "cuda", 0) is None
    assert _probe.torch_device(cuda, "cuda", 1) is None
    assert _probe.torch_device(cuda, "cuda", 0)["architecture"] == "sm_80"
    assert _probe.torch_device(hip, "rocm", 0)["architecture"] == "gfx000"
    hip.cuda.is_available = lambda: False
    assert _probe.torch_device(hip, "rocm", 0) is None


def test_mps_requires_built_and_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_probe, "cpu_model", lambda: "Fixture Apple processor")
    torch = fake_torch()
    assert _probe.torch_device(torch, "mps", 0)["physical_id"] == "apple-integrated-0"
    torch.backends.mps.is_built = lambda: False
    assert _probe.torch_device(torch, "mps", 0) is None
    torch.backends.mps.is_built = lambda: True
    torch.backends.mps.is_available = lambda: False
    assert _probe.torch_device(torch, "mps", 0) is None


def test_mlx_requires_actual_metal_and_records_shared_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_probe.sys, "platform", "darwin")
    monkeypatch.setattr(_probe.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(_probe, "cpu_model", lambda: "Fixture Apple processor")
    monkeypatch.setattr(_probe, "inventory", lambda: [])
    monkeypatch.setattr(_probe.importlib.util, "find_spec", lambda _: object())
    monkeypatch.setattr(_probe.importlib.metadata, "version", lambda _: "synthetic")
    metal = SimpleNamespace(
        is_available=lambda: False, device_info=lambda: {"device_name": "Fixture"}
    )
    monkeypatch.setattr(_probe.importlib, "import_module", lambda _: SimpleNamespace(metal=metal))
    assert _probe.observe("mlx-metal", 0) == {"status": "backend_unavailable"}
    metal.is_available = lambda: True
    observation = _probe.observe("mlx-metal", 0)
    assert observation["descriptor"]["device"]["physical_id"] == "apple-integrated-0"
    assert observation["descriptor"]["device"]["unified_memory"]
    assert _probe.observe("mlx-metal", 1) == {"status": "backend_unavailable"}


def test_missing_dependency_and_unsupported_platform(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_probe.importlib.util, "find_spec", lambda _: None)
    assert _probe.observe("pytorch-cuda", 0) == {"status": "dependency_missing"}
    monkeypatch.setattr(_probe.sys, "platform", "linux")
    assert _probe.observe("pytorch-mps", 0) == {"status": "platform_unsupported"}


@pytest.fixture
def cpu_registration(tmp_path: Path) -> EnvironmentRegistration:
    return register_environment(
        tmp_path / "registration" / "environment.json",
        profile="python-cpu",
        environment_contracts=(CONTRACT,),
    )


def test_registration_restart_contention_and_changed_inputs(tmp_path: Path) -> None:
    target = tmp_path / "registration" / "environment.json"
    with ThreadPoolExecutor(max_workers=3) as pool:
        rows = list(
            pool.map(
                lambda _: register_environment(
                    target,
                    profile="python-cpu",
                    environment_contracts=(CONTRACT,),
                ),
                range(3),
            )
        )
    assert rows[0] == rows[1] == rows[2] == read_registration(target)
    original = target.read_bytes()
    with pytest.raises(ResearchError) as failure:
        register_environment(
            target,
            profile="python-cpu",
            environment_contracts=(content_digest(b"other"),),
        )
    assert failure.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert target.read_bytes() == original
    rows[0].require_environment(CONTRACT)
    with pytest.raises(ResearchError):
        rows[0].require_environment(content_digest(b"other"))


def test_environment_drift_rejected_without_rewriting_registration(
    cpu_registration: EnvironmentRegistration,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = registration.observe_engine

    def changed(*args: Any, **kwargs: Any) -> ProbeExecution:
        value = original(*args, **kwargs)
        observed = value.report.descriptor.model_copy(update={"release": "changed"})
        return ProbeExecution(
            value.report.model_copy(
                update={
                    "descriptor": observed,
                    "descriptor_digest": observed.fingerprint,
                }
            ),
            value.identity,
        )

    monkeypatch.setattr(registration, "observe_engine", changed)
    with pytest.raises(ResearchError, match="changed"):
        verify_registration(cpu_registration)


def test_selected_virtual_environment_is_not_resolved_to_base(tmp_path: Path) -> None:
    root = tmp_path / "isolated-python"
    venv.EnvBuilder(with_pip=False, symlinks=os.name != "nt").create(root)
    interpreter = root / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    registered = register_environment(
        tmp_path / "registration" / "environment.json",
        profile="python-cpu",
        environment_contracts=(CONTRACT,),
        executable=interpreter,
    )
    assert registered.executable == str(interpreter)
    assert registered.interpreter.prefix == str(root)
    assert registered.descriptor.packages == ()
    verify_registration(registered)


def test_physical_pool_ignores_framework_ordinal_and_capacity(
    cpu_registration: EnvironmentRegistration,
) -> None:
    shared = DeviceObservation(
        index=0,
        name="Fixture Apple",
        physical_id="apple-integrated-0",
        unified_memory=True,
    )
    mps = cpu_registration.model_copy(
        update={
            "descriptor": descriptor(
                profile="pytorch-mps",
                framework="pytorch",
                device_backend="mps",
                framework_version="fixture",
                device=shared,
            ),
            "memory_bytes": 4096,
        }
    )
    mlx = cpu_registration.model_copy(
        update={
            "descriptor": descriptor(
                profile="mlx-metal",
                framework="mlx",
                device_backend="metal",
                framework_version="different",
                device=shared.model_copy(update={"index": 1}),
            ),
            "memory_bytes": 8192,
        }
    )
    assert mps.device_pool == mlx.device_pool
    assert mps.profile.execution_fingerprint != mlx.profile.execution_fingerprint
    assert mps.profile.accelerator_memory_bytes == mps.profile.memory_bytes
    unknown = mps.model_copy(
        update={
            "descriptor": mps.descriptor.model_copy(
                update={
                    "device": shared.model_copy(update={"physical_id": None}),
                }
            )
        }
    )
    with pytest.raises(ResearchError, match="unresolved"):
        _ = unknown.device_pool


def test_fingerprint_omits_paths_contracts_and_includes_policy(
    cpu_registration: EnvironmentRegistration,
    tmp_path: Path,
) -> None:
    other = cpu_registration.model_copy(
        update={
            "executable": str(tmp_path / "another-python"),
            "environment_contracts": (content_digest(b"other"),),
            "interpreter": InterpreterIdentity(
                executable=str(tmp_path / "other"),
                prefix=str(tmp_path),
                base_prefix=str(tmp_path),
            ),
        }
    )
    assert other.profile == cpu_registration.profile
    changed = cpu_registration.model_copy(update={"policy": ExecutionPolicy(precision="fp32")})
    assert changed.profile.execution_fingerprint != cpu_registration.profile.execution_fingerprint
    public = canonical_bytes(cpu_registration.public_description())
    assert cpu_registration.executable.encode() not in public
    assert cpu_registration.lock_qualification == "operator_authorized_unverified"


def test_descriptor_rejects_ambiguous_packages_and_profile() -> None:
    assert (
        descriptor().fingerprint
        == "sha256:c506f0f7b95345fe55ff6696c88b0dbb54adc84b7fc1c623365d6b3ecc475dcb"
    )
    with pytest.raises(ValidationError):
        descriptor(
            packages=(
                PackageVersion(name="x", version="1"),
                PackageVersion(name="x", version="2"),
            )
        )
    with pytest.raises(ValidationError):
        descriptor(framework="mlx")


def test_private_corruption_error_does_not_echo_paths(
    cpu_registration: EnvironmentRegistration,
    tmp_path: Path,
) -> None:
    target = tmp_path / "registration" / "environment.json"
    write_private(target, b'{"executable":"private-invalid-value"}')
    with pytest.raises(ResearchError) as failure:
        read_registration(target)
    assert "private-invalid-value" not in str(failure.value)


def test_cli_probe_registration_and_verification(tmp_path: Path) -> None:
    target = tmp_path / "registration" / "environment.json"

    def run(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "research_swarm.cli", "engine", *arguments],
            capture_output=True,
            text=True,
            timeout=30,
        )

    listed = run("list")
    assert listed.returncode == 0, listed.stderr
    assert len(json.loads(listed.stdout)["profiles"]) == 5
    probed = run("probe", "python-cpu", "--probe-dir", str(tmp_path / "probe-state"))
    assert probed.returncode == 0, probed.stderr
    registered = run(
        "register",
        "python-cpu",
        "--file",
        str(target),
        "--environment-contract",
        CONTRACT,
        "--probe-dir",
        str(tmp_path / "probe-state"),
    )
    assert registered.returncode == 0, registered.stderr
    verified = run("verify", "--file", str(target))
    assert verified.returncode == 0, verified.stderr
    assert json.loads(registered.stdout) == json.loads(verified.stdout)
    assert str(tmp_path) not in registered.stdout + verified.stdout


@pytest.mark.skipif(os.name == "nt", reason="POSIX special-file probe")
def test_interpreter_fifo_is_refused_before_read(tmp_path: Path) -> None:
    target = tmp_path / "interpreter"
    os.mkfifo(target)
    with pytest.raises(ValueError, match="regular files"):
        registration.executable_digest(target)
