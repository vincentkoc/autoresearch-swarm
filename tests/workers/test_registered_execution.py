from __future__ import annotations

import json
import subprocess
import sys
import venv
from pathlib import Path
from typing import Any

import pytest

from research_swarm.domain.errors import ResearchError
from research_swarm.domain.experiments import ResourceRequirements
from research_swarm.engines import execution
from research_swarm.engines.models import DeviceObservation
from research_swarm.engines.registration import EnvironmentRegistration, register_environment
from research_swarm.workers.supervision import base_environment
from tests.workers.test_engines import descriptor


def registration(tmp_path: Path, *, isolated: bool = False) -> EnvironmentRegistration:
    interpreter = Path(sys.executable)
    if isolated:
        environment = tmp_path / "python-environment"
        venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
        interpreter = environment / "bin/python"
    return register_environment(
        tmp_path / "registration" / "engine.json",
        profile="python-cpu",
        executable=interpreter,
        environment_contracts=("sha256:" + "a" * 64,),
        probe_directory=tmp_path / "probe",
    )


@pytest.mark.parametrize(
    "options",
    [
        {"accelerator_count": 0},
        {"accelerator_count": 2},
        {"accelerator_count": 1, "exclusive_device": False},
        {"accelerator_count": 1, "memory_bytes": 700, "accelerator_memory_bytes": 700},
    ],
)
def test_accelerator_admission_refuses_sharing_and_double_counted_memory(
    tmp_path: Path,
    options: dict[str, Any],
) -> None:
    registered = registration(tmp_path).model_copy(
        update={
            "descriptor": descriptor(
                profile="mlx-metal",
                framework="mlx",
                device_backend="metal",
                framework_version="fixture",
                device=DeviceObservation(
                    index=0, name="Fixture", physical_id="apple-integrated-0", unified_memory=True
                ),
            ),
            "memory_bytes": 1024,
        }
    )
    with pytest.raises(ResearchError):
        execution.demand(ResourceRequirements(engine_profile="mlx-metal", **options), registered)


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        (
            (
                "-c",
                "import os,sys,json,__main__; "
                "assert 'RESEARCH_PARENT_ONLY' not in os.environ; "
                "print(json.dumps([sys.argv,sys.flags.isolated,hasattr(__main__,'payload')]))",
                "tail",
            ),
            [["-c", "tail"], 1, False],
        ),
        (("-m", "project_module", "tail"), ["tail"]),
        (("project_script.py", "tail"), ["tail"]),
    ],
)
def test_registered_launch_python_invocations(
    tmp_path: Path,
    arguments: tuple[str, ...],
    expected: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RESEARCH_PARENT_ONLY", "synthetic-parent-value")
    registered = registration(tmp_path, isolated=True)
    frozen = execution.prepare_engine(tmp_path / "frozen-engine", registered)
    project = tmp_path / "project"
    project.mkdir()
    code = (
        "import os,json,sys\n"
        "assert 'RESEARCH_PARENT_ONLY' not in os.environ\n"
        "print(json.dumps(sys.argv[1:]))\n"
    )
    (project / "project_module.py").write_text(code)
    (project / "project_script.py").write_text(code)
    process = subprocess.run(
        [*frozen, *arguments],
        cwd=project,
        env=base_environment(),
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert process.returncode == 0, process.stderr
    assert json.loads(process.stdout) == expected


def test_registered_script_argv_and_inline_dynamic_import_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RESEARCH_PARENT_ONLY", "synthetic-parent-value")
    registered = registration(tmp_path, isolated=True)
    frozen = execution.prepare_engine(tmp_path / "frozen-engine", registered)
    project = tmp_path / "project"
    nested = project / "nested"
    nested.mkdir(parents=True)
    (project / "script.py").write_text(
        "import os,sys; assert 'RESEARCH_PARENT_ONLY' not in os.environ; print(sys.argv[0])"
    )
    (nested / "after_chdir.py").write_text("VALUE = 41")

    def launch(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [*frozen, *arguments],
            cwd=project,
            env=base_environment(),
            capture_output=True,
            text=True,
            timeout=20,
        )

    script = launch("script.py")
    assert script.returncode == 0, script.stderr
    assert script.stdout.strip() == "script.py"
    inline = launch(
        "-c",
        "import os; assert 'RESEARCH_PARENT_ONLY' not in os.environ; "
        "os.chdir('nested'); import after_chdir; print(after_chdir.VALUE)",
    )
    assert inline.returncode == 0, inline.stderr
    assert inline.stdout.strip() == "41"
