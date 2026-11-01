"""Opt-in qualification of the installed wheel, outside the source checkout."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from mcp import Client, StdioServerParameters

from scripts.release import digest, prebuilt_wheel, source_identity
from tests.api.test_cli_mcp import process_environment
from tests.integration.test_agent_research_cycle import command, context, record, tool
from tests.paths import REPO_ROOT

pytestmark = pytest.mark.skipif(
    os.environ.get("RESEARCH_PACKAGE_TESTS") != "1",
    reason="Installed-wheel qualification is explicitly enabled in its CI job",
)


@pytest.fixture
def package_root(tmp_path: Path) -> Iterator[Path]:
    with TemporaryDirectory(prefix="wheel-proof-", dir=tmp_path) as directory:
        yield Path(directory)


async def test_fresh_wheel_runs_native_research_and_mcp_without_source_imports(
    package_root: Path,
) -> None:
    uv = shutil.which("uv")
    assert uv is not None, "Package qualification requires uv"
    repository = REPO_ROOT
    neutral = package_root / "outside-checkout"
    neutral.mkdir()
    environment = {**process_environment(), "UV_NO_PROGRESS": "1", "UV_PYTHON_DOWNLOADS": "never"}

    def run(arguments: list[str], *, cwd: Path = neutral, timeout: int = 120) -> str:
        result = subprocess.run(
            arguments,
            cwd=cwd,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
        )
        assert result.returncode == 0, result.stderr[-8000:]
        return result.stdout

    constraints = package_root / "runtime-constraints.txt"
    run(
        [
            uv,
            "export",
            "--locked",
            "--no-dev",
            "--no-emit-project",
            "--no-hashes",
            "--format",
            "requirements-txt",
            "--output-file",
            str(constraints),
        ],
        cwd=repository,
    )
    wheel = prebuilt_wheel(os.environ)
    if wheel is None:
        distributions = package_root / "dist"
        run(
            [uv, "build", "--wheel", "--python", sys.executable, "--out-dir", str(distributions)],
            cwd=repository,
        )
        wheels = list(distributions.glob("*.whl"))
        assert len(wheels) == 1
        wheel = wheels[0]
    expected_digest = digest(wheel)
    expected_version = source_identity(repository)["version"]
    installed = package_root / "installed"
    run([uv, "venv", "--python", sys.executable, str(installed)])
    scripts = installed / ("Scripts" if sys.platform == "win32" else "bin")
    python = scripts / ("python.exe" if sys.platform == "win32" else "python")
    research = scripts / ("research.exe" if sys.platform == "win32" else "research")
    run(
        [
            uv,
            "pip",
            "install",
            "--python",
            str(python),
            "--constraints",
            str(constraints),
            str(wheel),
        ]
    )
    origin = json.loads(
        run(
            [
                str(python),
                "-I",
                "-c",
                (
                    "import hashlib,json,importlib.util,research_swarm; "
                    "from importlib.metadata import distribution; "
                    "d=distribution('autoresearch-swarm'); "
                    "print(json.dumps({'module':research_swarm.__file__,'version':d.version,"
                    "'module_version':research_swarm.__version__,"
                    "'files':{str(p):hashlib.sha256(d.locate_file(p).read_bytes()).hexdigest() "
                    "for p in d.files if str(p).startswith('research_swarm/') "
                    "and '__pycache__' not in p.parts},"
                    "'direct_url':json.loads(d.read_text('direct_url.json')),"
                    "'temporal':importlib.util.find_spec('temporalio') is not None,"
                    "'torch':importlib.util.find_spec('torch') is not None,"
                    "'mlx':importlib.util.find_spec('mlx') is not None}))"
                ),
            ]
        )
    )
    assert Path(origin["module"]).resolve().is_relative_to(installed.resolve())
    assert origin["version"] == origin["module_version"] == expected_version
    assert origin["direct_url"]["url"].endswith(wheel.name)
    assert digest(wheel) == expected_digest
    with zipfile.ZipFile(wheel) as archive:
        expected_files = {
            name: hashlib.sha256(archive.read(name)).hexdigest()
            for name in archive.namelist()
            if name.startswith("research_swarm/") and not name.endswith("/")
        }
    assert origin["files"] == expected_files
    assert "dir_info" not in origin["direct_url"]
    assert not any(origin[name] for name in ("temporal", "torch", "mlx"))

    source = neutral / "ordinary-project"
    shutil.copytree(repository / "examples" / "cpu_bigram", source)
    state = neutral / "state"
    arguments = [
        str(research),
        "--state-dir",
        str(state),
        "run",
        str(source),
        "--key",
        "wheel-baseline",
        "--max-trials",
        "1",
        "--machine-registry",
        str(neutral / "machine" / "registry.sqlite"),
    ]
    baseline = json.loads(run(arguments))
    assert baseline["state"] == "awaiting_completion"
    assert baseline["driver"]["executed_attempt_count"] == 2
    campaign_id = baseline["campaign_id"]
    connection = StdioServerParameters(
        command=str(python),
        args=["-I", "-m", "research_swarm.cli", "--state-dir", str(state), "mcp"],
        env=environment,
        cwd=neutral,
    )
    async with Client(connection, read_timeout_seconds=30, cache=None) as agent:
        capabilities = (await tool(agent, "capabilities"))["result"]
        assert "campaign.complete" in capabilities["commands"]
        receipts = (
            await tool(
                agent,
                "query_records",
                request={"kind": "evaluation_receipt", "campaign_id": campaign_id},
            )
        )["result"]["items"]
        assert len(receipts) == 1 and receipts[0]["state"] == "valid"
        assessment = await command(
            agent,
            "promotion.assess",
            {
                "campaign_id": campaign_id,
                "candidate_evaluation_id": receipts[0]["id"],
                "expected_incumbent_revision": 0,
            },
            "wheel-assess",
        )
        await command(
            agent, "promotion.commit", {"decision_id": assessment["result"]["id"]}, "wheel-select"
        )
        view = await context(agent, campaign_id)
        completed = await command(
            agent,
            "campaign.complete",
            {"campaign_id": campaign_id, "reason": "Installed wheel baseline completed"},
            "wheel-complete",
            view["campaign"]["revision"],
        )
        report = await record(agent, "report", completed["result"]["report"]["id"])
        assert report["data"]["snapshot"]["campaign_state"] == "completed"
        assert report["data"]["snapshot"]["incumbent"]["evidence_level"] == "observed"
        counts = {item["kind"]: item["count"] for item in report["data"]["snapshot"]["counts"]}
        assert counts["attempt"] == 2 and counts["evaluation_receipt"] == 1
    shutil.rmtree(source)
    resumed = json.loads(run(arguments))
    assert resumed["state"] == "terminal"
    assert resumed["campaign_id"] == campaign_id and resumed["trial_id"] == baseline["trial_id"]
    assert resumed["driver"]["executed_attempt_count"] == 0
