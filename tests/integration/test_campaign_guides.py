"""Run the shipped campaign and inspect it using the commands in the operator guide."""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from tests.paths import REPO_ROOT


def test_campaign_skill_run_inspect_and_resume(tmp_path: Path) -> None:
    skill = REPO_ROOT / ".agents/skills/run-a-campaign/SKILL.md"
    links = re.findall(r"\[[^\]]+\]\(([^)#]+)(?:#[^)]*)?\)", skill.read_text())

    def linked_guide(name: str) -> str:
        (link,) = [link for link in links if link.endswith("/" + name)]
        return (skill.parent / link).read_text()

    (tmp_path / "benchmarks").mkdir()
    shutil.copyfile(REPO_ROOT / "benchmarks/program.yaml", tmp_path / "benchmarks/program.yaml")
    env = {
        **os.environ,
        "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"],
        "PYTHONPATH": str(REPO_ROOT),
        "UV_PYTHON": sys.executable,
        "UV_OFFLINE": "1",
        "VIRTUAL_ENV": sys.prefix,
    }

    def run(command: str) -> str:
        result = subprocess.run(
            ["sh", "-ec", command],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        return result.stdout

    (campaign,) = (
        line
        for line in linked_guide("run-a-campaign.md").splitlines()
        if line.startswith("uv run ") and " swarm run " in line
    )
    examples = re.findall(r"^~~~sh\n(.*?)^~~~$", linked_guide("operator-guide.md"), re.M | re.S)
    (queue,) = [command for command in examples if "FROM swarm_trials" in command]
    (panels,) = [command for command in examples if "FROM panel_looks" in command]
    first = json.loads(run(campaign))
    assert first["reason"] == "trial_limit" and first["trials"] == first["attempts"] == 3
    assert first["failed"] == first["lost"] == 0
    winner = first["selected"]["result"]["attempt"]["trial"]["candidate"]
    assert winner["candidate_id"] == "normalized"
    assert first["selected"]["score"] == first["confirmed_score"] == 1
    assert "program-normalization ok 3" in run(queue).splitlines()
    evidence = run(panels).splitlines()
    assert "search 3" in evidence
    decisions = [json.loads(line) for line in evidence if line.startswith("{")]
    assert any(
        item["recipe"] == winner["params"]
        and item["decision"]["promoted"]
        and item["decision"]["reason"] == "confirmation"
        for item in decisions
    )
    assert json.loads(run(campaign)) == first
    assert run(panels).splitlines() == evidence
