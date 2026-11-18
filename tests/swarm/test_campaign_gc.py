from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from research_swarm.swarm.campaign_gc import _stages
from research_swarm.swarm.data_stage import prepare


def gc(state: Path, *args: str) -> tuple[int, dict[str, dict[str, Any]]]:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_swarm",
            "--state-dir",
            str(state),
            "campaign",
            "gc",
            "demo",
            *args,
        ],
        capture_output=True,
    )
    data = json.loads(result.stdout)
    assert data["campaign"] == "demo", result.stderr
    return result.returncode, {entry["id"]: entry for entry in data["resources"]}


def test_stage_gc_lists_preserves_keep_and_ignores_foreign_roots(tmp_path: Path) -> None:
    source = tmp_path / "input"
    source.mkdir()
    (source / "sample").write_text("v1")
    volume = tmp_path / "volume"
    for campaign, revision in [("demo", "keep"), ("demo", "old"), ("another", "old")]:
        prepare(source, volume, campaign, revision)
    foreign = volume / "demo" / "foreign"
    foreign.mkdir()
    (foreign / "data").mkdir()
    (foreign / "stage.json").write_text('{"owner": "someone-else"}')
    (volume / "demo" / "link").symlink_to(volume / "another" / "old")
    options = ("--volume", str(volume), "--keep", "data:keep")
    code, entries = gc(tmp_path, *options)
    assert code == 0 and set(entries) == {"data:old", "data:keep"}
    assert (volume / "demo/old/data/sample").exists()
    code, entries = gc(tmp_path, *options, "--apply")
    assert code == 0 and entries["data:old"]["action"] == "removed"
    assert entries["data:keep"]["action"] == "kept"
    assert not (volume / "demo/old").exists()
    assert (volume / "another/old/data/sample").exists() and foreign.is_dir()
    assert gc(tmp_path, *options, "--apply")[0] == 0


def test_interrupted_stage_deletion_can_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, volume = tmp_path / "source", tmp_path / "volume"
    source.mkdir()
    prepare(source, volume, "demo", "old")
    with monkeypatch.context() as patch:

        def interrupt(path: Path) -> None:
            raise OSError("interrupted after data removal")

        patch.setattr(Path, "rmdir", interrupt)
        assert _stages("demo", [volume], set(), True)[0].action.startswith("error:")
    assert not (volume / "demo/old/data").exists()
    with pytest.raises(ValueError, match="cleanup"):
        prepare(source, volume, "demo", "old")
    assert gc(tmp_path, "--volume", str(volume), "--apply")[1]["data:old"]["action"] == "removed"
    assert prepare(source, volume, "demo", "old").files == 0
