from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

import pytest

from research_swarm.cli import main
from research_swarm.projects.manifest import resolve_project
from research_swarm.swarm.files import CampaignFiles


def test_capture_register_and_download_in_campaign_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    project = Path(__file__).resolve().parents[2] / "examples/cpu_bigram"
    db = tmp_path / "campaign.sqlite"
    assert main(["project", "capture", str(project), "--db", str(db)]) == 0
    ref = json.loads(capsys.readouterr().out)
    assert main(["project", "register", str(project), "--db", str(db)]) == 0
    snapshot = json.loads(capsys.readouterr().out)
    assert snapshot["source_artifact"] == ref
    with closing(sqlite3.connect(db)) as connection:
        (stored,) = connection.execute(
            "SELECT snapshot FROM swarm_projects WHERE project_id=?", (snapshot["id"],)
        ).fetchone()
        assert json.loads(stored) == snapshot
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
        ).fetchone() == (6,)
    archive = tmp_path / "source.zip"
    assert main(["artifact", "download", ref["id"], str(archive), "--db", str(db)]) == 0
    assert resolve_project(archive).manifest.project.slug == snapshot["id"]


def test_concurrent_publication_and_verified_exclusive_download(tmp_path: Path) -> None:
    files = CampaignFiles(tmp_path / "queue.db")
    original = tmp_path / "result.txt"
    original.write_bytes(b"measured")
    with ThreadPoolExecutor(max_workers=4) as pool:
        refs = list(pool.map(files.put, [original] * 4))
    assert all(ref == refs[0] for ref in refs)
    ref = refs[0]
    with pytest.raises(FileExistsError):
        files.download(ref.id, original)
    assert original.read_bytes() == b"measured"
    (files.root / ref.id / "content").write_bytes(b"replaced")
    destination = tmp_path / "copy"
    with pytest.raises(ValueError, match="bytes"):
        files.download(ref.id, destination)
    assert not destination.exists()
    with pytest.raises(ValueError, match="bytes"):
        files.put(original)
