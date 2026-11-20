from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from research_swarm.projects import source as source_module
from research_swarm.projects.capture import capture_project
from research_swarm.swarm import process as process_module
from research_swarm.swarm import project as capture_module
from tests.projects.test_manifest import project as project


def committed_source(root: Path) -> Path:
    for arguments in (
        ("init", "--quiet"),
        ("add", "."),
        (
            "-c",
            "user.name=Research Test",
            "-c",
            "user.email=research@example.test",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--quiet",
            "-m",
            "test: capture fixture",
        ),
    ):
        subprocess.run(
            ["git", "-C", str(root), *arguments],
            check=True,
            capture_output=True,
            timeout=10,
        )
    return root


@pytest.mark.parametrize("seconds", [0, -1, 3601, float("inf"), float("nan"), True])
def test_capture_rejects_invalid_timeout_before_reading(tmp_path: Path, seconds: float) -> None:
    destination = tmp_path / "source.zip"
    with pytest.raises(ValueError, match="capture timeout"):
        capture_project(tmp_path / "absent", destination, capture_timeout_seconds=seconds)
    assert not destination.exists()


def test_capture_timeout_does_not_change_source_identity(project: Path, tmp_path: Path) -> None:
    root = committed_source(project)
    (root / "train.py").write_text("dirty checkout remains untouched\n")
    (root / ".git" / "info" / "attributes").write_text("train.py export-ignore\n")
    first = capture_project(root, tmp_path / "default.zip", revision="HEAD")
    second = capture_project(
        root, tmp_path / "longer.zip", revision="HEAD", capture_timeout_seconds=120
    )
    assert first == second
    assert (tmp_path / "default.zip").read_bytes() == (tmp_path / "longer.zip").read_bytes()
    assert (root / "train.py").read_text() == "dirty checkout remains untouched\n"


def test_capture_shares_manifest_and_blob_deadline_and_removes_partial_bundle(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = committed_source(project)

    class Clock:
        now = 100.0

        @classmethod
        def monotonic(cls) -> float:
            return cls.now

    monkeypatch.setattr(capture_module, "time", Clock)
    monkeypatch.setattr(source_module, "time", Clock)
    monkeypatch.setattr(process_module, "time", Clock)
    original = source_module._git_read
    reads: list[list[str]] = []

    def read(root: Path, arguments: list[str], *, limit: int, deadline: float) -> bytes:
        content = original(root, arguments, limit=limit, deadline=deadline)
        reads.append(arguments)
        Clock.now += 1
        return content

    monkeypatch.setattr(source_module, "_git_read", read)
    destination = tmp_path / "source.zip"
    with pytest.raises(TimeoutError, match="capture deadline"):
        capture_project(root, destination, revision="HEAD", capture_timeout_seconds=5)
    assert len(reads) == 5  # Two manifest reads, tree resolution/listing, then one blob.
    assert reads[-1][0] == "cat-file"
    assert not destination.exists()
