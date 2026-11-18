import subprocess
from pathlib import Path

import pytest

from scripts.ci_test_budget import check_budget, count_test_lines


def test_budget_counts_moves_across_test_boundary_and_nested_test_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    def git(*arguments: str) -> None:
        subprocess.run(["git", *arguments], check=True, capture_output=True)

    git("init", "-q")
    Path("tests").mkdir()
    Path("tests/old.py").write_text("one\ntwo\n")
    Path("outside.py").write_text("three\nfour\nfive\n")
    git("add", ".")
    git(
        "-c",
        "user.name=CI",
        "-c",
        "user.email=ci@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-qm",
        "base",
    )
    git("mv", "tests/old.py", "moved.py")
    git("mv", "outside.py", "tests/new.py")
    nested = Path("adapters/example/tests/case.mjs")
    nested.parent.mkdir(parents=True)
    nested.write_text("nested\n")
    Path("tests/binary.dat").write_bytes(b"\x00\xff\x00")
    Path("standalone.test.ts").write_text("standalone\n")
    git("add", ".")
    tree = subprocess.check_output(["git", "write-tree"], text=True).strip()
    assert count_test_lines("HEAD", tree) == (5, 2)


def test_budget_accepts_boundary_and_requires_visible_reason_for_overage() -> None:
    check_budget(600, 200, "")
    with pytest.raises(SystemExit, match="Consolidate"):
        check_budget(601, 200, "<!-- Test-line budget: examples -->\nTest-line budget: <reason>")
    check_budget(601, 200, "Test-line budget: covers the wheel install across platforms.")
