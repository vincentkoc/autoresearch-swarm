from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import pytest

from research_swarm.domain.errors import ResearchError
from research_swarm.projects import source as source_module
from research_swarm.projects.source import (
    MANIFEST_NAME,
    MutationPolicy,
    SourceManifest,
    bundle_directory,
    bundle_git,
    extract_bundle,
    inspect_bundle,
    matches,
    portable_path,
    validate_mutation,
)


def project(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    root.mkdir()
    (root / "train.py").write_text("rate = 0.1\n")
    (root / "evaluate.py").write_text("metric = 'loss'\n")
    (root / "data-manifest.json").write_text('{"version": 1}\n')
    (root / "private.env").write_text("excluded fixture\n")
    return root


def capture(root: Path, destination: Path) -> SourceManifest:
    return bundle_directory(root, destination, paths=("*.py", "data-manifest.json"))


def test_directory_bundle_is_deterministic_and_isolated(tmp_path: Path) -> None:
    root = project(tmp_path)
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    manifest = capture(root, first)
    assert capture(root, second) == manifest
    assert (
        hashlib.sha256(first.read_bytes()).digest() == hashlib.sha256(second.read_bytes()).digest()
    )
    assert {entry.path for entry in manifest.entries} == {
        "train.py",
        "evaluate.py",
        "data-manifest.json",
    }
    (root / "train.py").write_text("rate = 0.2\n")
    output = tmp_path / "candidate"
    extract_bundle(first, output, expected_tree_digest=manifest.tree_digest)
    assert (output / "train.py").read_text() == "rate = 0.1\n"
    (output / "train.py").write_text("rate = 0.9\n")
    assert (root / "train.py").read_text() == "rate = 0.2\n"
    assert not (output / "private.env").exists()


def test_mutation_allows_declared_changes_and_rejects_protected_deletions(tmp_path: Path) -> None:
    root = project(tmp_path)
    baseline = capture(root, tmp_path / "base.zip")
    (root / "train.py").write_text("rate = 0.2\n")
    candidate = capture(root, tmp_path / "candidate.zip")
    policy = MutationPolicy(allow=("*.py",), deny=("evaluate.py",))
    assert validate_mutation(baseline, candidate, policy) == ("train.py",)
    (root / "evaluate.py").unlink()
    tampered = capture(root, tmp_path / "tampered.zip")
    with pytest.raises(ResearchError, match="protected"):
        validate_mutation(baseline, tampered, policy)


def test_protected_dependencies_override_mutation_allow(tmp_path: Path) -> None:
    root = project(tmp_path)
    baseline = capture(root, tmp_path / "base.zip")
    (root / "data-manifest.json").write_text('{"version": 2}\n')
    candidate = capture(root, tmp_path / "candidate.zip")
    with pytest.raises(ResearchError) as error:
        validate_mutation(
            baseline,
            candidate,
            MutationPolicy(allow=("**",)),
            protected_paths=("data-manifest.json",),
        )
    assert error.value.details["paths"] == ["data-manifest.json"]


def test_path_globs_do_not_include_unselected_subdirectories() -> None:
    assert matches("train.py", ("*.py",))
    assert not matches("private/train.py", ("*.py",))
    assert matches("evaluation/nested/check.py", ("evaluation/**",))
    assert matches("check.py", ("**/*.py",))


@pytest.mark.parametrize(
    "path",
    [
        "../escape",
        "/absolute",
        "x//y",
        "a/./b",
        "C:/x",
        "a\\b",
        "NUL.py",
        "a.",
        "x/CON",
        MANIFEST_NAME,
        ".git/config",
    ],
)
def test_nonportable_source_paths_are_rejected(path: str) -> None:
    with pytest.raises(ValueError):
        portable_path(path)


def test_existing_destination_is_never_removed(tmp_path: Path) -> None:
    root = project(tmp_path)
    bundle = tmp_path / "bundle.zip"
    manifest = capture(root, bundle)
    output = tmp_path / "existing"
    output.mkdir()
    (output / "owned").write_text("preserve")
    with pytest.raises(FileExistsError):
        extract_bundle(bundle, output, expected_tree_digest=manifest.tree_digest)
    assert (output / "owned").read_text() == "preserve"
    with pytest.raises(FileExistsError):
        capture(root, bundle)
    assert inspect_bundle(bundle) == manifest


def test_corrupt_bundle_content_removes_only_created_output(tmp_path: Path) -> None:
    root = project(tmp_path)
    bundle = tmp_path / "bundle.zip"
    manifest = capture(root, bundle)
    changed = tmp_path / "changed.zip"
    with zipfile.ZipFile(bundle) as source, zipfile.ZipFile(changed, "w") as output:
        for info in source.infolist():
            content = source.read(info)
            if info.filename == "train.py":
                content = content.replace(b"0.1", b"0.2")
            output.writestr(info, content)
    destination = tmp_path / "candidate"
    with pytest.raises(ResearchError, match="digest mismatch"):
        extract_bundle(changed, destination, expected_tree_digest=manifest.tree_digest)
    assert not destination.exists()
    assert (root / "train.py").read_text() == "rate = 0.1\n"


def test_archive_traversal_and_duplicate_members_are_rejected(tmp_path: Path) -> None:
    root = project(tmp_path)
    bundle = tmp_path / "bundle.zip"
    capture(root, bundle)
    with zipfile.ZipFile(bundle, "a") as archive:
        archive.writestr("../escape", b"bad")
    with pytest.raises(ValueError, match="membership"):
        inspect_bundle(bundle)
    assert not (tmp_path / "escape").exists()


def test_tree_identity_is_checked_before_materialization(tmp_path: Path) -> None:
    root = project(tmp_path)
    bundle = tmp_path / "bundle.zip"
    capture(root, bundle)
    destination = tmp_path / "candidate"
    with pytest.raises(ResearchError, match="identity"):
        extract_bundle(bundle, destination, expected_tree_digest="sha256:" + "0" * 64)
    assert not destination.exists()


def test_source_manifest_cannot_forge_its_tree_digest(tmp_path: Path) -> None:
    root = project(tmp_path)
    manifest = capture(root, tmp_path / "bundle.zip")
    data = json.loads(manifest.model_dump_json())
    data["entries"][0]["digest"] = "sha256:" + "0" * 64
    with pytest.raises(ValueError, match="digest mismatch"):
        SourceManifest.model_validate(data)


def test_symlink_source_is_rejected_without_reading_target(tmp_path: Path) -> None:
    root = project(tmp_path)
    outside = tmp_path / "outside.txt"
    outside.write_text("private fixture")
    try:
        (root / "linked.py").symlink_to(outside)
    except OSError:
        pytest.skip("Symlink creation requires platform privileges")
    destination = tmp_path / "bundle.zip"
    with pytest.raises(ValueError, match="regular files"):
        capture(root, destination)
    assert not destination.exists()


def test_git_bundle_reads_pinned_commit_and_preserves_dirty_checkout(tmp_path: Path) -> None:
    root = project(tmp_path)

    def git(*arguments: str) -> str:
        return subprocess.run(
            ["git", "-C", str(root), *arguments],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init", "--quiet")
    git("add", "train.py", "evaluate.py", "data-manifest.json")
    git(
        "-c",
        "user.name=Research Test",
        "-c",
        "user.email=research@example.test",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--quiet",
        "-m",
        "test: initial source",
    )
    revision = git("rev-parse", "HEAD")
    (root / "train.py").write_text("rate = 0.9\n")
    (root / ".git" / "info" / "attributes").write_text("train.py export-ignore\n")
    status = git("status", "--porcelain")
    bundle = tmp_path / "git.zip"
    manifest = bundle_git(root, revision, bundle, paths=("*.py", "data-manifest.json"))
    assert manifest.base_revision == revision
    destination = tmp_path / "candidate"
    extract_bundle(bundle, destination, expected_tree_digest=manifest.tree_digest)
    assert (destination / "train.py").read_text() == "rate = 0.1\n"
    assert (root / "train.py").read_text() == "rate = 0.9\n"
    assert git("status", "--porcelain") == status


def test_directory_swap_is_rejected_before_reading_outside_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = project(tmp_path)
    folder = root / "selected"
    folder.mkdir()
    (folder / "train.py").write_text("local source")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "train.py").write_text("private outside source")
    # Check symlink privileges before the controlled race.
    probe = tmp_path / "probe"
    try:
        probe.symlink_to(outside, target_is_directory=True)
        probe.unlink()
    except OSError:
        pytest.skip("Symlink creation requires platform privileges")
    original = source_module._selected_files

    def replaced(root: Path, patterns: tuple[str, ...]):
        entries = list(original(root, patterns))
        folder.rename(root / "previous")
        folder.symlink_to(outside, target_is_directory=True)
        yield from entries

    monkeypatch.setattr(source_module, "_selected_files", replaced)
    destination = tmp_path / "bundle.zip"
    with pytest.raises((ValueError, OSError)):
        bundle_directory(root, destination, paths=("selected/**",))
    assert not destination.exists()
    assert (outside / "train.py").read_text() == "private outside source"


def test_git_reader_bounds_output_and_entire_child_lifetime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = subprocess.Popen

    def fake_git(*args, **kwargs):
        return original([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)

    monkeypatch.setattr(subprocess, "Popen", fake_git)
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        source_module._git_read(tmp_path, [], limit=32, deadline=started + 0.2)
    assert time.monotonic() - started < 5

    def noisy_git(*args, **kwargs):
        return original([sys.executable, "-c", "print('x' * 10000)"], **kwargs)

    monkeypatch.setattr(subprocess, "Popen", noisy_git)
    with pytest.raises(ValueError, match="bound"):
        source_module._git_read(tmp_path, [], limit=32, deadline=time.monotonic() + 5)


def test_inherited_git_output_does_not_extend_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = subprocess.Popen
    program = (
        "import subprocess, sys, time; "
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
        "time.sleep(30)"
    )

    def fake_git(*args, **kwargs):
        return original([sys.executable, "-c", program], **kwargs)

    monkeypatch.setattr(subprocess, "Popen", fake_git)
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        source_module._git_read(tmp_path, [], limit=32, deadline=started + 0.5)
    assert time.monotonic() - started < 5


def test_git_reads_disable_implicit_fetch_and_ambient_repository_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = subprocess.Popen
    program = (
        "import os; print(os.getenv('GIT_NO_LAZY_FETCH'), "
        "os.getenv('GIT_NO_REPLACE_OBJECTS'), os.getenv('GIT_DIR'))"
    )

    def fake_git(*args, **kwargs):
        return original([sys.executable, "-c", program], **kwargs)

    monkeypatch.setattr(subprocess, "Popen", fake_git)
    monkeypatch.setenv("GIT_DIR", str(tmp_path / "wrong-project"))
    result = source_module._git_read(tmp_path, [], limit=64, deadline=time.monotonic() + 5)
    assert result.strip() == b"1 1 None"
