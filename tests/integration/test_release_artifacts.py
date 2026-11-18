"""Release checks reject corrupt identity and unsafe distribution contents."""

from __future__ import annotations

import io
import subprocess
import tarfile
import zipfile
from pathlib import Path

import pytest

from scripts.release import (
    archive_files,
    check_release,
    committed_files,
    digest,
    prebuilt_wheel,
    source_identity,
)
from scripts.release import validate_distributions as validate
from tests.paths import REPO_ROOT


def fixture_git(root: Path, *arguments: str, data: bytes | None = None) -> bytes:
    return subprocess.check_output(
        [
            "git",
            "-c",
            "user.name=Release fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.autocrlf=false",
            "-c",
            f"core.hooksPath={root / '.git' / 'no-hooks'}",
            *arguments,
        ],
        cwd=root,
        input=data,
    )


@pytest.fixture
def distributions(tmp_path: Path) -> tuple[Path, dict]:
    tmp_path = tmp_path / "dist"
    tmp_path.mkdir()
    project = source_identity(REPO_ROOT)
    version = project["version"]
    metadata = (
        f"Metadata-Version: 2.4\nName: autoresearch-swarm\nVersion: {version}\n"
        "Requires-Python: >=3.11\nLicense-Expression: MIT\n"
        + "".join(f"Provides-Extra: {name}\n" for name in project["optional-dependencies"])
    ).encode()
    code = f'__version__ = "{version}"\n'.encode()
    info = f"autoresearch_swarm-{version}.dist-info"
    with zipfile.ZipFile(tmp_path / f"autoresearch_swarm-{version}-py3-none-any.whl", "w") as wheel:
        for name, body in {
            "research_swarm/__init__.py": code,
            f"{info}/METADATA": metadata,
            f"{info}/licenses/LICENSE": b"MIT fixture",
            f"{info}/entry_points.txt": b"[console_scripts]\nresearch = research_swarm.cli:main\n",
        }.items():
            wheel.writestr(name, body)
    with tarfile.open(tmp_path / f"autoresearch_swarm-{version}.tar.gz", "w:gz") as source:
        for name, body in {
            "src/research_swarm/__init__.py": code,
            "LICENSE": b"MIT fixture",
            "PKG-INFO": metadata,
            "pyproject.toml": (REPO_ROOT / "pyproject.toml").read_bytes(),
            "uv.lock": b"fixture",
            "README.md": b"fixture",
            ".gitignore": b"dist/\n",
        }.items():
            member = tarfile.TarInfo(f"autoresearch_swarm-{version}/{name}")
            member.size = len(body)
            source.addfile(member, io.BytesIO(body))
    return tmp_path, project


def test_distribution_identity_and_exact_prebuilt_wheel(distributions: tuple[Path, dict]) -> None:
    directory, project = distributions
    records = validate(directory, project)
    wheel = next(directory.glob("*.whl"))
    assert len(records) == 2
    assert prebuilt_wheel({}) is None
    assert (
        prebuilt_wheel(
            {"RESEARCH_PACKAGE_WHEEL": str(wheel), "RESEARCH_PACKAGE_WHEEL_SHA256": digest(wheel)}
        )
        == wheel
    )


@pytest.mark.parametrize(
    "environment",
    [
        {"RESEARCH_PACKAGE_WHEEL": "missing.whl"},
        {"RESEARCH_PACKAGE_WHEEL_SHA256": "0" * 64},
        {"RESEARCH_PACKAGE_WHEEL": "missing.whl", "RESEARCH_PACKAGE_WHEEL_SHA256": "bad"},
        {"RESEARCH_PACKAGE_WHEEL": "missing.whl", "RESEARCH_PACKAGE_WHEEL_SHA256": "0" * 64},
    ],
)
def test_invalid_prebuilt_inputs_never_choose_build(environment: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        prebuilt_wheel(environment)


def test_tampered_prebuilt_wheel_is_rejected(tmp_path: Path) -> None:
    wheel = tmp_path / "fixture.whl"
    wheel.write_bytes(b"fixture")
    expected = digest(wheel)
    with wheel.open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        prebuilt_wheel(
            {"RESEARCH_PACKAGE_WHEEL": str(wheel), "RESEARCH_PACKAGE_WHEEL_SHA256": expected}
        )


def test_tag_cannot_publish_another_version() -> None:
    with pytest.raises(ValueError, match="Tag must exactly match"):
        source_identity(REPO_ROOT, "v9.9.9")


def test_development_version_is_preview_only(tmp_path: Path) -> None:
    version = "0.1.0.dev0"
    identity = f'name = "autoresearch-swarm"\nversion = "{version}"\n'
    (tmp_path / "pyproject.toml").write_text("[project]\n" + identity)
    (tmp_path / "uv.lock").write_text("[[package]]\n" + identity)
    package = tmp_path / "src/research_swarm"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text(f'__version__ = "{version}"\n')
    assert source_identity(tmp_path)["version"] == version
    with pytest.raises(ValueError, match="Development versions are preview-only"):
        source_identity(tmp_path, f"v{version}")


@pytest.mark.parametrize("removed", ["METADATA", "licenses/LICENSE", "entry_points.txt"])
def test_incomplete_wheel_is_rejected(distributions: tuple[Path, dict], removed: str) -> None:
    directory, project = distributions
    wheel = next(directory.glob("*.whl"))
    files = archive_files(wheel)
    with zipfile.ZipFile(wheel, "w") as output:
        for name, body in files.items():
            if not name.endswith(f".dist-info/{removed}"):
                output.writestr(name, body)
    with pytest.raises(ValueError):
        validate(directory, project)


@pytest.mark.parametrize(
    "member", ["../escape", "/absolute", "research_swarm/.env", "state.sqlite"]
)
def test_unexpected_or_unsafe_members_are_rejected(
    distributions: tuple[Path, dict], member: str
) -> None:
    directory, project = distributions
    with zipfile.ZipFile(next(directory.glob("*.whl")), "a") as wheel:
        wheel.writestr(member, "private")
    with pytest.raises(ValueError):
        validate(directory, project)


def test_mismatched_sdist_code_is_rejected(distributions: tuple[Path, dict]) -> None:
    directory, project = distributions
    wheel = next(directory.glob("*.whl"))
    files = archive_files(wheel)
    files["research_swarm/extra.py"] = b"unexpected = True\n"
    with zipfile.ZipFile(wheel, "w") as output:
        for name, body in files.items():
            output.writestr(name, body)
    with pytest.raises(ValueError, match="differs from sdist"):
        validate(directory, project)


def test_extra_distribution_is_rejected(distributions: tuple[Path, dict]) -> None:
    directory, project = distributions
    (directory / "unexpected.whl").write_bytes(b"wrong")
    with pytest.raises(ValueError, match="exactly one"):
        validate(directory, project)


def test_stale_artifacts_cannot_be_relabelled_with_a_new_commit(
    distributions: tuple[Path, dict], tmp_path: Path
) -> None:
    directory, project = distributions
    root = tmp_path / "repository"
    root.mkdir()
    archive = archive_files(next(directory.glob("*.tar.gz")))
    prefix = f"autoresearch_swarm-{project['version']}/"
    for name, body in archive.items():
        relative = name.removeprefix(prefix)
        if relative == "PKG-INFO":
            continue
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    (root / "uv.lock").write_text(
        f'[[package]]\nname = "autoresearch-swarm"\nversion = "{project["version"]}"\n'
    )
    # Keep the sdist's lock bytes in step with the first committed fixture.
    source_path = next(directory.glob("*.tar.gz"))
    archive[prefix + "uv.lock"] = (root / "uv.lock").read_bytes()
    with tarfile.open(source_path, "w:gz") as output:
        for name, body in archive.items():
            member = tarfile.TarInfo(name)
            member.size = len(body)
            output.addfile(member, io.BytesIO(body))

    fixture_git(root, "init", "-q")
    fixture_git(root, "add", ".")
    fixture_git(root, "commit", "-qm", "First same-version source")
    fixture_git(root, "config", "core.autocrlf", "true")
    check_release(root, directory, write=True)
    (root / "src/research_swarm/__init__.py").write_text(
        f'__version__ = "{project["version"]}"\nchanged = True\n'
    )
    fixture_git(root, "add", ".")
    fixture_git(root, "commit", "-qm", "Second same-version source")
    with pytest.raises(ValueError, match="differs from the committed source"):
        check_release(root, directory, write=True)


def test_committed_source_batch_exceeds_pipe_capacity(tmp_path: Path) -> None:
    fixture_git(tmp_path, "init", "-q")
    bodies = [bytes(range(64)), b"line one\r\nline two\n" * 4]
    objects = {
        body: fixture_git(tmp_path, "hash-object", "-w", "--stdin", data=body).strip()
        for body in bodies
    }
    # Reuse a pack for repeated reads of these two blobs in the fixture repo.
    fixture_git(
        tmp_path,
        "pack-objects",
        ".git/objects/pack/pack",
        data=b"\n".join(objects.values()) + b"\n",
    )
    # Both the requested object IDs and the returned blobs exceed 1 MiB.
    expected = {f"entry-{index:05d}": bodies[index % 2] for index in range(32_768)}
    entries = b"".join(
        b"100644 blob " + objects[body] + b"\t" + name.encode() + b"\n"
        for name, body in expected.items()
    )
    tree = fixture_git(tmp_path, "mktree", data=entries).decode().strip()
    commit = fixture_git(tmp_path, "commit-tree", tree, "-m", "Synthetic batch").decode().strip()
    fixture_git(tmp_path, "update-ref", "HEAD", commit)
    assert committed_files(tmp_path, ["."]) == expected
