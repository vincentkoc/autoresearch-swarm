"""Validate release identity and the exact distributions used by CI."""

from __future__ import annotations

import argparse
import ast
import configparser
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import tarfile
import tomllib
import zipfile
from collections.abc import Mapping
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

from packaging.utils import canonicalize_name, parse_sdist_filename, parse_wheel_filename
from packaging.version import Version

NAME = "autoresearch-swarm"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def prebuilt_wheel(environment: Mapping[str, str]) -> Path | None:
    """A supplied artifact is mandatory: invalid inputs must never trigger a rebuild."""
    name = environment.get("RESEARCH_PACKAGE_WHEEL")
    expected = environment.get("RESEARCH_PACKAGE_WHEEL_SHA256")
    if name is None and expected is None:
        return None
    require(bool(name and expected), "Supply both the wheel path and its SHA-256")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", expected or "")), "Invalid wheel SHA-256")
    wheel = Path(name or "")
    require(wheel.is_absolute() and wheel.is_file(), "Wheel must be an existing absolute path")
    require(wheel.suffix == ".whl" and not wheel.is_symlink(), "Expected a regular wheel file")
    require(digest(wheel) == expected, "Wheel SHA-256 mismatch")
    return wheel


def module_version(source: bytes) -> str:
    for statement in ast.parse(source).body:
        if isinstance(statement, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in statement.targets
        ):
            value = ast.literal_eval(statement.value)
            require(isinstance(value, str), "Package version must be a string")
            return str(value)
    raise ValueError("Missing package version")


def source_identity(root: Path, tag: str | None = None) -> dict:
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    version = project["version"]
    require(project["name"] == NAME, "Unexpected project name")
    require(str(Version(version)) == version, "Version must use canonical PEP 440 spelling")
    require(
        module_version((root / "src/research_swarm/__init__.py").read_bytes()) == version,
        "Module and project versions differ",
    )
    lock = tomllib.loads((root / "uv.lock").read_text())
    locked = [package for package in lock["package"] if package["name"] == NAME]
    require(len(locked) == 1 and locked[0]["version"] == version, "Lock version differs")
    if tag is not None:
        require(tag == f"v{version}", "Tag must exactly match the project version")
        require(not Version(version).is_devrelease, "Development versions are preview-only")
        require(
            Version(version).local is None, "PyPI releases cannot use local version identifiers"
        )
        changelog = (root / "CHANGELOG.md").read_text()
        require(
            re.search(rf"^## {re.escape(version)}(?: - \d{{4}}-\d{{2}}-\d{{2}})?$", changelog, re.M)
            is not None,
            "Tagged release needs its own changelog section",
        )
    return project


def safe_path(name: str) -> PurePosixPath:
    path = PurePosixPath(name)
    require(
        bool(name)
        and not path.is_absolute()
        and "\\" not in name
        and ":" not in name
        and all(part not in {"", ".", ".."} for part in name.split("/")),
        f"Unsafe archive member: {name}",
    )
    return path


def archive_files(path: Path) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    seen: set[str] = set()

    def admit(name: str) -> None:
        safe_path(name)
        require(name not in seen, f"Duplicate archive member: {name}")
        seen.add(name)

    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            for member in archive.infolist():
                admit(member.filename.rstrip("/"))
                require(not stat.S_ISLNK(member.external_attr >> 16), "Archive contains a symlink")
                if not member.is_dir():
                    files[member.filename] = archive.read(member)
    else:
        with tarfile.open(path, "r:gz") as archive:
            for member in archive.getmembers():
                admit(member.name.rstrip("/"))
                require(member.isfile() or member.isdir(), "Archive contains a link or device")
                if member.isfile():
                    stream = archive.extractfile(member)
                    require(stream is not None, "Unreadable archive member")
                    assert stream is not None
                    files[member.name] = stream.read()
    return files


def validate_metadata(raw: bytes, project: dict) -> None:
    metadata = BytesParser().parsebytes(raw)
    require(canonicalize_name(metadata["Name"] or "") == NAME, "Distribution name differs")
    for field, key in (("Version", "version"), ("Requires-Python", "requires-python")):
        require(metadata[field] == project[key], f"Distribution {field} differs")
    require(metadata["License-Expression"] == "MIT", "Missing MIT license metadata")
    require(
        set(metadata.get_all("Provides-Extra", [])) == set(project["optional-dependencies"]),
        "Distribution extras differ",
    )


def validate_distributions(directory: Path, project: dict) -> list[dict]:
    wheel_paths = list(directory.glob("*.whl"))
    source_paths = list(directory.glob("*.tar.gz"))
    require(len(wheel_paths) == len(source_paths) == 1, "Expected exactly one wheel and one sdist")
    require(
        {p.name for p in directory.iterdir()}
        <= {wheel_paths[0].name, source_paths[0].name, "manifest.json", "SHA256SUMS"},
        "Unexpected release file",
    )
    wheel, sdist = wheel_paths[0], source_paths[0]
    require(not wheel.is_symlink() and not sdist.is_symlink(), "Distribution must not be a symlink")
    wheel_name, wheel_version, _, tags = parse_wheel_filename(wheel.name)
    source_name, source_version = parse_sdist_filename(sdist.name)
    require(wheel_name == source_name == NAME, "Distribution filename name differs")
    require(str(wheel_version) == str(source_version) == project["version"], "Version differs")
    require({str(tag) for tag in tags} == {"py3-none-any"}, "Expected a portable Python wheel")
    wheel_files, source_files = archive_files(wheel), archive_files(sdist)
    info = f"autoresearch_swarm-{project['version']}.dist-info"
    source_prefix = f"autoresearch_swarm-{project['version']}/"
    require(all(name.startswith(source_prefix) for name in source_files), "Wrong sdist root")
    source_files = {name.removeprefix(source_prefix): body for name, body in source_files.items()}
    required_root = {"pyproject.toml", "uv.lock", "README.md", "LICENSE", "PKG-INFO"}
    allowed_root = required_root | {".gitignore"}
    require(
        all(
            name in allowed_root or name.startswith("src/research_swarm/") for name in source_files
        ),
        "Unexpected sdist content",
    )
    require(required_root <= source_files.keys(), "Incomplete sdist")
    require(
        all(name.startswith(("research_swarm/", f"{info}/")) for name in wheel_files),
        "Unexpected wheel content",
    )
    for name in (*wheel_files, *source_files):
        if name == ".gitignore":
            continue
        parts = PurePosixPath(name).parts
        require(
            not any(part.startswith(".") or part == "__pycache__" for part in parts)
            and not name.endswith((".pyc", ".sqlite", ".db", ".pem", ".key")),
            f"Forbidden package content: {name}",
        )
    metadata = wheel_files.get(f"{info}/METADATA")
    require(metadata is not None, "Missing wheel metadata")
    validate_metadata(metadata or b"", project)
    validate_metadata(source_files["PKG-INFO"], project)
    license_body = wheel_files.get(f"{info}/licenses/LICENSE")
    require(
        bool(license_body) and license_body == source_files["LICENSE"], "Missing/different license"
    )
    entries = configparser.ConfigParser()
    entries.read_string(wheel_files.get(f"{info}/entry_points.txt", b"").decode())
    require(
        entries.get("console_scripts", "research", fallback="") == "research_swarm.cli:main",
        "Missing research entry point",
    )
    package = {
        name: body for name, body in wheel_files.items() if name.startswith("research_swarm/")
    }
    source_package = {
        name.removeprefix("src/"): body
        for name, body in source_files.items()
        if name.startswith("src/research_swarm/")
    }
    require(package == source_package, "Wheel code differs from sdist")
    require(
        module_version(package.get("research_swarm/__init__.py", b"")) == project["version"],
        "Packaged module version differs",
    )
    return [
        {"filename": path.name, "size": path.stat().st_size, "sha256": digest(path)}
        for path in sorted((wheel, sdist))
    ]


def check_release(root: Path, directory: Path, *, write: bool, tag: str | None = None) -> dict:
    status = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=normal"], cwd=root, text=True
    )
    require(not status, "Commit source changes before recording release identity")
    project = source_identity(root, tag)
    artifacts = validate_distributions(directory, project)
    source = archive_files(next(directory.glob("*.tar.gz")))
    prefix = f"autoresearch_swarm-{project['version']}/"
    packaged = {
        name.removeprefix(prefix): body
        for name, body in source.items()
        if name != prefix + "PKG-INFO"
    }
    paths = [
        "src/research_swarm",
        "pyproject.toml",
        "uv.lock",
        "README.md",
        "LICENSE",
        ".gitignore",
    ]
    committed = subprocess.check_output(["git", "archive", "HEAD", "--", *paths], cwd=root)
    with tarfile.open(fileobj=io.BytesIO(committed)) as archive:
        expected = {}
        for member in archive.getmembers():
            if member.isfile():
                stream = archive.extractfile(member)
                assert stream is not None
                expected[member.name] = stream.read()
    require(
        packaged == expected,
        "Distribution content differs from the committed source; rebuild it",
    )
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    require(bool(re.fullmatch(r"[0-9a-f]{40}", commit)), "Invalid source commit")
    manifest = {
        "schema_version": 1,
        "name": NAME,
        "version": project["version"],
        "commit": commit,
        "artifacts": artifacts,
    }
    checksums = "".join(f"{item['sha256']}  {item['filename']}\n" for item in artifacts)
    if write:
        (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        (directory / "SHA256SUMS").write_text(checksums)
    else:
        require(
            json.loads((directory / "manifest.json").read_text()) == manifest,
            "Release manifest differs from files or source commit",
        )
        require((directory / "SHA256SUMS").read_text() == checksums, "Release checksums differ")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--tag")
    parser.add_argument("--github-env", action="store_true")
    args = parser.parse_args()
    manifest = check_release(args.root, args.directory, write=args.write, tag=args.tag)
    if args.github_env:
        wheel = next(item for item in manifest["artifacts"] if item["filename"].endswith(".whl"))
        with Path(os.environ["GITHUB_ENV"]).open("a") as stream:
            stream.write(
                f"RESEARCH_PACKAGE_WHEEL={(args.directory / wheel['filename']).resolve()}\n"
            )
            stream.write(f"RESEARCH_PACKAGE_WHEEL_SHA256={wheel['sha256']}\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
