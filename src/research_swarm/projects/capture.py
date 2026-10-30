"""Resolve source selection once and capture a repository into a caller-owned bundle."""

from pathlib import Path

from research_swarm.projects.manifest import MAX_PROJECT_MANIFEST_BYTES, parse_manifest
from research_swarm.projects.native_files import SourceReader
from research_swarm.projects.source import (
    SourceManifest,
    bundle_directory,
    bundle_git,
    portable_path,
    read_git_file,
)


def capture_project(
    root: Path,
    destination: Path,
    *,
    manifest_path: str = "research.toml",
    revision: str | None = None,
) -> SourceManifest:
    root = root.resolve()
    portable_path(manifest_path)
    if revision is not None:
        revision, content = read_git_file(
            root, revision, manifest_path, limit=MAX_PROJECT_MANIFEST_BYTES
        )
        manifest = parse_manifest(content)
    else:
        with SourceReader(root) as reader, reader.open(manifest_path) as source:
            manifest = parse_manifest(source.read(MAX_PROJECT_MANIFEST_BYTES + 1))
        if manifest.source.mode == "git":
            revision, content = read_git_file(
                root,
                manifest.source.base_revision,
                manifest_path,
                limit=MAX_PROJECT_MANIFEST_BYTES,
            )
            manifest = parse_manifest(content)
    if revision is not None:
        return bundle_git(root, revision, destination, paths=manifest.source.paths)
    return bundle_directory(root, destination, paths=manifest.source.paths)
