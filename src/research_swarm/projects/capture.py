"""Resolve source selection once and capture a repository into a caller-owned bundle."""

import math
import time
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
    capture_timeout_seconds: float = 30,
) -> SourceManifest:
    """Share a bounded Git-read budget across manifest resolution and bundling."""
    if (
        isinstance(capture_timeout_seconds, bool)
        or not math.isfinite(capture_timeout_seconds)
        or not 0 < capture_timeout_seconds <= 3600
    ):
        raise ValueError("Source capture timeout must be within (0, 3600] seconds")
    deadline = time.monotonic() + capture_timeout_seconds

    def remaining() -> float:
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise TimeoutError("Source capture deadline expired")
        return seconds

    root = root.resolve()
    portable_path(manifest_path)
    if revision is not None:
        revision, content = read_git_file(
            root,
            revision,
            manifest_path,
            limit=MAX_PROJECT_MANIFEST_BYTES,
            timeout_seconds=remaining(),
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
                timeout_seconds=remaining(),
            )
            manifest = parse_manifest(content)
    if revision is not None:
        return bundle_git(
            root,
            revision,
            destination,
            paths=manifest.source.paths,
            timeout_seconds=remaining(),
        )
    return bundle_directory(root, destination, paths=manifest.source.paths)
