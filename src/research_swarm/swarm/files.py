"""Local project bundles and immutable artifact bytes for a campaign database."""

from __future__ import annotations

import errno
import hashlib
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO
from uuid import NAMESPACE_URL, uuid5

from pydantic import TypeAdapter

from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import ID
from research_swarm.projects.capture import capture_project
from research_swarm.projects.manifest import resolve_project
from research_swarm.projects.native_files import SourceReader
from research_swarm.state.private_files import private_state_directory, sync_directory
from research_swarm.swarm.store import SQLiteStore


def _digest(source: BinaryIO, output: BinaryIO | None = None) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while chunk := source.read(1024 * 1024):
        if output is not None:
            output.write(chunk)
        digest.update(chunk)
        size += len(chunk)
    if output is not None:
        output.flush()
        os.fsync(output.fileno())
    return f"sha256:{digest.hexdigest()}", size


class CampaignFiles:
    def __init__(self, database: Path) -> None:
        self.store = SQLiteStore(database)
        self.root = database.with_name(database.name + ".files")

    def put(self, path: Path, media_type: str = "application/octet-stream") -> ArtifactRef:
        with private_state_directory(self.root), TemporaryDirectory(dir=self.root) as temporary:
            staged = Path(temporary) / "object"
            staged.mkdir(mode=0o700)
            with path.open("rb") as source, (staged / "content").open("xb") as output:
                digest, size = _digest(source, output)
            identifier = str(uuid5(NAMESPACE_URL, digest))
            ref = ArtifactRef(id=identifier, digest=digest, size_bytes=size, media_type=media_type)
            with (staged / "reference.json").open("xb") as metadata:
                metadata.write(ref.model_dump_json().encode())
                metadata.flush()
                os.fsync(metadata.fileno())
            sync_directory(staged)
            target = self.root / identifier
            try:
                staged.rename(target)
            except OSError as error:
                if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                    raise
                # A concurrent producer may already have published the same bytes.
                with SourceReader(self.root) as reader:
                    existing = self._reference(reader, identifier)
                    with reader.open(f"{identifier}/content") as content:
                        if _digest(content) != (digest, size):
                            raise ValueError(
                                "Stored artifact bytes do not match their reference"
                            ) from error
                if existing.digest != digest or existing.size_bytes != size:
                    raise ValueError(
                        "Stored artifact metadata does not match its content address"
                    ) from error
                return existing
            sync_directory(self.root)
            return ref

    @staticmethod
    def _reference(reader: SourceReader, identifier: str) -> ArtifactRef:
        with reader.open(f"{identifier}/reference.json") as metadata:
            ref = ArtifactRef.model_validate_json(metadata.read(4096))
        if ref.id != identifier or str(uuid5(NAMESPACE_URL, ref.digest)) != identifier:
            raise ValueError("Stored artifact reference does not match its content address")
        return ref

    def download(self, identifier: str, destination: Path) -> ArtifactRef:
        TypeAdapter(ID).validate_python(identifier)
        with SourceReader(self.root) as reader:
            ref = self._reference(reader, identifier)
            with reader.open(f"{identifier}/content") as source, destination.open("xb") as output:
                try:
                    digest, size = _digest(source, output)
                    if digest != ref.digest or size != ref.size_bytes:
                        raise ValueError("Stored artifact bytes do not match their reference")
                except BaseException:
                    destination.unlink(missing_ok=True)
                    raise
            return ref

    def project(
        self,
        path: Path,
        *,
        register: bool,
        manifest_path: str,
        revision: str | None,
        timeout: float,
    ) -> dict[str, Any]:
        with TemporaryDirectory(prefix="swarm-project-") as temporary:
            bundle = Path(temporary) / "source.zip"
            source = capture_project(
                path,
                bundle,
                manifest_path=manifest_path,
                revision=revision,
                capture_timeout_seconds=timeout,
            )
            if not register:
                return self.put(bundle, "application/zip").model_dump(mode="json")
            project = resolve_project(bundle, manifest_path=manifest_path)
            ref = self.put(bundle, "application/zip")
            snapshot = {
                "id": project.manifest.project.slug,
                "source_artifact": ref.model_dump(mode="json"),
                "source_revision": source.base_revision,
                "tree_digest": source.tree_digest,
                "manifest": project.model_dump(mode="json"),
            }
            self.store.register_project(project.manifest.project.slug, snapshot)
            return snapshot
