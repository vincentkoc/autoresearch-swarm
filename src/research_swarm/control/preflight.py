"""Bounded source verification outside the controller write transaction."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import BadZipFile

from research_swarm.domain.artifacts import StoredBlob
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.projects.source import SourceManifest, extract_bundle, inspect_bundle
from research_swarm.state.artifacts import ArtifactService


@contextmanager
def verified_source(
    artifacts: ArtifactService, context: CommandContext, reference: ArtifactRef
) -> Iterator[tuple[Path, SourceManifest]]:
    """Keep the catalog pin through the caller's evidence-acceptance transaction."""
    with (
        artifacts.pinned(context, reference) as record,
        TemporaryDirectory(prefix="research-source-") as temporary,
    ):
        bundle = Path(temporary) / "source.zip"
        artifacts.store.fetch(
            context.principal.workspace_id,
            StoredBlob(reference=reference, generation=record.storage_generation),
            bundle,
        )
        try:
            manifest = inspect_bundle(bundle)
            extract_bundle(
                bundle, Path(temporary) / "verified", expected_tree_digest=manifest.tree_digest
            )
        except (ValueError, KeyError, BadZipFile) as error:
            raise ResearchError(
                ErrorCode.ARTIFACT_UNVERIFIED, "Source bundle violates its content contract"
            ) from error
        yield bundle, manifest
