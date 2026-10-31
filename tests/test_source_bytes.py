"""Native and memory-backed source inspection use identical validation and hashes."""

import io
import stat
import zipfile
from pathlib import Path

import pytest

from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ResearchError
from research_swarm.projects import source as source_module
from research_swarm.projects.manifest import resolve_project
from research_swarm.projects.source import extract_bundle, inspect_bundle, verify_bundle
from tests.test_manifest import capture
from tests.test_manifest import project as project


def test_project_resolution_and_complete_verification_need_no_filesystem(
    project, tmp_path, monkeypatch
):
    path = tmp_path / "source.zip"
    capture(project, path)
    content = path.read_bytes()
    expected = resolve_project(path)
    manifest = verify_bundle(path)
    output = tmp_path / "native"
    assert extract_bundle(path, output, expected_tree_digest=manifest.tree_digest) == manifest

    def forbidden(*args, **kwargs):
        pytest.fail("Memory source processing accessed a filesystem")

    with monkeypatch.context() as isolated:
        for method in ("open", "stat", "mkdir"):
            isolated.setattr(Path, method, forbidden)
        assert inspect_bundle(content) == verify_bundle(content) == manifest
        actual = resolve_project(content)
        assert canonical_bytes(actual.model_dump(mode="json")) == canonical_bytes(
            expected.model_dump(mode="json")
        )
    assert (output / "train.py").read_bytes() == (project / "train.py").read_bytes()


@pytest.mark.parametrize("fault", ["content", "compression", "symlink", "size", "extra"])
def test_byte_verification_rejects_corruption_and_unsafe_archive_metadata(project, tmp_path, fault):
    path = tmp_path / "source.zip"
    capture(project, path)
    valid = path.read_bytes()
    output = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(valid)) as original, zipfile.ZipFile(output, "w") as changed:
        for item in original.infolist():
            body = original.read(item)
            if item.filename == "train.py":
                if fault == "content":
                    body = body.replace(b"0.1", b"0.2")
                elif fault == "compression":
                    item.compress_type = zipfile.ZIP_DEFLATED
                elif fault == "symlink":
                    item.external_attr = (stat.S_IFLNK | 0o777) << 16
                elif fault == "size":
                    body += b"x"
            changed.writestr(item, body)
        if fault == "extra":
            changed.writestr("../unexpected", b"x")
    corrupted = output.getvalue()
    if fault == "content":
        # ZIP metadata and CRC are plausible; only the embedded source digest catches this.
        assert inspect_bundle(corrupted) == inspect_bundle(valid)
        with pytest.raises(ResearchError, match="digest mismatch"):
            verify_bundle(corrupted)
    else:
        with pytest.raises(ValueError):
            verify_bundle(corrupted)
    assert not (tmp_path / "unexpected").exists()


def test_byte_limit_is_checked_before_parsing_archive_metadata(monkeypatch):
    monkeypatch.setattr(source_module, "MAX_SOURCE_BYTES", 1)
    monkeypatch.setattr(source_module, "MAX_MANIFEST_BYTES", 1)
    monkeypatch.setattr(zipfile, "ZipFile", lambda *args: pytest.fail("Oversized input was parsed"))
    with pytest.raises(ValueError, match="size limit"):
        verify_bundle(b"xxxx")
