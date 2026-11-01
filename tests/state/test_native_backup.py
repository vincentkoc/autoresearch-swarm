"""Real sealed SQLite and filesystem snapshots, without cloning native workers."""

import os
from pathlib import Path

import pytest

from research_swarm.checkpoints.backup import export_backup, verify_backup
from research_swarm.cli import main
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.private_files import private_state_directory, write_private
from tests.projects.test_workspace_seal import seal_request


@pytest.fixture
def sealed_runtime(tmp_path):
    runtime = LocalRuntime.open(tmp_path / "runtime")
    context = CommandContext(principal=runtime.principal(), idempotency_key="evidence")
    artifact = runtime.artifacts.publish_bytes(context, b"checkpoint bytes", purpose="backup")
    # These are owned by the original installation, never part of a controller bundle.
    write_private(runtime.root / "worker-journal.sqlite", b"original custody")
    write_private(runtime.root / "machine-resources.sqlite", b"original custody")
    seal = context.model_copy(update={"idempotency_key": "seal", "expected_revision": 0})
    request = seal_request(runtime.database, seal)
    runtime.service.seal_workspace(seal, request)
    return runtime, context, request, artifact


def test_export_roundtrip_has_complete_inventory_and_preserves_source_cut(sealed_runtime, tmp_path):
    runtime, context, request, artifact = sealed_runtime
    before = runtime.database.snapshot(context)
    destination = tmp_path / "bundle"
    manifest = export_backup(runtime, context, destination, backup_id=request.backup_id)
    verified = verify_backup(destination, expected_digest=manifest.digest)
    assert verified.manifest == manifest
    assert verified.identity == runtime.identity
    assert [item.reference for item in manifest.artifacts] == [artifact]
    assert manifest.snapshot_evidence_through == manifest.seal.evidence_through + 2
    assert verified.custody_pin is not None
    assert verified.custody_pin.public_key == manifest.source_public_key
    after = runtime.database.snapshot(context)
    assert after["watermark"] == before["watermark"] + 1
    assert [row for row in after["records"] if row["kind"] != "backup_custody"] == before["records"]
    assert (
        destination / "blobs" / manifest.artifacts[0].generation
    ).read_bytes() == b"checkpoint bytes"
    assert (runtime.root / "backup-custody.json").is_file()
    assert {path.name for path in destination.iterdir()} == {
        "manifest.json",
        "research.sqlite",
        "private",
        "blobs",
    }
    assert {path.name for path in (destination / "private").iterdir()} == {
        "identity.json",
        "operator.token",
        "dispatch.key",
        "assignment.key",
    }
    assert export_backup(runtime, context, destination, backup_id=request.backup_id) == manifest
    assert runtime.database.snapshot(context) == after
    with pytest.raises(ResearchError):
        runtime.artifacts.publish_bytes(
            context.model_copy(update={"idempotency_key": "new"}), b"new bytes", purpose="new"
        )


@pytest.mark.parametrize("corruption", ["blob", "missing", "key", "extra", "signature", "hardlink"])
def test_verification_rejects_corrupt_or_unsafe_bundle(sealed_runtime, tmp_path, corruption):
    runtime, context, request, _ = sealed_runtime
    destination = tmp_path / "bundle"
    manifest = export_backup(runtime, context, destination, backup_id=request.backup_id)
    blob = destination / "blobs" / manifest.artifacts[0].generation
    if corruption == "blob":
        write_private(blob, b"changed bytes")
    elif corruption == "missing":
        blob.unlink()
    elif corruption == "key":
        write_private(destination / "private" / "assignment.key", b"x" * 32)
    elif corruption == "extra":
        write_private(destination / "unexpected", b"unknown")
    elif corruption == "signature":
        payload = strict_json_loads((destination / "manifest.json").read_bytes())
        payload["signature"] = "0" * 128
        from research_swarm.domain.canonical import canonical_bytes

        write_private(destination / "manifest.json", canonical_bytes(payload))
    else:
        os.link(blob, tmp_path / "linked")
    with pytest.raises((ResearchError, FileNotFoundError, ValueError)):
        verify_backup(destination, expected_digest=manifest.digest)


def test_failed_export_never_publishes_manifest_or_changes_research_state(
    sealed_runtime, tmp_path, monkeypatch
):
    runtime, context, request, _ = sealed_runtime
    destination = tmp_path / "interrupted"
    original = os.rename

    def rename(source, target):
        if Path(target) == destination / "manifest.json":
            raise OSError("Fixture interrupted publication")
        return original(source, target)

    monkeypatch.setattr(os, "rename", rename)
    before = runtime.database.snapshot(context)
    with pytest.raises(OSError):
        export_backup(runtime, context, destination, backup_id=request.backup_id)
    assert not (destination / "manifest.json").exists()
    with pytest.raises(FileNotFoundError):
        verify_backup(destination)
    after = runtime.database.snapshot(context)
    assert after["watermark"] == before["watermark"] + 1
    assert [row for row in after["records"] if row["kind"] != "backup_custody"] == before["records"]
    assert not list(tmp_path.glob(".research-backup-*"))


def test_backup_cli_verifies_without_creating_runtime(sealed_runtime, tmp_path, capsys):
    runtime, _, request, _ = sealed_runtime
    destination = tmp_path / "bundle"
    assert (
        main(
            [
                "--state-dir",
                str(runtime.root),
                "backup",
                "export",
                str(destination),
                "--backup-id",
                request.backup_id,
            ]
        )
        == 0
    )
    exported = strict_json_loads(capsys.readouterr().out)
    assert (
        main(["backup", "verify", str(destination), "--digest", exported["manifest_digest"]]) == 0
    )
    assert strict_json_loads(capsys.readouterr().out) == exported
    assert main(["backup", "verify", str(destination), "--digest", "sha256:" + "0" * 64]) == 2
    assert "ARTIFACT_UNVERIFIED" in capsys.readouterr().err


def test_exclusive_private_directory_never_adopts_existing_path(tmp_path):
    path = tmp_path / "exclusive"
    with private_state_directory(path, exclusive=True):
        pass
    with pytest.raises(OSError), private_state_directory(path, exclusive=True):
        pass


def test_unsealed_export_refuses_before_creating_custody_or_destination(tmp_path):
    runtime = LocalRuntime.open(tmp_path / "runtime")
    context = CommandContext(principal=runtime.principal(), idempotency_key="export")
    with pytest.raises(ResearchError):
        export_backup(
            runtime, context, tmp_path / "bundle", backup_id=runtime.identity.workspace_id
        )
    assert not (tmp_path / "bundle").exists()
    assert not (runtime.root / "backup-custody.json").exists()


def test_scoped_service_manager_cannot_export_root_credentials(sealed_runtime, tmp_path):
    runtime, _, request, _ = sealed_runtime
    context = CommandContext(
        principal=Principal(
            id=new_id(),
            workspace_id=runtime.identity.workspace_id,
            scopes=frozenset({"service.manage", "read"}),
        ),
        idempotency_key="export",
    )
    with pytest.raises(ResearchError):
        export_backup(runtime, context, tmp_path / "bundle", backup_id=request.backup_id)
    assert not (tmp_path / "bundle").exists()
    assert not (runtime.root / "backup-custody.json").exists()


def test_export_requires_existing_parent_before_custody_creation(sealed_runtime, tmp_path):
    runtime, context, request, _ = sealed_runtime
    with pytest.raises(ResearchError, match="parent must already exist"):
        export_backup(runtime, context, tmp_path / "absent" / "bundle", backup_id=request.backup_id)
    assert not (tmp_path / "absent").exists()
    assert not (runtime.root / "backup-custody.json").exists()


@pytest.mark.parametrize("succeeds", [True, False])
def test_windows_private_replacement_requires_write_through_without_fallback(
    tmp_path, monkeypatch, succeeds
):
    from research_swarm.state import windows_privacy

    calls = []

    def move(source, target, flags):
        calls.append((source, target, flags))
        return succeeds

    monkeypatch.setattr(windows_privacy, "_api", lambda: {"MoveFileExW": move})
    source, target = tmp_path / "staged", tmp_path / "published"
    if succeeds:
        windows_privacy.replace_write_through(source, target)
    else:
        with pytest.raises(OSError):
            windows_privacy.replace_write_through(source, target)
    assert calls == [(str(source), str(target), 0x1 | 0x8)]
