"""The shared source-cut inspector refuses omitted custody and unresolved byte operations."""

import subprocess
import sys

import pytest

from research_swarm.checkpoints.backup import export_backup, verify_backup
from research_swarm.checkpoints.cut import inspect_sealed_cut
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction
from tests.test_native_backup import sealed_runtime as sealed_runtime


@pytest.fixture
def exported(sealed_runtime, tmp_path):
    runtime, context, request, _ = sealed_runtime
    path = tmp_path / "bundle"
    manifest = export_backup(runtime, context, path, backup_id=request.backup_id)
    return runtime, context, manifest, path


def inspect(runtime, **overrides):
    return runtime.database.store.read(
        lambda sql: inspect_sealed_cut(
            sql,
            workspace_id=runtime.identity.workspace_id,
            controller_id=runtime.identity.controller_id,
            storage_limit_bytes=runtime.identity.storage_limit_bytes,
            **overrides,
        )
    )


def test_shared_cut_matches_native_verified_manifest_without_mutation(exported):
    runtime, context, manifest, path = exported
    before = runtime.database.snapshot(context)
    cut = inspect(
        runtime,
        expected_backup_id=manifest.seal.backup_id,
        require_custody=True,
        require_seal_history=True,
    )
    assert cut.seal == manifest.seal
    assert cut.seal_revision == manifest.seal_revision and cut.seal_digest == manifest.seal_digest
    assert cut.evidence_through == manifest.snapshot_evidence_through
    assert cut.evidence_through > cut.seal.evidence_through
    assert cut.artifacts == manifest.artifacts
    assert cut.custody_pin == verify_backup(path).custody_pin
    assert runtime.database.snapshot(context) == before


@pytest.mark.parametrize(
    "change",
    [
        "foreign",
        "seal",
        "pin",
        "unknown-attempt",
        "upload",
        "worker-upload",
        "closed-worker-upload",
        "reader",
    ],
)
def test_cut_refuses_omitted_or_unclosed_state(exported, change):
    runtime, context, manifest, _ = exported

    def corrupt(sql):
        transaction = Transaction(sql, context)
        if change == "foreign":
            sql.execute(
                "INSERT INTO budget_settlements VALUES (?,?,?,?)",
                (new_id(), new_id(), "digest", "{}"),
            )
        elif change == "seal":
            sql.execute("DELETE FROM records WHERE kind='workspace_seal'")
        elif change == "pin":
            sql.execute("DELETE FROM records WHERE kind='backup_custody'")
        elif change == "unknown-attempt":
            transaction.insert(
                "attempt", new_id(), {"publication_status": "pending"}, state="unknown"
            )
        elif change == "reader":
            transaction.insert(
                "artifact_reference", new_id(), {"reader_session_id": new_id()}, state="retained"
            )
        else:
            identifier = new_id()
            transaction.insert(
                "upload" if change == "upload" else "worker_upload",
                identifier,
                {"upload_id": identifier},
                state="finalized" if change == "closed-worker-upload" else "pending",
            )

    runtime.database.store.write(corrupt)
    before = runtime.database.snapshot(context)
    with pytest.raises(ResearchError):
        inspect(
            runtime,
            expected_backup_id=manifest.seal.backup_id,
            require_custody=True,
            require_seal_history=True,
        )
    assert runtime.database.snapshot(context) == before


def test_native_legacy_history_is_optional_but_conflicting_history_refuses(exported):
    runtime, _, _, _ = exported
    runtime.database.store.write(
        lambda sql: sql.execute("UPDATE records SET state='changed' WHERE kind='workspace_seal'")
    )
    with pytest.raises(ResearchError, match="immutable seal differs"):
        inspect(runtime)
    runtime.database.store.write(
        lambda sql: sql.execute("DELETE FROM records WHERE kind='workspace_seal'")
    )
    assert inspect(runtime).seal.workspace_id == runtime.identity.workspace_id
    with pytest.raises(ResearchError, match="immutable seal differs"):
        inspect(runtime, require_seal_history=True)


def test_wrong_source_cut_or_storage_budget_refuses(exported):
    runtime, _, _, _ = exported
    with pytest.raises(ResearchError) as mismatch:
        inspect(runtime, expected_backup_id=new_id())
    assert mismatch.value.code == ErrorCode.STALE_FENCE
    with pytest.raises(ResearchError) as quota:
        runtime.database.store.read(
            lambda sql: inspect_sealed_cut(
                sql,
                workspace_id=runtime.identity.workspace_id,
                controller_id=runtime.identity.controller_id,
                storage_limit_bytes=0,
            )
        )
    assert quota.value.code == ErrorCode.ARTIFACT_UNVERIFIED


def test_cut_inspection_imports_no_native_storage_or_key_custody():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import research_swarm.checkpoints.cut; "
            "assert not {'sqlite3', 'cryptography', 'filelock', 'research_swarm.runtime', "
            "'research_swarm.checkpoints.backup'}.intersection(sys.modules)",
        ],
        check=True,
        capture_output=True,
        timeout=10,
    )
