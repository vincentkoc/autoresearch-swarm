"""The shared restore transaction composes atomically without native runtime imports."""

import subprocess
import sys

import pytest

from research_swarm.checkpoints.backup import export_backup
from research_swarm.checkpoints.inactive import RestorationCut, restore_inactive_in
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction
from tests.state.test_native_backup import sealed_runtime as sealed_runtime


@pytest.mark.parametrize("module", ["inactive", "admission_state"])
def test_shared_recovery_state_does_not_import_native_backend_or_custody_implementation(module):
    subprocess.run(
        [
            sys.executable,
            "-c",
            f"import sys; import research_swarm.checkpoints.{module}; "
            "assert not {'cryptography', 'sqlite3', 'filelock', 'research_swarm.runtime', "
            "'research_swarm.checkpoints.backup', 'research_swarm.checkpoints.restore'} "
            ".intersection(sys.modules)",
        ],
        check=True,
        capture_output=True,
        timeout=10,
    )


def test_caller_transaction_rolls_back_staged_rows_and_inactive_transform_together(
    sealed_runtime, tmp_path
):
    runtime, context, seal, _ = sealed_runtime
    manifest = export_backup(runtime, context, tmp_path / "bundle", backup_id=seal.backup_id)
    source = RestorationCut(
        seal=manifest.seal,
        seal_revision=manifest.seal_revision,
        snapshot_evidence_through=manifest.snapshot_evidence_through,
        manifest_digest=manifest.digest,
        source_custody_id=manifest.source_custody_id,
        source_public_key=manifest.source_public_key,
    )
    identifier, marker = new_id(), new_id()
    before = runtime.database.snapshot(context)

    class SqlOnly:
        def __init__(self, sql):
            self.sql = sql

        def execute(self, statement, parameters=()):
            return self.sql.execute(statement, parameters)

        def atomic(self, callback):
            return self.sql.atomic(lambda sql: callback(SqlOnly(sql)))

    def transform(sql, *, fail):
        sql = SqlOnly(sql)
        Transaction(sql, context).insert("staged_import", marker, {}, state="staged")
        receipt = restore_inactive_in(
            sql,
            context,
            source,
            restore_id=identifier,
            target_public_key="a" * 64,
            restored_at="2026-01-01T00:00:00.000Z",
        )
        assert receipt.target_control_epoch == receipt.source_control_epoch + 1
        if fail:
            raise RuntimeError("Synthetic interruption before import commit")
        return receipt

    with pytest.raises(RuntimeError, match="before import commit"):
        runtime.database.store.write(lambda sql: transform(sql, fail=True))
    assert runtime.database.snapshot(context) == before
    receipt = runtime.database.store.write(lambda sql: transform(sql, fail=False))
    assert receipt.restored_at == "2026-01-01T00:00:00.000Z"
    assert runtime.database.read_record(context, "restoration", identifier)["state"] == "inactive"
    assert runtime.database.read_record(context, "staged_import", marker)["state"] == "staged"
    assert (
        runtime.database.read_record(context, "workspace_gate", receipt.workspace_id)["state"]
        == "restoring"
    )
    assert (
        runtime.database.store.write(
            lambda sql: restore_inactive_in(
                SqlOnly(sql),
                context,
                source,
                restore_id=identifier,
                target_public_key="a" * 64,
            )
        )
        == receipt
    )
