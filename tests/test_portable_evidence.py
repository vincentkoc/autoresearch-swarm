"""Controllers parse worker evidence without loading native worker infrastructure."""

import subprocess
import sys
from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest
from pydantic import ValidationError

from research_swarm.control.worker_outputs import WorkerOutputIntent
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.types import new_id
from research_swarm.projects.workload import WorkloadReport
from research_swarm.workers.contracts import NativeCommand
from research_swarm.workers.journal_records import JournalEntry
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_reconciliation import closed as closed
from tests.test_reconciliation import reconcile, submit


def test_service_import_does_not_load_native_worker_database_or_process_modules():
    script = """
import builtins
import pydantic
import sys
original = builtins.__import__
blocked = {"sqlite3", "psutil", "ctypes", "filelock"}
def guarded(name, *args, **kwargs):
    if name.split(".")[0] in blocked:
        raise AssertionError("Native import reached portable controller: " + name)
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
from research_swarm.control.service import ResearchService
from research_swarm.workers.journal_records import JournalEntry
assert "research_swarm.workers.journal" not in sys.modules
assert "research_swarm.state.sqlite" not in sys.modules
assert "research_swarm.projects.native_files" not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr[-6000:]


@pytest.mark.parametrize(
    "cwd", ["/worker/run", r"C:\worker\run", "C:/worker/run", r"\\worker\share\run"]
)
def test_command_path_shape_accepts_both_platforms_without_rewriting(cwd):
    command = NativeCommand(argv=("python", "train.py"), cwd=cwd, timeout_ms=1000)
    assert NativeCommand.model_validate_json(command.model_dump_json()).cwd == cwd


@pytest.mark.parametrize("cwd", ["run", "C:run", r"\run", "", "/worker/\0run"])
def test_command_path_shape_rejects_relative_or_ambiguous_paths(cwd):
    with pytest.raises(ValidationError):
        NativeCommand(argv=("python", "train.py"), cwd=cwd, timeout_ms=1000)


@pytest.mark.parametrize("flavor", ["posix", "windows"])
def test_controller_reconciles_original_closure_with_portable_worker_paths(
    pipeline, closed, tmp_path, flavor
):
    # Controlled path metadata, not a claim of executing another operating system.
    original = closed.native.journal.get(closed.publication.attempt_id)
    prepared = original.prepared
    assert prepared is not None
    root = (
        PurePosixPath("/synthetic-worker/run")
        if flavor == "posix"
        else PureWindowsPath(r"C:\synthetic-worker\run")
    )
    context = prepared.context.model_copy(
        update={
            "inputs": {
                name: item.model_copy(update={"path": str(root / "inputs" / name)})
                for name, item in prepared.context.inputs.items()
            },
            "output_root": str(root / "outputs"),
            "report_path": str(root / "report.json"),
            "context_digest": None,
        }
    ).sealed()
    relocated = prepared.model_copy(
        update={
            "context": context,
            "context_path": str(root / "context.json"),
            "executable": str(root / "python"),
            "resource_registry": str(root / "registry.sqlite"),
            "working_directory": str(root),
            "engine": prepared.engine.model_copy(
                update={
                    name: getattr(prepared.engine, name).model_copy(
                        update={"path": str(root / name)}
                    )
                    for name in ("registration", "bootstrap", "probe", "payload")
                }
            )
            if prepared.engine is not None
            else None,
        }
    )
    changed = original.model_copy(
        update={
            "command": original.command.model_copy(update={"cwd": str(root)}),
            "prepared": relocated,
        }
    )
    encoded = canonical_bytes(changed.model_dump(mode="json"))
    restored = JournalEntry.model_validate_json(encoded)
    restored.prepared.context.verify()
    assert restored.prepared.context.context_digest == context.context_digest
    assert restored.native == original.native and restored.outcome == original.outcome
    assert (
        restored.dispatch == original.dispatch
        and restored.signed_assignment == original.signed_assignment
    )
    report = WorkloadReport.model_validate_json(Path(prepared.context.report_path).read_bytes())
    updated_report = report.model_copy(update={"context_digest": context.context_digest})
    uploads = {}
    for role, content in (
        ("journal", encoded),
        ("report", canonical_bytes(updated_report.model_dump(mode="json"))),
    ):
        path = tmp_path / ("portable-" + role + ".json")
        path.write_bytes(content)
        uploads[role + "_artifact"] = closed.client.worker_upload_file(
            WorkerOutputIntent(
                worker_id=original.dispatch.worker_id,
                session_id=original.dispatch.worker_session_id,
                attempt_id=original.dispatch.attempt_id,
                role=role,
            ),
            path,
            idempotency_key=new_id(),
        )
    publication = closed.publication.model_copy(update=uploads)
    submission = submit(closed, publication)
    accepted = reconcile(pipeline, submission)
    assert accepted["attempt"]["state"] == "succeeded"
    assert reconcile(pipeline, submission)["disposition"] == "already_accepted"
