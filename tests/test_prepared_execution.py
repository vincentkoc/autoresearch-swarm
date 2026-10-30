from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.experiments import StageSpec
from research_swarm.domain.types import new_id
from research_swarm.projects.workload import WorkloadContext, read_report, verify_outputs
from research_swarm.workers.authority import VerifiedDispatch, WorkerBinding
from research_swarm.workers.journal import JournalState, WorkerJournal
from research_swarm.workers.prepared import PreparedExecution
from research_swarm.workers.resources import MachineResources, ResourceDemand
from research_swarm.workers.supervisor import run_attempt, spawn_supervisor
from tests.worker_support import dispatch


def prepare(
    root: Path, stage: StageSpec | None = None
) -> tuple[WorkerJournal, MachineResources, PreparedExecution]:
    source = root / "source"
    shutil.copytree(Path(__file__).resolve().parents[1] / "examples" / "cpu_bigram", source)
    stage = stage or StageSpec(
        id="train",
        kind="train",
        argv=("python", "train.py"),
        timeout_ms=10000,
        output_names=("model",),
    )
    verified, command = dispatch(source, stage=stage)
    binding = WorkerBinding.model_validate(
        {name: getattr(verified.envelope, name) for name in WorkerBinding.model_fields}
    )
    resources = MachineResources(root / "machine.sqlite")
    demand = ResourceDemand(amounts={"cpu": 1})
    resources.enroll(binding, demand)
    claim = resources.reserve(verified, demand)
    output = root / "model-output"
    output.mkdir()
    context = WorkloadContext(
        trial_id=verified.envelope.trial_id,
        attempt_id=verified.envelope.attempt_id,
        stage_id=stage.id,
        spec_digest=verified.envelope.spec_digest,
        seed=verified.spec.seed,
        time_budget_ms=100,
        output_root=str(output),
        report_path=str(root / "report.json"),
    ).sealed()
    context_path = root / "context.json"
    context_path.write_text(context.model_dump_json(), encoding="utf-8")
    prepared = PreparedExecution(
        context=context,
        context_path=str(context_path),
        executable=sys.executable,
        profile_digest=content_digest(b"synthetic-python-profile"),
        resource_registry=str(resources.path),
        machine_claim_id=claim.id,
    )
    journal = WorkerJournal(root / "worker.sqlite")
    journal.receive(verified, command, prepared=prepared)
    return journal, resources, prepared


def test_dedicated_supervisor_runs_the_sealed_context_and_releases_capacity(tmp_path: Path) -> None:
    journal, resources, prepared = prepare(tmp_path)
    attempt_id = prepared.context.attempt_id
    process = spawn_supervisor(journal, attempt_id)
    assert process.wait(timeout=20) == 0
    entry = journal.get(attempt_id)
    assert entry.state == JournalState.CLOSED and entry.outcome is not None
    assert entry.outcome.exit_code == 0 and entry.outcome.closure.scope_closed
    assert entry.command.argv[0] == "python"
    report = read_report(prepared.context, entry.spec.stages[0])
    assert report.observed_seed == 7
    assert report.diagnostics["optimizer_steps"] > 0
    assert len(verify_outputs(prepared.context, report, maximum_bytes=1024 * 1024)) == 1
    resources.release(entry)
    assert resources.availability() == {"cpu": 1}
    with pytest.raises(ResearchError):
        run_attempt(journal, attempt_id)
    assert journal.get(attempt_id) == entry


@pytest.mark.parametrize("defect", ["changed_context", "quarantined", "wrong_claim"])
def test_invalid_preparation_does_not_cross_native_start_barrier(
    tmp_path: Path, defect: str
) -> None:
    journal, resources, prepared = prepare(tmp_path)
    attempt_id = prepared.context.attempt_id
    entry = journal.get(attempt_id)
    claim = resources.verify_held(entry, prepared.machine_claim_id)
    if defect == "changed_context":
        changed = prepared.context.model_copy(update={"seed": 99, "context_digest": None}).sealed()
        Path(prepared.context_path).write_text(changed.model_dump_json(), encoding="utf-8")
    elif defect == "quarantined":
        resources.quarantine(claim, "Synthetic unresolved observation")
    else:
        # A different local registry cannot satisfy the persisted binding.
        resources.path.rename(tmp_path / "unrelated.sqlite")
    with pytest.raises(ResearchError):
        run_attempt(journal, attempt_id)
    assert journal.get(attempt_id).state == JournalState.RECEIVED
    assert not Path(prepared.context.report_path).exists()


def test_preparation_cannot_be_replaced_on_dispatch_retry(tmp_path: Path) -> None:
    journal, _, prepared = prepare(tmp_path)
    entry = journal.get(prepared.context.attempt_id)
    with pytest.raises(ResearchError, match="dispatch changed"):
        journal.receive(
            VerifiedDispatch(entry.dispatch, entry.spec),
            entry.command,
            prepared=prepared.model_copy(update={"profile_digest": content_digest(b"different")}),
        )
    with pytest.raises(ResearchError, match="identity differs"):
        prepared.model_copy(
            update={
                "context": prepared.context.model_copy(
                    update={"attempt_id": new_id(), "context_digest": None}
                ).sealed()
            }
        ).verify(entry.dispatch, entry.spec)


def test_cancelled_preparation_closes_even_when_its_context_is_missing(tmp_path: Path) -> None:
    journal, resources, prepared = prepare(tmp_path)
    attempt_id = prepared.context.attempt_id
    journal.request_cancel(attempt_id, 1)
    Path(prepared.context_path).unlink()
    result = run_attempt(journal, attempt_id)
    assert result.state == JournalState.NOT_STARTED and result.native is None
    resources.release(result)
    assert resources.availability() == {"cpu": 1}


def test_prepared_process_does_not_inherit_controller_secrets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SYNTHETIC_CONTROLLER_CREDENTIAL", "fixture-only")
    stage = StageSpec(
        id="train",
        kind="train",
        timeout_ms=4000,
        argv=(
            "python",
            "-c",
            "import os; assert 'RESEARCH_CONTEXT' in os.environ; "
            "assert 'SYNTHETIC_CONTROLLER_CREDENTIAL' not in os.environ",
        ),
    )
    journal, resources, prepared = prepare(tmp_path, stage)
    entry = run_attempt(journal, prepared.context.attempt_id)
    assert entry.state == JournalState.CLOSED and entry.outcome is not None
    assert entry.outcome.exit_code == 0
    resources.release(entry)
