from __future__ import annotations

import json
import os
import subprocess
import sys
import venv
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport
from research_swarm.control.models import TrialData
from research_swarm.control.worker_queries import WorkerQuery
from research_swarm.control.workers import AttemptData, EnrollWorker, WorkerData
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import ResourceRequirements
from research_swarm.domain.types import new_id
from research_swarm.engines import execution
from research_swarm.engines.models import DeviceObservation
from research_swarm.engines.registration import EnvironmentRegistration, register_environment
from research_swarm.workers.journal import JournalState, WorkerJournal
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.local_enrollment import ensure_local_enrollment
from research_swarm.workers.remote_enrollment import (
    connect_worker,
    describe_worker,
    provision_worker,
)
from research_swarm.workers.resources import MachineResources, ResourceDemand
from research_swarm.workers.supervisor import run_attempt
from tests.test_engines import descriptor
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_machine_resources import another_worker, binding
from tests.worker_support import dispatch


def registration(
    pipeline: Pipeline, tmp_path: Path, *, isolated: bool = False
) -> EnvironmentRegistration:
    interpreter = Path(sys.executable)
    if isolated:
        environment = tmp_path / "python-environment"
        venv.EnvBuilder(with_pip=False, symlinks=os.name != "nt").create(environment)
        interpreter = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    return register_environment(
        tmp_path / "environment-registration" / "engine.json",
        profile="python-cpu",
        executable=interpreter,
        environment_contracts=(pipeline.trial["data"]["spec"]["environment_digest"],),
        probe_directory=tmp_path / "probe-state",
    )


def worker(pipeline: Pipeline, registered: EnvironmentRegistration, tmp_path: Path) -> LocalWorker:
    transport = pipeline.worker.client.transport
    assert isinstance(transport, LocalTransport)
    principal = transport.resolve_principal()
    row = pipeline.client.command(
        "worker.enroll",
        EnrollWorker(
            worker_id=new_id(),
            session_id=new_id(),
            principal_id=principal.id,
            name="Registered CPU",
            profiles=(registered.profile,),
            environment_contracts=registered.environment_contracts,
        ),
        idempotency_key="registered-worker",
    )["result"]
    return LocalWorker(
        pipeline.worker.client,
        WorkerData.model_validate(row["data"]),
        root=tmp_path / "registered-worker",
        machine_registry=pipeline.worker.resources.path,
        registration=registered,
    )


def test_real_registered_venv_runs_complete_stage_graph(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    registered = registration(pipeline, tmp_path, isolated=True)
    executor = worker(pipeline, registered, tmp_path)
    assert registered.descriptor.packages == ()
    for _ in range(2):
        attempt = executor.claim(pipeline.ready())
        outcome = executor.execute(attempt["id"])
        assert outcome["state"] == "succeeded"
        entry = executor.journal.get(attempt["id"])
        assert entry.state == JournalState.CLOSED and entry.prepared is not None
        assert entry.prepared.executable == registered.executable
        assert entry.prepared.engine is not None
        assert (
            entry.prepared.context.parameters["engine"]["execution_fingerprint"]
            == registered.profile.execution_fingerprint
        )
        assert entry.prepared.command(entry.command).argv[:2] == (registered.executable, "-I")
        assert executor.resources.availability() == {"cpu": 1}
    assert pipeline.client.get("trial", pipeline.trial["id"])["state"] == "succeeded"


def test_registered_local_enrollment_replays_and_refuses_silent_mode_change(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    registered = registration(pipeline, tmp_path, isolated=True)
    registry = tmp_path / "local-machine" / "resources.sqlite"
    first = ensure_local_enrollment(
        pipeline.runtime, registration=registered, machine_registry=registry
    )
    resumed = ensure_local_enrollment(
        pipeline.runtime, registration=registered, machine_registry=registry
    )
    assert first.worker.worker == resumed.worker.worker
    assert first.worker.executable == Path(registered.executable)
    with pytest.raises(ResearchError):
        ensure_local_enrollment(pipeline.runtime, machine_registry=registry)
    attempt = resumed.worker.claim(pipeline.ready())
    assert resumed.worker.execute(attempt["id"])["state"] == "succeeded"


def test_registered_environment_mismatch_fails_inside_owned_scope(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    registered = registration(pipeline, tmp_path)
    stale = registered.model_copy(
        update={
            "descriptor": registered.descriptor.model_copy(
                update={"release": "synthetic-stale-release"}
            ),
        }
    )
    executor = worker(pipeline, stale, tmp_path)
    attempt = executor.claim(pipeline.ready())
    outcome = executor.execute(attempt["id"])
    assert outcome["state"] == "failed"
    entry = executor.journal.get(attempt["id"])
    assert entry.outcome.exit_code == 78
    assert entry.outcome.closure.scope_closed
    assert entry.prepared is not None
    assert not Path(entry.prepared.context.report_path).exists()
    assert not list(Path(entry.prepared.context.output_root).iterdir())
    assert executor.resources.availability() == {"cpu": 1}


def test_registered_worker_and_prepared_marker_cannot_drop_the_engine_binding(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    registered = registration(pipeline, tmp_path)
    executor = worker(pipeline, registered, tmp_path)
    with pytest.raises(ResearchError, match="pinned environment"):
        LocalWorker(
            executor.client,
            executor.worker,
            root=executor.root,
            machine_registry=executor.resources.path,
        )
    attempt = executor.claim(pipeline.ready())
    admitted = AttemptData.model_validate(attempt["data"])
    trial = TrialData.model_validate(pipeline.trial["data"])
    entry = executor._prepare(admitted, trial)
    stripped = entry.prepared.model_copy(update={"engine": None})
    with pytest.raises(ResearchError, match="present together"):
        stripped.verify(entry.dispatch, entry.spec)
    from research_swarm.runtime import write_private

    marker = executor.root / "work" / attempt["id"] / "prepared.json"
    write_private(marker, stripped.model_dump_json().encode())
    with pytest.raises(ResearchError, match="present together"):
        executor._prepare(admitted, trial)
    assert executor.journal.get(attempt["id"]).native is None


def test_publication_recovery_survives_missing_interpreter_and_frozen_launch_files(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registered = registration(pipeline, tmp_path, isolated=True)
    executor = worker(pipeline, registered, tmp_path)
    attempt = executor.claim(pipeline.ready())
    command = executor.client.command

    def offline(operation: str, *args: Any, **kwargs: Any) -> Any:
        if operation == "worker.publish_attempt":
            raise OSError("Synthetic unavailable publication")
        return command(operation, *args, **kwargs)

    monkeypatch.setattr(executor.client, "command", offline)
    with pytest.raises(OSError, match="publication"):
        executor.execute(attempt["id"])
    original = executor.journal.get(attempt["id"])
    assert original.state == JournalState.CLOSED
    Path(registered.executable).unlink()
    Path(original.prepared.engine.bootstrap.path).unlink()
    monkeypatch.setattr(executor.client, "command", command)
    restarted = LocalWorker(
        executor.client,
        executor.worker,
        root=executor.root,
        machine_registry=executor.resources.path,
        registration=registered,
    )
    outcome = restarted.execute(attempt["id"])
    assert outcome["state"] == "succeeded"
    assert restarted.journal.get(attempt["id"]).native == original.native
    assert restarted.resources.availability() == {"cpu": 1}


@pytest.mark.parametrize("tamper", ["bootstrap", "demand", "profile"])
def test_registered_binding_tampering_is_refused_before_native_creation(
    pipeline: Pipeline,
    tmp_path: Path,
    tamper: str,
) -> None:
    registered = registration(pipeline, tmp_path)
    executor = worker(pipeline, registered, tmp_path)
    attempt = executor.claim(pipeline.ready())
    entry = executor._prepare(
        AttemptData.model_validate(attempt["data"]),
        TrialData.model_validate(pipeline.trial["data"]),
    )
    if tamper == "bootstrap":
        Path(entry.prepared.engine.bootstrap.path).write_text("raise SystemExit(0)")
    elif tamper == "demand":
        with executor.resources._connect() as connection:
            row = connection.execute(
                "SELECT payload FROM claims WHERE attempt_id=?", (attempt["id"],)
            ).fetchone()
            claim = json.loads(row["payload"])
            claim["demand"]["amounts"]["cpu"] = 2
            connection.execute(
                "UPDATE claims SET payload=? WHERE attempt_id=?", (json.dumps(claim), attempt["id"])
            )
    else:
        # The serialized owner binding cannot substitute a different public profile.
        wrong = entry.prepared.model_copy(update={"profile_digest": content_digest(b"wrong")})
        with pytest.raises(ResearchError, match="context differs"):
            wrong.verify(entry.dispatch, entry.spec)
        return
    with pytest.raises(ResearchError):
        run_attempt(executor.journal, attempt["id"])
    assert executor.journal.get(attempt["id"]).native is None


def test_environment_contract_rejection_precedes_budget_reservation(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    registered = registration(pipeline, tmp_path).model_copy(
        update={
            "environment_contracts": (content_digest(b"unauthorized-environment"),),
        }
    )
    executor = worker(pipeline, registered, tmp_path)
    before = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError) as failure:
        executor.claim(pipeline.ready())
    assert failure.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    with pipeline.runtime.database.connect() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM records WHERE kind='attempt'").fetchone()[0]
            == 0
        )


def test_registered_http_worker_executes_scoped_assignment(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    registered = registration(pipeline, tmp_path, isolated=True)
    root = tmp_path / "http-worker"
    description = describe_worker(
        root,
        registration=registered,
        machine_registry=tmp_path / "http-machine" / "resources.sqlite",
    )
    assert str(tmp_path) not in description.model_dump_json()
    with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:
        destination = tmp_path / "bundle" / "worker.json"
        provision_worker(
            pipeline.client,
            description,
            url=url,
            project_ids=(pipeline.campaign["data"]["contract"]["project_id"],),
            destination=destination,
        )
        with connect_worker(root, bundle_path=destination) as enrolled:
            bound = enrolled.worker.worker.binding
            offers = enrolled.worker.client.worker_list(
                WorkerQuery(
                    worker_id=bound.worker_id,
                    session_id=bound.worker_session_id,
                    kind="offers",
                )
            )["items"]
            attempt = enrolled.worker.claim(offers[0])
            result = enrolled.worker.execute(attempt["id"])
            assert result["state"] == "succeeded"
        with connect_worker(root) as restarted:
            assert restarted.worker.execute(attempt["id"])["state"] == "succeeded"


def test_mps_mlx_share_exclusive_device_and_unknown_retains_capacity(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    registered = registration(pipeline, tmp_path)
    shared = DeviceObservation(
        index=0,
        name="Fixture Apple device",
        physical_id="apple-integrated-0",
        unified_memory=True,
    )
    profiles = [
        registered.model_copy(
            update={
                "descriptor": descriptor(
                    profile=name,
                    framework=framework,
                    device_backend=backend,
                    framework_version="fixture",
                    device=shared,
                ),
                "cpu_slots": 4,
                "memory_bytes": 4096,
            }
        )
        for name, framework, backend in (
            ("pytorch-mps", "pytorch", "mps"),
            ("mlx-metal", "mlx", "metal"),
        )
    ]
    first, command = dispatch(tmp_path)
    second = another_worker(first)
    machine = MachineResources(tmp_path / "apple-machine.sqlite")
    for profile in profiles:
        machine.enroll(binding(first), ResourceDemand(amounts=execution.capacities(profile)))
    demands = [
        ResourceDemand(
            amounts=execution.demand(
                ResourceRequirements(
                    engine_profile=profile.descriptor.profile,
                    accelerator_count=1,
                    memory_bytes=256,
                    accelerator_memory_bytes=512,
                ),
                profile,
            )
        )
        for profile in profiles
    ]
    assert demands[0] == demands[1]
    assert demands[0].amounts["memory"] == 768
    claim = machine.reserve(first, demands[0])
    machine.quarantine(claim, "Synthetic unresolved native scope")
    with pytest.raises(ResearchError, match="occupied"):
        MachineResources(machine.path).reserve(second, demands[1])
    journal = WorkerJournal(tmp_path / "held-journal.sqlite")
    entry = journal.receive(first, command)
    with pytest.raises(ResearchError):
        machine.release(entry)
    journal.request_cancel(entry.dispatch.attempt_id, 1)
    machine.release(journal.settle_unstarted(entry.dispatch.attempt_id))
    machine.reserve(second, demands[1])


@pytest.mark.parametrize(
    "options",
    [
        {"accelerator_count": 0},
        {"accelerator_count": 2},
        {"accelerator_count": 1, "exclusive_device": False},
        {"accelerator_count": 1, "memory_bytes": 700, "accelerator_memory_bytes": 700},
    ],
)
def test_accelerator_admission_refuses_sharing_and_double_counted_memory(
    pipeline: Pipeline,
    tmp_path: Path,
    options: dict[str, Any],
) -> None:
    registered = registration(pipeline, tmp_path).model_copy(
        update={
            "descriptor": descriptor(
                profile="mlx-metal",
                framework="mlx",
                device_backend="metal",
                framework_version="fixture",
                device=DeviceObservation(
                    index=0, name="Fixture", physical_id="apple-integrated-0", unified_memory=True
                ),
            ),
            "memory_bytes": 1024,
        }
    )
    with pytest.raises(ResearchError):
        execution.demand(ResourceRequirements(engine_profile="mlx-metal", **options), registered)


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        (
            (
                "-c",
                "import sys,json,__main__; "
                "print(json.dumps([sys.argv,sys.flags.isolated,hasattr(__main__,'payload')]))",
                "tail",
            ),
            [["-c", "tail"], 1, False],
        ),
        (("-m", "project_module", "tail"), ["tail"]),
        (("project_script.py", "tail"), ["tail"]),
    ],
)
def test_registered_launch_python_invocations(
    pipeline: Pipeline,
    tmp_path: Path,
    arguments: tuple[str, ...],
    expected: Any,
) -> None:
    registered = registration(pipeline, tmp_path, isolated=True)
    frozen = execution.prepare_engine(tmp_path / "frozen-engine", registered)
    project = tmp_path / "project"
    project.mkdir()
    code = "import json,sys\nprint(json.dumps(sys.argv[1:]))\n"
    (project / "project_module.py").write_text(code)
    (project / "project_script.py").write_text(code)
    process = subprocess.run(
        [registered.executable, "-I", frozen.bootstrap.path, frozen.payload.path, *arguments],
        cwd=project,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert process.returncode == 0, process.stderr
    assert json.loads(process.stdout) == expected


def test_registered_script_argv_and_inline_dynamic_import_path(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    registered = registration(pipeline, tmp_path, isolated=True)
    frozen = execution.prepare_engine(tmp_path / "frozen-engine", registered)
    project = tmp_path / "project"
    nested = project / "nested"
    nested.mkdir(parents=True)
    (project / "script.py").write_text("import sys; print(sys.argv[0])")
    (nested / "after_chdir.py").write_text("VALUE = 41")

    def launch(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [registered.executable, "-I", frozen.bootstrap.path, frozen.payload.path, *arguments],
            cwd=project,
            capture_output=True,
            text=True,
            timeout=20,
        )

    script = launch("script.py")
    assert script.returncode == 0, script.stderr
    assert script.stdout.strip() == "script.py"
    inline = launch(
        "-c", "import os; os.chdir('nested'); import after_chdir; print(after_chdir.VALUE)"
    )
    assert inline.returncode == 0, inline.stderr
    assert inline.stdout.strip() == "41"
