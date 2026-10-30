from __future__ import annotations

import secrets
from pathlib import Path
from typing import Any

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.models import TrialData
from research_swarm.control.queries import RecordQuery
from research_swarm.control.service import ResearchService
from research_swarm.control.workers import AttemptData
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import utc_now
from research_swarm.runtime import LocalRuntime, read_private, write_private
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.local_enrollment import ensure_local_enrollment
from research_swarm.workers.signatures import AssignmentVerifier
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline


def test_signed_native_execution_retains_complete_assignment_and_needs_no_private_key(
    pipeline: Pipeline,
) -> None:
    assert pipeline.worker.verifier is None
    assert pipeline.worker.assignment_verifier is not None
    admitted = pipeline.worker.claim(pipeline.ready())
    original = AttemptData.model_validate(admitted["data"])
    assert original.signed_assignment is not None
    verified = pipeline.worker.assignment_verifier.verify(
        original.signed_assignment, authority_now=utc_now()
    )
    assert verified.assignment.payload.inputs == original.inputs
    assert pipeline.worker.execute(admitted["id"])["state"] == "succeeded"
    entry = pipeline.worker.journal.get(admitted["id"])
    assert entry.signed_assignment == original.signed_assignment
    assert entry.prepared is not None and entry.native is not None
    assert entry.prepared.profile_digest == original.profile.execution_fingerprint
    with pytest.raises(ResearchError, match="dispatch changed"):
        pipeline.worker.journal.receive(verified.dispatch, entry.command, require_preparation=True)


@pytest.mark.parametrize("field", ["inputs", "source", "profile", "unsigned"])
def test_assignment_presentation_cannot_change_before_native_journaling(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    admitted = pipeline.worker.claim(pipeline.ready())
    get = pipeline.worker.client.get

    def altered(kind: str, identifier: str) -> dict[str, Any]:
        row = get(kind, identifier)
        if kind != "attempt":
            return row
        if field == "inputs":
            row["data"]["inputs"] = {"unexpected": row["data"]["source_artifact"]}
        elif field == "source":
            row["data"]["source_artifact"]["digest"] = content_digest(b"changed source")
        elif field == "profile":
            row["data"]["profile"]["execution_fingerprint"] = content_digest(b"changed profile")
        else:
            row["data"]["signed_assignment"] = None
        return row

    monkeypatch.setattr(pipeline.worker.client, "get", altered)
    with pytest.raises(ResearchError) as denied:
        pipeline.worker.execute(admitted["id"])
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pytest.raises(ResearchError) as absent:
        pipeline.worker.journal.get(admitted["id"])
    assert absent.value.code == ErrorCode.NOT_FOUND
    assert not (pipeline.worker.root / "work").exists()
    assert pipeline.worker.resources.availability() == {"cpu": 1}


def test_changed_signing_authority_rolls_back_attempt_and_budget(pipeline: Pipeline) -> None:
    runtime = pipeline.runtime
    service = ResearchService(
        runtime.database,
        runtime.artifacts,
        dispatch_key=read_private(runtime.root, "dispatch.key", limit=32),
        assignment_key=secrets.token_bytes(32),
    )
    object.__setattr__(runtime, "service", service)
    with pytest.raises(ResearchError) as stale:
        pipeline.worker.claim(pipeline.ready())
    assert stale.value.code == ErrorCode.STALE_FENCE
    assert not pipeline.client.list(RecordQuery(kind="attempt"))["items"]
    with runtime.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM budget_reservations").fetchone()[0] == 0
    assert pipeline.ready()["state"] == "ready"


def test_runtime_pins_distinct_signing_key_and_rejects_replacement(tmp_path: Path) -> None:
    runtime = LocalRuntime.open(tmp_path / "runtime")
    enrollment = ensure_local_enrollment(runtime, machine_registry=tmp_path / "machine.sqlite")
    identity = runtime.identity.assignment_key_id
    signing = read_private(runtime.root, "assignment.key", limit=32)
    assert signing != read_private(runtime.root, "dispatch.key", limit=32)
    reopened = LocalRuntime.open(runtime.root)
    assert reopened.identity.assignment_key_id == identity
    assert enrollment.worker.worker.assignment_authority is not None
    assert enrollment.worker.worker.assignment_authority.key_id == identity
    write_private(runtime.root / "assignment.key", secrets.token_bytes(32))
    with pytest.raises(ResearchError, match="pinned identity"):
        LocalRuntime.open(runtime.root)


def test_legacy_runtime_upgrades_once_without_replacing_existing_worker(tmp_path: Path) -> None:
    runtime = LocalRuntime.open(tmp_path / "runtime")
    # Emulate the pre-signing runtime and its immutable legacy worker admission.
    object.__setattr__(
        runtime,
        "service",
        ResearchService(
            runtime.database,
            runtime.artifacts,
            dispatch_key=read_private(runtime.root, "dispatch.key", limit=32),
        ),
    )
    worker = ensure_local_enrollment(runtime, machine_registry=tmp_path / "machine.sqlite")
    identity = runtime.identity.model_copy(update={"assignment_key_id": None})
    write_private(runtime.root / "identity.json", identity.model_dump_json().encode())
    (runtime.root / "assignment.key").unlink()
    reopened = LocalRuntime.open(runtime.root)
    assert reopened.identity.assignment_key_id is not None
    again = ensure_local_enrollment(reopened, machine_registry=tmp_path / "machine.sqlite")
    assert again.worker.worker == worker.worker.worker
    assert again.worker.assignment_verifier is None and again.worker.verifier is not None


def test_signed_state_cannot_silently_recreate_missing_identity(tmp_path: Path) -> None:
    runtime = LocalRuntime.open(tmp_path / "runtime")
    ensure_local_enrollment(runtime, machine_registry=tmp_path / "machine.sqlite")
    identity = runtime.identity.model_copy(update={"assignment_key_id": None})
    write_private(runtime.root / "identity.json", identity.model_dump_json().encode())
    (runtime.root / "assignment.key").unlink()
    with pytest.raises(ResearchError, match="recovery is required"):
        LocalRuntime.open(runtime.root)
    assert not (runtime.root / "assignment.key").exists()


def test_signed_admission_replays_after_controller_restart(pipeline: Pipeline) -> None:
    job = pipeline.ready()
    accepted = pipeline.worker.claim(job)
    reopened = LocalRuntime.open(pipeline.runtime.root)
    transport = pipeline.worker.client.transport
    assert isinstance(transport, LocalTransport)
    pipeline.worker.client = ResearchClient(LocalTransport(reopened, transport.resolve_principal))
    assert pipeline.worker.claim(job) == accepted
    assignment = AttemptData.model_validate(accepted["data"]).signed_assignment
    assert assignment is not None
    authority = pipeline.worker.worker.assignment_authority
    assert authority is not None
    AssignmentVerifier(authority).verify(assignment, authority_now=utc_now())


def test_prepared_signed_assignment_survives_public_only_worker_restart(pipeline: Pipeline) -> None:
    admitted = pipeline.worker.claim(pipeline.ready())
    attempt = AttemptData.model_validate(admitted["data"])
    trial = TrialData.model_validate(pipeline.trial["data"])
    prepared = pipeline.worker._prepare(attempt, trial)
    assert prepared.prepared is not None and prepared.native is None
    restarted = LocalWorker(
        pipeline.worker.client,
        pipeline.worker.worker,
        root=pipeline.worker.root,
        machine_registry=pipeline.worker.resources.path,
    )
    assert restarted.verifier is None
    result = restarted.execute(admitted["id"])
    assert result["state"] == "succeeded"
    completed = restarted.journal.get(admitted["id"])
    assert completed.signed_assignment == prepared.signed_assignment
    assert completed.prepared == prepared.prepared and completed.native is not None
    assert restarted.execute(admitted["id"]) == result
    assert restarted.journal.get(admitted["id"]) == completed
    assert restarted.resources.availability() == {"cpu": 1}
