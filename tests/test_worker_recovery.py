from __future__ import annotations

import json
import shutil
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from filelock import FileLock

from research_swarm.api.http import create_app
from research_swarm.client import ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.runtime import private_state_directory, read_private, write_private
from research_swarm.workers import remote_enrollment
from research_swarm.workers.journal import JournalState, WorkerJournal
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.recovery import RecoveryIntent, recover_worker_attempt
from research_swarm.workers.remote_enrollment import RemoteEnrollment
from research_swarm.workers.resources import MachineResources
from tests.test_closure import close_operationally
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_scoped_driver import enrollment
from tests.test_worker_sessions import advance_epoch


@dataclass
class Recoverable:
    root: Path
    enrollment: RemoteEnrollment
    attempt_id: str

    @property
    def recovery(self) -> Path:
        return self.root / "execution/recovery" / self.attempt_id

    def intent(self) -> RecoveryIntent:
        return RecoveryIntent.model_validate_json(
            read_private(self.recovery, "intent.json", limit=2 * 1024 * 1024)
        )


@pytest.fixture
def recoverable(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Recoverable]:
    root = tmp_path / "remote"
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, root, url) as connected,
    ):
        native = connected.worker
        attempt = native.claim(pipeline.ready())

        def lost_release(*_: Any, **__: Any) -> None:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Synthetic driver loss before release"
            )

        with monkeypatch.context() as patch:
            patch.setattr(native.resources, "release", lost_release)
            with pytest.raises(ResearchError, match="driver loss"):
                native.execute(attempt["id"])
        entry = native.journal.get(attempt["id"])
        if entry.state != JournalState.CLOSED or entry.outcome is None:

            def bounded(value: str) -> str:
                for private in (
                    connected.bundle.token,
                    str(root),
                    str(pipeline.runtime.root),
                    sys.executable,
                ):
                    value = value.replace(private, "<redacted>")
                return value[:1024]

            timing, outcome = entry.timing, entry.outcome
            diagnostic = {
                "state": entry.state.value,
                "finding": bounded(entry.finding),
                "clock_mode": entry.clock_mode,
                "prepared": entry.prepared is not None,
                "supervisor": entry.supervisor_id is not None,
                "native": entry.native is not None,
                "cancel_requested": entry.cancel_requested,
                "stage_timeout_ms": entry.command.timeout_ms,
                "timing": None
                if timing is None
                else {
                    "roundtrip_ms": (
                        int(timing.received.continuous_ns) - int(timing.sent.continuous_ns)
                    )
                    / 1e6,
                    "sample_age_ms": (
                        int(timing.qualified_at.continuous_ns) - int(timing.received.continuous_ns)
                    )
                    / 1e6,
                    "launch_headroom_ms": (
                        int(timing.caps.launch_ns) - int(timing.qualified_at.continuous_ns)
                    )
                    / 1e6,
                    "maximum_roundtrip_ms": timing.reply.payload.policy.maximum_roundtrip_ms,
                    "maximum_sample_age_ms": timing.reply.payload.policy.maximum_sample_age_ms,
                },
                "outcome": None
                if outcome is None
                else {
                    "reason": outcome.reason,
                    "exit_code": outcome.exit_code,
                    "prepared_at": outcome.prepared_at,
                    "released_at": outcome.released_at,
                    "finished_at": outcome.finished_at,
                    "elapsed_ms": outcome.elapsed_ms,
                    "scope_closed": outcome.closure.scope_closed,
                    "findings": [bounded(item) for item in outcome.closure.findings[:8]],
                },
            }
            pytest.fail(
                "Native recovery setup did not finish: " + json.dumps(diagnostic, sort_keys=True)
            )
        assert native.resources.availability() == {"cpu": 0}
        yield Recoverable(root, connected, attempt["id"])


def test_recovery_after_epoch_change_releases_then_quarantines_without_execution(
    pipeline: Pipeline, recoverable: Recoverable, monkeypatch: pytest.MonkeyPatch
) -> None:
    native = recoverable.enrollment.worker
    before = native.journal.get(recoverable.attempt_id)
    advance_epoch(pipeline)
    shutil.rmtree(recoverable.root / "execution/work")
    shutil.rmtree(recoverable.root / "execution/executions")

    def forbidden(*_: Any, **__: Any) -> Any:
        raise AssertionError("Recovery entered a launch-sensitive path")

    for owner, name in (
        (LocalWorker, "__init__"),
        (LocalWorker, "claim"),
        (LocalWorker, "execute"),
        (MachineResources, "enroll"),
        (MachineResources, "reserve"),
        (WorkerJournal, "receive"),
        (remote_enrollment, "_verify_machine"),
        (remote_enrollment, "verify_registration"),
    ):
        monkeypatch.setattr(owner, name, forbidden)
    result = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert result.delivery_state == "quarantined" and result.custody_state == "released"
    assert result.native_state == JournalState.CLOSED
    assert result.next_action == "operator_reconcile"
    assert recover_worker_attempt(recoverable.root, recoverable.attempt_id) == result
    assert native.journal.get(recoverable.attempt_id) == before
    assert native.resources.availability() == {"cpu": 1}
    assert (
        pipeline.client.get("attempt", recoverable.attempt_id)["data"]["publication_status"]
        == "pending"
    )
    assert pipeline.client.list(RecordQuery(kind="run_result"))["items"] == []
    submission = pipeline.client.get("attempt_submission", result.submission_id)
    assert submission["data"]["publication"]["report_artifact"] is None
    assert submission["data"]["publication"]["outputs"] == {}
    settled = close_operationally(pipeline, submission)
    assert settled["attempt"]["state"] == "closed_unaccepted"
    assert not (recoverable.root / "execution/driver/commands").exists()


@pytest.mark.parametrize("operation", ["worker.finalize_upload", "worker.submit_evidence"])
def test_lost_acknowledgment_replays_frozen_evidence_and_command(
    pipeline: Pipeline, recoverable: Recoverable, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    original = ResearchClient.command
    lost = False
    calls = []

    def lose(self: ResearchClient, name: str, request: Any, **kwargs: Any) -> Any:
        nonlocal lost
        reply = original(self, name, request, **kwargs)
        if name == operation:
            calls.append(kwargs["idempotency_key"])
            if not lost:
                lost = True
                raise ResearchError(ErrorCode.DELIVERY_UNCERTAIN, "Synthetic lost acknowledgment")
        return reply

    # Upload uses its transport directly, so inject the same post-commit failure there.
    if operation == "worker.finalize_upload":
        from research_swarm.client import HTTPTransport

        transport_command = HTTPTransport.command

        def lose_upload(self: Any, name: str, payload: Any, key: str, revision: Any) -> Any:
            nonlocal lost
            reply = transport_command(self, name, payload, key, revision)
            if name == operation:
                calls.append(key)
                if not lost:
                    lost = True
                    raise ResearchError(ErrorCode.DELIVERY_UNCERTAIN, "Synthetic lost upload reply")
            return reply

        monkeypatch.setattr(HTTPTransport, "command", lose_upload)
    else:
        monkeypatch.setattr(ResearchClient, "command", lose)
    first = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert first.delivery_state == "pending" and first.custody_state == "released"
    frozen = recoverable.intent()
    again = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert again.delivery_state == "quarantined"
    delivered = recoverable.intent()
    assert delivered.journal == frozen.journal
    assert delivered.upload_key == frozen.upload_key
    assert delivered.submission_key == frozen.submission_key
    assert len(set(calls)) == 1
    assert len(pipeline.client.list(RecordQuery(kind="attempt_submission"))["items"]) == 1


def test_revoked_delivery_does_not_undo_local_native_release(
    pipeline: Pipeline, recoverable: Recoverable
) -> None:
    pipeline.runtime.credentials.revoke(
        pipeline.runtime.principal(), recoverable.enrollment.bundle.credential_id
    )
    report = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert report.delivery_state == "blocked" and report.error == ErrorCode.AUTHORIZATION_DENIED
    assert report.custody_state == "released" and report.submission_id is None
    assert recoverable.enrollment.worker.resources.availability() == {"cpu": 1}


@pytest.mark.parametrize("store", ["journal", "registry"])
def test_missing_original_store_is_not_created(
    recoverable: Recoverable, monkeypatch: pytest.MonkeyPatch, store: str
) -> None:
    native = recoverable.enrollment.worker
    path = native.journal.path if store == "journal" else native.resources.path
    path.rename(path.with_suffix(".original"))
    with pytest.raises(ResearchError) as refused:
        recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert refused.value.code == ErrorCode.EXECUTION_UNKNOWN
    assert not path.exists()
    assert not recoverable.recovery.exists()


@pytest.mark.parametrize("fault", ["signature", "driver", "driver_missing", "registry_identity"])
def test_changed_original_authority_fails_before_release_or_upload(
    recoverable: Recoverable, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    native = recoverable.enrollment.worker
    if fault == "signature":
        entry = native.journal.get(recoverable.attempt_id)
        assert entry.signed_assignment is not None
        changed = entry.model_dump(mode="json")
        changed["signed_assignment"]["signature"] = "00" * 64
        with native.journal._transaction() as connection:
            connection.execute(
                "UPDATE attempts SET payload=? WHERE id=?",
                (json.dumps(changed), recoverable.attempt_id),
            )
    elif fault.startswith("driver"):
        driver = recoverable.root / "execution/driver"
        driver.mkdir(mode=0o700)
        if fault == "driver":
            write_private(driver / "identity.json", b"{}")
        else:
            (driver / "commands").mkdir()
    else:
        with native.resources._connect() as connection:
            connection.execute(
                "UPDATE storage_identity SET id=?", ("00000000-0000-4000-8000-000000000000",)
            )

    def forbidden(*_: Any, **__: Any) -> Any:
        raise AssertionError("Invalid original authority reached a side effect")

    monkeypatch.setattr(MachineResources, "release", forbidden)
    monkeypatch.setattr(ResearchClient, "connect", forbidden)
    with pytest.raises((ResearchError, ValueError)):
        recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert not recoverable.recovery.exists()


def test_original_driver_and_attempt_locks_are_respected(recoverable: Recoverable) -> None:
    driver = recoverable.root / "execution/driver"
    with private_state_directory(driver):
        pass
    for path in (
        driver / "driver.lock",
        recoverable.root / "execution/locks" / (recoverable.attempt_id + ".lock"),
    ):
        with FileLock(path), pytest.raises(ResearchError) as busy:
            recover_worker_attempt(recoverable.root, recoverable.attempt_id)
        assert busy.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert recoverable.enrollment.worker.resources.availability() == {"cpu": 0}


def test_real_cli_reports_original_evidence_without_secret_fields(
    pipeline: Pipeline, recoverable: Recoverable
) -> None:
    advance_epoch(pipeline)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_swarm.cli",
            "worker",
            "recover",
            "--worker-dir",
            str(recoverable.root),
            "--attempt",
            recoverable.attempt_id,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["delivery_state"] == "quarantined" and report["custody_state"] == "released"
    assert recoverable.enrollment.bundle.token not in result.stdout + result.stderr
    assert str(recoverable.root) not in result.stdout + result.stderr
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1


def test_operator_closure_racing_upload_is_reported_without_borrowing_local_proof(
    pipeline: Pipeline, recoverable: Recoverable, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = ResearchClient.worker_upload_file
    first = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    submission = pipeline.client.get("attempt_submission", first.submission_id)
    # A new observation creates a new frozen snapshot and forces a fresh upload.
    native = recoverable.enrollment.worker
    native.journal.note_reconciliation(recoverable.attempt_id, "Additional original observation")

    def race(self: ResearchClient, *args: Any, **kwargs: Any) -> Any:
        close_operationally(pipeline, submission)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(ResearchClient, "worker_upload_file", race)
    result = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert result.delivery_state == "controller_terminal"
    assert result.controller_disposition == "closed_unaccepted"
    assert result.custody_state == "released" and result.submission_id is None
    assert result.next_action == "none"


@pytest.mark.parametrize("pipeline", [{"source_finish_delay": 4}], indirect=True)
def test_live_supervisor_keeps_custody_and_pending_snapshot_survives_later_closure(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import time

    from research_swarm.control.models import TrialData
    from research_swarm.control.worker_records import AttemptData
    from research_swarm.workers.timed_supervision import start_timed_supervisor

    root = tmp_path / "remote"
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, root, url) as connected,
    ):
        native = connected.worker
        row = native.claim(pipeline.ready())
        attempt = AttemptData.model_validate(row["data"])
        trial = TrialData.model_validate(pipeline.trial["data"])
        native._verify(attempt, trial)
        entry = native._prepare(attempt, trial)
        supervised = start_timed_supervisor(native.journal, row["id"], native._timed_origin(entry))
        try:
            until = time.monotonic() + 10
            while native.journal.get(row["id"]).state != JournalState.RUNNING:
                assert supervised.process.poll() is None and time.monotonic() < until
                time.sleep(0.01)
            running = native.journal.get(row["id"])
            original = ResearchClient.command
            lost = False

            def lose(self: ResearchClient, name: str, request: Any, **kwargs: Any) -> Any:
                nonlocal lost
                reply = original(self, name, request, **kwargs)
                if name == "worker.submit_evidence" and not lost:
                    lost = True
                    raise ResearchError(ErrorCode.DELIVERY_UNCERTAIN, "Synthetic lost observation")
                return reply

            with monkeypatch.context() as patch:
                patch.setattr(ResearchClient, "command", lose)
                first = recover_worker_attempt(root, row["id"])
            assert first.delivery_state == "pending" and first.custody_state == "retained"
            assert first.native_state == JournalState.RUNNING
            assert supervised.process.poll() is None
            assert native.resources.availability() == {"cpu": 0}
            assert native.journal.get(row["id"]).cancel_requested is False
            supervised.process.wait(timeout=15)
            closed = native.journal.get(row["id"])
            assert closed.state == JournalState.CLOSED and closed.native == running.native
            second = recover_worker_attempt(root, row["id"])
            assert second.custody_state == "released" and second.delivery_state == "quarantined"
            assert second.native_state == JournalState.CLOSED
            assert second.submitted_native_state == JournalState.RUNNING
            assert second.next_action == "observe_native"
            third = recover_worker_attempt(root, row["id"])
            assert third.submitted_native_state == JournalState.CLOSED
            assert third.submission_id != second.submission_id
            assert third.next_action == "operator_reconcile"
            assert native.resources.availability() == {"cpu": 1}
            assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1
            assert len(pipeline.client.list(RecordQuery(kind="attempt_submission"))["items"]) == 2
        finally:
            supervised.close()
            supervised.process.wait(timeout=15)


@pytest.mark.parametrize(
    "pipeline", [{"source_finish_gate": True, "source_train_timeout_ms": 30000}], indirect=True
)
def test_controller_process_restart_preserves_live_native_scope_and_original_settlement(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    import time

    from research_swarm.control.models import TrialData
    from research_swarm.control.worker_records import AttemptData
    from research_swarm.workers.timed_supervision import start_timed_supervisor
    from tests.test_reconciliation import reconcile
    from tests.test_scoped_driver import controller_process

    controllers = []
    root = tmp_path / "remote"
    with (
        controller_process(pipeline, processes=controllers) as url,
        enrollment(pipeline, root, url) as connected,
    ):
        native = connected.worker
        attempt = native.claim(pipeline.ready())
        domain = AttemptData.model_validate(attempt["data"])
        trial = TrialData.model_validate(pipeline.trial["data"])
        native._verify(domain, trial)
        entry = native._prepare(domain, trial)
        supervised = start_timed_supervisor(
            native.journal, attempt["id"], native._timed_origin(entry)
        )
        try:
            until = time.monotonic() + 10
            while not list(native.root.rglob("payload-finished")):
                assert supervised.process.poll() is None and time.monotonic() < until
                time.sleep(0.01)
            marker = next(native.root.rglob("payload-finished"))
            running = native.journal.get(attempt["id"])
            assert running.state == JournalState.RUNNING and running.native is not None
            assert native.resources.availability() == {"cpu": 0}
            budget = pipeline.client.budget(pipeline.campaign["id"])
            binding = connected.bundle.worker.binding
            controllers[0].kill_and_wait()
            assert not controllers[0].process.is_running()
            assert supervised.process.poll() is None
            assert native.journal.get(attempt["id"]) == running
            assert pipeline.client.budget(pipeline.campaign["id"]) == budget
            port = int(url.rsplit(":", 1)[1])
            with controller_process(pipeline, port=port) as restarted_url:
                assert restarted_url == url
                observed = recover_worker_attempt(root, attempt["id"])
                assert observed.native_state == JournalState.RUNNING
                assert observed.custody_state == "retained"
                assert native.resources.availability() == {"cpu": 0}
                assert native.journal.get(attempt["id"]).cancel_requested is False
                marker.with_name("payload-release").touch()
                supervised.process.wait(timeout=20)
                closed = native.journal.get(attempt["id"])
                assert closed.native == running.native and closed.state == JournalState.CLOSED
                assert closed.timing.caps == running.timing.caps
                assert closed.signed_assignment == running.signed_assignment
                assert closed.outcome.closure.scope_closed
                with pytest.raises(ResearchError) as expired_origin:
                    native.execute(attempt["id"])
                assert expired_origin.value.code == ErrorCode.ACCEPTANCE_EXPIRED
                submissions = pipeline.client.list(RecordQuery(kind="attempt_submission"))["items"]
                full = [
                    pipeline.client.get("attempt_submission", item["id"]) for item in submissions
                ]
                submission = next(
                    item
                    for item in full
                    if item["data"]["publication"]["report_artifact"] is not None
                )
                token = pipeline.runtime.credentials.issue(
                    pipeline.runtime.principal(), pipeline.runtime.principal()
                ).token
                with ResearchClient.connect(url, token) as remote:
                    settled = reconcile(pipeline, submission, client=remote)
                    assert settled["attempt"]["state"] == "succeeded"
                    assert settled["attempt"]["data"]["dispatch"] == attempt["data"]["dispatch"]
                    assert remote.get("worker", binding.worker_id)["data"][
                        "binding"
                    ] == binding.model_dump(mode="json")
                    after = remote.budget(pipeline.campaign["id"])
                    assert all(
                        item["reserved"] == item["uncertain"] == "0" for item in after["accounts"]
                    )
                assert (
                    recover_worker_attempt(root, attempt["id"]).delivery_state
                    == "controller_terminal"
                )
                assert pipeline.client.budget(pipeline.campaign["id"]) == after
                assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1
                markers = list(native.root.rglob("payload-finished"))
                assert len(markers) == 1 and markers[0].read_text() == "1"
                assert native.resources.availability() == {"cpu": 1}
        finally:
            output_root = Path(entry.prepared.context.output_root)
            output_root.mkdir(parents=True, exist_ok=True)
            (output_root / "payload-release").touch()
            supervised.close()
            supervised.process.wait(timeout=20)


def test_recovery_does_not_require_registered_interpreter_or_source(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from research_swarm.workers.remote_enrollment import (
        connect_worker,
        describe_worker,
        provision_worker,
    )
    from tests.test_registered_execution import registration

    registered = registration(pipeline, tmp_path, isolated=True)
    root = tmp_path / "registered-remote"
    description = describe_worker(
        root, registration=registered, machine_registry=tmp_path / "machine/resources.sqlite"
    )
    with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:
        bundle_path = root / "bundle.json"
        provision_worker(
            pipeline.client,
            description,
            url=url,
            project_ids=(pipeline.trial["data"]["spec"]["project_id"],),
            destination=bundle_path,
        )
        with connect_worker(root, bundle_path=bundle_path) as connected:
            native = connected.worker
            attempt = native.claim(pipeline.ready())

            def disconnect(*_: Any, **__: Any) -> Any:
                raise ResearchError(
                    ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic disconnected driver"
                )

            with monkeypatch.context() as patch:
                patch.setattr(native, "_publish", disconnect)
                with pytest.raises(ResearchError, match="disconnected driver"):
                    native.execute(attempt["id"])
            assert native.journal.get(attempt["id"]).outcome.exit_code == 0
        shutil.rmtree(tmp_path / "python-environment")
        shutil.rmtree(root / "execution/work")
        advance_epoch(pipeline)
        result = recover_worker_attempt(root, attempt["id"])
        assert result.delivery_state == "quarantined" and result.custody_state == "released"


def test_durable_quota_rejection_is_blocked_instead_of_promising_replay_will_retry(
    pipeline: Pipeline, recoverable: Recoverable
) -> None:
    previous = pipeline.runtime.artifacts.storage_limit_bytes
    pipeline.runtime.artifacts.storage_limit_bytes = 1
    first = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert first.delivery_state == "blocked" and first.error == ErrorCode.RESOURCE_UNAVAILABLE
    assert first.rejected_command_id is not None and first.next_action == "review_rejection"
    assert first.custody_state == "released"
    pipeline.runtime.artifacts.storage_limit_bytes = previous
    again = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert again == first
    assert recoverable.intent().publication is None
    assert pipeline.client.list(RecordQuery(kind="attempt_submission"))["items"] == []


@pytest.mark.parametrize("error", [ErrorCode.TRANSPORT_UNAVAILABLE, ErrorCode.AUTHORIZATION_DENIED])
def test_terminal_reread_failure_retains_local_custody_report_and_frozen_intent(
    recoverable: Recoverable, monkeypatch: pytest.MonkeyPatch, error: ErrorCode
) -> None:
    original = ResearchClient.worker_get
    calls = 0

    def lost(self: ResearchClient, request: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise ResearchError(error, "Synthetic terminal reread failure")
        return original(self, request)

    def rejected(*_: Any, **__: Any) -> Any:
        raise ResearchError(ErrorCode.INVALID_TRANSITION, "Synthetic racing settlement")

    monkeypatch.setattr(ResearchClient, "worker_get", lost)
    monkeypatch.setattr(ResearchClient, "worker_upload_file", rejected)
    report = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert report.error == error and report.custody_state == "released"
    assert report.delivery_state == (
        "pending" if error == ErrorCode.TRANSPORT_UNAVAILABLE else "blocked"
    )
    assert report.controller_disposition is None and report.submission_id is None
    assert recoverable.intent().publication is None
