"""An explicit elected destination cannot rewrite original worker custody."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event

import pytest

from research_swarm.api.http import create_app
from research_swarm.checkpoints.admission import admit_recovery
from research_swarm.checkpoints.routes import issue_recovery_route
from research_swarm.cli import main
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.reconciliation import ReconcileAttempt
from research_swarm.control.recovery_route_issuance import route_from_snapshot
from research_swarm.control.recovery_routes import verify_recovery_route
from research_swarm.domain.backup import BackupCustodyPin
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.recovery_routes import RecoveryRouteRequest, SignedWorkerRecoveryRoute
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime, read_private, write_private
from research_swarm.workers.local import LocalWorker
from research_swarm.workers.recovery import RecoveryIntent, recover_worker_attempt
from research_swarm.workers.recovery_routes import install_recovery_route
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_http_delivery import serving
from tests.state.test_recovery_admission import copy_pipeline
from tests.workers.test_worker_recovery import Recoverable
from tests.workers.test_worker_recovery import recoverable as recoverable


@dataclass
class Routed:
    native: Recoverable
    target: LocalRuntime
    signed: SignedWorkerRecoveryRoute
    pin: BackupCustodyPin
    request: RecoveryRouteRequest
    original_intent: bytes = field(repr=False)
    installation: bytes = field(repr=False)

    @property
    def directory(self) -> Path:
        return self.native.recovery / "routes" / self.signed.digest.split(":", 1)[-1]


@pytest.fixture
def routed(pipeline, recoverable, tmp_path, monkeypatch):
    def unavailable(*args, **kwargs):
        raise ResearchError(
            ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture original destination unavailable"
        )

    with monkeypatch.context() as patch:
        patch.setattr(ResearchClient, "connect", unavailable)
        before = recover_worker_attempt(recoverable.root, recoverable.attempt_id)
    assert before.delivery_state == "pending"
    original = read_private(recoverable.recovery, "intent.json", limit=2 * 1024 * 1024)
    installation = read_private(recoverable.root, "installation.json", limit=1024 * 1024)
    target, context, bundle, claim = copy_pipeline(pipeline, tmp_path)
    admit_recovery(target, context, bundle, claim)
    pin = BackupCustodyPin.model_validate(
        pipeline.runtime.database.read_record(
            CommandContext(principal=pipeline.runtime.principal(), idempotency_key=new_id()),
            "backup_custody",
            claim.claim.request.source_custody_id,
        )["data"]
    )
    with serving(create_app(target.service, target.credentials)) as destination:
        request = RecoveryRouteRequest(
            route_id=new_id(),
            attempt_id=recoverable.attempt_id,
            credential_id=recoverable.enrollment.bundle.credential_id,
            source_controller_url=recoverable.enrollment.bundle.controller_url,
            destination_url=destination,
        )
        signed = issue_recovery_route(target, context, request)
        yield Routed(recoverable, target, signed, pin, request, original, installation)


def install(routed):
    return install_recovery_route(routed.native.root, routed.signed, routed.pin)


def test_native_route_cannot_strip_logical_election_verification(routed):
    signed = routed.signed
    admission = signed.route.admission.model_copy(
        update={"logical_election_digest": "sha256:" + "a" * 64}
    )
    changed = signed.model_copy(
        update={"route": signed.route.model_copy(update={"admission": admission})}
    )
    with pytest.raises(ResearchError) as refused:
        verify_recovery_route(changed, routed.pin)
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


def recover(routed):
    return recover_worker_attempt(
        routed.native.root, routed.native.attempt_id, route_id=routed.signed.route.route_id
    )


def assert_original_unchanged(routed):
    assert (
        read_private(routed.native.recovery, "intent.json", limit=2 * 1024 * 1024)
        == routed.original_intent
    )
    assert (
        read_private(routed.native.root, "installation.json", limit=1024 * 1024)
        == routed.installation
    )


def test_explicit_route_releases_and_delivers_without_replacing_original_authority(
    routed, monkeypatch
):
    installed = install(routed)
    assert install(routed) == installed

    def forbidden(*args, **kwargs):
        raise AssertionError("Recovery entered execution")

    monkeypatch.setattr(LocalWorker, "__init__", forbidden)
    monkeypatch.setattr(LocalWorker, "execute", forbidden)
    result = recover(routed)
    assert result.delivery_state == "quarantined" and result.custody_state == "released"
    assert recover(routed) == result
    assert_original_unchanged(routed)
    old = RecoveryIntent.model_validate_json(routed.original_intent)
    new = RecoveryIntent.model_validate_json(
        read_private(routed.directory, "intent.json", limit=2 * 1024 * 1024)
    )
    assert old.submission_id is None
    assert new.submission_id == result.submission_id
    assert old.upload_key != new.upload_key and old.submission_key != new.submission_key
    assert old.identity_digest != new.identity_digest
    operator = ResearchClient(LocalTransport(routed.target))
    attempt = operator.get("attempt", routed.native.attempt_id)
    intent = ReconcileAttempt(
        attempt_id=routed.native.attempt_id,
        submission_id=result.submission_id,
        expected_controller_id=routed.target.identity.controller_id,
        expected_control_epoch=routed.signed.route.restoration.target_control_epoch,
        reason="Original worker delivered to elected target",
    )
    closed = operator.command(
        "attempt.reconcile_closure",
        intent,
        idempotency_key="route-close",
        expected_revision=attempt["revision"],
    )
    assert closed["result"]["attempt"]["state"] == "closed_unaccepted"
    assert (
        operator.command(
            "attempt.reconcile_closure",
            intent,
            idempotency_key="route-close",
            expected_revision=attempt["revision"],
        )
        == closed
    )
    assert_original_unchanged(routed)
    assert (
        operator.get("workspace_gate", routed.target.identity.workspace_id)["state"] == "recovering"
    )


@pytest.mark.parametrize(
    "field", ["destination_url", "attempt_id", "credential_id", "source_controller_url"]
)
def test_route_substitution_is_refused_before_installation_or_network(routed, monkeypatch, field):
    value = "https://substituted.example" if "url" in field else new_id()
    changed = routed.signed.model_copy(
        update={"route": routed.signed.route.model_copy(update={field: value})}
    )

    def forbidden(*args, **kwargs):
        pytest.fail("Route validation contacted a network destination")

    monkeypatch.setattr(ResearchClient, "connect", forbidden)
    with pytest.raises(ResearchError):
        install_recovery_route(routed.native.root, changed, routed.pin)
    assert not (routed.native.root / "recovery-routes").exists()
    assert_original_unchanged(routed)


def test_embedded_source_key_cannot_replace_independent_pin(routed):
    changed = routed.pin.model_copy(update={"public_key": "0" * 64})
    with pytest.raises(ResearchError, match="trusted source"):
        install_recovery_route(routed.native.root, routed.signed, changed)
    verify_recovery_route(routed.signed, routed.pin)


def test_issued_route_ids_cannot_be_rebound(routed):
    context = CommandContext(principal=routed.target.principal(), idempotency_key=new_id())
    assert issue_recovery_route(routed.target, context, routed.request) == routed.signed
    with pytest.raises(ResearchError) as conflict:
        issue_recovery_route(
            routed.target,
            context,
            routed.request.model_copy(update={"destination_url": "https://another.example"}),
        )
    assert conflict.value.code == ErrorCode.IDEMPOTENCY_CONFLICT


@pytest.mark.parametrize(
    "damage",
    [
        "revoked",
        "credential_owner",
        "credential_scope",
        "driver",
        "session_state",
        "session_revision",
        "session_size",
        "attempt",
    ],
)
def test_issuance_rechecks_original_authority_before_returning_saved_route(routed, damage):
    r = routed
    route = r.signed.route
    context = CommandContext(principal=r.target.principal(), idempotency_key=new_id())
    path = r.target.root / "recovery-routes" / (route.route_id + ".json")
    before = path.read_bytes()

    def change(sql):
        if damage == "revoked":
            sql.execute("UPDATE credentials SET revoked=1 WHERE id=?", (route.credential_id,))
        elif damage.startswith("credential_"):
            raw = strict_json_loads(
                sql.execute(
                    "SELECT principal FROM credentials WHERE id=?", (route.credential_id,)
                ).one()[0]
            )
            raw["id" if damage == "credential_owner" else "scopes"] = (
                new_id() if damage == "credential_owner" else ["worker.query"]
            )
            sql.execute(
                "UPDATE credentials SET principal=? WHERE id=?",
                (canonical_bytes(raw).decode(), route.credential_id),
            )
        elif damage == "driver":
            from research_swarm.state.database import Transaction

            Transaction(sql, context).insert(
                "orchestration_owner", context.principal.id, {}, state="assigned"
            )
        elif damage.startswith("session_"):
            column, value = {
                "session_state": ("state", "active"),
                "session_revision": ("revision", 2),
                "session_size": ("payload", " " * (1024**2 + 1)),
            }[damage]
            sql.execute(f"UPDATE records SET {column}=? WHERE kind='worker_session'", (value,))
        else:
            raw = strict_json_loads(
                sql.execute(
                    "SELECT payload FROM records WHERE kind='attempt' AND id=?", (route.attempt_id,)
                ).one()[0]
            )
            raw["dispatch"]["worker_session_id"] = new_id()
            sql.execute(
                "UPDATE records SET payload=? WHERE kind='attempt' AND id=?",
                (canonical_bytes(raw).decode(), route.attempt_id),
            )

    r.target.database.store.write(change)
    with pytest.raises(ResearchError):
        issue_recovery_route(r.target, context, r.request)
    assert path.read_bytes() == before
    assert_original_unchanged(r)


def test_route_worker_and_credential_reads_share_one_snapshot_during_revocation(routed):
    r, reached, release = routed, Event(), Event()
    context = CommandContext(principal=r.target.principal(), idempotency_key=new_id())

    def read(sql):
        class Paused:
            def execute(self, statement, parameters=()):
                result = sql.execute(statement, parameters)
                if "worker_session" in parameters:
                    reached.set()
                    assert release.wait(5)
                return result

        return route_from_snapshot(
            Paused(), context, r.request, r.signed.route.restoration, r.signed.route.admission
        )

    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(r.target.database.store.read, read)
        try:
            assert reached.wait(5)
            r.target.database.store.write(
                lambda sql: sql.execute(
                    "UPDATE credentials SET revoked=1 WHERE id=?", (r.request.credential_id,)
                )
            )
        finally:
            release.set()
        assert pending.result() == r.signed.route
    with pytest.raises(ResearchError, match="credential is unavailable"):
        r.target.database.store.read(read)


@pytest.mark.parametrize("surface", ["route", "installation", "journal", "registry"])
def test_tampered_or_missing_original_custody_refuses_delivery(routed, monkeypatch, surface):
    install(routed)
    if surface == "route":
        path = routed.native.root / "recovery-routes" / (routed.request.route_id + ".json")
        content = strict_json_loads(read_private(path.parent, path.name, limit=4 * 1024 * 1024))
        content["journal_identity"] = new_id()
        write_private(path, canonical_bytes(content))
    elif surface == "installation":
        content = strict_json_loads(routed.installation)
        content["bundle"]["controller_url"] = "https://different.example"
        write_private(routed.native.root / "installation.json", canonical_bytes(content))
    else:
        native = routed.native.enrollment.worker
        path = native.journal.path if surface == "journal" else native.resources.path
        path.rename(path.with_suffix(".original"))

    def forbidden(*args, **kwargs):
        pytest.fail("Unverified custody sent a credential")

    monkeypatch.setattr(ResearchClient, "connect", forbidden)
    with pytest.raises(ResearchError):
        recover(routed)
    if surface in {"journal", "registry"}:
        assert not path.exists()


@pytest.mark.parametrize("operation", ["worker.finalize_upload", "worker.submit_evidence"])
def test_target_lost_acknowledgments_do_not_resolve_original_uncertainty(
    routed, monkeypatch, operation
):
    from research_swarm.client import HTTPTransport

    install(routed)
    original = HTTPTransport.command
    keys = []

    def lose(self, name, payload, key, revision):
        reply = original(self, name, payload, key, revision)
        if name == operation:
            keys.append(key)
            if len(keys) == 1:
                raise ResearchError(
                    ErrorCode.DELIVERY_UNCERTAIN, "Fixture target acknowledgment lost"
                )
        return reply

    monkeypatch.setattr(HTTPTransport, "command", lose)
    first = recover(routed)
    assert first.delivery_state == "pending"
    frozen = read_private(routed.directory, "intent.json", limit=2 * 1024 * 1024)
    second = recover(routed)
    assert second.delivery_state == "quarantined"
    acknowledged = RecoveryIntent.model_validate_json(
        read_private(routed.directory, "intent.json", limit=2 * 1024 * 1024)
    )
    prior = RecoveryIntent.model_validate_json(frozen)
    assert prior.journal == acknowledged.journal
    assert prior.upload_key == acknowledged.upload_key
    assert prior.submission_key == acknowledged.submission_key
    assert len(set(keys)) == 1
    assert_original_unchanged(routed)


def test_cli_requires_explicit_route_installation_and_selection(
    routed, tmp_path, capsys, monkeypatch
):
    from research_swarm import cli

    def forbidden(*args):
        pytest.fail("Native recovery command opened an ordinary default controller")

    monkeypatch.setattr(cli, "_client", forbidden)
    request_path = tmp_path / "request.json"
    request_path.write_bytes(canonical_bytes(routed.request.model_dump(mode="json")))
    assert (
        main(
            ["--state-dir", str(routed.target.root), "backup", "route", "--file", str(request_path)]
        )
        == 0
    )
    assert (
        SignedWorkerRecoveryRoute.model_validate(strict_json_loads(capsys.readouterr().out))
        == routed.signed
    )
    route = tmp_path / "route.json"
    pin = tmp_path / "pin.json"
    route.write_bytes(canonical_bytes(routed.signed.model_dump(mode="json")))
    pin.write_bytes(canonical_bytes(routed.pin.model_dump(mode="json")))
    assert (
        main(
            [
                "worker",
                "install-recovery-route",
                "--worker-dir",
                str(routed.native.root),
                "--file",
                str(route),
                "--source-pin",
                str(pin),
            ]
        )
        == 0
    )
    installed = strict_json_loads(capsys.readouterr().out)
    assert installed["route_id"] == routed.request.route_id and "token" not in installed
    assert (
        main(
            [
                "worker",
                "recover",
                "--worker-dir",
                str(routed.native.root),
                "--attempt",
                routed.native.attempt_id,
                "--route-id",
                routed.request.route_id,
            ]
        )
        == 0
    )
    assert strict_json_loads(capsys.readouterr().out)["delivery_state"] == "quarantined"
    assert_original_unchanged(routed)


def test_valid_target_signature_still_requires_exact_original_installation(routed):
    context = CommandContext(principal=routed.target.principal(), idempotency_key=new_id())
    changed = issue_recovery_route(
        routed.target,
        context,
        routed.request.model_copy(
            update={
                "route_id": new_id(),
                "source_controller_url": "https://different-source.example",
            }
        ),
    )
    verify_recovery_route(changed, routed.pin)
    with pytest.raises(ResearchError, match="original installation"):
        install_recovery_route(routed.native.root, changed, routed.pin)


def test_route_installation_respects_original_driver_lock(routed):
    from filelock import FileLock

    with (
        FileLock(routed.native.root / "execution/driver/driver.lock"),
        pytest.raises(ResearchError) as busy,
    ):
        install(routed)
    assert busy.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert not (routed.native.root / "recovery-routes").exists()


@pytest.mark.parametrize(
    "pipeline",
    [{"source_finish_gate": True, "source_train_timeout_ms": 30000}],
    indirect=True,
)
def test_live_original_scope_survives_target_recovery_until_native_closure(
    pipeline, tmp_path, monkeypatch
):
    import time

    from research_swarm.checkpoints.activation import activate_recovery
    from research_swarm.checkpoints.claims import claim_restore, request_restore_claim
    from research_swarm.checkpoints.restore import restore_inactive
    from research_swarm.control.models import TrialData
    from research_swarm.control.workers import AttemptData
    from research_swarm.workers.journal import JournalState
    from research_swarm.workers.timed_supervision import start_timed_supervisor
    from tests.state.test_recovery_activation import intent as activation_intent
    from tests.state.test_recovery_reseal import reseal
    from tests.workers.test_scoped_driver import enrollment

    root = tmp_path / "live-worker"
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as source_url,
        enrollment(pipeline, root, source_url) as connected,
    ):
        native = connected.worker
        row = native.claim(pipeline.ready())
        attempt = AttemptData.model_validate(row["data"])
        entry = native._prepare(attempt, TrialData.model_validate(pipeline.trial["data"]))
        output_root = Path(entry.prepared.context.output_root)
        supervised = start_timed_supervisor(native.journal, row["id"], native._timed_origin(entry))
        try:
            deadline = time.monotonic() + 10
            while not (output_root / "payload-finished").exists():
                assert supervised.process.poll() is None and time.monotonic() < deadline
                time.sleep(0.01)
            running = native.journal.get(row["id"])
            assert running.state == JournalState.RUNNING

            def unavailable(*args, **kwargs):
                raise ResearchError(
                    ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture original delivery uncertain"
                )

            with monkeypatch.context() as patch:
                patch.setattr(ResearchClient, "connect", unavailable)
                old = recover_worker_attempt(root, row["id"])
            assert old.delivery_state == "pending" and old.custody_state == "retained"
            original = read_private(
                root / "execution/recovery" / row["id"], "intent.json", limit=2 * 1024 * 1024
            )
            target, context, backup, claim = copy_pipeline(pipeline, tmp_path)
            admit_recovery(target, context, backup, claim)
            competing_target = tmp_path / "competing-target"
            competing = restore_inactive(backup, competing_target, restore_id=new_id())
            competing_request = request_restore_claim(
                backup, competing_target, restore_id=competing.restore_id
            )
            with pytest.raises(ResearchError):
                claim_restore(pipeline.runtime, context, backup, competing_request)
            activation_context, activation = activation_intent(target)
            pin = BackupCustodyPin.model_validate(
                pipeline.runtime.database.read_record(
                    CommandContext(
                        principal=pipeline.runtime.principal(), idempotency_key=new_id()
                    ),
                    "backup_custody",
                    claim.claim.request.source_custody_id,
                )["data"]
            )
            with serving(create_app(target.service, target.credentials)) as destination:
                route = issue_recovery_route(
                    target,
                    context,
                    RecoveryRouteRequest(
                        route_id=new_id(),
                        attempt_id=row["id"],
                        credential_id=connected.bundle.credential_id,
                        source_controller_url=source_url,
                        destination_url=destination,
                    ),
                )
                install_recovery_route(root, route, pin)
                before = recover_worker_attempt(root, row["id"], route_id=route.route.route_id)
                assert before.delivery_state == "quarantined" and before.custody_state == "retained"
                assert supervised.process.poll() is None
                assert native.resources.availability() == {"cpu": 0}
                operator = ResearchClient(LocalTransport(target))
                request = ReconcileAttempt(
                    attempt_id=row["id"],
                    submission_id=before.submission_id,
                    expected_controller_id=target.identity.controller_id,
                    expected_control_epoch=route.route.restoration.target_control_epoch,
                    reason="Observe original native scope",
                )
                budget = operator.budget(pipeline.campaign["id"])
                with pytest.raises(ResearchError):
                    operator.command(
                        "attempt.reconcile_closure",
                        request,
                        idempotency_key="still-live",
                        expected_revision=row["revision"],
                    )
                assert operator.budget(pipeline.campaign["id"]) == budget
                with pytest.raises(ResearchError, match="execution must close"):
                    activate_recovery(target, activation_context, activation)
                (output_root / "payload-release").touch()
                supervised.process.wait(timeout=15)
                closed = native.journal.get(row["id"])
                assert closed.state == JournalState.CLOSED and closed.native == running.native
                after = recover_worker_attempt(root, row["id"], route_id=route.route.route_id)
                assert after.delivery_state == "quarantined" and after.custody_state == "released"
                assert after.submission_id != before.submission_id
                request = request.model_copy(update={"submission_id": after.submission_id})
                result = operator.command(
                    "attempt.reconcile_closure",
                    request,
                    idempotency_key="now-closed",
                    expected_revision=operator.get("attempt", row["id"])["revision"],
                )
                assert result["result"]["attempt"]["state"] == "closed_unaccepted"
                assert native.resources.availability() == {"cpu": 1}
                assert (
                    read_private(
                        root / "execution/recovery" / row["id"],
                        "intent.json",
                        limit=2 * 1024 * 1024,
                    )
                    == original
                )
                assert (output_root / "payload-finished").read_text() == "1"
                activated = activate_recovery(target, activation_context, activation)
                assert activate_recovery(target, activation_context, activation) == activated
                assert operator.get("campaign", pipeline.campaign["id"])["state"] == "paused"
                assert operator.get("worker", native.worker.binding.worker_id)["state"] == "fenced"
                following = reseal(target, tmp_path / "following-bundle")
                assert following.manifest.seal.control_epoch == activation.expected_control_epoch
                assert not following.manifest.seal.unresolved_attempt_ids
                assert following.manifest.source_public_key != pin.public_key
        finally:
            (output_root / "payload-release").touch()
            supervised.close()
            supervised.process.wait(timeout=15)
