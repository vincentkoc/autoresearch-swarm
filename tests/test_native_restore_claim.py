"""Native source custody elects one target without granting execution authority."""

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from research_swarm.checkpoints import claims
from research_swarm.checkpoints.backup import export_backup, source_custody, verify_backup
from research_swarm.checkpoints.claims import (
    claim_restore,
    request_restore_claim,
    verify_claim,
    verify_request,
)
from research_swarm.checkpoints.restore import restore_inactive
from research_swarm.cli import main
from research_swarm.domain.backup import RegisterBackupCustody, SignedRestoreClaim
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.private_files import write_private
from tests.test_native_backup import sealed_runtime as sealed_runtime


@pytest.fixture
def election(sealed_runtime, tmp_path):
    runtime, context, seal, _ = sealed_runtime
    bundle = tmp_path / "bundle"
    manifest = export_backup(runtime, context, bundle, backup_id=seal.backup_id)
    target = tmp_path / "target"
    receipt = restore_inactive(bundle, target, restore_id=new_id())
    request = request_restore_claim(bundle, target, restore_id=receipt.restore_id)
    return runtime, context, bundle, target, manifest, request


def test_claim_survives_restart_with_original_identity_and_no_ledger_mutation(election):
    runtime, context, bundle, target, manifest, request = election
    before = runtime.database.snapshot(context)
    signed = claim_restore(runtime, context, bundle, request)
    assert verify_request(request) == request.request
    assert (
        verify_claim(signed, source_public_key=manifest.source_public_key).request
        == request.request
    )
    restarted = LocalRuntime.open(runtime.root)
    assert claim_restore(restarted, context, bundle, request) == signed
    assert source_custody(restarted).claim == signed
    assert runtime.database.snapshot(context) == before
    restored = LocalRuntime.open(target)
    assert (
        restored.database.read_record(context, "workspace_gate", manifest.seal.workspace_id)[
            "state"
        ]
        == "restoring"
    )
    with pytest.raises(ResearchError):
        restored.credentials.issue(restored.principal(), restored.principal())
    assert not (target / "backup-custody.json").exists()


def test_competing_targets_elect_once_across_independent_native_openers(election, tmp_path):
    runtime, context, bundle, _, manifest, first = election
    target = tmp_path / "second"
    receipt = restore_inactive(bundle, target, restore_id=new_id())
    second = request_restore_claim(bundle, target, restore_id=receipt.restore_id)
    barrier = Barrier(2)

    def elect(request):
        opened = LocalRuntime.open(runtime.root)
        barrier.wait(timeout=10)
        try:
            return claim_restore(opened, context, bundle, request)
        except ResearchError as error:
            assert error.code == ErrorCode.IDEMPOTENCY_CONFLICT
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(elect, [first, second]))
    winner = next(result for result in results if result is not None)
    assert sum(result is not None for result in results) == 1
    assert verify_claim(winner, source_public_key=manifest.source_public_key)
    for request, outcome in zip([first, second], results, strict=True):
        if outcome is None:
            with pytest.raises(ResearchError, match="already elected"):
                claim_restore(LocalRuntime.open(runtime.root), context, bundle, request)
        else:
            assert (
                claim_restore(LocalRuntime.open(runtime.root), context, bundle, request) == winner
            )


def test_lost_publication_reply_replays_identical_claim(election, monkeypatch):
    runtime, context, bundle, _, _, request = election
    original = claims.write_private
    persisted = []

    def lost_reply(path, content):
        original(path, content)
        persisted.append(strict_json_loads(content)["claim"])
        raise OSError("Fixture lost publication reply")

    with monkeypatch.context() as patch:
        patch.setattr(claims, "write_private", lost_reply)
        with pytest.raises(OSError, match="lost publication"):
            claim_restore(runtime, context, bundle, request)
    assert claim_restore(runtime, context, bundle, request) == SignedRestoreClaim.model_validate(
        persisted[0]
    )


def test_native_exit_after_election_preserves_claim_and_rejects_second_target(election, tmp_path):
    runtime, context, bundle, _, _, request = election
    script = """
import os, sys
from pathlib import Path
from research_swarm.checkpoints import claims
from research_swarm.domain.backup import SignedRestoreRequest
from research_swarm.domain.identity import CommandContext
from research_swarm.runtime import LocalRuntime
runtime = LocalRuntime.open(Path(sys.argv[1]))
original = claims.write_private
def interrupt(path, content):
    original(path, content)
    os._exit(73)
claims.write_private = interrupt
claims.claim_restore(runtime, CommandContext(principal=runtime.principal(), idempotency_key='exit'),
    Path(sys.argv[2]), SignedRestoreRequest.model_validate_json(sys.argv[3]))
"""
    child = subprocess.run(
        [sys.executable, "-c", script, str(runtime.root), str(bundle), request.model_dump_json()],
        capture_output=True,
        timeout=15,
    )
    assert child.returncode == 73, child.stderr.decode(errors="replace")
    persisted = source_custody(runtime).claim
    assert persisted is not None
    assert claim_restore(runtime, context, bundle, request) == persisted
    second = tmp_path / "second"
    receipt = restore_inactive(bundle, second, restore_id=new_id())
    proof = request_restore_claim(bundle, second, restore_id=receipt.restore_id)
    with pytest.raises(ResearchError, match="already elected"):
        claim_restore(runtime, context, bundle, proof)


@pytest.mark.parametrize("damage", ["missing", "replacement", "epoch"])
def test_missing_or_substituted_custody_cannot_recreate_source_authority(
    election, damage, tmp_path
):
    runtime, context, bundle, _, manifest, request = election
    claim_restore(runtime, context, bundle, request)
    path = runtime.root / "backup-custody.json"
    if damage == "missing":
        path.unlink()
    else:
        custody = strict_json_loads(path.read_bytes())
        if damage == "replacement":
            custody["private_key"] = Ed25519PrivateKey.generate().private_bytes_raw().hex()
            custody.pop("claim")
        else:
            custody["control_epoch"] += 1
        write_private(path, canonical_bytes(custody))
    with pytest.raises(ResearchError, match="custody"):
        claim_restore(runtime, context, bundle, request)
    with pytest.raises(ResearchError, match="custody"):
        export_backup(runtime, context, tmp_path / "replacement", backup_id=manifest.seal.backup_id)
    assert not (tmp_path / "replacement").exists()


def test_pinned_custody_is_immutable_and_full_operator_only(election):
    runtime, context, bundle, _, manifest, request = election
    pin = verify_backup(bundle).custody_pin
    assert pin is not None
    scoped = context.model_copy(
        update={
            "principal": Principal(
                id=new_id(),
                workspace_id=runtime.identity.workspace_id,
                scopes=frozenset({"service.manage", "read"}),
            )
        }
    )
    with pytest.raises(ResearchError, match="scope"):
        claim_restore(runtime, scoped, bundle, request)
    replacement = RegisterBackupCustody(
        backup_id=manifest.seal.backup_id,
        pin=pin.model_copy(update={"custody_id": new_id()}),
    )
    with pytest.raises(ResearchError, match="scope"):
        runtime.service.register_backup_custody(scoped, replacement)
    with pytest.raises(ResearchError, match="finalized the sealed cut"):
        runtime.service.register_backup_custody(
            context.model_copy(update={"idempotency_key": "replacement-pin"}),
            replacement,
        )
    assert source_custody(runtime).claim is None


def test_fresh_noop_commands_cannot_change_exported_cut_but_exact_acks_replay(election):
    runtime, context, bundle, _, manifest, request = election
    custody = source_custody(runtime)
    pin = verify_backup(bundle).custody_pin
    assert pin is not None
    registration = RegisterBackupCustody(backup_id=manifest.seal.backup_id, pin=pin)
    before = runtime.database.snapshot(context)
    with runtime.database.connect() as connection:
        command_count = connection.execute("SELECT COUNT(*) AS n FROM commands").one()["n"]
    for key in ("fresh-register", "replacement-register"):
        with pytest.raises(ResearchError, match="finalized the sealed cut"):
            runtime.service.register_backup_custody(
                context.model_copy(update={"idempotency_key": key}),
                registration,
            )
    replay = runtime.service.register_backup_custody(
        context.model_copy(update={"idempotency_key": "backup-custody:" + custody.id}),
        registration,
    )
    assert replay["result"]["data"] == pin.model_dump(mode="json")
    with runtime.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) AS n FROM commands").one()["n"] == command_count
    assert runtime.database.snapshot(context) == before
    assert claim_restore(runtime, context, bundle, request).claim.request == request.request


def test_committed_claim_replays_without_bundle_preflight(election):
    runtime, context, bundle, _, manifest, request = election
    signed = claim_restore(runtime, context, bundle, request)
    (bundle / "blobs" / manifest.artifacts[0].generation).unlink()
    assert claim_restore(LocalRuntime.open(runtime.root), context, bundle, request) == signed


def test_elected_source_only_replays_original_export(election, tmp_path):
    runtime, context, bundle, _, manifest, request = election
    other = tmp_path / "other"
    other_manifest = export_backup(runtime, context, other, backup_id=manifest.seal.backup_id)
    assert other_manifest.digest != manifest.digest
    claim_restore(runtime, context, bundle, request)
    assert export_backup(runtime, context, bundle, backup_id=manifest.seal.backup_id) == manifest
    with pytest.raises(ResearchError, match="Another manifest"):
        export_backup(runtime, context, other, backup_id=manifest.seal.backup_id)
    with pytest.raises(ResearchError, match="copy the original bundle"):
        export_backup(runtime, context, tmp_path / "new", backup_id=manifest.seal.backup_id)
    assert not (tmp_path / "new").exists()


@pytest.mark.parametrize("damage", ["signature", "manifest", "epoch"])
def test_wrong_target_proof_or_manifest_never_consumes_election(election, damage):
    runtime, context, bundle, _, _, request = election
    if damage == "signature":
        bad = request.model_copy(update={"signature": "0" * 128})
    else:
        # A valid proof of possession still cannot change the selected source cut.
        key = Ed25519PrivateKey.generate()
        updates = {"target_public_key": key.public_key().public_bytes_raw().hex()}
        updates.update(
            {"manifest_digest": "sha256:" + "0" * 64}
            if damage == "manifest"
            else {
                "target_control_epoch": request.request.target_control_epoch + 1,
            }
        )
        changed = request.request.model_copy(update=updates)
        bad = request.model_copy(
            update={"request": changed, "signature": key.sign(changed.digest.encode("ascii")).hex()}
        )
    with pytest.raises(ResearchError):
        claim_restore(runtime, context, bundle, bad)
    assert source_custody(runtime).claim is None
    assert claim_restore(runtime, context, bundle, request).claim.request == request.request


def test_copied_controller_cannot_use_bundle_to_elect_itself(election):
    _, context, bundle, target, _, request = election
    with pytest.raises(ResearchError, match="custody"):
        claim_restore(LocalRuntime.open(target), context, bundle, request)
    assert not (target / "backup-custody.json").exists()


def test_claim_cli_requires_source_and_repeats_public_receipt(election, tmp_path, capsys):
    runtime, _, bundle, target, manifest, request = election
    arguments = [
        "backup",
        "request-claim",
        str(bundle),
        "--destination",
        str(target),
        "--restore-id",
        request.request.restore_id,
        "--digest",
        manifest.digest,
    ]
    assert main(arguments) == 0
    assert strict_json_loads(capsys.readouterr().out) == request.model_dump(mode="json")
    path = tmp_path / "request.json"
    path.write_bytes(canonical_bytes(request.model_dump(mode="json")))
    arguments = ["backup", "claim", str(bundle), "--request-file", str(path)]
    assert main(arguments) == 2
    assert "explicit existing source" in capsys.readouterr().err
    arguments = ["--state-dir", str(runtime.root), *arguments]
    assert main(arguments) == 0
    first = strict_json_loads(capsys.readouterr().out)
    assert main(arguments) == 0
    assert strict_json_loads(capsys.readouterr().out) == first
    assert "private_key" not in first
