"""Logical admission binds the full election without changing native signed bytes."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from research_swarm.checkpoints.admission import verify_admitted
from research_swarm.checkpoints.elections import verify_election
from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.control.recovery_proofs import verify_activation, verify_admission
from research_swarm.crypto import sign
from research_swarm.domain.backup import RecoveryAdmission, RestoreReceipt
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.state.database import Database, Transaction, encode
from research_swarm.state.recovery_elections import LOGICAL_ELECTION_KIND, retained_election
from research_swarm.state.recovery_guard import require_recovery_authority
from tests.adapters.cloudflare.recovery.test_checkpoint_elections import TARGET_SEED
from tests.adapters.cloudflare.recovery.test_checkpoint_elections import election as election


@pytest.fixture
def proof(election):
    e = election
    request = e.request.request.signed_request.request
    receipt = RestoreReceipt(
        **request.model_dump(exclude={"schema_version"}),
        source_public_key=e.manifest.source_custody.public_key,
        restored_at="2026-01-01T00:00:01.000Z",
        unresolved_attempt_ids=(),
        worker_ids=(),
    )
    native = RecoveryAdmission(
        signed_claim=e.election.election.signed_claim,
        restoration_digest=receipt.digest,
        orchestration_campaign_ids=(),
        paused_campaign_ids=(),
        operator_id=e.intent.operator_id,
        admitted_at="2026-01-01T00:00:02.000Z",
        target_signature="0" * 128,
    )

    def signed(admission):
        return admission.model_copy(
            update={
                "target_signature": sign(
                    e.crypto, TARGET_SEED, admission.proof_digest.encode("ascii")
                ).hex()
            }
        )

    return SimpleNamespace(
        e=e,
        receipt=receipt,
        native=signed(native),
        logical=signed(native.model_copy(update={"logical_election_digest": e.election.digest})),
    )


def test_native_bytes_stay_identical_and_logical_field_is_signed(proof):
    p = proof
    wire = p.native.model_dump(mode="json")
    assert "logical_election_digest" not in wire
    assert canonical_bytes(wire) == canonical_bytes(
        p.native.model_copy(update={"logical_election_digest": None}).model_dump(mode="json")
    )
    assert p.native.proof_digest == (
        "sha256:c6e77a297de6b5d2279615e89649359ddebe7598db855a198693c5a86f73b31b"
    )
    assert p.native.target_signature == (
        "cfc4a33a997ee36378e5c0352e806f89d83aa2f7290a265a1565dd2245584fb414"
        "1d0060f013ad6afc02206a3be012ef59c8a7fdfc40c263590e4968b2bb180c"
    )
    assert p.e.election.digest == (
        "sha256:7d0597dca65030091a7c034518314d51b801634538cbc9319081c745370cf83a"
    )
    assert p.logical.proof_digest == (
        "sha256:85805fcecd8cee7caa054a64f9468b7af45a3c947aa2bce47854042c4d56d3bf"
    )
    assert p.logical.target_signature == (
        "e77582f033eb67a61e88f61eca108664be5faae197af62482f9e54cec08a8781eb"
        "eb54d5310f01fd3e2f1ae33e091a2a311d2fb6635c68c3e77804422663c60b"
    )
    for admission in (p.native, p.logical):
        verify_admission(admission, p.receipt, crypto_provider=p.e.crypto)
    for changed in (
        p.logical.model_copy(update={"logical_election_digest": None}),
        p.native.model_copy(update={"logical_election_digest": p.e.election.digest}),
    ):
        with pytest.raises(ResearchError) as refused:
            verify_admission(changed, p.receipt, crypto_provider=p.e.crypto)
        assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert p.e.election.model_copy(update={"signature": "0" * 128}).digest != p.e.election.digest


@pytest.fixture
def retained(proof, tmp_path):
    p = proof
    receipt = p.receipt
    native = Database(tmp_path / "proof.sqlite")
    native.bootstrap(receipt.workspace_id, receipt.controller_id)
    context = CommandContext(
        principal=Principal(
            id=p.e.intent.operator_id, workspace_id=receipt.workspace_id, scopes=frozenset({"*"})
        ),
        idempotency_key="proof-read",
    )

    def seed(sql):
        sql.execute("UPDATE workspaces SET control_epoch=?", (receipt.target_control_epoch,))
        tx = Transaction(sql, context)
        tx.insert(
            "workspace_gate",
            receipt.workspace_id,
            p.e.manifest.seal.model_dump(mode="json"),
            state="recovering",
        )
        tx.insert(
            "restoration", receipt.restore_id, receipt.model_dump(mode="json"), state="inactive"
        )
        tx.insert(
            "recovery_admission",
            receipt.restore_id,
            p.logical.model_dump(mode="json"),
            state="immutable",
        )
        tx.insert(
            LOGICAL_ELECTION_KIND,
            receipt.restore_id,
            p.e.election.model_dump(mode="json"),
            state="immutable",
        )

    native.store.write(seed)

    class Verified:
        def verify(self, sql, *, receipt, admission, crypto, **kwargs):
            verify_admission(admission, receipt, crypto_provider=crypto)
            elected = retained_election(sql, receipt, admission)
            verify_election(elected, **p.e.pins)

    database = Database.from_store(native.store, recovery_verifier=Verified())
    authority = recovery_authority(database, context)
    assert authority is not None
    return SimpleNamespace(
        p=p, native=native, database=database, context=context, authority=authority
    )


@pytest.mark.parametrize(
    "damage",
    [
        "delete",
        "signature",
        "state",
        "revision",
        "campaign",
        "parent",
        "oversize",
        "claim",
    ],
)
def test_companion_change_after_capability_minting_refuses_at_transaction_boundary(
    retained, damage
):
    r = retained
    sql_value = {
        "signature": (
            "payload",
            encode(
                r.p.e.election.model_copy(update={"signature": "0" * 128}).model_dump(mode="json")
            ),
        ),
        "state": ("state", "pending"),
        "revision": ("revision", 2),
        "campaign": ("campaign_id", r.p.receipt.restore_id),
        "parent": ("parent_id", r.p.receipt.restore_id),
        "oversize": ("payload", " " * 16385),
    }

    def guard(sql):
        return require_recovery_authority(
            sql, r.context, r.database.authority_identity, r.authority
        )

    assert r.database.store.read(guard) == r.p.receipt
    if damage == "delete":
        r.native.store.write(
            lambda sql: sql.execute("DELETE FROM records WHERE kind=?", (LOGICAL_ELECTION_KIND,))
        )
    elif damage == "claim":
        # Even an updated capability digest cannot hide disagreement with the signed admission.
        admitted = r.p.logical.model_copy(
            update={
                "signed_claim": r.p.logical.signed_claim.model_copy(update={"signature": "0" * 128})
            }
        )
        r.native.store.write(
            lambda sql: sql.execute(
                "UPDATE records SET payload=? WHERE kind='recovery_admission'",
                (encode(admitted.model_dump(mode="json")),),
            )
        )
        r.authority = replace(
            r.authority,
            admission_digest=record_digest(
                "research.recovery-admission-envelope.v1", admitted.model_dump(mode="json")
            ),
        )
    else:
        column, value = sql_value[damage]
        r.native.store.write(
            lambda sql: sql.execute(
                f"UPDATE records SET {column}=? WHERE kind=?", (value, LOGICAL_ELECTION_KIND)
            )
        )
    with pytest.raises(ResearchError) as refused:
        r.database.store.read(guard)
    assert refused.value.code == ErrorCode.STALE_FENCE


def test_native_authority_lifecycle_and_activation_reject_logical_admission(retained):
    r = retained
    for call in (
        lambda: recovery_authority(r.native, r.context),
        lambda: verify_admitted(r.native, r.p.receipt),
        # No activation fields may be consumed before the profile refusal.
        lambda: verify_activation(None, r.p.logical, r.p.receipt),
    ):
        with pytest.raises(ResearchError) as refused:
            call()
        assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
