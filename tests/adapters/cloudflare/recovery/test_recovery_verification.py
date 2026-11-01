"""A generic store cannot inherit native recovery authority from copied records."""

import pytest

from research_swarm.checkpoints.admission import admit_recovery
from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.control.recovery_operations import recovery_context
from research_swarm.control.recovery_readers import recovery_reader_context
from research_swarm.control.recovery_turns import recovery_turn_context
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Database
from research_swarm.state.recovery_verification import NativeRecoveryVerifier
from research_swarm.state.sql import StoreIdentity
from tests.state.test_native_backup import sealed_runtime as sealed_runtime
from tests.state.test_native_restore_claim import election as election
from tests.state.test_recovery_admission import prepared


@pytest.fixture
def admitted(election):
    runtime, context, bundle, claim = prepared(election)
    admit_recovery(runtime, context, bundle, claim)
    return runtime, context


def test_generic_store_has_no_implicit_native_recovery_even_with_valid_copied_admission(admitted):
    runtime, context = admitted
    assert recovery_authority(runtime.database, context) is not None
    generic = Database.from_store(runtime.database.store)
    with pytest.raises(ResearchError) as refused:
        recovery_authority(generic, context)
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    explicit = Database.from_store(
        runtime.database.store, recovery_verifier=NativeRecoveryVerifier()
    )
    assert recovery_authority(explicit, context) == recovery_authority(runtime.database, context)


def test_owner_verifier_reads_the_same_single_snapshot_before_authority_is_returned(admitted):
    runtime, context = admitted
    reads, verifications = [], []

    class Wrapped:
        identity = runtime.database.authority_identity

        def read(self, callback):
            def read(sql):
                reads.append(sql)
                return callback(sql)

            return runtime.database.store.read(read)

    class Verify:
        def verify(self, sql, **kwargs):
            assert sql is reads[-1]
            assert kwargs["authority"] == runtime.database.authority_identity
            assert kwargs["context"] is context
            assert sql.execute("SELECT state FROM records WHERE kind='workspace_gate'").one()[
                0
            ] == ("recovering")
            NativeRecoveryVerifier().verify(sql, **kwargs)
            verifications.append(True)

    database = Database.from_store(Wrapped(), recovery_verifier=Verify())
    assert recovery_authority(database, context) is not None
    assert len(reads) == 1 and verifications == [True]


@pytest.mark.parametrize("route", ["worker", "reader", "turn"])
def test_all_recovery_capability_paths_require_the_owners_verifier(admitted, route):
    runtime, context = admitted
    calls = []

    class Refuse:
        def verify(self, sql, **kwargs):
            calls.append(True)
            raise ResearchError(ErrorCode.STALE_FENCE, "Synthetic deployment proof differs")

    database = Database.from_store(runtime.database.store, recovery_verifier=Refuse())
    with pytest.raises(ResearchError, match="deployment proof"):
        if route == "worker":
            recovery_context(database, context, "worker.submit_evidence", {}, attempt_id="unused")
        elif route == "reader":
            recovery_reader_context(database, context, {})
        else:
            recovery_turn_context(database, context, "orchestration.reconcile_turn", {})
    assert calls == [True]


@pytest.mark.parametrize("kind", ["async", "value"])
def test_invalid_verifier_return_cannot_mint_authority(admitted, kind):
    runtime, context = admitted

    class Async:
        async def verify(self, *args, **kwargs):
            pytest.fail("Async verifier must never execute")

    class Value:
        def verify(self, *args, **kwargs):
            return True

    database = Database.from_store(
        runtime.database.store, recovery_verifier=Async() if kind == "async" else Value()
    )
    before = runtime.database.snapshot(context)
    with pytest.raises(TypeError, match="synchronously"):
        recovery_authority(database, context)
    assert runtime.database.snapshot(context) == before


def test_native_verifier_is_not_a_hosted_fallback(admitted):
    runtime, context = admitted

    class Hosted:
        identity = StoreIdentity("cloudflare-do", "synthetic/authority")

        def read(self, callback):
            return runtime.database.store.read(callback)

    database = Database.from_store(Hosted(), recovery_verifier=NativeRecoveryVerifier())
    with pytest.raises(ResearchError) as refused:
        recovery_authority(database, context)
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


def test_generic_active_workspace_does_not_need_recovery_capability(admitted, tmp_path):
    runtime, context = admitted
    fresh = Database(tmp_path / "active.sqlite")
    identity = runtime.identity
    fresh.bootstrap(identity.workspace_id, identity.controller_id)
    assert recovery_authority(Database.from_store(fresh.store), context) is None
