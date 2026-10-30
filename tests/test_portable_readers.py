from __future__ import annotations

import builtins
from contextlib import contextmanager

import pytest

from research_swarm.control.service import ResearchService
from research_swarm.crypto_native import NativeEd25519
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.artifacts import ArtifactService
from research_swarm.state.database import Database
from research_swarm.state.reader_sessions import (
    NativeReaderOwnership,
    OpaqueReaderOwnership,
    ReaderOwner,
    capture_owner,
    require_owner_closed,
)
from tests.test_artifacts import publish
from tests.test_artifacts import service as service
from tests.test_artifacts import store as store
from tests.test_portable_preflights import CallbackGuard


def portable_service(service):
    return ArtifactService(
        Database.from_store(CallbackGuard(service.database.store)),
        service.store,
        reader_ownership=OpaqueReaderOwnership(),
    )


def retained(service):
    return service.database.store.read(
        lambda sql: sql.execute(
            "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' AND state='retained'"
        ).one()[0]
    )


@contextmanager
def abandoned(service, context, reference, monkeypatch):
    original = service.database.execute

    def execute(ctx, operation, *args, **kwargs):
        if operation == "artifact.unpin":
            raise OSError("synthetic interruption before pin cleanup")
        return original(ctx, operation, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(service.database, "execute", execute)
        with pytest.raises(OSError, match="interruption"), service.pinned(context(), reference):
            yield


def test_portable_controller_construction_and_pin_lifecycle_without_native_imports(
    service, context, monkeypatch
):
    record = publish(service, context, b"portable bytes")
    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.split(".")[0] in {"psutil", "sqlite3", "ctypes", "cryptography"}:
            raise AssertionError("Native runtime leaked into portable construction: " + name)
        return original(name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(builtins, "__import__", guarded)
        portable = portable_service(service)
        controller = ResearchService(portable.database, portable, crypto_provider=NativeEd25519())
        assert controller.artifacts is portable
        with portable.pinned(context(), record.reference):
            assert retained(portable) == 1
            with pytest.raises(ResearchError, match="retained references"):
                portable.delete(context(revision=1), record.reference.id, "during-read")
        assert retained(portable) == 0
        portable.delete(context(revision=1), record.reference.id, "after-read")
    assert portable.get(context(), record.reference.id).state == "deleted"


def test_replacement_incarnation_and_native_provider_cannot_release_opaque_orphans(
    service, context, monkeypatch
):
    record = publish(service, context, b"retained")
    portable = portable_service(service)
    with abandoned(portable, context, record.reference, monkeypatch):
        assert retained(portable) == 1
    replacement = portable_service(service)
    assert replacement.reader_owner != portable.reader_owner
    for consumer in (portable, replacement, service):
        with pytest.raises(ResearchError) as denied:
            consumer.reconcile_reader_session(context(), portable.reader_session_id)
        assert denied.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
        assert retained(portable) == 1
        with pytest.raises(ResearchError, match="retained references"):
            consumer.delete(context(revision=1), record.reference.id, "orphan")


def change_session(service, context, session_id, *, change_owner):
    def update(transaction):
        row = transaction.get("artifact_read_session", session_id)
        data = dict(row["data"])
        if change_owner:
            data["identity"] = new_id()
        return transaction.update(
            "artifact_read_session",
            session_id,
            expected_revision=row["revision"],
            data=data,
            state="active",
        )

    service.database.execute(context(), "fixture.change_owner", {}, "artifact.admin", update)


def test_changed_session_owner_cannot_acquire_another_pin(service, context):
    record = publish(service, context, b"pin")
    portable = portable_service(service)
    with portable.pinned(context(), record.reference):
        change_session(portable, context, portable.reader_session_id, change_owner=True)
        with pytest.raises(ResearchError) as denied, portable.pinned(context(), record.reference):
            pytest.fail("A different owner acquired this session")
        assert denied.value.code == ErrorCode.STALE_FENCE
        assert retained(portable) == 1
    assert retained(portable) == 0


@pytest.mark.parametrize("change_owner", [False, True])
def test_reconciliation_rechecks_the_owner_and_revision_after_closure_proof(
    service, context, monkeypatch, change_owner
):
    record = publish(service, context, b"proof")
    portable = portable_service(service)
    with abandoned(portable, context, record.reference, monkeypatch):
        pass

    class RacingProof(OpaqueReaderOwnership):
        def require_closed(self, owner):
            change_session(portable, context, portable.reader_session_id, change_owner=change_owner)

    reconciler = ArtifactService(portable.database, portable.store, reader_ownership=RacingProof())
    with pytest.raises(ResearchError) as changed:
        reconciler.reconcile_reader_session(context(), portable.reader_session_id)
    assert changed.value.code == (
        ErrorCode.STALE_FENCE if change_owner else ErrorCode.REVISION_CONFLICT
    )
    assert retained(portable) == 1


@pytest.mark.parametrize("operation", ["capture", "require_closed"])
def test_async_ownership_is_not_accepted_as_closure(operation):
    created = []

    class AsyncOwnership(OpaqueReaderOwnership):
        pass

    def asynchronous(*args):
        async def result():
            return None

        value = result()
        created.append(value)
        return value

    setattr(AsyncOwnership, operation, asynchronous)
    with pytest.raises(RuntimeError, match="synchronously"):
        if operation == "capture":
            capture_owner(AsyncOwnership())
        else:
            require_owner_closed(AsyncOwnership(), OpaqueReaderOwnership().capture())
    assert created[0].cr_frame is None


def test_non_native_profile_is_never_interpreted_as_a_process():
    with pytest.raises(ResearchError) as denied:
        NativeReaderOwnership().require_closed(ReaderOwner(profile="foreign-v1", identity="1"))
    assert denied.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


@pytest.mark.parametrize("value", [True, False, {}, "closed"])
def test_returned_value_cannot_substitute_for_closure_proof(value):
    class InvalidProof(OpaqueReaderOwnership):
        def require_closed(self, owner):
            return value

    with pytest.raises(TypeError, match="return None"):
        require_owner_closed(InvalidProof(), OpaqueReaderOwnership().capture())
