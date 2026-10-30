"""Real local execution keeps artifact I/O outside shared SQL callbacks."""

from contextlib import contextmanager

import pytest

from research_swarm.control.queries import RecordQuery
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_promotions import evaluated
from tests.test_reconciliation import close_without_publication, reconcile, submit
from tests.test_state_store import CallbacksOnly
from tests.test_worker_outputs import output_client


class CallbackGuard(CallbacksOnly):
    def __init__(self, store):
        super().__init__(store)
        self.active = False

    def _run(self, operation, callback):
        def guarded(session):
            assert not self.active, "Service opened another store callback within a transaction"
            self.active = True
            try:
                return callback(session)
            finally:
                self.active = False

        return operation(guarded)

    def read(self, callback):
        return self._run(super().read, callback)

    def write(self, callback):
        return self._run(super().write, callback)


class CheckedStream:
    def __init__(self, stream, check):
        self.stream, self.check = stream, check

    def __getattr__(self, name):
        value = getattr(self.stream, name)
        if not callable(value):
            return value

        def checked(*args, **kwargs):
            self.check()
            return value(*args, **kwargs)

        return checked


class CheckedBlobs:
    def __init__(self, store, callbacks):
        self.store, self.callbacks = store, callbacks
        self.observed = set()

    def __getattr__(self, name):
        operation = getattr(self.store, name)

        def check():
            assert not self.callbacks.active, f"Artifact {name} ran inside a SQL callback"
            self.observed.add(name)

        def checked(*args, **kwargs):
            check()
            return operation(*args, **kwargs)

        @contextmanager
        def scope(*args, **kwargs):
            check()
            with operation(*args, **kwargs) as result:
                check()
                yield CheckedStream(result, check) if name == "reader" else result
            check()

        return scope if name in {"reader", "upload_guard"} else checked


@pytest.fixture
def guarded(pipeline, monkeypatch):
    database = pipeline.runtime.database
    callbacks = CallbackGuard(database.store)
    blobs = CheckedBlobs(pipeline.runtime.service.artifacts.store, callbacks)
    monkeypatch.setattr(database, "store", callbacks)
    monkeypatch.setattr(pipeline.runtime.service.artifacts, "store", blobs)

    def no_native_connection():
        pytest.fail("Shared service accessed a native database connection")

    monkeypatch.setattr(database, "connect", no_native_connection)
    return callbacks, blobs


def test_real_training_and_evaluation_use_callbacks_without_transactional_blob_io(
    pipeline, guarded
):
    callbacks, blobs = guarded
    receipt = evaluated(pipeline)
    assert receipt["state"] == "valid"
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 2
    assert callbacks.reads > 0 and callbacks.writes > 0 and not callbacks.active
    assert {"upload_guard", "begin", "append", "finalize", "reader", "verify"} <= blobs.observed


def test_quarantined_native_closure_reconciles_without_native_connection(
    pipeline, guarded, tmp_path, monkeypatch
):
    client, worker, _, target = output_client(pipeline)
    closed = close_without_publication(client, worker, target.attempt_id, tmp_path, monkeypatch)
    submission = submit(closed)
    assert submission["state"] == "quarantined"
    accepted = reconcile(pipeline, submission)
    assert accepted["attempt"]["state"] == "succeeded"
    assert reconcile(pipeline, submission)["disposition"] == "already_accepted"
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1
    callbacks, blobs = guarded
    assert callbacks.reads > 0 and callbacks.writes > 0 and not callbacks.active
    assert "reader" in blobs.observed
