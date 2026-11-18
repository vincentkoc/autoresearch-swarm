from __future__ import annotations

from dataclasses import replace

import pytest

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database


class PassBoundary:
    def invoke(self, transaction, *, name, payload, perform, reject):
        return perform()

    def rejected(self, transaction, **kwargs):
        pass


@pytest.mark.parametrize("failure", [False, True])
def test_query_reader_rolls_back_and_fence_failure_aborts_the_outer_transaction(
    database, context, failure
):
    class Boundary(PassBoundary):
        def rejected(self, tx, **kwargs):
            tx.insert("fixture_fence", new_id(), {}, state="fenced")
            if failure:
                raise RuntimeError("Injected query fence failure")

    guarded = Database.from_store(database.store, execution_boundary=Boundary())
    command = context()

    def reader(tx):
        tx.insert("fixture_effect", new_id(), {}, state="temporary")
        raise ResearchError(ErrorCode.INVALID_TRANSITION, "Injected query refusal")

    with pytest.raises(RuntimeError if failure else ResearchError):
        guarded.query(command, "fixture.query", {}, "read", reader)
    records = guarded.snapshot(command)["records"]
    assert not any(item["kind"] == "fixture_effect" for item in records)
    assert sum(item["kind"] == "fixture_fence" for item in records) == (0 if failure else 1)
    assert (
        guarded.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM commands").one()[0]) == 0
    )


@pytest.mark.parametrize("hosted", [False, True])
@pytest.mark.parametrize("kind", ["coroutine", "generator", "async_generator"])
def test_query_outcome_cannot_hide_an_async_reader(database, context, hosted, kind):
    guarded = Database.from_store(
        database.store, execution_boundary=PassBoundary() if hosted else None
    )

    async def coroutine(tx):
        pytest.fail("Coroutine executed")

    def generator(tx):
        pytest.fail("Generator executed")
        yield

    async def async_generator(tx):
        pytest.fail("Async generator executed")
        yield

    with pytest.raises(TypeError, match="synchronously"):
        guarded.query(
            context(),
            "fixture.query",
            {},
            "read",
            {"coroutine": coroutine, "generator": generator, "async_generator": async_generator}[
                kind
            ],
        )


@pytest.mark.parametrize("violation", ["repeat", "rewrite"])
def test_query_boundary_must_return_its_one_original_outcome(database, context, violation):
    class Boundary(PassBoundary):
        def invoke(self, transaction, *, name, payload, perform, reject):
            result = perform()
            if violation == "repeat":
                perform()
            return replace(result, value="rewritten")

    guarded = Database.from_store(database.store, execution_boundary=Boundary())
    with pytest.raises(RuntimeError):
        guarded.query(context(), "fixture.query", {}, "read", lambda tx: None)


def test_successful_null_query_is_not_a_refusal(database, context):
    guarded = Database.from_store(database.store, execution_boundary=PassBoundary())
    assert guarded.query(context(), "fixture.query", {}, "read", lambda tx: None) is None
