from __future__ import annotations

import contextvars
from dataclasses import replace

import pytest

from research_swarm.control.witness_commands import WitnessExecutionBoundary
from research_swarm.control.witness_time import WitnessAuthorityTimeProvider
from research_swarm.control.workers import EnrollWorker, WorkerControl, WorkerData
from research_swarm.domain.assignments import signing_key_id
from research_swarm.domain.clocks import ClockChallenge
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.domain.workers import EngineCapabilities
from research_swarm.state.database import Database
from research_swarm.workers.clock_signatures import ClockVerifier
from tests.test_time_providers import Registry
from tests.test_time_witnesses import SEED, identifier, source
from tests.test_witness_time import clock_scope, response


@pytest.fixture
def clock(database, context):
    registry = Registry(database, context, assignment_key_id=signing_key_id(source().public_key))
    registry.qualify()
    provider = WitnessAuthorityTimeProvider(database.authority_identity)
    boundary = WitnessExecutionBoundary(registry.providers, provider)
    guarded = Database.from_store(database.store, execution_boundary=boundary)
    native = WorkerControl(database, b"\x00" * 32, SEED, authority_time_provider=provider)
    workers = WorkerControl(guarded, b"\x00" * 32, SEED, authority_time_provider=provider)
    principal = Principal(
        id=new_id(), workspace_id=context().principal.workspace_id, scopes={"worker.query"}
    )
    request = EnrollWorker(
        worker_id=new_id(),
        session_id=new_id(),
        principal_id=principal.id,
        name="Synthetic query worker",
        clock_mode="bounded-continuous-v1",
        profiles=(
            EngineCapabilities(
                name="cpu",
                backend="process",
                execution_fingerprint="sha256:" + "b" * 64,
                cpu_slots=1,
            ),
        ),
    )
    worker = WorkerData.model_validate(native.enroll(context(), request)["result"]["data"])
    command = CommandContext(principal=principal, idempotency_key=new_id())
    challenge = ClockChallenge(
        worker_id=request.worker_id, session_id=request.session_id, nonce=new_id()
    )
    return registry, provider, boundary, guarded, native, workers, command, challenge, worker


def test_signed_clock_query_uses_its_own_checked_witness_without_an_acknowledgment(clock):
    registry, _, boundary, database, _, workers, context, challenge, worker = clock
    assert workers.validate_clock(context, challenge) == worker
    count = database.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM commands").one()[0])
    with boundary.prepare(
        context, registry.current(), "worker.clock", challenge.model_dump(mode="json")
    ) as turn:
        turn.complete(response(turn))
        reply = workers.clock(context, challenge)
    verified = ClockVerifier(worker.assignment_authority).reply(reply, challenge)
    assert verified.reply.payload.clock_profile == "hosted-native-witness-v1"
    assert (
        database.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM commands").one()[0])
        == count
    )


def test_query_source_change_commits_retirement_before_refusal_and_reopen(clock):
    registry, _, boundary, database, _, workers, context, challenge, _ = clock
    before = database.snapshot(registry.context())
    with boundary.prepare(
        context, registry.current(), "worker.clock", challenge.model_dump(mode="json")
    ) as turn:
        turn.complete(response(turn, source_epoch=identifier(8)))
        with pytest.raises(ResearchError) as refused:
            workers.clock(context, challenge)
    assert refused.value.code == ErrorCode.STALE_FENCE
    reopened = Database(registry.database.path)
    assert (
        reopened.read_record(registry.context(), "time_provider", registry.authority.activation_id)[
            "state"
        ]
        == "unqualified"
    )
    after = reopened.snapshot(registry.context())
    assert after["watermark"] == before["watermark"] + 1
    assert sum(item["kind"] == "time_source_retirement" for item in after["records"]) == 1
    assert (
        reopened.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM commands").one()[0]) == 1
    )  # Enrollment only.


@pytest.mark.parametrize("changed", ["principal", "scope", "session"])
def test_unauthorized_challenge_is_refused_before_witness_acquisition(clock, changed):
    registry, _, boundary, _, _, workers, context, challenge, _ = clock
    if changed == "principal":
        context = context.model_copy(
            update={"principal": context.principal.model_copy(update={"id": new_id()})}
        )
    elif changed == "scope":
        context = context.model_copy(
            update={"principal": context.principal.model_copy(update={"scopes": frozenset()})}
        )
    else:
        challenge = challenge.model_copy(update={"session_id": new_id()})
    calls = []
    before = registry.database.snapshot(registry.context())
    with pytest.raises(ResearchError):
        workers.validate_clock(context, challenge)
        with boundary.prepare(
            context, registry.current(), "worker.clock", challenge.model_dump(mode="json")
        ) as turn:
            calls.append(turn.request)
            pytest.fail("Unauthorized request acquired a witness")
    assert calls == []
    assert registry.database.snapshot(registry.context()) == before


def test_query_revalidates_worker_ownership_after_preflight(clock):
    registry, _, boundary, _, _, workers, context, challenge, worker = clock
    workers.validate_clock(context, challenge)
    with boundary.prepare(
        context, registry.current(), "worker.clock", challenge.model_dump(mode="json")
    ) as turn:
        turn.complete(response(turn))

        def change(tx):
            row = tx.get("worker", worker.binding.worker_id)
            tx.update(
                "worker",
                row["id"],
                expected_revision=row["revision"],
                data={**row["data"], "principal_id": new_id()},
                state=row["state"],
            )

        registry.write(registry.context(), change)
        with pytest.raises(ResearchError) as refused:
            workers.clock(context, challenge)
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert registry.current().state == "qualified"


@pytest.mark.parametrize("outer", ["command", "query"])
def test_nested_clock_query_cannot_borrow_an_outer_timestamp_even_with_copied_context(clock, outer):
    registry, provider, boundary, database, native, _, context, challenge, _ = clock
    context = registry.context()
    worker_context = clock[6]
    with boundary.prepare(context, registry.current(), "fixture.operation", {}) as turn:
        turn.complete(response(turn))

        def callback(tx):
            before = provider.observe(clock_scope(turn))
            copied = contextvars.copy_context()
            with pytest.raises(ResearchError) as nested:
                copied.run(native.clock, worker_context, challenge)
            assert nested.value.code == ErrorCode.STALE_FENCE
            assert provider.observe(clock_scope(turn)) is before
            return {}

        if outer == "command":
            database.execute(context, "fixture.operation", {}, "write", callback)
        else:
            database.query(context, "fixture.operation", {}, "read", callback)


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
