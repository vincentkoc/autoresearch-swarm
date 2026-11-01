from __future__ import annotations

import pytest

from research_swarm.control.witness_commands import WitnessCommandBoundary
from research_swarm.control.witness_time import WitnessAuthorityTimeProvider
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database
from tests.workers.test_time_providers import registry as registry
from tests.workers.test_time_witnesses import identifier
from tests.workers.test_witness_time import clock_scope, response


def witnessed(registry):
    provider = WitnessAuthorityTimeProvider(registry.database.authority_identity)
    boundary = WitnessCommandBoundary(registry.providers, provider)
    database = Database.from_store(registry.database.store, execution_boundary=boundary)
    return database, boundary, provider


@pytest.mark.parametrize("hook_failure", [False, True])
def test_domain_rejection_hook_keeps_the_same_witness_until_its_outcome(registry, hook_failure):
    registry.qualify()
    database, boundary, provider = witnessed(registry)
    context = registry.context()
    observed = []
    before = database.snapshot(context)
    with boundary.prepare(context, registry.current(), "fixture.operation", {}) as turn:
        turn.complete(response(turn))

        def handler(tx):
            observed.append(provider.observe(clock_scope(turn)))
            tx.insert("temporary_effect", new_id(), {}, state="temporary")
            raise ResearchError(ErrorCode.ACCEPTANCE_EXPIRED, "Injected expired acceptance")

        def hook(tx, error):
            assert error.code == ErrorCode.ACCEPTANCE_EXPIRED
            assert provider.observe(clock_scope(turn)) is observed[0]
            tx.insert("fixture_fence", new_id(), {}, state="fenced")
            if hook_failure:
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Injected hook failure")

        with pytest.raises(ResearchError) as refused:
            database.execute(context, "fixture.operation", {}, "write", handler, on_rejection=hook)
        assert refused.value.code == (
            ErrorCode.INVALID_TRANSITION if hook_failure else ErrorCode.ACCEPTANCE_EXPIRED
        )
        with pytest.raises(ResearchError):
            provider.observe(clock_scope(turn))
    after = database.snapshot(context)
    if hook_failure:
        assert after == before
        assert database.replay(context, "fixture.operation", {}, "write") is None
    else:
        assert not any(item["kind"] == "temporary_effect" for item in after["records"])
        assert sum(item["kind"] == "fixture_fence" for item in after["records"]) == 1
        with pytest.raises(ResearchError) as replay:
            database.execute(
                context,
                "fixture.operation",
                {},
                "write",
                lambda tx: pytest.fail("Replay entered handler"),
            )
        assert replay.value.code == ErrorCode.ACCEPTANCE_EXPIRED


@pytest.mark.parametrize("hook_failure", [False, True])
def test_prehandler_source_change_fences_and_rejects_exactly_once(registry, hook_failure):
    registry.qualify()
    database, boundary, _ = witnessed(registry)
    context = registry.context()
    before = database.snapshot(context)
    calls = []
    with boundary.prepare(context, registry.current(), "fixture.operation", {}) as turn:
        turn.complete(response(turn, source_epoch=identifier(8)))

        def hook(tx, error):
            calls.append(error.code)
            assert registry.providers.load(tx, registry.authority).state == "unqualified"
            if hook_failure:
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Injected fence hook failure")

        with pytest.raises(ResearchError) as refused:
            database.execute(
                context,
                "fixture.operation",
                {},
                "write",
                lambda tx: pytest.fail("Changed source entered handler"),
                on_rejection=hook,
            )
        assert refused.value.code == (
            ErrorCode.INVALID_TRANSITION if hook_failure else ErrorCode.STALE_FENCE
        )
    assert calls == [ErrorCode.STALE_FENCE]
    if hook_failure:
        assert database.snapshot(context) == before
        assert database.replay(context, "fixture.operation", {}, "write") is None
    else:
        assert registry.current().state == "unqualified"
        with pytest.raises(ResearchError) as replay:
            database.with_preflight(
                context,
                "fixture.operation",
                {},
                "write",
                lambda: pytest.fail("Replay acquired witness"),
            )
        assert replay.value.code == ErrorCode.STALE_FENCE


def test_accepted_acknowledgment_replays_without_a_prepared_boundary(registry):
    registry.qualify()
    database, boundary, provider = witnessed(registry)
    context = registry.context()
    with boundary.prepare(context, registry.current(), "fixture.operation", {}) as turn:
        turn.complete(response(turn))
        accepted = database.execute(
            context,
            "fixture.operation",
            {},
            "write",
            lambda tx: {"time": provider.observe(clock_scope(turn)).milliseconds},
        )
    assert (
        database.execute(
            context,
            "fixture.operation",
            {},
            "write",
            lambda tx: pytest.fail("Replay entered boundary"),
        )
        == accepted
    )
    with pytest.raises(ResearchError) as missing:
        database.execute(registry.context(), "fixture.operation", {}, "write", lambda tx: {})
    assert missing.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


@pytest.mark.parametrize("violation", ["skip", "repeat", "rewrite", "retry_hook"])
def test_boundary_contract_violation_rolls_back_every_effect(database, context, violation):
    calls = []

    class BrokenBoundary:
        def invoke(self, transaction, *, name, payload, perform, reject):
            if violation == "skip":
                return {"disposition": "invented"}
            try:
                result = perform()
            except ResearchError as error:
                return reject(error)
            if violation == "repeat":
                perform()
            if violation == "rewrite":
                result["result"]["changed"] = True
            return result

        def rejected(self, transaction, **kwargs):
            calls.append("boundary fence")
            transaction.insert("fixture_fence", new_id(), {}, state="fenced")

    guarded = Database.from_store(database.store, execution_boundary=BrokenBoundary())
    command = context()
    before = guarded.snapshot(command)

    def handler(tx):
        tx.insert("fixture_effect", new_id(), {}, state="saved")
        if violation == "retry_hook":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Injected handler rejection")
        return {}

    def failed_hook(tx, error):
        calls.append("domain hook")
        raise ResearchError(ErrorCode.REVISION_CONFLICT, "Abort outer transaction")

    with pytest.raises(RuntimeError):
        guarded.execute(
            command, "fixture.operation", {}, "write", handler, on_rejection=failed_hook
        )
    assert guarded.snapshot(command) == before
    assert guarded.replay(command, "fixture.operation", {}, "write") is None
    assert calls == (["boundary fence", "domain hook"] if violation == "retry_hook" else [])
